"""Explicitly bounded, public WebSocket recording; no order submission interface."""

from __future__ import annotations

import asyncio
import hashlib
import json
import resource
import ssl
import sys
import threading
import time
import traceback
from collections import Counter, defaultdict
from datetime import datetime
from uuid import uuid4

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from oddsfox_pipeline.config.acquisition_ownership import require_acquisition_url
from oddsfox_pipeline.ingestion.polymarket.sports_data.books import (
    normalize_message,
    reset_rows,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.catalog import (
    active_catalog,
    catalog_targets,
    refresh_catalog,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.storage import (
    RAW_SCHEMA,
    UPDATE_SCHEMA,
    BudgetStop,
    Segments,
    Store,
    json_bytes,
    now,
    sha256,
)
from oddsfox_pipeline.resources.outbound_url import validate_outbound_wss_url

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class NoRedirectConnect(connect):
    def process_redirect(self, exc):
        return exc


async def receive(ws):
    """Translate one pinned asyncio TLS-close race into an ordinary disconnect."""
    try:
        return await ws.recv(decode=False)
    except AttributeError as exc:
        if str(exc) == "'NoneType' object has no attribute 'resume_reading'":
            raise ConnectionError(
                "TLS transport closed during receive flow-control resume"
            ) from exc
        raise


def exception_category(exc):
    if isinstance(
        exc,
        (
            ConnectionClosed,
            InvalidHandshake,
            OSError,
            TimeoutError,
            asyncio.TimeoutError,
        ),
    ):
        return "transport"
    if isinstance(exc, OverflowError):
        return "overflow"
    if isinstance(exc, (ValueError, TypeError, KeyError, UnicodeError)):
        return "source_data_uncertainty"
    return "internal"


def shards(targets, size=500):
    if size < 2:
        raise ValueError("shard size must accommodate a binary pair")
    groups = defaultdict(list)
    for token, row in targets.items():
        groups[row["condition_id"]].append(token)
    result, pending = [], []
    for condition in sorted(groups):
        group = sorted(groups[condition])
        if pending and len(pending) + len(group) > size:
            result.append(pending)
            pending = []
        pending.extend(group)
    if pending:
        result.append(pending)
    return result


async def record(
    store: Store,
    duration: int,
    *,
    shard_size=500,
    connector=NoRedirectConnect,
    refresh_seconds=300,
    rotation_seconds=30,
    heartbeat_seconds=10,
    heartbeat_timeout=35,
    queue_messages=131072,
    queue_bytes=128 * 1024**2,
    connection_start_stagger_seconds=0.75,
):
    if duration <= 0:
        raise ValueError("recording requires an explicit positive bounded duration")
    store.check(256 * 1024**2)
    endpoint = validate_outbound_wss_url(require_acquisition_url("polymarket", WS_URL))
    catalog = active_catalog(store)
    catalog_inputs = [{"path": catalog["path"], "sha256": catalog["sha256"]}]
    record_inputs = []
    targets = catalog_targets(store)
    if not targets:
        raise ValueError("activated catalog admits no tokens")
    session = now().strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex
    started_at = now()
    deadline = time.monotonic() + duration
    if queue_messages <= 0 or queue_bytes <= 0:
        raise ValueError("queue limits must be positive")
    minimum_queue_bytes = max(64 * 1024, min(shard_size, len(targets)) * 2048)
    if queue_bytes < minimum_queue_bytes:
        raise ValueError(
            "queue byte limit is too small to retain one shard-wide control event"
        )
    queue = asyncio.Queue(maxsize=queue_messages)
    queue_space = asyncio.Event()
    queue_space.set()
    queue_admission = asyncio.Lock()
    pressure_bytes = 0
    status = {token: "pending" for token in targets}
    initialized_ever = set()
    active_tokens = set(targets)
    resolved_tokens = set()
    failed_tokens = set()
    failure_reasons = defaultdict(set)
    exception_events = []
    observed_until = {}
    metrics = Counter()
    rate_second, rate_count = 0, 0
    workers = {}
    stop = asyncio.Event()
    cancel_refresh = threading.Event()
    lifecycle_refresh = asyncio.Event()
    lifecycle_conditions = set()
    rotation = 0
    segment_hour = now().strftime("%Y-%m-%d/%H")
    raw = updates = None
    last_rotation = time.monotonic()
    source = "polymarket-wss"

    def new_segments():
        base = f"books/{source}/{segment_hour}/{session}/{rotation:06d}"
        return Segments(store, base + "/raw", RAW_SCHEMA), Segments(
            store, base + "/updates", UPDATE_SCHEMA
        )

    raw, updates = new_segments()

    async def enqueue(row, normalized, *, critical=False):
        nonlocal pressure_bytes
        size = (
            1024
            + len(row["payload"])
            + sum(
                1024 + 192 * (len(item.get("bids") or []) + len(item.get("asks") or []))
                for item in normalized
            )
        )
        if size > queue_bytes:
            raise OverflowError("bounded recorder queue exhausted")
        blocked_at = None
        if queue_admission.locked():
            blocked_at = time.monotonic()
        async with queue_admission:
            while queue.full() or pressure_bytes + size > queue_bytes:
                if blocked_at is None:
                    blocked_at = time.monotonic()
                queue_space.clear()
                await queue_space.wait()
            # No await is permitted between the capacity check and accounting.
            pressure_bytes += size
            queue.put_nowait((row, normalized, size))
            metrics["peak_queue_bytes"] = max(
                metrics["peak_queue_bytes"], pressure_bytes
            )
        if blocked_at is not None:
            metrics[
                "critical_queue_backpressure_events"
                if critical
                else "queue_backpressure_events"
            ] += 1
            metrics["max_queue_backpressure_seconds"] = max(
                metrics["max_queue_backpressure_seconds"],
                time.monotonic() - blocked_at,
            )

    async def worker(number, token_list, start_delay):
        nonlocal rate_second, rate_count
        if start_delay:
            try:
                await asyncio.wait_for(stop.wait(), timeout=start_delay)
                return
            except asyncio.TimeoutError:
                pass
        token_set = set(token_list)
        retired_tokens = set()
        connection_attempt = 0
        while not stop.is_set() and time.monotonic() < deadline:
            subscribed = token_set.intersection(active_tokens)
            if not subscribed:
                break
            if connection_attempt:
                metrics["reconnects"] += 1
            stream = f"{session}/{number:05d}/{connection_attempt:05d}"
            sequence = 0

            async def emit(
                kind, payload, normalized=None, critical=False, received_at=None
            ):
                nonlocal sequence
                received = received_at or now()
                row = {
                    "source": source,
                    "session": session,
                    "connection": stream,
                    "sequence": sequence,
                    "received_at": received,
                    "kind": kind,
                    "payload": payload,
                }
                if normalized is None:
                    normalized = list(
                        reset_rows(
                            subscribed,
                            source=source,
                            stream=stream,
                            sequence=sequence,
                            received_at=received,
                            reason=kind,
                        )
                    )
                else:
                    normalized = [
                        item | {"received_at": received} for item in normalized
                    ]
                await enqueue(row, normalized, critical=critical)
                sequence += 1

            reason = "disconnect"
            exception_context = None
            receive_task = None
            try:
                await emit(
                    "session_start",
                    json_bytes({"tokens": sorted(subscribed)}),
                    critical=True,
                )
                async with connector(
                    endpoint,
                    ssl=ssl.create_default_context(),
                    proxy=None,
                    open_timeout=15,
                    close_timeout=5,
                    ping_interval=None,
                    max_size=16 * 1024**2,
                    max_queue=1,
                ) as ws:
                    subscription = json_bytes(
                        {
                            "assets_ids": sorted(subscribed),
                            "type": "market",
                            "initial_dump": True,
                            "custom_feature_enabled": True,
                        }
                    )
                    await ws.send(subscription.decode())
                    await emit("subscription", subscription, [], True)
                    last_ping = time.monotonic()
                    waiting_since = None
                    while not stop.is_set() and time.monotonic() < deadline:
                        removed = subscribed - active_tokens
                        if removed:
                            payload = json_bytes(
                                {
                                    "operation": "unsubscribe",
                                    "assets_ids": sorted(removed),
                                }
                            )
                            await ws.send(payload.decode())
                            await emit(
                                "subscription_removed",
                                payload,
                                list(
                                    reset_rows(
                                        removed,
                                        source=source,
                                        stream=stream,
                                        sequence=sequence,
                                        received_at=now(),
                                        reason="catalog_not_admitted",
                                    )
                                ),
                                True,
                            )
                            subscribed.difference_update(removed)
                            token_set.difference_update(removed)
                            retired_tokens.update(removed)
                            for token in removed - resolved_tokens:
                                status[token] = "invalid"
                            if not subscribed:
                                break
                        if time.monotonic() - last_ping >= heartbeat_seconds:
                            await ws.send("PING")
                            await emit("heartbeat_sent", b"PING", [])
                            last_ping = time.monotonic()
                        if waiting_since is None:
                            waiting_since = time.monotonic()
                            receive_task = asyncio.create_task(receive(ws))
                        done, _ = await asyncio.wait(
                            {receive_task},
                            timeout=min(
                                1,
                                heartbeat_timeout,
                                max(0.01, deadline - time.monotonic()),
                            ),
                        )
                        if not done:
                            if time.monotonic() - waiting_since > heartbeat_timeout:
                                receive_task.cancel()
                                await asyncio.gather(
                                    receive_task, return_exceptions=True
                                )
                                receive_task = None
                                raise TimeoutError(
                                    "application receive-activity timeout"
                                )
                            continue
                        payload = receive_task.result()
                        receive_task = None
                        waiting_since = None
                        received = now()
                        payload = (
                            payload.encode() if isinstance(payload, str) else payload
                        )
                        row = {
                            "source": source,
                            "session": session,
                            "connection": stream,
                            "sequence": sequence,
                            "received_at": received,
                            "kind": "message",
                            "payload": payload,
                        }
                        try:
                            normalized = normalize_message(
                                payload,
                                source=source,
                                stream=stream,
                                sequence=sequence,
                                received_at=received,
                                identity=f"{stream}:{sequence}",
                            )
                            # Lifecycle notifications can be broadcast beyond this
                            # shard. Their original bytes are still retained.
                            applicable = []
                            for item in normalized:
                                token = item["token_id"]
                                if token in retired_tokens:
                                    metrics["retired_token_late_updates"] += 1
                                    continue
                                if (
                                    item["kind"] == "market_resolved"
                                    and token not in token_set
                                ):
                                    metrics["unsubscribed_lifecycle_outcomes"] += 1
                                    continue
                                if (
                                    token not in token_set
                                    or item["condition_id"]
                                    != targets[token]["condition_id"]
                                ):
                                    raise ValueError(
                                        "unexpected token/condition identity"
                                    )
                                applicable.append(item)
                            normalized = applicable
                        except (ValueError, TypeError, KeyError) as exc:
                            await enqueue(
                                row,
                                list(
                                    reset_rows(
                                        subscribed,
                                        source=source,
                                        stream=stream,
                                        sequence=sequence,
                                        received_at=received,
                                        reason="message_parsing_uncertainty",
                                    )
                                ),
                                critical=True,
                            )
                            sequence += 1
                            raise ValueError(
                                "unrecoverable message parsing uncertainty"
                            ) from exc
                        try:
                            await enqueue(row, normalized)
                        except OverflowError:
                            metrics["overflow_dropped_messages"] += 1
                            metrics["overflow_dropped_bytes"] += len(payload)
                            normalized.clear()
                            await emit(
                                "queue_overflow",
                                json_bytes(
                                    {
                                        "payload_sha256": hashlib.sha256(
                                            payload
                                        ).hexdigest(),
                                        "dropped_bytes": len(payload),
                                    }
                                ),
                                critical=True,
                                received_at=received,
                            )
                            raise
                        sequence += 1
                        for item in normalized:
                            if item["kind"] == "market_resolved":
                                resolved_tokens.add(item["token_id"])
                                active_tokens.discard(item["token_id"])
                                status[item["token_id"]] = "market_resolved"
                            if item["kind"] == "book":
                                failed_tokens.discard(item["token_id"])
                                status[item["token_id"]] = "initialized"
                                observed_at = targets[item["token_id"]].get(
                                    "observed_at"
                                )
                                if (
                                    item["token_id"] not in initialized_ever
                                    and observed_at is not None
                                ):
                                    metrics[
                                        "max_catalog_observation_to_initial_seconds"
                                    ] = max(
                                        metrics[
                                            "max_catalog_observation_to_initial_seconds"
                                        ],
                                        (received - observed_at).total_seconds(),
                                    )
                                initialized_ever.add(item["token_id"])
                        metrics["messages"] += 1
                        metrics["received_bytes"] += len(payload)
                        current_second = int(time.monotonic())
                        rate_count = (
                            rate_count + 1 if current_second == rate_second else 1
                        )
                        rate_second = current_second
                        metrics["peak_messages_per_second"] = max(
                            metrics["peak_messages_per_second"], rate_count
                        )
                        if (
                            b'"new_market"' in payload
                            or b'"market_resolved"' in payload
                        ):
                            metrics["lifecycle_notifications"] += 1
                            value = json.loads(payload)
                            for item in value if isinstance(value, list) else [value]:
                                if item.get("market"):
                                    lifecycle_conditions.add(item["market"])
                            lifecycle_refresh.set()
            except asyncio.CancelledError:
                reason = "controlled_stop"
                raise
            except Exception as exc:
                reason = type(exc).__name__
                metrics[reason] += 1
                category = exception_category(exc)
                metrics[f"{category}_exception_events"] += 1
                exception_context = {
                    "category": category,
                    "message": str(exc)[:2048],
                    "trace": [
                        {
                            "file": frame.filename.rsplit("/", 1)[-1],
                            "line": frame.lineno,
                            "function": frame.name,
                        }
                        for frame in traceback.extract_tb(exc.__traceback__)[-12:]
                    ],
                }
                exception_events.append(
                    {
                        "stream": stream,
                        "sequence": sequence,
                        "type": reason,
                        **exception_context,
                    }
                )
                affected = subscribed - initialized_ever - resolved_tokens
                failed_tokens.update(affected)
                for token in affected:
                    failure_reasons[token].add(reason)
            finally:
                if receive_task is not None:
                    receive_task.cancel()
                    await asyncio.gather(receive_task, return_exceptions=True)
                for token in subscribed:
                    if token in resolved_tokens:
                        status[token] = "market_resolved"
                    else:
                        status[token] = (
                            "pending"
                            if connection_attempt == 0 and status[token] == "pending"
                            else "invalid"
                        )
                metrics["connection_closures"] += 1
                await emit(
                    reason,
                    json_bytes(
                        {
                            "tokens": sorted(subscribed),
                            "reason": reason,
                            "exception": exception_context,
                        }
                    ),
                    critical=True,
                )
            connection_attempt += 1
            if not token_set.intersection(active_tokens):
                break
            if not stop.is_set():
                await asyncio.sleep(min(5, max(0, deadline - time.monotonic())))

    async def refresh_loop():
        clock = time.monotonic()
        last_full = clock - max(
            0,
            (
                now() - datetime.fromisoformat(catalog["last_full_refresh_at"])
            ).total_seconds(),
        )
        last_open = clock - max(
            0,
            (
                now() - datetime.fromisoformat(catalog["last_open_refresh_at"])
            ).total_seconds(),
        )
        while not stop.is_set():
            while (
                not stop.is_set()
                and not lifecycle_refresh.is_set()
                and time.monotonic() - last_open < refresh_seconds
                and time.monotonic() - last_full < 86400
            ):
                await asyncio.sleep(0.25)
            if stop.is_set():
                break
            started = time.monotonic()
            metrics["max_refresh_start_delay_seconds"] = max(
                metrics["max_refresh_start_delay_seconds"],
                max(0, started - last_open - refresh_seconds),
            )
            try:
                full = time.monotonic() - last_full >= 86400
                targeted = (
                    set(lifecycle_conditions)
                    if not full and time.monotonic() - last_open < refresh_seconds
                    else None
                )
                lifecycle_conditions.clear()
                lifecycle_refresh.clear()
                result = await asyncio.to_thread(
                    refresh_catalog,
                    store,
                    open_only=not full,
                    cancelled=cancel_refresh,
                    target_conditions=targeted,
                )
                catalog_inputs.append(
                    {"path": result["path"], "sha256": result["sha256"]}
                )
                if full:
                    last_full = time.monotonic()
                if not targeted:
                    last_open = started
                latest = await asyncio.to_thread(catalog_targets, store)
                latest = {
                    token: row
                    for token, row in latest.items()
                    if token not in resolved_tokens
                }
                new = {
                    token: row
                    for token, row in latest.items()
                    if token not in active_tokens
                }
                metrics["readmitted_tokens"] += sum(token in targets for token in new)
                targets.update(latest)
                active_tokens.clear()
                active_tokens.update(latest)
                status.update({token: "pending" for token in new})
                initialized_ever.difference_update(new)
                launch(new)
                metrics["catalog_refreshes"] += 1
                metrics["catalog_observation_lag_seconds"] = int(
                    time.monotonic() - started
                )
                metrics["catalog_tokens"] = result["counts"]["admitted_tokens"]
            except Exception as exc:
                metrics["catalog_refresh_failures"] += 1
                print(
                    json.dumps({"catalog_refresh_error": type(exc).__name__}),
                    flush=True,
                )
                # A failing endpoint must not cause a tight refresh/retry loop.
                try:
                    await asyncio.wait_for(stop.wait(), timeout=30)
                except asyncio.TimeoutError:
                    pass

    def launch(new_targets):
        for ordinal, group in enumerate(shards(new_targets, shard_size)):
            index = len(workers)
            start_delay = min(600, ordinal * connection_start_stagger_seconds)
            workers[index] = asyncio.create_task(worker(index, group, start_delay))

    def metric_snapshot(*, include_rss=False):
        snapshot = dict(metrics)
        if include_rss:
            snapshot["process_peak_rss_bytes"] = resource.getrusage(
                resource.RUSAGE_SELF
            ).ru_maxrss * (1 if sys.platform == "darwin" else 1024)
            metrics["process_peak_rss_bytes"] = snapshot["process_peak_rss_bytes"]
        return snapshot

    def publish(metric_values, catalog_input_values):
        raw_files, update_files = raw.close(), updates.close()
        if raw_files or update_files:
            manifest = store.commit(
                "record",
                f"{session}-{rotation:06d}",
                raw_files + update_files,
                source=source,
                session=session,
                catalog_manifest_sha256=catalog["sha256"],
                inputs=catalog_input_values,
                raw=raw_files,
                updates=update_files,
                metrics=metric_values,
                observed_until=dict(observed_until),
                configuration={
                    "tokens_per_connection": shard_size,
                    "queue_messages": queue_messages,
                    "queue_bytes": queue_bytes,
                    "duration_seconds": duration,
                    "refresh_seconds": refresh_seconds,
                    "rotation_seconds": rotation_seconds,
                    "heartbeat_seconds": heartbeat_seconds,
                    "heartbeat_timeout_seconds": heartbeat_timeout,
                    "source_endpoint": endpoint,
                    "websocket_max_queue": 1,
                    "websocket_native_ping": False,
                    "connection_start_stagger_seconds": connection_start_stagger_seconds,
                },
            )
            record_inputs.append(
                {"path": manifest["path"], "sha256": manifest["sha256"]}
            )
            return manifest

    def consume(row, normalized, metric_values, catalog_input_values):
        nonlocal raw, updates, rotation, segment_hour, last_rotation
        hour = row["received_at"].strftime("%Y-%m-%d/%H")
        if hour != segment_hour:
            publish(metric_values, catalog_input_values)
            rotation += 1
            segment_hour = hour
            raw, updates = new_segments()
            last_rotation = time.monotonic()
        observed_until[row["connection"]] = row["received_at"]
        raw.append(row)
        for item in normalized:
            updates.append(item)

    def consume_batch(batch, metric_values, catalog_input_values):
        for row, normalized, _ in batch:
            consume(row, normalized, metric_values, catalog_input_values)

    async def dequeue_batch(timeout):
        nonlocal pressure_bytes
        first = await asyncio.wait_for(queue.get(), timeout=timeout)
        batch = [first]
        batch_bytes = first[2]
        while len(batch) < 256 and batch_bytes < 8 * 1024**2:
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            batch.append(item)
            batch_bytes += item[2]
        pressure_bytes -= batch_bytes
        queue_space.set()
        return batch

    launch(targets)
    refresher = asyncio.create_task(refresh_loop())
    outcome = "duration_complete"
    try:
        while time.monotonic() < deadline:
            try:
                batch = await dequeue_batch(0.5)
                await asyncio.to_thread(
                    consume_batch,
                    batch,
                    metric_snapshot(),
                    list(catalog_inputs),
                )
            except asyncio.TimeoutError:
                pass
            if time.monotonic() - last_rotation >= rotation_seconds:
                await asyncio.to_thread(
                    publish,
                    metric_snapshot(include_rss=True),
                    list(catalog_inputs),
                )
                rotation += 1
                raw, updates = new_segments()
                last_rotation = time.monotonic()
                remaining = store.check()
                print(
                    json.dumps(
                        {
                            "session": session,
                            "tokens": dict(Counter(status.values())),
                            "metrics": dict(metrics),
                            "queue_messages": queue.qsize(),
                            **remaining,
                        }
                    ),
                    flush=True,
                )
                # Leave bounded space to drain accepted messages and commit final invalidations.
                if (
                    min(
                        remaining["remaining_retained_bytes"],
                        remaining["free_bytes"] - store.limits.reserve_bytes,
                    )
                    < 256 * 1024**2
                ):
                    outcome = "budget_stop"
                    break
    except BudgetStop:
        outcome = "budget_stop"
    except BaseException as exc:
        outcome = (
            "interrupted"
            if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError))
            else "failed"
        )
        raise
    finally:
        stop.set()
        cancel_refresh.set()
        for task in workers.values():
            task.cancel()
        # Drain while workers append their final invalidation records.
        while any(not task.done() for task in workers.values()) or not queue.empty():
            try:
                batch = await dequeue_batch(0.2)
                await asyncio.to_thread(
                    consume_batch,
                    batch,
                    metric_snapshot(),
                    list(catalog_inputs),
                )
            except asyncio.TimeoutError:
                pass
        await asyncio.gather(*workers.values(), refresher, return_exceptions=True)
        await asyncio.to_thread(
            publish,
            metric_snapshot(include_rss=True),
            list(catalog_inputs),
        )
        if outcome == "duration_complete" and metrics["internal_exception_events"]:
            outcome = "completed_with_internal_errors"
        summary = {
            "session": session,
            "status": outcome,
            "duration_seconds": duration,
            "started_at": started_at,
            "completed_at": now(),
            "tokens": {
                token: "recorded"
                if token in initialized_ever
                else "unavailable"
                if token in resolved_tokens
                else "failed"
                if token in failed_tokens
                else "pending"
                for token in targets
            },
            "final_states": status,
            "metrics": dict(metrics),
            "admitted": len(targets),
            "token_failure_reasons": {
                token: sorted(failure_reasons[token])
                for token in sorted(failed_tokens - initialized_ever - resolved_tokens)
            },
            "exception_events": exception_events,
        }
        relative = f"sessions/{session}.json"
        store.atomic_json(relative, summary)
        store.commit(
            "session",
            session,
            [],
            inputs=record_inputs,
            summary={"path": relative, "sha256": sha256(store.path(relative))},
        )
    return summary

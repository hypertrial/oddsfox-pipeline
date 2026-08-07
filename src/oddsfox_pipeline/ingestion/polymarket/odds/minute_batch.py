"""Shared high-throughput helpers for minute-fidelity CLOB backfills.

Ports the hourly pipeline's batch POST, preemptive window chunking, status-hook
rate accounting, and RPS auto-tune onto the match/futures minute paths while
preserving their all-success atomic publish contract.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock, local
from typing import Any, Callable, Iterator, Protocol, Sequence

from oddsfox_pipeline.config.settings import CLOB_API_URL, ODDS_REQUESTS_PER_SECOND
from oddsfox_pipeline.ingestion.polymarket.odds.execution import (
    fetch_group_window_with_auto_split,
    fetch_window_with_auto_split,
    iter_windows,
)
from oddsfox_pipeline.ingestion.polymarket.odds.fetch import build_client
from oddsfox_pipeline.ingestion.polymarket.odds.writer import maybe_auto_tune_rps
from oddsfox_pipeline.resources.http import RateLimiter
from oddsfox_pipeline.resources.progress_guardrails import ProgressGuardrail

logger = logging.getLogger(__name__)

DEFAULT_MINUTE_WORKERS = 40
DEFAULT_MINUTE_REQUESTS_PER_SECOND = 40
DEFAULT_MINUTE_BATCH_GROUP_SIZE = 20
DEFAULT_MINUTE_WINDOW_HOURS = 24
DEFAULT_MINUTE_AUTO_TUNE_MAX_RPS = 90
MIN_SPLIT_WINDOW_SECONDS = 300
FIDELITY_MINUTES = 1


class MinutePlanLike(Protocol):
    market_id: str
    token_id: str
    started_at: datetime
    finished_at: datetime


@contextmanager
def borrow_duckdb_connection(
    conn: Any | None = None,
    *,
    connection_factory: Callable[[], AbstractContextManager[Any]] | None = None,
) -> Iterator[Any]:
    """Open a DuckDB connection for one sync phase, then release it.

    Production minute assets pass ``connection_factory=get_connection`` so the
    warehouse file lock is not held across long CLOB fetches. Unit tests may
    keep passing a single in-memory ``conn`` for the whole sync.
    """
    if (conn is None) == (connection_factory is None):
        raise ValueError("Provide exactly one of conn or connection_factory")
    if connection_factory is not None:
        with connection_factory() as active:
            yield active
        return
    yield conn


@dataclass(frozen=True)
class MinuteFetchResult:
    plan: MinutePlanLike
    fetch_status: str
    history: tuple[tuple[str, int, float], ...]
    request_start_epoch: int
    request_end_epoch: int
    source_row_count: int
    history_sha256: str | None
    fetch_started_at: datetime
    fetch_finished_at: datetime
    error_type: str | None = None
    error_message: str | None = None


def sanitize_error_message(exc: Exception) -> str:
    return " ".join(str(exc).split())[:500]


def padded_epoch_bounds(started_at: datetime, finished_at: datetime) -> tuple[int, int]:
    exact_start = started_at.timestamp()
    exact_end = finished_at.timestamp()
    padded_start = (math.floor(exact_start) // 60) * 60
    padded_end = math.ceil(math.ceil(exact_end) / 60) * 60
    return int(padded_start), int(padded_end)


def group_minute_plans(
    plans: Sequence[MinutePlanLike],
    *,
    batch_group_size: int = DEFAULT_MINUTE_BATCH_GROUP_SIZE,
) -> list[tuple[MinutePlanLike, ...]]:
    """Cluster plans that share an exact window, then chunk to batch size."""
    size = max(1, int(batch_group_size))
    by_window: dict[tuple[datetime, datetime], list[MinutePlanLike]] = defaultdict(
        list
    )
    for plan in plans:
        key = (plan.started_at, plan.finished_at)
        by_window[key].append(plan)
    groups: list[tuple[MinutePlanLike, ...]] = []
    for key in sorted(by_window):
        bucket = by_window[key]
        for offset in range(0, len(bucket), size):
            groups.append(tuple(bucket[offset : offset + size]))
    return groups


def _normalize_rows(
    raw_rows: Sequence[tuple[Any, Any, Any]],
) -> list[tuple[str, int, float]]:
    return sorted(
        (
            (str(token), int(timestamp), float(price))
            for token, timestamp, price in raw_rows
        ),
        key=lambda row: (row[1], row[0], row[2]),
    )


def _finalize_history(
    *,
    plan: MinutePlanLike,
    raw_rows: Sequence[tuple[str, int, float]],
    exact_start: float,
    exact_end: float,
    padded_start: int,
    padded_end: int,
    source_row_count: int,
    fetch_started_at: datetime,
    empty_error_message: str,
) -> MinuteFetchResult:
    filtered = tuple(row for row in raw_rows if exact_start <= row[1] <= exact_end)
    if not filtered:
        return MinuteFetchResult(
            plan=plan,
            fetch_status="empty",
            history=(),
            request_start_epoch=padded_start,
            request_end_epoch=padded_end,
            source_row_count=source_row_count,
            history_sha256=None,
            fetch_started_at=fetch_started_at,
            fetch_finished_at=datetime.now(timezone.utc),
            error_type="EmptyHistory",
            error_message=empty_error_message,
        )
    if any(token != plan.token_id for token, _, _ in filtered):
        raise ValueError(f"CLOB returned a mismatched token for {plan.token_id}")
    if any(not 0.0 <= price <= 1.0 for _, _, price in filtered):
        raise ValueError(f"CLOB returned an invalid probability for {plan.token_id}")
    history_sha256 = hashlib.sha256(
        json.dumps(filtered, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return MinuteFetchResult(
        plan=plan,
        fetch_status="success",
        history=filtered,
        request_start_epoch=padded_start,
        request_end_epoch=padded_end,
        source_row_count=source_row_count,
        history_sha256=history_sha256,
        fetch_started_at=fetch_started_at,
        fetch_finished_at=datetime.now(timezone.utc),
    )


def fetch_minute_plan(
    plan: MinutePlanLike,
    client: Any,
    fetch_window_fn: Callable[..., Any],
    *,
    transient_retries: int,
    transient_backoff_seconds: float,
    window_seconds: int,
    status_hook: Callable[[int], None] | None = None,
    empty_error_message: str | None = None,
) -> MinuteFetchResult:
    """Per-token fetch with preemptive window chunking (hourly parity)."""
    fetch_started_at = datetime.now(timezone.utc)
    source_row_count = 0
    exact_start = plan.started_at.timestamp()
    exact_end = plan.finished_at.timestamp()
    padded_start, padded_end = padded_epoch_bounds(plan.started_at, plan.finished_at)
    empty_message = empty_error_message or (
        f"Empty in-window CLOB history for token {plan.token_id}"
    )
    try:
        collected: list[tuple[str, int, float]] = []
        for window_start, window_end in iter_windows(
            padded_start, padded_end, max(1, int(window_seconds))
        ):
            rows = fetch_window_fn(
                client,
                plan.token_id,
                window_start,
                window_end,
                FIDELITY_MINUTES,
                MIN_SPLIT_WINDOW_SECONDS,
                transient_retries,
                transient_backoff_seconds,
                status_hook,
            )
            if rows is None:
                raise RuntimeError(f"Transient CLOB failure for token {plan.token_id}")
            chunk = list(rows)
            source_row_count += len(chunk)
            collected.extend(_normalize_rows(chunk))
        return _finalize_history(
            plan=plan,
            raw_rows=_normalize_rows(collected),
            exact_start=exact_start,
            exact_end=exact_end,
            padded_start=padded_start,
            padded_end=padded_end,
            source_row_count=source_row_count,
            fetch_started_at=fetch_started_at,
            empty_error_message=empty_message,
        )
    except Exception as exc:
        return MinuteFetchResult(
            plan=plan,
            fetch_status="error",
            history=(),
            request_start_epoch=padded_start,
            request_end_epoch=padded_end,
            source_row_count=source_row_count,
            history_sha256=None,
            fetch_started_at=fetch_started_at,
            fetch_finished_at=datetime.now(timezone.utc),
            error_type=exc.__class__.__name__,
            error_message=sanitize_error_message(exc),
        )


def fetch_minute_plan_group(
    plans: Sequence[MinutePlanLike],
    client: Any,
    fetch_group_window_fn: Callable[..., Any],
    *,
    transient_retries: int,
    transient_backoff_seconds: float,
    window_seconds: int,
    status_hook: Callable[[int], None] | None = None,
    empty_error_message_fn: Callable[[MinutePlanLike], str] | None = None,
) -> list[MinuteFetchResult]:
    """Batch-fetch tokens that share one exact window via POST /batch-prices-history."""
    if not plans:
        return []
    if len({(p.started_at, p.finished_at) for p in plans}) != 1:
        raise ValueError("Minute batch group requires identical exact windows")
    fetch_started_at = datetime.now(timezone.utc)
    first = plans[0]
    exact_start = first.started_at.timestamp()
    exact_end = first.finished_at.timestamp()
    padded_start, padded_end = padded_epoch_bounds(first.started_at, first.finished_at)
    token_ids = [plan.token_id for plan in plans]
    accumulated: dict[str, list[tuple[str, int, float]]] = {
        token_id: [] for token_id in token_ids
    }
    source_counts = {token_id: 0 for token_id in token_ids}
    permanent: dict[str, Exception] = {}
    transient: set[str] = set()
    active = set(token_ids)

    try:
        for window_start, window_end in iter_windows(
            padded_start, padded_end, max(1, int(window_seconds))
        ):
            active_ids = [token_id for token_id in token_ids if token_id in active]
            if not active_ids:
                break
            chunk_map = fetch_group_window_fn(
                client,
                active_ids,
                window_start,
                window_end,
                FIDELITY_MINUTES,
                MIN_SPLIT_WINDOW_SECONDS,
                transient_retries,
                transient_backoff_seconds,
                status_hook,
            )
            if not isinstance(chunk_map, dict):
                for token_id in active_ids:
                    transient.add(token_id)
                    active.discard(token_id)
                continue
            for token_id in active_ids:
                value = chunk_map.get(token_id)
                if isinstance(value, Exception):
                    permanent[token_id] = value
                    active.discard(token_id)
                    continue
                if value is None:
                    transient.add(token_id)
                    active.discard(token_id)
                    continue
                rows = _normalize_rows(list(value))
                source_counts[token_id] += len(rows)
                accumulated[token_id].extend(rows)
    except Exception as exc:
        return [
            MinuteFetchResult(
                plan=plan,
                fetch_status="error",
                history=(),
                request_start_epoch=padded_start,
                request_end_epoch=padded_end,
                source_row_count=source_counts.get(plan.token_id, 0),
                history_sha256=None,
                fetch_started_at=fetch_started_at,
                fetch_finished_at=datetime.now(timezone.utc),
                error_type=exc.__class__.__name__,
                error_message=sanitize_error_message(exc),
            )
            for plan in plans
        ]

    results: list[MinuteFetchResult] = []
    for plan in plans:
        token_id = plan.token_id
        if token_id in permanent:
            exc = permanent[token_id]
            results.append(
                MinuteFetchResult(
                    plan=plan,
                    fetch_status="error",
                    history=(),
                    request_start_epoch=padded_start,
                    request_end_epoch=padded_end,
                    source_row_count=source_counts[token_id],
                    history_sha256=None,
                    fetch_started_at=fetch_started_at,
                    fetch_finished_at=datetime.now(timezone.utc),
                    error_type=exc.__class__.__name__,
                    error_message=sanitize_error_message(exc),
                )
            )
            continue
        if token_id in transient:
            results.append(
                MinuteFetchResult(
                    plan=plan,
                    fetch_status="error",
                    history=(),
                    request_start_epoch=padded_start,
                    request_end_epoch=padded_end,
                    source_row_count=source_counts[token_id],
                    history_sha256=None,
                    fetch_started_at=fetch_started_at,
                    fetch_finished_at=datetime.now(timezone.utc),
                    error_type="TransientAPIError",
                    error_message=f"Transient CLOB failure for token {token_id}",
                )
            )
            continue
        empty_message = (
            empty_error_message_fn(plan)
            if empty_error_message_fn
            else f"Empty in-window CLOB history for token {token_id}"
        )
        try:
            results.append(
                _finalize_history(
                    plan=plan,
                    raw_rows=_normalize_rows(accumulated[token_id]),
                    exact_start=exact_start,
                    exact_end=exact_end,
                    padded_start=padded_start,
                    padded_end=padded_end,
                    source_row_count=source_counts[token_id],
                    fetch_started_at=fetch_started_at,
                    empty_error_message=empty_message,
                )
            )
        except Exception as exc:
            results.append(
                MinuteFetchResult(
                    plan=plan,
                    fetch_status="error",
                    history=(),
                    request_start_epoch=padded_start,
                    request_end_epoch=padded_end,
                    source_row_count=source_counts[token_id],
                    history_sha256=None,
                    fetch_started_at=fetch_started_at,
                    fetch_finished_at=datetime.now(timezone.utc),
                    error_type=exc.__class__.__name__,
                    error_message=sanitize_error_message(exc),
                )
            )
    return results


def execute_minute_fetches(
    plans: Sequence[MinutePlanLike],
    *,
    asset_name: str,
    log: Any = logger,
    workers: int = DEFAULT_MINUTE_WORKERS,
    requests_per_second: int | None = DEFAULT_MINUTE_REQUESTS_PER_SECOND,
    batch_group_size: int = DEFAULT_MINUTE_BATCH_GROUP_SIZE,
    window_hours: int = DEFAULT_MINUTE_WINDOW_HOURS,
    auto_tune_rps: bool = True,
    auto_tune_max_rps: int | None = DEFAULT_MINUTE_AUTO_TUNE_MAX_RPS,
    auto_tune_window_requests: int = 40,
    auto_tune_threshold_429: float = 0.05,
    auto_tune_threshold_error: float = 0.15,
    transient_retries: int = 2,
    transient_backoff_seconds: float = 0.25,
    progress_log_interval_seconds: int = 60,
    no_progress_soft_timeout_seconds: int | None = 900,
    no_progress_hard_timeout_seconds: int | None = 2700,
    progress_poll_seconds: int = 5,
    client_factory: Callable[[], Any] | None = None,
    fetch_window_fn: Callable[..., Any] = fetch_window_with_auto_split,
    fetch_group_window_fn: Callable[..., Any] = fetch_group_window_with_auto_split,
    empty_error_message_fn: Callable[[MinutePlanLike], str] | None = None,
) -> list[MinuteFetchResult]:
    """Run minute fetches with hourly-parity concurrency, batching, and auto-tune."""
    configured_rps = (
        requests_per_second
        if requests_per_second is not None
        else (ODDS_REQUESTS_PER_SECOND or DEFAULT_MINUTE_REQUESTS_PER_SECOND)
    )
    configured_rps = max(1, int(configured_rps))
    limiter = RateLimiter(configured_rps)
    runtime_status = {"total": 0, "429": 0, "error": 0}
    runtime_status_lock = Lock()
    tune_state = {"last_total": 0, "last_429": 0, "last_error": 0}
    worker_state = local()
    window_seconds = max(60, int(window_hours) * 3600)
    use_batch = int(batch_group_size) > 1

    def on_http_status(status_code: int) -> None:
        with runtime_status_lock:
            runtime_status["total"] += 1
            code = int(status_code)
            if code == 429:
                runtime_status["429"] += 1
            if code < 200 or code >= 400:
                runtime_status["error"] += 1

    def client() -> Any:
        value = getattr(worker_state, "client", None)
        if value is None:
            value = (
                client_factory()
                if client_factory
                else build_client(CLOB_API_URL, rate_limiter=limiter)
            )
            worker_state.client = value
        return value

    def fetch_group_unit(
        group: tuple[MinutePlanLike, ...],
    ) -> list[MinuteFetchResult]:
        return fetch_minute_plan_group(
            group,
            client(),
            fetch_group_window_fn,
            transient_retries=transient_retries,
            transient_backoff_seconds=transient_backoff_seconds,
            window_seconds=window_seconds,
            status_hook=on_http_status,
            empty_error_message_fn=empty_error_message_fn,
        )

    def fetch_single_unit(plan: MinutePlanLike) -> list[MinuteFetchResult]:
        return [
            fetch_minute_plan(
                plan,
                client(),
                fetch_window_fn,
                transient_retries=transient_retries,
                transient_backoff_seconds=transient_backoff_seconds,
                window_seconds=window_seconds,
                status_hook=on_http_status,
                empty_error_message=(
                    empty_error_message_fn(plan) if empty_error_message_fn else None
                ),
            )
        ]

    work_units: list[Any]
    fetch_unit: Callable[[Any], list[MinuteFetchResult]]
    if use_batch:
        work_units = group_minute_plans(plans, batch_group_size=batch_group_size)
        fetch_unit = fetch_group_unit
    else:
        work_units = list(plans)
        fetch_unit = fetch_single_unit

    guardrail = ProgressGuardrail(
        asset=asset_name,
        logger=log,
        progress_log_interval_seconds=progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=no_progress_hard_timeout_seconds,
        work_log_interval=25,
    )
    fetched: list[MinuteFetchResult] = []
    effective_workers = max(1, int(workers))
    with ThreadPoolExecutor(max_workers=effective_workers) as pool:
        futures: dict[Future[list[MinuteFetchResult]], Any] = {
            pool.submit(fetch_unit, unit): unit for unit in work_units
        }
        pending = set(futures.keys())
        while pending:
            done, pending = wait(
                pending,
                timeout=max(1, progress_poll_seconds),
                return_when=FIRST_COMPLETED,
            )
            if auto_tune_rps:
                with runtime_status_lock:
                    status_snapshot = dict(runtime_status)
                maybe_auto_tune_rps(
                    limiter=limiter,
                    runtime_status=status_snapshot,
                    tune_state=tune_state,
                    window_requests=auto_tune_window_requests,
                    threshold_429=auto_tune_threshold_429,
                    threshold_error=auto_tune_threshold_error,
                    min_rps=max(1, configured_rps),
                    max_rps=max(
                        configured_rps,
                        int(auto_tune_max_rps or configured_rps),
                    ),
                )
            if not done:
                guardrail.check(
                    phase="fetch_token_wait",
                    diagnostics={"inflight_futures": len(pending)},
                )
                continue
            for future in done:
                unit = futures[future]
                try:
                    results = future.result()
                except Exception as exc:  # pragma: no cover - defensive worker boundary
                    now = datetime.now(timezone.utc)
                    unit_plans = list(unit) if use_batch else [unit]
                    results = [
                        MinuteFetchResult(
                            plan=plan,
                            fetch_status="error",
                            history=(),
                            request_start_epoch=int(plan.started_at.timestamp()),
                            request_end_epoch=int(plan.finished_at.timestamp()),
                            source_row_count=0,
                            history_sha256=None,
                            fetch_started_at=now,
                            fetch_finished_at=now,
                            error_type=exc.__class__.__name__,
                            error_message=sanitize_error_message(exc),
                        )
                        for plan in unit_plans
                    ]
                fetched.extend(results)
                for result in results:
                    guardrail.record_progress(
                        work_increment=1,
                        phase="fetch_token",
                        diagnostics={
                            "token_id": result.plan.token_id,
                            "status": result.fetch_status,
                        },
                    )
                    guardrail.check(
                        phase="fetch_token",
                        diagnostics={
                            "token_id": result.plan.token_id,
                            "status": result.fetch_status,
                        },
                    )

    fetched.sort(key=lambda result: result.plan.token_id)
    return fetched


__all__ = [
    "DEFAULT_MINUTE_AUTO_TUNE_MAX_RPS",
    "DEFAULT_MINUTE_BATCH_GROUP_SIZE",
    "DEFAULT_MINUTE_REQUESTS_PER_SECOND",
    "DEFAULT_MINUTE_WINDOW_HOURS",
    "DEFAULT_MINUTE_WORKERS",
    "FIDELITY_MINUTES",
    "MIN_SPLIT_WINDOW_SECONDS",
    "MinuteFetchResult",
    "MinutePlanLike",
    "borrow_duckdb_connection",
    "execute_minute_fetches",
    "fetch_minute_plan",
    "fetch_minute_plan_group",
    "group_minute_plans",
    "padded_epoch_bounds",
    "sanitize_error_message",
]

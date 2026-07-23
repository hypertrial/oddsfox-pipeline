from __future__ import annotations

import contextlib
import os
from queue import Empty, Queue
from threading import Thread
from typing import Any

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource

from oddsfox_pipeline.orchestration.config import DbtBuildConfig
from oddsfox_pipeline.resources.progress_guardrails import (
    NoProgressTimeoutError,
    ProgressGuardrail,
)
from oddsfox_pipeline.storage.duckdb.connection import (
    active_duckdb_path,
    assert_disposable_duckdb_path,
    ensure_duck_db,
)


def _cleanup_dbt_adapter(invocation: Any) -> None:
    adapter = getattr(invocation, "adapter", None)
    cleanup_connections = getattr(adapter, "cleanup_connections", None)
    if callable(cleanup_connections):
        with contextlib.suppress(Exception):
            cleanup_connections()
    connections = getattr(adapter, "connections", None)
    cleanup_all = getattr(connections, "cleanup_all", None)
    if callable(cleanup_all):
        with contextlib.suppress(Exception):
            cleanup_all()


def stream_dbt_build(
    *,
    asset_name: str,
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    config: DbtBuildConfig,
    heartbeat_diagnostics_fn=None,
):
    guardrail = ProgressGuardrail(
        asset=asset_name,
        logger=context.log,
        progress_log_interval_seconds=config.progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
        work_log_interval=config.progress_log_interval_events,
    )
    guardrail.record_progress(
        work_increment=0,
        phase="start",
        diagnostics={},
        force_log=True,
    )

    if config.expected_duckdb_path is not None:
        assert_disposable_duckdb_path(config.expected_duckdb_path)
    ensure_duck_db()
    os.environ["DUCKDB_PATH"] = str(active_duckdb_path())

    build_args = ["build"]
    if config.full_refresh:
        build_args.append("--full-refresh")
    is_subset = getattr(context, "is_subset", False) is True
    if not is_subset:
        if config.dbt_select:
            build_args.extend(["--select", config.dbt_select])
        if config.dbt_exclude:
            build_args.extend(["--exclude", config.dbt_exclude])
    elif config.dbt_select is None and config.dbt_exclude == "tag:polygon_settlement":
        # Dagster owns subset selection, but the dedicated Polygon graph must
        # remain opt-in even when the whole dbt multi-asset is materialized.
        build_args.extend(["--exclude", config.dbt_exclude])
    invocation = dbt.cli(build_args, context=context)
    sentinel = object()
    event_queue: Queue[Any] = Queue()
    producer_error: list[Exception] = []

    def _producer() -> None:
        try:
            event_stream = invocation.stream()
            if config.fetch_dbt_metadata and hasattr(event_stream, "fetch_row_counts"):
                event_stream = event_stream.fetch_row_counts()
            if config.fetch_dbt_metadata and hasattr(
                event_stream, "fetch_column_metadata"
            ):
                event_stream = event_stream.fetch_column_metadata(
                    with_column_lineage=False
                )
            for event in event_stream:
                event_queue.put(event)
        except Exception as exc:  # pragma: no cover
            producer_error.append(exc)
        finally:
            _cleanup_dbt_adapter(invocation)
            event_queue.put(sentinel)

    producer = Thread(target=_producer, daemon=True)
    producer.start()

    events_emitted = 0
    while True:
        try:
            item = event_queue.get(timeout=max(1, config.progress_poll_seconds))
        except Empty:
            diagnostics = {
                "events_emitted": events_emitted,
                "queue_size": event_queue.qsize(),
                "dbt_return_code": getattr(invocation.process, "returncode", None),
            }
            if callable(heartbeat_diagnostics_fn):
                extra = heartbeat_diagnostics_fn()
                if isinstance(extra, dict):
                    diagnostics.update(extra)
            try:
                guardrail.check(
                    phase="dbt_build_stream_wait",
                    diagnostics=diagnostics,
                )
            except NoProgressTimeoutError:
                context.log.error(
                    "%s dbt build no-progress hard timeout; terminating dbt process",
                    asset_name,
                )
                with contextlib.suppress(Exception):
                    invocation.process.terminate()
                raise
            continue

        if item is sentinel:
            break

        events_emitted += 1
        guardrail.record_progress(
            work_increment=1,
            phase="dbt_build_event",
            diagnostics={"events_emitted": events_emitted},
        )
        yield item

    producer.join(timeout=max(1, config.progress_poll_seconds) * 2)
    if producer_error:
        raise producer_error[0]

    returncode = getattr(invocation.process, "returncode", None)
    if returncode not in (None, 0):
        raise RuntimeError(f"{asset_name} dbt build failed with exit code {returncode}")

    guardrail.record_progress(
        work_increment=0,
        phase="dbt_build_complete",
        diagnostics={"events_emitted": events_emitted},
        force_log=True,
    )


__all__ = ["stream_dbt_build"]

from __future__ import annotations

import contextlib
import os
import subprocess
from pathlib import Path
from queue import Empty, Queue
from threading import Thread
from typing import Any

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource

from oddsfox_pipeline.orchestration.config import DbtBuildConfig
from oddsfox_pipeline.orchestration.failure_metrics import save_asset_failure_metrics
from oddsfox_pipeline.resources.progress_guardrails import (
    NoProgressTimeoutError,
    ProgressGuardrail,
)
from oddsfox_pipeline.storage.duckdb.connection import (
    active_duckdb_path,
    assert_disposable_duckdb_path,
    ensure_duck_db,
)
from oddsfox_pipeline.storage.duckdb.metadata import (
    POLYMARKET_TOKEN_HOURLY_ODDS_INCREMENTAL_MODEL,
    clear_polymarket_token_hourly_odds_incremental_in_progress,
    mark_polymarket_token_hourly_odds_incremental_in_progress,
    polymarket_token_hourly_odds_incremental_recovery_needed,
    save_sync_run_metrics,
)

_POLYMARKET_HOURLY_ODDS_MART = "polymarket_wc2026_market_hourly_odds"
_INCREMENTAL_MODEL_SUBJECTS = (
    POLYMARKET_TOKEN_HOURLY_ODDS_INCREMENTAL_MODEL,
    "token_hourly_odds",
)
_MART_SUBJECTS = (
    _POLYMARKET_HOURLY_ODDS_MART,
    "market_hourly_odds",
)
_ISOLATED_DBT_SELECT_MARKERS = (
    "match_minute",
    "minute_odds",
    "polygon_settlement",
    "pmxt_order_book",
    "market_portrait",
)


def _polymarket_token_hourly_odds_incremental_in_scope(
    *,
    config: DbtBuildConfig,
    context: AssetExecutionContext,
    is_subset: bool,
) -> bool:
    if is_subset:
        selected = getattr(context, "selected_asset_keys", None)
        if not selected:
            return False
        subjects = _INCREMENTAL_MODEL_SUBJECTS + _MART_SUBJECTS
        for key in selected:
            text = str(key).lower()
            if any(subject in text for subject in subjects):
                return True
        return False

    dbt_select = (config.dbt_select or "").strip().lower()
    if not dbt_select:
        return True
    if any(
        subject in dbt_select
        for subject in _MART_SUBJECTS + _INCREMENTAL_MODEL_SUBJECTS
    ):
        return True
    if "tag:kalshi" in dbt_select:
        return False
    if any(marker in dbt_select for marker in _ISOLATED_DBT_SELECT_MARKERS):
        return False
    return False


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


# A large DuckDB query (e.g. a full unsampled minute-odds rebuild) can keep
# every thread in the dbt subprocess busy inside a C extension call. SIGTERM's
# default disposition still terminates the process without interpreter
# involvement, but DuckDB's worker threads can hold it pending until the
# query yields control back. SIGKILL cannot be blocked, so escalate to it if
# the process outlives a bounded grace period -- otherwise it survives as an
# orphan holding an exclusive lock on the warehouse file, wedging every
# subsequent run against the same database.
_DBT_TERMINATE_GRACE_SECONDS = 30.0


def _terminate_dbt_process(
    invocation: Any,
    *,
    context: AssetExecutionContext,
    asset_name: str,
) -> None:
    process = getattr(invocation, "process", None)
    if process is None:
        return
    try:
        process.terminate()
        process.wait(timeout=_DBT_TERMINATE_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        context.log.error(
            "%s dbt process pid=%s ignored SIGTERM after %.0fs; sending SIGKILL",
            asset_name,
            process.pid,
            _DBT_TERMINATE_GRACE_SECONDS,
        )
        try:
            process.kill()
            process.wait(timeout=_DBT_TERMINATE_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            # Observed in practice on a wedged external volume: a thread stuck
            # in uninterruptible kernel I/O can outlive even SIGKILL for
            # several minutes. Surface it loudly instead of returning quietly
            # -- the DuckDB warehouse lock stays held until pid actually dies.
            context.log.error(
                "%s dbt process pid=%s still alive %.0fs after SIGKILL; "
                "it is likely blocked on uninterruptible I/O and must clear "
                "on its own (check `ps -p %s`) before the warehouse is usable",
                asset_name,
                process.pid,
                _DBT_TERMINATE_GRACE_SECONDS,
                process.pid,
            )
        except Exception:
            context.log.warning(
                "%s failed to force-kill dbt process pid=%s",
                asset_name,
                process.pid,
                exc_info=True,
            )
    except Exception:
        context.log.warning(
            "%s failed to terminate dbt process cleanly", asset_name, exc_info=True
        )


def _run_dbt_cli_to_completion(
    *,
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    build_args: list[str],
) -> None:
    invocation = dbt.cli(build_args, context=context)
    try:
        for _event in invocation.stream():
            pass
    finally:
        _cleanup_dbt_adapter(invocation)
    returncode = getattr(invocation.process, "returncode", None)
    if returncode not in (None, 0):
        raise RuntimeError(
            f"dbt {' '.join(build_args)} failed with exit code {returncode}"
        )


def _maybe_recover_polymarket_token_hourly_odds_incremental(
    *,
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    config: DbtBuildConfig,
    is_subset: bool,
) -> None:
    if config.full_refresh:
        return
    if not _polymarket_token_hourly_odds_incremental_in_scope(
        config=config,
        context=context,
        is_subset=is_subset,
    ):
        return
    if not polymarket_token_hourly_odds_incremental_recovery_needed():
        return
    context.log.warning(
        "Detected interrupted prior build for %s; running targeted full-refresh "
        "before the ordinary dbt build",
        POLYMARKET_TOKEN_HOURLY_ODDS_INCREMENTAL_MODEL,
    )
    _run_dbt_cli_to_completion(
        context=context,
        dbt=dbt,
        build_args=[
            "build",
            "--select",
            POLYMARKET_TOKEN_HOURLY_ODDS_INCREMENTAL_MODEL,
            "--full-refresh",
        ],
    )
    clear_polymarket_token_hourly_odds_incremental_in_progress()


def _dir_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_size
            except OSError:
                continue
    return total


def _dbt_spill_directories(warehouse_path: Path) -> tuple[Path, ...]:
    """DuckDB spill roots used by dbt profiles and warehouse-adjacent defaults."""
    runtime_root = Path(
        os.getenv(
            "ODDSFOX_RUNTIME_ROOT",
            Path(os.getenv("ODDSFOX_PIPELINE_ROOT", ".")).resolve()
            / ".cache"
            / "runtime",
        )
    ).expanduser()
    return (
        runtime_root / "duckdb-temp",
        Path(str(warehouse_path) + ".tmp"),
    )


def _dbt_liveness_fingerprint(
    *,
    invocation: Any,
    warehouse_path: Path,
) -> dict[str, Any]:
    """Evidence that a long DuckDB query is still working despite zero dbt events.

    dbt emits nothing while a single large node runs. Treat growth in the
    configured ``duckdb-temp`` spill directory, warehouse ``.tmp``, WAL size, or
    an alive child pid as progress so the no-progress hard timeout does not kill
    an active build.
    """
    process = getattr(invocation, "process", None)
    pid = getattr(process, "pid", None)
    alive = process is not None and process.poll() is None
    spill_dirs = _dbt_spill_directories(warehouse_path)
    spill_bytes = sum(_dir_bytes(path) for path in spill_dirs)
    wal_path = Path(str(warehouse_path) + ".wal")
    wal_bytes = wal_path.stat().st_size if wal_path.is_file() else 0
    warehouse_bytes = warehouse_path.stat().st_size if warehouse_path.is_file() else 0
    return {
        "dbt_pid": pid,
        "dbt_alive": alive,
        "duckdb_temp_bytes": spill_bytes,
        "duckdb_wal_bytes": wal_bytes,
        "duckdb_warehouse_bytes": warehouse_bytes,
        "liveness_fingerprint": (
            f"alive={int(alive)}:tmp={spill_bytes}:wal={wal_bytes}:db={warehouse_bytes}"
        ),
    }


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
    warehouse_path = Path(active_duckdb_path())
    os.environ["DUCKDB_PATH"] = str(warehouse_path)

    is_subset = getattr(context, "is_subset", False) is True
    incremental_in_progress = False
    if not config.full_refresh and _polymarket_token_hourly_odds_incremental_in_scope(
        config=config,
        context=context,
        is_subset=is_subset,
    ):
        mark_polymarket_token_hourly_odds_incremental_in_progress()
        incremental_in_progress = True

    build_args = ["build"]
    if config.full_refresh:
        build_args.append("--full-refresh")
    if not is_subset:
        if config.dbt_select:
            build_args.extend(["--select", config.dbt_select])
        if config.dbt_exclude:
            build_args.extend(["--exclude", config.dbt_exclude])
    elif config.dbt_exclude:
        # Dagster owns subset selection; still honor configured excludes so
        # scoped jobs and integration fixtures can opt out of polygon, logical
        # atlas, and cross-domain graphs without widening the asset selection.
        build_args.extend(["--exclude", config.dbt_exclude])
    invocation: Any | None = None
    events_emitted = 0
    completed_cleanly = False
    last_liveness: str | None = None
    try:
        _maybe_recover_polymarket_token_hourly_odds_incremental(
            context=context,
            dbt=dbt,
            config=config,
            is_subset=is_subset,
        )
        invocation = dbt.cli(build_args, context=context)
        sentinel = object()
        event_queue: Queue[Any] = Queue()
        producer_error: list[Exception] = []

        def _producer() -> None:
            try:
                event_stream = invocation.stream()
                if config.fetch_dbt_metadata and hasattr(
                    event_stream, "fetch_row_counts"
                ):
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

        while True:
            try:
                item = event_queue.get(timeout=max(1, config.progress_poll_seconds))
            except Empty:
                diagnostics = {
                    "events_emitted": events_emitted,
                    "queue_size": event_queue.qsize(),
                    "dbt_return_code": getattr(invocation.process, "returncode", None),
                }
                liveness = _dbt_liveness_fingerprint(
                    invocation=invocation,
                    warehouse_path=warehouse_path,
                )
                diagnostics.update(liveness)
                if callable(heartbeat_diagnostics_fn):
                    extra = heartbeat_diagnostics_fn()
                    if isinstance(extra, dict):
                        diagnostics.update(extra)
                fingerprint = str(liveness["liveness_fingerprint"])
                if fingerprint != last_liveness and liveness["dbt_alive"]:
                    last_liveness = fingerprint
                    guardrail.record_progress(
                        work_increment=0,
                        phase="dbt_build_liveness",
                        diagnostics=diagnostics,
                    )
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
                    _terminate_dbt_process(
                        invocation, context=context, asset_name=asset_name
                    )
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
            raise RuntimeError(
                f"{asset_name} dbt build failed with exit code {returncode}"
            )
        if incremental_in_progress:
            clear_polymarket_token_hourly_odds_incremental_in_progress()
        completed_cleanly = True
    except Exception as exc:
        save_asset_failure_metrics(
            "dbt_build",
            exc,
            extra={"asset": asset_name},
        )
        raise
    finally:
        # Dagster cancellation closes the generator with GeneratorExit (a
        # BaseException). Without this, the dbt child can be reparented to
        # launchd while still holding the DuckDB warehouse lock.
        if invocation is not None and not completed_cleanly:
            process = getattr(invocation, "process", None)
            if process is not None and process.poll() is None:
                context.log.error(
                    "%s dbt build interrupted; terminating dbt process pid=%s",
                    asset_name,
                    getattr(process, "pid", None),
                )
                _terminate_dbt_process(
                    invocation, context=context, asset_name=asset_name
                )

    guardrail.record_progress(
        work_increment=0,
        phase="dbt_build_complete",
        diagnostics={"events_emitted": events_emitted},
        force_log=True,
    )
    save_sync_run_metrics(
        "dbt_build",
        {
            "status": "success",
            "asset": asset_name,
            "events_emitted": events_emitted,
        },
    )


__all__ = ["stream_dbt_build"]

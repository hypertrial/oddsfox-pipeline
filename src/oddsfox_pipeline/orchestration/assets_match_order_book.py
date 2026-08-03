"""Dagster asset for an approved WC2026 PMXT historical order-book backfill."""

from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, AssetSpec, MaterializeResult, multi_asset

from oddsfox_pipeline.ingestion.polymarket.match_order_book import (
    MatchOrderBookSyncError,
    sync_match_order_book_history,
)
from oddsfox_pipeline.naming import SCOPE_WC2026, SOURCE_POLYMARKET, asset_key
from oddsfox_pipeline.orchestration.assets_openfootball import (
    OPENFOOTBALL_WC2026_RAW_SCHEDULE_FIXTURES,
)
from oddsfox_pipeline.orchestration.config import MatchOrderBookBackfillConfig
from oddsfox_pipeline.resources.progress_guardrails import ProgressGuardrail
from oddsfox_pipeline.storage.duckdb.connection import get_connection

POLYMARKET_WC2026_RAW_MATCH_ORDER_BOOK_SNAPSHOTS = asset_key(
    SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "match_order_book_snapshots"
)


def _sync_match_order_book(
    conn,
    config: MatchOrderBookBackfillConfig,
    *,
    lease_owner: str,
    progress_callback,
) -> dict[str, Any]:
    kwargs = {
        "requests_per_minute": config.requests_per_minute,
        "monthly_credit_budget": config.monthly_credit_budget,
        "transient_retries": config.transient_retries,
        "transient_backoff_seconds": config.transient_backoff_seconds,
        "force": config.force,
        "lease_owner": lease_owner,
        "progress_callback": progress_callback,
    }
    if config.manifest_path:
        kwargs["manifest_path"] = Path(config.manifest_path)
    return sync_match_order_book_history(conn, **kwargs)


@multi_asset(
    name="polymarket_wc2026_raw_match_order_book_snapshots",
    specs=[
        AssetSpec(
            key=POLYMARKET_WC2026_RAW_MATCH_ORDER_BOOK_SNAPSHOTS,
            deps=[OPENFOOTBALL_WC2026_RAW_SCHEDULE_FIXTURES],
        )
    ],
    group_name="ingestion",
)
def polymarket_wc2026_raw_match_order_book_snapshots(
    context: AssetExecutionContext,
    config: MatchOrderBookBackfillConfig,
) -> MaterializeResult:
    guardrail = ProgressGuardrail(
        asset="polymarket_wc2026_raw_match_order_book_snapshots",
        logger=context.log,
        progress_log_interval_seconds=config.progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
    )

    def progress(phase: str, diagnostics: dict[str, Any]) -> None:
        guardrail.record_progress(
            work_increment=1,
            phase=phase,
            diagnostics=diagnostics,
        )
        guardrail.check(phase=phase, diagnostics=diagnostics)

    try:
        with get_connection() as conn:
            summary = _sync_match_order_book(
                conn,
                config,
                lease_owner=context.run.run_id,
                progress_callback=progress,
            )
    except MatchOrderBookSyncError as exc:
        context.add_output_metadata(
            {
                key: value
                for key, value in exc.summary.items()
                if value is not None and isinstance(value, (str, int, float, bool))
            }
        )
        raise
    return MaterializeResult(metadata=summary)


__all__ = [
    "POLYMARKET_WC2026_RAW_MATCH_ORDER_BOOK_SNAPSHOTS",
    "polymarket_wc2026_raw_match_order_book_snapshots",
]

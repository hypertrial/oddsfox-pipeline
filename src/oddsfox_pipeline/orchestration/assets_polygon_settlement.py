"""Dagster assets for independent WC2026 Polygon settlement analytics."""

from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, AssetSpec, MaterializeResult, multi_asset

from oddsfox_pipeline.config.settings import (
    POLYGON_RPC_PROVIDER_LABEL,
    POLYGON_RPC_URL,
    POLYGON_VERIFY_RPC_PROVIDER_LABEL,
    POLYGON_VERIFY_RPC_URL,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    DEFAULT_POLYGON_MARKET_SEED_PATH,
)
from oddsfox_pipeline.naming import SCOPE_WC2026, SOURCE_POLYMARKET, asset_key
from oddsfox_pipeline.orchestration.config import (
    PolygonSettlementReleaseConfig,
    PolygonSettlementSyncConfig,
)
from oddsfox_pipeline.publishing.polygon_settlement import (
    PolygonSettlementBundleSpec,
    build_polygon_settlement_release,
    current_generator_commit,
)
from oddsfox_pipeline.storage.duckdb.connection import (
    assert_disposable_duckdb_path,
    get_connection,
)
from oddsfox_pipeline.storage.duckdb.polygon_settlement import (
    load_polygon_settlement_release_provenance,
    set_polygon_verification_status,
)

POLYMARKET_WC2026_RAW_POLYGON_SETTLEMENT_FILLS = asset_key(
    SOURCE_POLYMARKET, SCOPE_WC2026, "raw", "polygon_settlement_fills"
)
POLYMARKET_WC2026_MARTS_POLYGON_SETTLEMENT_MINUTE_ODDS = asset_key(
    SOURCE_POLYMARKET,
    SCOPE_WC2026,
    "marts",
    "polygon_settlement_minute_odds",
)
POLYMARKET_WC2026_RELEASE_POLYGON_SETTLEMENT_ODDS_BUNDLE = asset_key(
    SOURCE_POLYMARKET,
    SCOPE_WC2026,
    "release",
    "polygon_settlement_odds_bundle",
)


def _sync_polygon_settlement_fills(
    conn,
    config: PolygonSettlementSyncConfig,
    *,
    log,
) -> dict[str, Any]:
    from oddsfox_pipeline.ingestion.polymarket.polygon_settlement import (
        PolygonSettlementSyncConfig as CoreSyncConfig,
    )
    from oddsfox_pipeline.ingestion.polymarket.polygon_settlement import (
        sync_polygon_settlement_fills,
    )

    return sync_polygon_settlement_fills(
        conn,
        seed_path=DEFAULT_POLYGON_MARKET_SEED_PATH,
        rpc_url=POLYGON_RPC_URL,
        provider_label=POLYGON_RPC_PROVIDER_LABEL,
        config=CoreSyncConfig(
            requests_per_second=config.requests_per_second,
            workers=config.workers,
            initial_block_chunk_size=config.initial_block_chunk_size,
            initial_receipt_batch_size=config.initial_receipt_batch_size,
            transient_retries=config.transient_retries,
            transient_backoff_seconds=config.transient_backoff_seconds,
            progress_log_interval_seconds=config.progress_log_interval_seconds,
            no_progress_soft_timeout_seconds=config.no_progress_soft_timeout_seconds,
            no_progress_hard_timeout_seconds=config.no_progress_hard_timeout_seconds,
        ),
        log=log,
    )


def _verify_polygon_settlement_scan(conn) -> dict[str, Any] | None:
    if not POLYGON_VERIFY_RPC_URL and not POLYGON_VERIFY_RPC_PROVIDER_LABEL:
        return None
    from oddsfox_pipeline.ingestion.polymarket.polygon_settlement import (
        verify_polygon_settlement_scan,
    )

    return verify_polygon_settlement_scan(
        conn,
        seed_path=DEFAULT_POLYGON_MARKET_SEED_PATH,
        rpc_url=POLYGON_VERIFY_RPC_URL,
        provider_label=POLYGON_VERIFY_RPC_PROVIDER_LABEL,
    )


@multi_asset(
    name="polymarket_wc2026_raw_polygon_settlement_fills",
    specs=[AssetSpec(key=POLYMARKET_WC2026_RAW_POLYGON_SETTLEMENT_FILLS)],
    group_name="ingestion",
)
def polymarket_wc2026_raw_polygon_settlement_fills(
    context: AssetExecutionContext,
    config: PolygonSettlementSyncConfig,
) -> MaterializeResult:
    if config.expected_duckdb_path is not None:
        assert_disposable_duckdb_path(config.expected_duckdb_path)
    with get_connection() as conn:
        summary = _sync_polygon_settlement_fills(conn, config, log=context.log)
    return MaterializeResult(metadata=summary)


@multi_asset(
    name="polymarket_wc2026_release_polygon_settlement_odds_bundle",
    specs=[
        AssetSpec(
            key=POLYMARKET_WC2026_RELEASE_POLYGON_SETTLEMENT_ODDS_BUNDLE,
            deps=[POLYMARKET_WC2026_MARTS_POLYGON_SETTLEMENT_MINUTE_ODDS],
        )
    ],
    group_name="release",
)
def polymarket_wc2026_release_polygon_settlement_odds_bundle(
    context: AssetExecutionContext,
    config: PolygonSettlementReleaseConfig,
) -> MaterializeResult:
    verification: dict[str, Any] | None = None
    with get_connection() as conn:
        try:
            verification = _verify_polygon_settlement_scan(conn)
        except Exception as exc:  # verification is explicitly advisory
            stale_provenance = load_polygon_settlement_release_provenance(conn)
            set_polygon_verification_status(
                conn,
                str(stale_provenance["scan_id"]),
                "error",
            )
            verification = {
                "scan_id": str(stale_provenance["scan_id"]),
                "verification_status": "error",
                "error_type": exc.__class__.__name__,
            }
            context.log.warning(
                "Secondary Polygon verification failed (%s); release remains advisory.",
                exc.__class__.__name__,
            )
        provenance = load_polygon_settlement_release_provenance(conn)
        summary = build_polygon_settlement_release(
            conn,
            Path(config.output_root).expanduser(),
            PolygonSettlementBundleSpec(
                dataset_version=config.dataset_version,
                publisher_name=config.publisher_name,
                attribution_url=config.attribution_url,
                rights_review_status=config.rights_review_status,
                rpc_provider_terms_url=config.rpc_provider_terms_url,
                rpc_provider_terms_snapshot_sha256=(
                    config.rpc_provider_terms_snapshot_sha256
                ),
                rpc_provider_terms_snapshot_at_utc=(
                    config.rpc_provider_terms_snapshot_at_utc
                ),
            ),
            provenance=provenance,
            generator_commit=current_generator_commit(),
        )
    return MaterializeResult(metadata={**summary, "verification": verification})


__all__ = [
    "POLYMARKET_WC2026_MARTS_POLYGON_SETTLEMENT_MINUTE_ODDS",
    "POLYMARKET_WC2026_RAW_POLYGON_SETTLEMENT_FILLS",
    "POLYMARKET_WC2026_RELEASE_POLYGON_SETTLEMENT_ODDS_BUNDLE",
    "polymarket_wc2026_raw_polygon_settlement_fills",
    "polymarket_wc2026_release_polygon_settlement_odds_bundle",
]

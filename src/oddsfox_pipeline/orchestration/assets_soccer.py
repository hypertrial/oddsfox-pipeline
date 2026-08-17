"""Dagster assets for the Polymarket soccer match-minute scope."""

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetSpec,
    MaterializeResult,
    asset_check,
    multi_asset,
)

from oddsfox_pipeline.ingestion.polymarket.dlt_source import (
    normalize_market_payloads_for_dlt,
)
from oddsfox_pipeline.ingestion.polymarket.event_catalog import (
    collect_soccer_event_catalog,
)
from oddsfox_pipeline.ingestion.polymarket.soccer_match import (
    refresh_soccer_match_result_registry,
    sync_soccer_match_minute_odds_history,
)
from oddsfox_pipeline.naming import SCOPE_SOCCER, SOURCE_POLYMARKET, asset_key
from oddsfox_pipeline.orchestration import polymarket_asset_helpers as asset_helpers
from oddsfox_pipeline.orchestration.config import (
    MarketScopeRegistryConfig,
    SoccerMatchMinuteOddsSyncConfig,
)
from oddsfox_pipeline.orchestration.failure_metrics import save_asset_failure_metrics
from oddsfox_pipeline.orchestration.soccer_monitoring import (
    monitor_soccer_step,
    record_soccer_check_failure,
    run_soccer_preflight,
)
from oddsfox_pipeline.storage.duckdb.connection import get_connection
from oddsfox_pipeline.storage.duckdb.dlt_batch_event_catalog import (
    merge_event_catalog_batch,
)
from oddsfox_pipeline.storage.duckdb.metadata import (
    get_sync_run_metrics,
    save_sync_run_metrics,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import ensure_polymarket_indexes

POLYMARKET_SOCCER_RAW_EVENT_CATALOG = asset_key(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "raw", "event_catalog"
)
POLYMARKET_SOCCER_OPS_PIPELINE_PREFLIGHT = asset_key(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "ops", "pipeline_preflight"
)
POLYMARKET_SOCCER_OPS_PIPELINE_RUNS = asset_key(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "ops", "pipeline_runs"
)
POLYMARKET_SOCCER_OPS_PIPELINE_STEP_RUNS = asset_key(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "ops", "pipeline_step_runs"
)
POLYMARKET_SOCCER_RAW_EVENT_SNAPSHOTS = asset_key(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "raw", "event_snapshots"
)
POLYMARKET_SOCCER_RAW_EVENT_MARKET_MEMBERSHIPS = asset_key(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "raw", "event_market_memberships"
)
POLYMARKET_SOCCER_OPS_MATCH_RESULT_REGISTRY = asset_key(
    SOURCE_POLYMARKET, SCOPE_SOCCER, "ops", "match_result_registry"
)
POLYMARKET_SOCCER_RAW_MATCH_RESULT_MINUTE = asset_key(
    SOURCE_POLYMARKET,
    SCOPE_SOCCER,
    "raw",
    "match_result_token_odds_history_minute",
)
POLYMARKET_SOCCER_MART_MATCH_RESULT_MINUTE = asset_key(
    SOURCE_POLYMARKET,
    SCOPE_SOCCER,
    "marts",
    "match_result_minute_odds",
)


def _blocking_check_result(
    context: AssetCheckExecutionContext,
    *,
    name: str,
    passed: bool,
    metadata: dict,
) -> AssetCheckResult:
    if not passed:
        record_soccer_check_failure(
            run_id=str(context.run.run_id), check_name=name, metadata=metadata
        )
    return AssetCheckResult(passed=passed, metadata=metadata)


@multi_asset(
    name="polymarket_soccer_ops_pipeline_preflight",
    specs=[
        AssetSpec(key=POLYMARKET_SOCCER_OPS_PIPELINE_PREFLIGHT),
        AssetSpec(key=POLYMARKET_SOCCER_OPS_PIPELINE_RUNS),
        AssetSpec(key=POLYMARKET_SOCCER_OPS_PIPELINE_STEP_RUNS),
    ],
    group_name="ingestion",
)
def polymarket_soccer_ops_pipeline_preflight(
    context: AssetExecutionContext,
):
    with monitor_soccer_step(context, "pipeline_preflight") as monitor:
        summary = run_soccer_preflight()
        monitor.complete(summary)
    for key in (
        POLYMARKET_SOCCER_OPS_PIPELINE_PREFLIGHT,
        POLYMARKET_SOCCER_OPS_PIPELINE_RUNS,
        POLYMARKET_SOCCER_OPS_PIPELINE_STEP_RUNS,
    ):
        yield MaterializeResult(asset_key=key, metadata=summary)


@multi_asset(
    name="polymarket_soccer_raw_event_catalog",
    specs=[
        AssetSpec(
            key=POLYMARKET_SOCCER_RAW_EVENT_CATALOG,
            deps=[POLYMARKET_SOCCER_OPS_PIPELINE_PREFLIGHT],
        ),
        AssetSpec(key=POLYMARKET_SOCCER_RAW_EVENT_SNAPSHOTS),
        AssetSpec(key=POLYMARKET_SOCCER_RAW_EVENT_MARKET_MEMBERSHIPS),
    ],
    group_name="ingestion",
)
def polymarket_soccer_raw_event_catalog(
    context: AssetExecutionContext,
    config: MarketScopeRegistryConfig,
):
    def merge_soccer_batch(**kwargs):
        return merge_event_catalog_batch(scope_name=SCOPE_SOCCER, **kwargs)

    with monitor_soccer_step(context, "event_catalog") as monitor:
        yield from asset_helpers._materialize_event_catalog(
            context,
            config,
            asset_name="polymarket_soccer_raw_event_catalog",
            scope_name=SCOPE_SOCCER,
            collect_event_catalog_fn=collect_soccer_event_catalog,
            merge_event_catalog_batch_fn=merge_soccer_batch,
            normalize_market_payloads_fn=normalize_market_payloads_for_dlt,
            ensure_indexes_fn=ensure_polymarket_indexes,
            get_connection_fn=get_connection,
            save_sync_run_metrics_fn=save_sync_run_metrics,
            event_catalog_key=POLYMARKET_SOCCER_RAW_EVENT_CATALOG,
            event_snapshots_key=POLYMARKET_SOCCER_RAW_EVENT_SNAPSHOTS,
            event_memberships_key=POLYMARKET_SOCCER_RAW_EVENT_MARKET_MEMBERSHIPS,
        )
        monitor.complete(
            get_sync_run_metrics("event_catalog", scope_name=SCOPE_SOCCER) or {}
        )


@multi_asset(
    name="polymarket_soccer_ops_match_result_registry",
    specs=[
        AssetSpec(
            key=POLYMARKET_SOCCER_OPS_MATCH_RESULT_REGISTRY,
            deps=[POLYMARKET_SOCCER_RAW_EVENT_CATALOG],
        )
    ],
    group_name="ingestion",
)
def polymarket_soccer_ops_match_result_registry(
    context: AssetExecutionContext,
) -> MaterializeResult:
    try:
        with monitor_soccer_step(context, "match_result_registry") as monitor:
            with get_connection() as conn:
                summary = refresh_soccer_match_result_registry(conn)
            save_sync_run_metrics(
                "match_result_registry", summary, scope_name=SCOPE_SOCCER
            )
            monitor.complete(summary)
    except Exception as exc:
        save_asset_failure_metrics(
            "match_result_registry", exc, scope_name=SCOPE_SOCCER
        )
        raise
    return MaterializeResult(metadata=summary)


@multi_asset(
    name="polymarket_soccer_raw_match_result_token_odds_history_minute",
    specs=[
        AssetSpec(
            key=POLYMARKET_SOCCER_RAW_MATCH_RESULT_MINUTE,
            deps=[POLYMARKET_SOCCER_OPS_MATCH_RESULT_REGISTRY],
        )
    ],
    group_name="ingestion",
)
def polymarket_soccer_raw_match_result_token_odds_history_minute(
    context: AssetExecutionContext,
    config: SoccerMatchMinuteOddsSyncConfig,
) -> MaterializeResult:
    try:
        with monitor_soccer_step(context, "match_minute_odds") as monitor:
            summary = sync_soccer_match_minute_odds_history(
                connection_factory=get_connection,
                log=context.log,
                workers=config.workers,
                requests_per_second=config.requests_per_second,
                batch_group_size=config.batch_group_size,
                window_hours=config.window_hours,
                auto_tune_rps=config.auto_tune_rps,
                auto_tune_max_rps=config.auto_tune_max_rps,
                transient_retries=config.transient_retries,
                transient_backoff_seconds=config.transient_backoff_seconds,
                completion_grace_minutes=config.completion_grace_minutes,
                empty_retry_hours=config.empty_retry_hours,
                force=config.force,
                retry_empty_only=config.retry_empty_only,
                game_sample_size=config.game_sample_size,
            )
            save_sync_run_metrics("match_minute_odds", summary, scope_name=SCOPE_SOCCER)
            monitor.complete(summary)
    except Exception as exc:
        save_asset_failure_metrics("match_minute_odds", exc, scope_name=SCOPE_SOCCER)
        raise
    return MaterializeResult(metadata=summary)


@asset_check(
    asset=POLYMARKET_SOCCER_OPS_PIPELINE_PREFLIGHT,
    name="local_contracts_valid",
    blocking=True,
)
def polymarket_soccer_preflight_check(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    try:
        metadata = run_soccer_preflight()
    except Exception as exc:
        return _blocking_check_result(
            context,
            name="local_contracts_valid",
            passed=False,
            metadata={"error": str(exc)},
        )
    return AssetCheckResult(passed=True, metadata=metadata)


@asset_check(
    asset=POLYMARKET_SOCCER_RAW_EVENT_CATALOG,
    name="catalog_converged",
    blocking=True,
)
def polymarket_soccer_catalog_check(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    metrics = get_sync_run_metrics("event_catalog", scope_name=SCOPE_SOCCER) or {}
    partitions = metrics.get("scan_partitions") or {}
    required = {"exact_soccer_tag:open", "exact_soccer_tag:closed"}
    passed = bool(metrics.get("all_scan_partitions_complete")) and all(
        key in partitions
        and partitions[key].get("complete") is True
        and partitions[key].get("stable") is True
        for key in required
    )
    return _blocking_check_result(
        context, name="catalog_converged", passed=passed, metadata=metrics
    )


@asset_check(
    asset=POLYMARKET_SOCCER_OPS_MATCH_RESULT_REGISTRY,
    name="three_roles_and_six_tokens",
    blocking=True,
)
def polymarket_soccer_registry_check(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    registry = polymarket_ops_tbl(SCOPE_SOCCER, "match_result_registry")
    with get_connection() as conn:
        invalid_events, duplicate_tokens, invalid_timing = conn.execute(
            f"""
            WITH event_health AS (
                SELECT event_id, count(*) AS markets,
                    count(DISTINCT result_role) AS roles
                FROM {registry} GROUP BY event_id
            ), tokens AS (
                SELECT unnest([yes_token_id, no_token_id]) AS token_id FROM {registry}
            )
            SELECT
                (SELECT count(*) FROM event_health WHERE markets <> 3 OR roles <> 3),
                (SELECT count(*) - count(DISTINCT token_id) FROM tokens),
                (SELECT count(*) FROM {registry}
                    WHERE window_start_at IS NULL OR window_end_at IS NULL
                    OR window_end_at < window_start_at)
            """
        ).fetchone()
    passed = invalid_events == 0 and duplicate_tokens == 0 and invalid_timing == 0
    return _blocking_check_result(
        context,
        name="three_roles_and_six_tokens",
        passed=passed,
        metadata={
            "invalid_events": invalid_events,
            "duplicate_tokens": duplicate_tokens,
            "invalid_timing": invalid_timing,
        },
    )


@asset_check(
    asset=POLYMARKET_SOCCER_RAW_MATCH_RESULT_MINUTE,
    name="exact_window_publication_reconciled",
    blocking=True,
)
def polymarket_soccer_minute_reconciliation_check(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    registry = polymarket_ops_tbl(SCOPE_SOCCER, "match_result_registry")
    audit = polymarket_ops_tbl(SCOPE_SOCCER, "match_minute_odds_fetch_audit")
    history = polymarket_raw_tbl(SCOPE_SOCCER, "match_minute_odds_history")
    with get_connection() as conn:
        unreconciled = conn.execute(
            f"""
            WITH active_tokens AS (
                SELECT DISTINCT market_id, "clobTokenId" AS token_id FROM {history}
            ), registry_tokens AS (
                SELECT market_id, window_start_at, window_end_at,
                    unnest([yes_token_id, no_token_id]) AS token_id
                FROM {registry}
            ), published AS (
                SELECT market_id, "clobTokenId" AS token_id,
                    exact_window_start_at, exact_window_end_at
                FROM {audit}
                WHERE fetch_status = 'success' AND raw_published
                QUALIFY row_number() OVER (
                    PARTITION BY "clobTokenId"
                    ORDER BY fetch_finished_at DESC, fetch_run_id DESC
                ) = 1
            )
            SELECT count(*) FROM active_tokens AS active
            LEFT JOIN registry_tokens AS registry
              ON active.market_id = registry.market_id
             AND active.token_id = registry.token_id
            LEFT JOIN published
              ON registry.market_id = published.market_id
             AND registry.token_id = published.token_id
             AND registry.window_start_at = published.exact_window_start_at
             AND registry.window_end_at = published.exact_window_end_at
            WHERE registry.token_id IS NOT NULL AND published.token_id IS NULL
            """
        ).fetchone()[0]
    return _blocking_check_result(
        context,
        name="exact_window_publication_reconciled",
        passed=unreconciled == 0,
        metadata={"unreconciled_snapshot_tokens": unreconciled},
    )


@asset_check(
    asset=POLYMARKET_SOCCER_RAW_MATCH_RESULT_MINUTE,
    name="production_health",
    blocking=False,
)
def polymarket_soccer_production_health_check() -> AssetCheckResult:
    try:
        with get_connection() as conn:
            row = conn.execute(
                """
                SELECT health_status, critical_count, warning_count,
                    latest_run_status
                FROM polymarket_soccer_observability
                    .polymarket_soccer_pipeline_health
                """
            ).fetchone()
    except Exception as exc:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            metadata={"monitoring_error": f"{type(exc).__name__}: {exc}"},
        )
    passed = row is not None and int(row[1] or 0) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "health_status": row[0] if row else "invalid",
            "critical_alerts": int(row[1] or 0) if row else 1,
            "warning_alerts": int(row[2] or 0) if row else 0,
            "latest_status": row[3] if row else "missing",
        },
    )


@asset_check(
    asset=POLYMARKET_SOCCER_MART_MATCH_RESULT_MINUTE,
    name="minute_mart_contracts_valid",
    blocking=True,
)
def polymarket_soccer_minute_mart_check(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    observed = (
        "polymarket_soccer_marts.polymarket_soccer_match_result_minute_odds_observed"
    )
    dense = "polymarket_soccer_marts.polymarket_soccer_match_result_minute_odds"
    with get_connection() as conn:
        metrics = conn.execute(
            f"""
            WITH expected_markets AS (
                SELECT market_id, window_start_at, window_end_at,
                    date_diff('minute', window_start_at, window_end_at) + 1
                        AS expected_minutes
                FROM polymarket_soccer_intermediate
                    .int_polymarket_soccer_match_result_market_state
            ), dense_health AS (
                SELECT expected.market_id, expected.expected_minutes,
                    count(dense_rows.market_id) AS actual_minutes
                FROM expected_markets AS expected
                LEFT JOIN {dense} AS dense_rows
                    ON expected.market_id = dense_rows.market_id
                GROUP BY expected.market_id, expected.expected_minutes
            ), dense_annotated AS (
                SELECT *,
                    last_value(
                        case when is_observed then close_odds end ignore nulls
                    ) over (
                        PARTITION BY market_id
                        ORDER BY odds_minute_epoch
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS expected_carried_close
                FROM {dense}
            ), carried_invalid AS (
                SELECT count(*) AS invalid_rows
                FROM dense_annotated
                WHERE
                    (last_observed_at IS NULL AND (
                        open_odds IS NOT NULL OR high_odds IS NOT NULL
                        OR low_odds IS NOT NULL OR close_odds IS NOT NULL
                        OR avg_odds IS NOT NULL
                    ))
                    OR (
                        NOT is_observed AND close_odds IS NOT NULL
                        AND (
                            open_odds IS DISTINCT FROM close_odds
                            OR high_odds IS DISTINCT FROM close_odds
                            OR low_odds IS DISTINCT FROM close_odds
                            OR avg_odds IS DISTINCT FROM close_odds
                            OR close_odds
                                IS DISTINCT FROM expected_carried_close
                            OR minutes_since_observation IS DISTINCT FROM
                                date_diff(
                                    'minute', last_observed_at, odds_minute_utc
                                )
                        )
                    )
            ), sparse_missing_from_dense AS (
                SELECT count(*) AS invalid_rows
                FROM {observed} AS observed_rows
                LEFT JOIN {dense} AS dense_rows
                  ON dense_rows.market_id = observed_rows.market_id
                 AND dense_rows.odds_minute_epoch = observed_rows.odds_minute_epoch
                WHERE
                    dense_rows.market_id IS NULL OR NOT dense_rows.is_observed
                    OR dense_rows.open_odds
                        IS DISTINCT FROM observed_rows.open_odds
                    OR dense_rows.high_odds
                        IS DISTINCT FROM observed_rows.high_odds
                    OR dense_rows.low_odds
                        IS DISTINCT FROM observed_rows.low_odds
                    OR dense_rows.close_odds
                        IS DISTINCT FROM observed_rows.close_odds
                    OR dense_rows.avg_odds
                        IS DISTINCT FROM observed_rows.avg_odds
                    OR dense_rows.observed_points
                        IS DISTINCT FROM observed_rows.observed_points
            ), dense_missing_from_sparse AS (
                SELECT count(*) AS invalid_rows
                FROM {dense} AS dense_rows
                LEFT JOIN {observed} AS observed_rows
                  ON dense_rows.market_id = observed_rows.market_id
                 AND dense_rows.odds_minute_epoch = observed_rows.odds_minute_epoch
                WHERE dense_rows.is_observed AND observed_rows.market_id IS NULL
            )
            SELECT
                (SELECT count(*) - count(DISTINCT (market_id, odds_minute_epoch))
                    FROM {observed}) AS duplicate_observed,
                (SELECT count(*) - count(DISTINCT (market_id, odds_minute_epoch))
                    FROM {dense}) AS duplicate_dense,
                (SELECT count(*) FROM dense_health
                    WHERE actual_minutes <> expected_minutes) AS invalid_spines,
                (SELECT count(*) FROM {dense}
                    WHERE odds_minute_utc
                        < date_trunc('minute', match_started_at_utc)
                    OR odds_minute_utc
                        > date_trunc('minute', match_finished_at_utc))
                    AS outside_window,
                (SELECT invalid_rows FROM carried_invalid) AS invalid_carry,
                (SELECT invalid_rows FROM sparse_missing_from_dense)
                    AS invalid_observed,
                (SELECT invalid_rows FROM dense_missing_from_sparse)
                    AS missing_sparse_observations
            """
        ).fetchone()
    names = (
        "duplicate_observed",
        "duplicate_dense",
        "invalid_spines",
        "outside_window",
        "invalid_carry",
        "invalid_observed",
        "missing_sparse_observations",
    )
    metadata = dict(zip(names, metrics, strict=True))
    return _blocking_check_result(
        context,
        name="minute_mart_contracts_valid",
        passed=all(value == 0 for value in metrics),
        metadata=metadata,
    )


__all__ = [
    "POLYMARKET_SOCCER_OPS_PIPELINE_PREFLIGHT",
    "POLYMARKET_SOCCER_OPS_PIPELINE_RUNS",
    "POLYMARKET_SOCCER_OPS_PIPELINE_STEP_RUNS",
    "POLYMARKET_SOCCER_OPS_MATCH_RESULT_REGISTRY",
    "POLYMARKET_SOCCER_RAW_EVENT_CATALOG",
    "POLYMARKET_SOCCER_RAW_EVENT_MARKET_MEMBERSHIPS",
    "POLYMARKET_SOCCER_RAW_EVENT_SNAPSHOTS",
    "POLYMARKET_SOCCER_RAW_MATCH_RESULT_MINUTE",
    "POLYMARKET_SOCCER_MART_MATCH_RESULT_MINUTE",
    "polymarket_soccer_ops_match_result_registry",
    "polymarket_soccer_ops_pipeline_preflight",
    "polymarket_soccer_catalog_check",
    "polymarket_soccer_minute_reconciliation_check",
    "polymarket_soccer_minute_mart_check",
    "polymarket_soccer_preflight_check",
    "polymarket_soccer_production_health_check",
    "polymarket_soccer_registry_check",
    "polymarket_soccer_raw_event_catalog",
    "polymarket_soccer_raw_match_result_token_odds_history_minute",
]

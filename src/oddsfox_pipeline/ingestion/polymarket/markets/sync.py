"""
Orchestration logic for syncing markets.

Coordinates fetch, transform, and persistence steps for markets without exposing
implementation details to callers.
"""

import logging
from typing import Any, Callable, Dict

from oddsfox_pipeline.config.settings import POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN
from oddsfox_pipeline.ingestion.polymarket.market_scope import (
    DISCOVERY_MODE_FULL_KEYSET,
    DISCOVERY_MODE_TARGETED,
    DiscoveryMode,
    load_market_scope_config,
    refresh_registry_and_collect_markets_from_events,
    refresh_registry_and_collect_markets_targeted,
    resolve_keyset_tag_slugs,
)
from oddsfox_pipeline.ingestion.polymarket.markets.fetch import build_client
from oddsfox_pipeline.ingestion.polymarket.markets.persistence import (
    market_records_to_dicts,
    prepare_batch_for_db,
)
from oddsfox_pipeline.ingestion.polymarket.markets.transform import (
    process_markets_dataframe,
)
from oddsfox_pipeline.resources.progress_guardrails import ProgressGuardrail
from oddsfox_pipeline.storage.duckdb.connection import ensure_duck_db
from oddsfox_pipeline.storage.duckdb.markets import save_market_tokens_batch
from oddsfox_pipeline.storage.duckdb.metadata import save_sync_run_metrics

logger = logging.getLogger(__name__)


def _resolve_discovery_mode(
    *,
    discovery_mode: DiscoveryMode,
    force_full_discovery: bool,
) -> DiscoveryMode:
    if force_full_discovery:
        return DISCOVERY_MODE_FULL_KEYSET
    return discovery_mode


def collect_market_scope_payload(
    client_factory: Callable[[], object] | None = None,
    *,
    scope_name: str | None = None,
    discovery_mode: DiscoveryMode = DISCOVERY_MODE_FULL_KEYSET,
    force_full_discovery: bool = False,
    max_event_pages: int | None = None,
    max_pages_without_progress: int | None = None,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: list[str] | None = None,
    keyset_volume_min: float | None = POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> Dict[str, Any]:
    """Collect WC2026 Gamma markets once and normalize both market and token rows."""
    effective_mode = _resolve_discovery_mode(
        discovery_mode=discovery_mode,
        force_full_discovery=force_full_discovery,
    )
    factory = client_factory or build_client
    client = factory()
    cfg = load_market_scope_config(scope_name=scope_name)
    effective_keyset_tag_slugs: list[str] | None = None
    if effective_mode == DISCOVERY_MODE_TARGETED:
        registry_summary, raw_markets, collect_meta = (
            refresh_registry_and_collect_markets_targeted(
                client,
                config=cfg,
                progress_callback=progress_callback,
            )
        )
    else:
        effective_keyset_tag_slugs = resolve_keyset_tag_slugs(
            keyset_tag_slugs, config=cfg, client=client
        )
        registry_summary, raw_markets, collect_meta = (
            refresh_registry_and_collect_markets_from_events(
                client,
                config=cfg,
                max_pages=max_event_pages,
                max_pages_without_progress=max_pages_without_progress,
                keyset_closed=keyset_closed,
                keyset_tag_slugs=effective_keyset_tag_slugs or None,
                keyset_volume_min=keyset_volume_min,
                progress_callback=progress_callback,
            )
        )

    market_data = []
    token_data = []
    total_fetched = 0
    if raw_markets:
        df = process_markets_dataframe(raw_markets)
        market_data, token_data = prepare_batch_for_db(df)
        total_fetched = len(market_data)

    collect_meta = {
        **collect_meta,
        "effective_keyset_tag_slugs": effective_keyset_tag_slugs or [],
        "keyset_closed": keyset_closed,
        "keyset_volume_min": keyset_volume_min,
        "markets_collected": total_fetched,
        "token_rows_collected": len(token_data),
    }

    if progress_callback:
        try:
            progress_callback(
                "discovery_complete",
                {"total_fetched": total_fetched, **collect_meta},
            )
        except Exception:
            logger.debug("Ignoring markets progress callback failure", exc_info=True)

    run_summary = {
        "task": "sync_markets",
        "mode": "market_scope_event_first",
        "scope_name": cfg.scope_name,
        "discovery_mode": effective_mode,
        "total_fetched": total_fetched,
        "registry_summary": registry_summary,
        "collect_meta": collect_meta,
        "registry_refreshed": collect_meta.get("registry_refreshed", True),
        "events_pages": collect_meta.get("events_pages", 0),
        "api_requests": collect_meta.get("api_requests", 0),
        "markets_collected": collect_meta.get("markets_collected", total_fetched),
        "token_rows_collected": collect_meta.get("token_rows_collected", 0),
        "effective_keyset_tag_slugs": collect_meta.get(
            "effective_keyset_tag_slugs", []
        ),
        "keyset_closed": keyset_closed,
        "keyset_volume_min": keyset_volume_min,
        "truncated": collect_meta.get("truncated", False),
        "skipped_reason": None,
        "reached_end": not collect_meta.get("truncated", False),
        "early_stopped": collect_meta.get("truncated", False),
        "marked_full_sync_complete": False,
        "error": None,
        "aborted": False,
        "abort_reason": None,
    }
    return {
        "raw_markets": list(raw_markets),
        "market_rows": market_records_to_dicts(market_data),
        "token_rows": list(token_data),
        "registry_summary": registry_summary,
        "collect_meta": collect_meta,
        "run_summary": run_summary,
    }


def _sync_markets_for_scope(
    client: object,
    *,
    scope_name: str | None = None,
    discovery_mode: DiscoveryMode,
    max_event_pages: int | None,
    max_pages_without_progress: int | None,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: list[str] | None = None,
    keyset_volume_min: float | None = POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> Dict[str, Any]:
    collection = collect_market_scope_payload(
        client_factory=lambda: client,
        scope_name=scope_name,
        discovery_mode=discovery_mode,
        max_event_pages=max_event_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=keyset_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_volume_min=keyset_volume_min,
        progress_callback=progress_callback,
    )
    if collection["token_rows"]:
        save_market_tokens_batch(collection["token_rows"])
    return dict(collection["run_summary"])


def sync_markets(
    client_factory: Callable[[], object] | None = None,
    *,
    discovery_mode: DiscoveryMode = DISCOVERY_MODE_FULL_KEYSET,
    force_full_discovery: bool = False,
    scope_name: str | None = None,
    max_event_pages: int | None = None,
    max_pages_without_progress: int | None = None,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: list[str] | None = None,
    keyset_volume_min: float | None = POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
    progress_log_interval_pages: int = 10,
    progress_log_interval_seconds: int = 60,
    no_progress_soft_timeout_seconds: int | None = 900,
    no_progress_hard_timeout_seconds: int | None = 2700,
    progress_poll_seconds: int = 5,
) -> Dict[str, Any]:
    """
    Sync WC2026 Polymarket markets from Gamma to DuckDB.

    Routine runs use tag-filtered full keyset discovery (``discovery_mode='full_keyset'``
    with ``keyset_tag_slugs`` from scope config). Targeted discovery (allowlisted slugs
    plus registry market IDs) remains available via ``discovery_mode='targeted'``.
    """
    effective_mode = _resolve_discovery_mode(
        discovery_mode=discovery_mode,
        force_full_discovery=force_full_discovery,
    )
    logger.info("Starting Polymarket Markets Sync (discovery_mode=%s)", effective_mode)
    guardrail = ProgressGuardrail(
        asset="polymarket_wc2026_raw_markets_snapshot",
        logger=logger,
        progress_log_interval_seconds=progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=no_progress_hard_timeout_seconds,
        work_log_interval=progress_log_interval_pages,
        progress_callback=progress_callback,
    )
    guardrail.record_progress(
        work_increment=0,
        phase="start",
        diagnostics={
            "mode": "market_scope_event_first",
            "discovery_mode": effective_mode,
            "scope_name": scope_name,
            "progress_poll_seconds": progress_poll_seconds,
        },
        force_log=True,
    )

    def _guardrailed_progress_callback(phase: str, diagnostics: dict[str, Any]) -> None:
        work = int(
            diagnostics.get("events_pages")
            or diagnostics.get("api_requests")
            or diagnostics.get("markets_fetched")
            or 0
        )
        guardrail.record_progress(
            work_increment=max(0, work),
            phase=phase,
            diagnostics=diagnostics,
        )
        guardrail.check(phase=phase, diagnostics=diagnostics)
        if progress_callback:
            try:
                progress_callback(phase, diagnostics)
            except Exception:
                logger.debug(
                    "Ignoring markets progress callback failure", exc_info=True
                )

    ensure_duck_db()
    factory = client_factory or build_client
    client = factory()
    run_summary = _sync_markets_for_scope(
        client,
        scope_name=scope_name,
        discovery_mode=effective_mode,
        max_event_pages=max_event_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=keyset_closed,
        keyset_tag_slugs=keyset_tag_slugs,
        keyset_volume_min=keyset_volume_min,
        progress_callback=_guardrailed_progress_callback,
    )
    guardrail_snapshot = guardrail.snapshot()
    run_summary.update(
        {
            "soft_warning_count": guardrail_snapshot.get("soft_warning_count", 0),
            "max_idle_seconds": guardrail_snapshot.get("max_idle_seconds", 0.0),
        }
    )
    save_sync_run_metrics("sync_markets", run_summary, scope_name=scope_name)
    logger.info(
        "Market sync complete. Total fetched this session: %s (discovery_mode=%s)",
        run_summary.get("total_fetched", 0),
        effective_mode,
    )
    return run_summary

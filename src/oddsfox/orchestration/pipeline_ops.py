"""Dagster-free Polymarket sync implementations.

Patch ``oddsfox.orchestration.polymarket_ops`` in tests; this module holds the
real callables re-exported by that facade.
"""

from __future__ import annotations

from threading import Thread
from typing import Any, Callable

from oddsfox.ingestion.polymarket.market_scope import (
    load_market_scope_config,
    refresh_registry_from_events,
    resolve_keyset_tag_slugs,
)
from oddsfox.ingestion.polymarket.markets import (
    backfill_end_dates,
    backfill_event_slugs,
    backfill_market_metadata,
    backfill_slugs,
    backfill_tokens,
    sync_markets,
)
from oddsfox.ingestion.polymarket.markets.fetch import build_client
from oddsfox.ingestion.polymarket.odds import reconcile_odds_ledger, sync_odds
from oddsfox.orchestration.dbt_build import stream_dbt_build
from oddsfox.resources.progress_guardrails import ProgressGuardrail
from oddsfox.storage.duckdb.markets import delete_orphan_market_tokens
from oddsfox.storage.duckdb.observability import (
    delta_dbt_models,
    delta_raw_layer,
    format_dbt_snapshot_log,
    format_raw_snapshot_log,
    snapshot_dbt_models,
    snapshot_raw_layer,
)


def sync_market_scope_registry(
    *,
    scope_name: str | None = None,
    max_event_pages: int | None = None,
    max_pages_without_progress: int | None = None,
    keyset_closed: bool | None = None,
    keyset_tag_slugs: list[str] | None = None,
    keyset_volume_min: float | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    client = build_client()
    cfg = load_market_scope_config(scope_name=scope_name)
    effective_keyset_tag_slugs = resolve_keyset_tag_slugs(
        keyset_tag_slugs, config=cfg, client=client
    )
    return refresh_registry_from_events(
        client,
        config=cfg,
        max_pages=max_event_pages,
        max_pages_without_progress=max_pages_without_progress,
        keyset_closed=keyset_closed,
        keyset_tag_slugs=effective_keyset_tag_slugs or None,
        keyset_volume_min=keyset_volume_min,
        progress_callback=progress_callback,
    )


__all__ = [
    "ProgressGuardrail",
    "Thread",
    "backfill_end_dates",
    "backfill_event_slugs",
    "backfill_market_metadata",
    "backfill_slugs",
    "backfill_tokens",
    "delta_dbt_models",
    "delta_raw_layer",
    "delete_orphan_market_tokens",
    "format_dbt_snapshot_log",
    "format_raw_snapshot_log",
    "reconcile_odds_ledger",
    "snapshot_dbt_models",
    "snapshot_raw_layer",
    "stream_dbt_build",
    "sync_markets",
    "sync_odds",
    "sync_market_scope_registry",
]

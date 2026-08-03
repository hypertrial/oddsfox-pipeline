"""DuckDB market storage helpers."""

from __future__ import annotations

from oddsfox_pipeline.storage.duckdb._market_mutations import (
    delete_orphan_market_tokens,
    mark_market_metadata_unresolved,
    save_end_dates_batch,
    save_event_slugs_batch,
    save_market_tokens_batch,
    save_slugs_batch,
    save_tokens_batch,
)
from oddsfox_pipeline.storage.duckdb._market_queries import (
    _fetch_market_ids,
    _validate_volume_bound,
    _volume_where_clause,
    count_candidate_market_tokens,
    count_due_market_token_exclusions,
    get_markets_missing_any_metadata,
    iter_due_market_tokens,
    iter_markets_with_tokens,
)
from oddsfox_pipeline.storage.duckdb.connection import get_connection

__all__ = [
    "_fetch_market_ids",
    "_validate_volume_bound",
    "_volume_where_clause",
    "count_candidate_market_tokens",
    "count_due_market_token_exclusions",
    "delete_orphan_market_tokens",
    "get_connection",
    "get_markets_missing_any_metadata",
    "iter_due_market_tokens",
    "iter_markets_with_tokens",
    "mark_market_metadata_unresolved",
    "save_end_dates_batch",
    "save_event_slugs_batch",
    "save_market_tokens_batch",
    "save_slugs_batch",
    "save_tokens_batch",
]

"""Shared Polymarket raw-relation column specs for dlt landing and DuckDB DDL."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ColumnDef:
    name: str
    duckdb_type: str
    dlt_type: str
    dlt_nullable: bool = False
    ddl_not_null: bool = False

    def to_dlt(self) -> dict[str, Any]:
        contract: dict[str, Any] = {"data_type": self.dlt_type}
        if self.dlt_nullable:
            contract["nullable"] = True
        return contract

    def to_ddl(self) -> str:
        suffix = " NOT NULL" if self.ddl_not_null else ""
        return f"{self.name} {self.duckdb_type}{suffix}"


def columns_to_dlt(columns: tuple[ColumnDef, ...]) -> dict[str, dict[str, Any]]:
    return {column.name: column.to_dlt() for column in columns}


def columns_to_ddl(
    columns: tuple[ColumnDef, ...],
    *,
    exclude: frozenset[str] = frozenset(),
) -> str:
    return ",\n                ".join(
        column.to_ddl() for column in columns if column.name not in exclude
    )


_MARKET_TOKEN = (
    ColumnDef("market_id", "TEXT", "text"),
    ColumnDef("clobTokenIds", "TEXT", "text"),
    ColumnDef("updated_at", "TIMESTAMP", "timestamp"),
    ColumnDef("row_order", "BIGINT", "bigint"),
)

_ODDS_HISTORY = (
    ColumnDef("clobTokenId", "TEXT", "text"),
    ColumnDef("timestamp", "BIGINT", "bigint"),
    ColumnDef("price", "DOUBLE", "double"),
    ColumnDef("ingested_at", "TIMESTAMP", "timestamp"),
    ColumnDef("row_order", "BIGINT", "bigint"),
)

_MATCH_MINUTE_ODDS_HISTORY = (
    ColumnDef("market_id", "TEXT", "text", ddl_not_null=True),
    ColumnDef("clobTokenId", "TEXT", "text", ddl_not_null=True),
    ColumnDef("timestamp", "BIGINT", "bigint", ddl_not_null=True),
    ColumnDef("price", "DOUBLE", "double", ddl_not_null=True),
    ColumnDef("fidelity_minutes", "INTEGER", "bigint", ddl_not_null=True),
    ColumnDef("window_start_at", "TIMESTAMP", "timestamp", ddl_not_null=True),
    ColumnDef("window_end_at", "TIMESTAMP", "timestamp", ddl_not_null=True),
    ColumnDef("ingested_at", "TIMESTAMP", "timestamp", ddl_not_null=True),
    ColumnDef("row_order", "BIGINT", "bigint"),
)

_FUTURES_MINUTE_ODDS_HISTORY = (
    ColumnDef("market_id", "TEXT", "text", ddl_not_null=True),
    ColumnDef("clobTokenId", "TEXT", "text", ddl_not_null=True),
    ColumnDef("timestamp", "BIGINT", "bigint", ddl_not_null=True),
    ColumnDef("price", "DOUBLE", "double", ddl_not_null=True),
    ColumnDef("fidelity_minutes", "INTEGER", "bigint", ddl_not_null=True),
    ColumnDef("window_start_at", "TIMESTAMP", "timestamp", ddl_not_null=True),
    ColumnDef("window_end_at", "TIMESTAMP", "timestamp", ddl_not_null=True),
    ColumnDef("ingested_at", "TIMESTAMP", "timestamp", ddl_not_null=True),
    ColumnDef("row_order", "BIGINT", "bigint"),
)

_MATCH_ORDER_BOOK_SNAPSHOT = (
    ColumnDef("scan_id", "TEXT", "text", ddl_not_null=True),
    ColumnDef("manifest_sha256", "TEXT", "text", ddl_not_null=True),
    ColumnDef("fifa_match_id", "BIGINT", "bigint", ddl_not_null=True),
    ColumnDef("stage", "TEXT", "text", ddl_not_null=True),
    ColumnDef("home_team", "TEXT", "text", ddl_not_null=True),
    ColumnDef("away_team", "TEXT", "text", ddl_not_null=True),
    ColumnDef("event_id", "TEXT", "text", ddl_not_null=True),
    ColumnDef("event_slug", "TEXT", "text", ddl_not_null=True),
    ColumnDef("market_id", "TEXT", "text", ddl_not_null=True),
    ColumnDef("market_slug", "TEXT", "text", ddl_not_null=True),
    ColumnDef("market_type", "TEXT", "text", ddl_not_null=True),
    ColumnDef("condition_id", "TEXT", "text", ddl_not_null=True),
    ColumnDef("outcome_label", "TEXT", "text", ddl_not_null=True),
    ColumnDef("landscape_role", "TEXT", "text", ddl_not_null=True),
    ColumnDef("clob_token_id", "TEXT", "text", ddl_not_null=True),
    ColumnDef("window_start_ms", "BIGINT", "bigint", ddl_not_null=True),
    ColumnDef("window_end_ms", "BIGINT", "bigint", ddl_not_null=True),
    ColumnDef("snapshot_timestamp_ms", "BIGINT", "bigint", ddl_not_null=True),
    ColumnDef("snapshot_at", "TIMESTAMP", "timestamp", ddl_not_null=True),
    ColumnDef("snapshot_sha256", "TEXT", "text", ddl_not_null=True),
    ColumnDef("provider_sequence", "BIGINT", "bigint", ddl_not_null=True),
    ColumnDef("bids_json", "TEXT", "text", ddl_not_null=True),
    ColumnDef("asks_json", "TEXT", "text", ddl_not_null=True),
    ColumnDef("is_neg_risk", "BOOLEAN", "bool", dlt_nullable=True),
    ColumnDef("last_trade_price", "TEXT", "text", dlt_nullable=True),
    ColumnDef("source_endpoint", "TEXT", "text", ddl_not_null=True),
    ColumnDef("ingested_at", "TIMESTAMP", "timestamp", ddl_not_null=True),
)

_INGESTION_RUN_EVENT = (
    ColumnDef("run_id", "TEXT", "text"),
    ColumnDef("task_name", "TEXT", "text", ddl_not_null=True),
    ColumnDef("recorded_at", "TIMESTAMP", "timestamp", ddl_not_null=True),
    ColumnDef("metrics_json", "TEXT", "text", ddl_not_null=True),
)

_MARKET_SCOPE_REGISTRY = (
    ColumnDef("scope_name", "TEXT", "text"),
    ColumnDef("market_id", "TEXT", "text"),
    ColumnDef("event_slug", "TEXT", "text", dlt_nullable=True),
    ColumnDef("event_id", "TEXT", "text", dlt_nullable=True),
    ColumnDef("source", "TEXT", "text"),
    ColumnDef("refreshed_at", "TIMESTAMP", "timestamp"),
    ColumnDef(
        "event_volume_usd_lifetime_reported",
        "DOUBLE",
        "double",
        dlt_nullable=True,
    ),
    ColumnDef(
        "is_event_volume_eligible",
        "BOOLEAN",
        "bool",
        dlt_nullable=True,
    ),
    ColumnDef("first_eligible_at", "TIMESTAMP", "timestamp", dlt_nullable=True),
    ColumnDef("row_order", "BIGINT", "bigint"),
)

_EVENT_SNAPSHOT = (
    ColumnDef("event_id", "TEXT", "text"),
    ColumnDef("event_slug", "TEXT", "text", dlt_nullable=True),
    ColumnDef("event_title", "TEXT", "text", dlt_nullable=True),
    ColumnDef("event_subtitle", "TEXT", "text", dlt_nullable=True),
    ColumnDef("event_description", "TEXT", "text", dlt_nullable=True),
    ColumnDef("resolution_source", "TEXT", "text", dlt_nullable=True),
    ColumnDef(
        "event_volume_usd_lifetime_reported", "DOUBLE", "double", dlt_nullable=True
    ),
    ColumnDef("volume_24h_usd", "DOUBLE", "double", dlt_nullable=True),
    ColumnDef("volume_1w_usd", "DOUBLE", "double", dlt_nullable=True),
    ColumnDef("volume_1m_usd", "DOUBLE", "double", dlt_nullable=True),
    ColumnDef("volume_1y_usd", "DOUBLE", "double", dlt_nullable=True),
    ColumnDef("liquidity_usd", "DOUBLE", "double", dlt_nullable=True),
    ColumnDef("open_interest_usd", "DOUBLE", "double", dlt_nullable=True),
    ColumnDef("is_active", "BOOLEAN", "bool", dlt_nullable=True),
    ColumnDef("is_closed", "BOOLEAN", "bool", dlt_nullable=True),
    ColumnDef("is_archived", "BOOLEAN", "bool", dlt_nullable=True),
    ColumnDef("created_at", "TIMESTAMP", "timestamp", dlt_nullable=True),
    ColumnDef("source_updated_at", "TIMESTAMP", "timestamp", dlt_nullable=True),
    ColumnDef("start_at", "TIMESTAMP", "timestamp", dlt_nullable=True),
    ColumnDef("end_at", "TIMESTAMP", "timestamp", dlt_nullable=True),
    ColumnDef("closed_at", "TIMESTAMP", "timestamp", dlt_nullable=True),
    ColumnDef("event_start_at", "TIMESTAMP", "timestamp", dlt_nullable=True),
    ColumnDef("finished_at", "TIMESTAMP", "timestamp", dlt_nullable=True),
    ColumnDef("game_id", "TEXT", "text", dlt_nullable=True),
    ColumnDef("parent_event_id", "TEXT", "text", dlt_nullable=True),
    ColumnDef("neg_risk", "BOOLEAN", "bool", dlt_nullable=True),
    ColumnDef("enable_neg_risk", "BOOLEAN", "bool", dlt_nullable=True),
    ColumnDef("neg_risk_market_id", "TEXT", "text", dlt_nullable=True),
    ColumnDef("show_all_outcomes", "BOOLEAN", "bool", dlt_nullable=True),
    ColumnDef("tags_json", "TEXT", "text", ddl_not_null=True),
    ColumnDef("series_slugs_json", "TEXT", "text", ddl_not_null=True),
    ColumnDef("candidate_sources_json", "TEXT", "text", ddl_not_null=True),
    ColumnDef("source_market_count", "BIGINT", "bigint", ddl_not_null=True),
    ColumnDef("observed_at", "TIMESTAMP", "timestamp", ddl_not_null=True),
    ColumnDef("source_endpoint", "TEXT", "text", ddl_not_null=True),
    ColumnDef("row_order", "BIGINT", "bigint"),
)

_EVENT_TAG_SNAPSHOT = (
    ColumnDef("event_id", "TEXT", "text"),
    ColumnDef("tag_key", "TEXT", "text"),
    ColumnDef("tag_id", "TEXT", "text", dlt_nullable=True),
    ColumnDef("tag_slug", "TEXT", "text", dlt_nullable=True),
    ColumnDef("tag_label", "TEXT", "text", dlt_nullable=True),
    ColumnDef("observed_at", "TIMESTAMP", "timestamp", ddl_not_null=True),
    ColumnDef("row_order", "BIGINT", "bigint"),
)

_EVENT_MARKET_SNAPSHOT = (
    ColumnDef("event_id", "TEXT", "text"),
    ColumnDef("market_id", "TEXT", "text"),
    ColumnDef("source_ordinal", "BIGINT", "bigint", ddl_not_null=True),
    ColumnDef("is_enclosing_event", "BOOLEAN", "bool", ddl_not_null=True),
    ColumnDef("observed_at", "TIMESTAMP", "timestamp", ddl_not_null=True),
    ColumnDef("row_order", "BIGINT", "bigint"),
)

_EVENT_CATALOG_MARKET = (
    ColumnDef("id", "TEXT", "text"),
    ColumnDef("question", "TEXT", "text"),
    ColumnDef("category", "TEXT", "text", dlt_nullable=True),
    ColumnDef("description", "TEXT", "text", dlt_nullable=True),
    ColumnDef("market_resolution_source", "TEXT", "text", dlt_nullable=True),
    ColumnDef("outcomes", "TEXT", "text"),
    ColumnDef("volume", "DOUBLE", "double"),
    ColumnDef("active", "BOOLEAN", "bool", dlt_nullable=True),
    ColumnDef("closed", "BOOLEAN", "bool", dlt_nullable=True),
    ColumnDef("created_at", "TIMESTAMP", "timestamp", dlt_nullable=True),
    ColumnDef("scraped_at", "TIMESTAMP", "timestamp"),
    ColumnDef("end_date", "TIMESTAMP", "timestamp", dlt_nullable=True),
    ColumnDef("slug", "TEXT", "text", dlt_nullable=True),
    ColumnDef("event_slug", "TEXT", "text", dlt_nullable=True),
    ColumnDef("event_id", "TEXT", "text", dlt_nullable=True),
    ColumnDef("event_title", "TEXT", "text", dlt_nullable=True),
    ColumnDef("event_start_time", "TIMESTAMP", "timestamp", dlt_nullable=True),
    ColumnDef("event_finished_time", "TIMESTAMP", "timestamp", dlt_nullable=True),
    ColumnDef("event_game_id", "TEXT", "text", dlt_nullable=True),
    ColumnDef("event_ended", "BOOLEAN", "bool", dlt_nullable=True),
    ColumnDef("condition_id", "TEXT", "text", dlt_nullable=True),
    ColumnDef("sports_market_type", "TEXT", "text", dlt_nullable=True),
    ColumnDef("game_start_time", "TIMESTAMP", "timestamp", dlt_nullable=True),
    ColumnDef("group_item_title", "TEXT", "text", dlt_nullable=True),
    ColumnDef("group_item_threshold", "TEXT", "text", dlt_nullable=True),
    ColumnDef("line", "DOUBLE", "double", dlt_nullable=True),
    ColumnDef("tags", "TEXT", "text", dlt_nullable=True),
    ColumnDef("clob_token_ids", "TEXT", "text", dlt_nullable=True),
    ColumnDef("is_resolved", "BOOLEAN", "bool", dlt_nullable=True),
    ColumnDef("winning_outcome", "TEXT", "text", dlt_nullable=True),
    ColumnDef("winning_clob_token_id", "TEXT", "text", dlt_nullable=True),
    ColumnDef("neg_risk_market_id", "TEXT", "text", dlt_nullable=True),
    ColumnDef("neg_risk_request_id", "TEXT", "text", dlt_nullable=True),
    ColumnDef("neg_risk_other", "BOOLEAN", "bool", dlt_nullable=True),
    ColumnDef("row_order", "BIGINT", "bigint"),
)

_EVENT_MARKET_PAYLOAD_SNAPSHOT = (
    ColumnDef("market_id", "TEXT", "text", ddl_not_null=True),
    *(
        column
        for column in _EVENT_CATALOG_MARKET
        if column.name not in {"id", "row_order"}
    ),
    ColumnDef("observed_at", "TIMESTAMP", "timestamp", ddl_not_null=True),
    ColumnDef("row_order", "BIGINT", "bigint"),
)

MARKET_TOKEN_COLUMNS = columns_to_dlt(_MARKET_TOKEN)
ODDS_HISTORY_COLUMNS = columns_to_dlt(_ODDS_HISTORY)
MATCH_MINUTE_ODDS_HISTORY_COLUMNS = columns_to_dlt(_MATCH_MINUTE_ODDS_HISTORY)
FUTURES_MINUTE_ODDS_HISTORY_COLUMNS = columns_to_dlt(_FUTURES_MINUTE_ODDS_HISTORY)
MATCH_ORDER_BOOK_SNAPSHOT_COLUMNS = columns_to_dlt(_MATCH_ORDER_BOOK_SNAPSHOT)
INGESTION_RUN_EVENT_COLUMNS = columns_to_dlt(_INGESTION_RUN_EVENT)
MARKET_SCOPE_REGISTRY_COLUMNS = columns_to_dlt(_MARKET_SCOPE_REGISTRY)
EVENT_SNAPSHOT_COLUMNS = columns_to_dlt(_EVENT_SNAPSHOT)
EVENT_TAG_SNAPSHOT_COLUMNS = columns_to_dlt(_EVENT_TAG_SNAPSHOT)
EVENT_MARKET_SNAPSHOT_COLUMNS = columns_to_dlt(_EVENT_MARKET_SNAPSHOT)
EVENT_CATALOG_MARKET_COLUMNS = columns_to_dlt(_EVENT_CATALOG_MARKET)
EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS = columns_to_dlt(_EVENT_MARKET_PAYLOAD_SNAPSHOT)


def polymarket_raw_ddl_body(
    relation: str,
    *,
    exclude: frozenset[str] = frozenset({"row_order"}),
) -> str:
    return columns_to_ddl(_DDL_COLUMNS_BY_RELATION[relation], exclude=exclude)


_DLT_COLUMNS_BY_RELATION: dict[str, dict[str, dict[str, Any]]] = {
    "market_tokens": MARKET_TOKEN_COLUMNS,
    "odds_history": ODDS_HISTORY_COLUMNS,
    "match_minute_odds_history": MATCH_MINUTE_ODDS_HISTORY_COLUMNS,
    "futures_minute_odds_history": FUTURES_MINUTE_ODDS_HISTORY_COLUMNS,
    "match_order_book_snapshots": MATCH_ORDER_BOOK_SNAPSHOT_COLUMNS,
    "ingestion_run_events": INGESTION_RUN_EVENT_COLUMNS,
    "market_scope_registry": MARKET_SCOPE_REGISTRY_COLUMNS,
    "event_snapshots": EVENT_SNAPSHOT_COLUMNS,
    "event_tag_snapshots": EVENT_TAG_SNAPSHOT_COLUMNS,
    "event_market_snapshots": EVENT_MARKET_SNAPSHOT_COLUMNS,
    "event_market_payload_snapshots": EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS,
}

_DDL_COLUMNS_BY_RELATION: dict[str, tuple[ColumnDef, ...]] = {
    "market_tokens": _MARKET_TOKEN,
    "odds_history": _ODDS_HISTORY,
    "match_minute_odds_history": _MATCH_MINUTE_ODDS_HISTORY,
    "futures_minute_odds_history": _FUTURES_MINUTE_ODDS_HISTORY,
    "match_order_book_snapshots": _MATCH_ORDER_BOOK_SNAPSHOT,
    "ingestion_run_events": _INGESTION_RUN_EVENT,
    "market_scope_registry": _MARKET_SCOPE_REGISTRY,
    "event_snapshots": _EVENT_SNAPSHOT,
    "event_tag_snapshots": _EVENT_TAG_SNAPSHOT,
    "event_market_snapshots": _EVENT_MARKET_SNAPSHOT,
    "event_market_payload_snapshots": _EVENT_MARKET_PAYLOAD_SNAPSHOT,
}


def dlt_column_names(
    relation: str,
    *,
    exclude: frozenset[str] = frozenset({"row_order"}),
) -> frozenset[str]:
    return frozenset(
        name for name in _DLT_COLUMNS_BY_RELATION[relation] if name not in exclude
    )


def ddl_column_names(
    relation: str,
    *,
    exclude: frozenset[str] = frozenset({"row_order"}),
) -> frozenset[str]:
    columns = _DDL_COLUMNS_BY_RELATION[relation]
    return frozenset(column.name for column in columns if column.name not in exclude)


__all__ = [
    "ColumnDef",
    "EVENT_CATALOG_MARKET_COLUMNS",
    "EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS",
    "EVENT_MARKET_SNAPSHOT_COLUMNS",
    "EVENT_SNAPSHOT_COLUMNS",
    "EVENT_TAG_SNAPSHOT_COLUMNS",
    "INGESTION_RUN_EVENT_COLUMNS",
    "MARKET_SCOPE_REGISTRY_COLUMNS",
    "MARKET_TOKEN_COLUMNS",
    "MATCH_MINUTE_ODDS_HISTORY_COLUMNS",
    "FUTURES_MINUTE_ODDS_HISTORY_COLUMNS",
    "MATCH_ORDER_BOOK_SNAPSHOT_COLUMNS",
    "ODDS_HISTORY_COLUMNS",
    "_DDL_COLUMNS_BY_RELATION",
    "_DLT_COLUMNS_BY_RELATION",
    "columns_to_ddl",
    "columns_to_dlt",
    "polymarket_raw_ddl_body",
    "dlt_column_names",
]

"""Polymarket raw/ops DDL, primary keys, and indexes."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone

import duckdb

from oddsfox_pipeline.naming import SCOPE_US_MIDTERMS_2026, SCOPE_WC2026
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    POLYMARKET_CATALOG_RAW_SCHEMA,
    polymarket_ops_tbl,
    polymarket_q,
    polymarket_raw_schema,
    polymarket_raw_tbl,
    polymarket_wc2026_ops_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket_raw_columns import (
    polymarket_raw_ddl_body,
)


def _add_column_if_missing(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    column_name: str,
    column_definition: str,
) -> None:
    """Add a migration column without rewriting an existing defaulted column.

    DuckDB currently reapplies the supplied default to every existing row when
    ``ADD COLUMN IF NOT EXISTS`` names a column that is already present.  Check
    the live schema first so repeat bootstraps remain data preserving.
    """
    columns = {
        str(description[0]).casefold()
        for description in conn.execute(f"SELECT * FROM {table} LIMIT 0").description
    }
    if column_name.casefold() not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_definition}")


logger = logging.getLogger(__name__)

_POLYMARKET_SCOPES = (SCOPE_WC2026, SCOPE_US_MIDTERMS_2026)


def ensure_polymarket_indexes(
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = SCOPE_WC2026,
) -> None:
    """Create indexes for existing Polymarket tables."""
    m = polymarket_raw_tbl(scope_name, "markets")
    tod = polymarket_raw_tbl(scope_name, "token_odds_daily")
    sk = polymarket_ops_tbl(scope_name, "token_sync_skips")
    scope_reg = polymarket_ops_tbl(scope_name, "market_scope_registry")
    raw_schema = polymarket_raw_schema(scope_name)
    index_statements = [
        "CREATE INDEX IF NOT EXISTS "
        f"idx_{scope_name}_market_scope_registry_scope_event_slug ON {scope_reg}"
        "(scope_name, event_slug)",
        "CREATE INDEX IF NOT EXISTS "
        f"idx_{scope_name}_market_scope_registry_market ON {scope_reg}(market_id)",
        f"CREATE INDEX IF NOT EXISTS idx_{scope_name}_token_odds_daily_token ON {tod}(clobTokenId)",
        f"CREATE INDEX IF NOT EXISTS idx_{scope_name}_token_odds_daily_date ON {tod}(odds_date_utc)",
        f"CREATE INDEX IF NOT EXISTS idx_{scope_name}_token_skip_reason ON {sk}(clobTokenId)",
    ]
    if scope_name == SCOPE_WC2026:
        event_snapshots = polymarket_raw_tbl(scope_name, "event_snapshots")
        event_tags = polymarket_raw_tbl(scope_name, "event_tag_snapshots")
        event_markets = polymarket_raw_tbl(scope_name, "event_market_snapshots")
        event_market_payloads = polymarket_raw_tbl(
            scope_name, "event_market_payload_snapshots"
        )
        index_statements.extend(
            [
                "CREATE INDEX IF NOT EXISTS idx_wc2026_event_snapshots_observed "
                f"ON {event_snapshots}(event_id, observed_at)",
                "CREATE INDEX IF NOT EXISTS idx_wc2026_event_tags_slug "
                f"ON {event_tags}(tag_slug, event_id)",
                "CREATE INDEX IF NOT EXISTS idx_wc2026_event_markets_market "
                f"ON {event_markets}(market_id, event_id)",
                "CREATE INDEX IF NOT EXISTS idx_wc2026_event_market_payloads_observed "
                f"ON {event_market_payloads}(market_id, observed_at)",
            ]
        )
    if scope_name == SCOPE_WC2026:
        polygon_fills = polymarket_raw_tbl(scope_name, "polygon_settlement_fills")
        polygon_chunks = polymarket_ops_tbl(
            scope_name, "polygon_settlement_scan_chunks"
        )
        index_statements.extend(
            [
                "CREATE INDEX IF NOT EXISTS idx_wc2026_polygon_fills_prop_time "
                f"ON {polygon_fills}(proposition_id, block_timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_wc2026_polygon_fills_token_time "
                f"ON {polygon_fills}(token_id, block_timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_wc2026_polygon_chunks_scan_status "
                f"ON {polygon_chunks}(scan_id, status, exchange_address, from_block)",
            ]
        )
    markets_exists = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = 'markets'
        """,
        [raw_schema],
    ).fetchone()
    if markets_exists and markets_exists[0]:
        index_statements.extend(
            [
                f"CREATE INDEX IF NOT EXISTS idx_{scope_name}_category ON {m}(category)",
                f"CREATE INDEX IF NOT EXISTS idx_{scope_name}_volume ON {m}(volume)",
                f"CREATE INDEX IF NOT EXISTS idx_{scope_name}_slug ON {m}(slug)",
                f"CREATE INDEX IF NOT EXISTS idx_{scope_name}_event_slug ON {m}(event_slug)",
            ]
        )
    for stmt in index_statements:
        try:
            conn.execute(stmt)
        except Exception as exc:
            logger.warning("Index statement skipped (%s): %s", stmt, exc)


def bootstrap_polymarket_tables(
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = SCOPE_WC2026,
) -> None:
    """CREATE TABLE IF NOT EXISTS for Polymarket core warehouse tables.

    ``{scope}_raw.markets`` is owned by the dlt landing asset, not bootstrap.
    """
    sm = polymarket_ops_tbl(scope_name, "scrape_metadata")
    mt = polymarket_raw_tbl(scope_name, "market_tokens")
    oh = polymarket_raw_tbl(scope_name, "odds_history")
    tod = polymarket_raw_tbl(scope_name, "token_odds_daily")
    led = polymarket_ops_tbl(scope_name, "token_sync_ledger")
    skip = polymarket_ops_tbl(scope_name, "token_sync_skips")
    mmu = polymarket_ops_tbl(scope_name, "market_metadata_unresolved")
    pre = polymarket_ops_tbl(scope_name, "ingestion_run_events")
    srm = polymarket_ops_tbl(scope_name, "sync_run_metrics")
    scope_reg = polymarket_ops_tbl(scope_name, "market_scope_registry")
    event_snapshots = polymarket_raw_tbl(scope_name, "event_snapshots")
    event_tag_snapshots = polymarket_raw_tbl(scope_name, "event_tag_snapshots")
    event_market_snapshots = polymarket_raw_tbl(scope_name, "event_market_snapshots")
    event_market_payload_snapshots = polymarket_raw_tbl(
        scope_name, "event_market_payload_snapshots"
    )
    reviewed_event_membership = polymarket_raw_tbl(
        scope_name, "reviewed_event_membership"
    )
    match_minute_audit = polymarket_ops_tbl(scope_name, "match_minute_odds_fetch_audit")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {sm} (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {mt} (
            market_id TEXT PRIMARY KEY,
            clobTokenIds TEXT,
            updated_at TIMESTAMP
        )
        """
    )
    conn.execute(f"ALTER TABLE {mt} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP")
    if scope_name == SCOPE_WC2026:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {event_snapshots} (
                {polymarket_raw_ddl_body("event_snapshots")},
                PRIMARY KEY (event_id, observed_at)
            )
            """
        )
        _add_column_if_missing(
            conn,
            event_snapshots,
            "candidate_sources_json",
            "candidate_sources_json TEXT DEFAULT '[]'",
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {event_tag_snapshots} (
                {polymarket_raw_ddl_body("event_tag_snapshots")},
                PRIMARY KEY (event_id, tag_key, observed_at)
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {event_market_snapshots} (
                {polymarket_raw_ddl_body("event_market_snapshots")},
                PRIMARY KEY (event_id, market_id, observed_at)
            )
            """
        )
        _add_column_if_missing(
            conn,
            event_market_snapshots,
            "source_ordinal",
            "source_ordinal BIGINT DEFAULT 0",
        )
        _add_column_if_missing(
            conn,
            event_market_snapshots,
            "is_enclosing_event",
            "is_enclosing_event BOOLEAN DEFAULT FALSE",
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {event_market_payload_snapshots} (
                {polymarket_raw_ddl_body("event_market_payload_snapshots")},
                PRIMARY KEY (market_id, observed_at)
            )
            """
        )
        for column_definition in (
            "neg_risk_market_id TEXT",
            "neg_risk_request_id TEXT",
            "neg_risk_other BOOLEAN",
        ):
            conn.execute(
                f"ALTER TABLE {event_market_payload_snapshots} "
                f"ADD COLUMN IF NOT EXISTS {column_definition}"
            )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {reviewed_event_membership} (
                event_id TEXT PRIMARY KEY,
                membership_status TEXT NOT NULL,
                membership_class TEXT NOT NULL,
                tournament_part TEXT NOT NULL,
                membership_basis TEXT NOT NULL,
                reason TEXT NOT NULL,
                reviewed_by TEXT NOT NULL,
                reviewed_at_utc TIMESTAMP NOT NULL,
                source_sha256 TEXT NOT NULL CHECK (
                    regexp_full_match(source_sha256, '[0-9a-f]{{64}}')
                ),
                loaded_at TIMESTAMP NOT NULL
            )
            """
        )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {oh} (
            {polymarket_raw_ddl_body("odds_history")},
            PRIMARY KEY (clobTokenId, timestamp)
        )
        """
    )
    conn.execute(f"ALTER TABLE {oh} ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP")
    if scope_name == SCOPE_WC2026:
        mmoh = polymarket_raw_tbl(scope_name, "match_minute_odds_history")
        order_book_snapshots = polymarket_raw_tbl(
            scope_name, "match_order_book_snapshots"
        )
        match_trades = polymarket_raw_tbl(scope_name, "match_trades")
        match_trade_runs = polymarket_ops_tbl(scope_name, "match_trade_scan_runs")
        match_trade_windows = polymarket_ops_tbl(scope_name, "match_trade_scan_windows")
        order_book_runs = polymarket_ops_tbl(scope_name, "match_order_book_scan_runs")
        order_book_windows = polymarket_ops_tbl(
            scope_name, "match_order_book_scan_windows"
        )
        polygon_fills = polymarket_raw_tbl(scope_name, "polygon_settlement_fills")
        polygon_runs = polymarket_ops_tbl(scope_name, "polygon_settlement_scan_runs")
        polygon_chunks = polymarket_ops_tbl(
            scope_name, "polygon_settlement_scan_chunks"
        )
        polygon_stage = polymarket_ops_tbl(scope_name, "polygon_settlement_fill_stage")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {mmoh} (
                {polymarket_raw_ddl_body("match_minute_odds_history")},
                CHECK (fidelity_minutes = 1),
                PRIMARY KEY (clobTokenId, timestamp)
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {match_minute_audit} (
                fetch_run_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                clobTokenId TEXT NOT NULL,
                fetch_status TEXT NOT NULL CHECK (
                    fetch_status IN ('success', 'empty', 'error', 'cancelled')
                ),
                raw_published BOOLEAN NOT NULL DEFAULT FALSE,
                fidelity_minutes INTEGER NOT NULL CHECK (fidelity_minutes = 1),
                exact_window_start_at TIMESTAMP NOT NULL,
                exact_window_end_at TIMESTAMP NOT NULL,
                request_start_epoch BIGINT NOT NULL,
                request_end_epoch BIGINT NOT NULL,
                source_row_count INTEGER NOT NULL CHECK (source_row_count >= 0),
                in_game_row_count INTEGER NOT NULL CHECK (
                    in_game_row_count >= 0 AND in_game_row_count <= source_row_count
                ),
                in_game_history_sha256 TEXT CHECK (
                    in_game_history_sha256 IS NULL
                    OR regexp_full_match(in_game_history_sha256, '[0-9a-f]{{64}}')
                ),
                source_endpoint TEXT NOT NULL,
                fetch_started_at TIMESTAMP NOT NULL,
                fetch_finished_at TIMESTAMP NOT NULL,
                error_type TEXT,
                error_message TEXT CHECK (
                    error_message IS NULL OR length(error_message) <= 500
                ),
                CHECK (exact_window_start_at <= exact_window_end_at),
                CHECK (request_start_epoch <= request_end_epoch),
                CHECK (fetch_started_at <= fetch_finished_at),
                PRIMARY KEY (fetch_run_id, clobTokenId)
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {order_book_snapshots} (
                {polymarket_raw_ddl_body("match_order_book_snapshots")},
                CHECK (
                    landscape_role IN (
                        'home', 'away', 'home_win', 'draw', 'away_win'
                    )
                ),
                CHECK (provider_sequence >= 0),
                PRIMARY KEY (
                    scan_id, clob_token_id, snapshot_timestamp_ms,
                    snapshot_sha256
                )
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {order_book_runs} (
                scan_id TEXT PRIMARY KEY,
                manifest_version INTEGER NOT NULL,
                manifest_sha256 TEXT NOT NULL CHECK (
                    regexp_full_match(manifest_sha256, '[0-9a-f]{{64}}')
                ),
                target_count INTEGER NOT NULL CHECK (target_count > 0),
                token_count INTEGER NOT NULL CHECK (token_count > 0),
                status TEXT NOT NULL CHECK (
                    status IN ('running', 'paused', 'failed', 'published')
                ),
                raw_published BOOLEAN NOT NULL DEFAULT FALSE,
                lease_owner TEXT,
                lease_expires_at TIMESTAMP,
                api_attempt_count BIGINT NOT NULL DEFAULT 0,
                snapshot_count BIGINT NOT NULL DEFAULT 0,
                aggregate_sha256 TEXT CHECK (
                    aggregate_sha256 IS NULL
                    OR regexp_full_match(aggregate_sha256, '[0-9a-f]{{64}}')
                ),
                started_at TIMESTAMP NOT NULL,
                last_checkpoint_at TIMESTAMP NOT NULL,
                finished_at TIMESTAMP,
                error_type TEXT,
                error_message TEXT CHECK (
                    error_message IS NULL OR length(error_message) <= 500
                )
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {match_trades} (
                scan_id TEXT NOT NULL,
                manifest_sha256 TEXT NOT NULL,
                fifa_match_id BIGINT NOT NULL,
                market_id TEXT NOT NULL,
                clob_token_id TEXT NOT NULL,
                landscape_role TEXT NOT NULL,
                trade_id TEXT NOT NULL,
                trade_timestamp_ms BIGINT NOT NULL,
                event_sequence BIGINT NOT NULL CHECK (event_sequence >= 0),
                price TEXT NOT NULL,
                amount TEXT NOT NULL,
                source_endpoint TEXT NOT NULL,
                ingested_at TIMESTAMP NOT NULL,
                PRIMARY KEY (scan_id, clob_token_id, trade_id)
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {match_trade_runs} (
                scan_id TEXT PRIMARY KEY,
                manifest_sha256 TEXT NOT NULL,
                status TEXT NOT NULL CHECK (
                    status IN ('running', 'paused', 'failed', 'published')
                ),
                trade_count BIGINT NOT NULL DEFAULT 0,
                aggregate_sha256 TEXT,
                started_at TIMESTAMP NOT NULL,
                finished_at TIMESTAMP,
                error_type TEXT,
                error_message TEXT
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {match_trade_windows} (
                scan_id TEXT NOT NULL,
                fifa_match_id BIGINT NOT NULL,
                market_id TEXT NOT NULL,
                clob_token_id TEXT NOT NULL,
                landscape_role TEXT NOT NULL,
                window_start_ms BIGINT NOT NULL,
                window_end_ms BIGINT NOT NULL,
                depth INTEGER NOT NULL CHECK (depth >= 0),
                status TEXT NOT NULL CHECK (
                    status IN ('pending', 'split', 'loaded', 'empty', 'failed')
                ),
                api_attempt_count INTEGER NOT NULL DEFAULT 0,
                trade_count INTEGER NOT NULL DEFAULT 0,
                trade_ids_sha256 TEXT,
                updated_at TIMESTAMP NOT NULL,
                error_type TEXT,
                error_message TEXT,
                PRIMARY KEY (
                    scan_id, clob_token_id, window_start_ms, window_end_ms
                )
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {order_book_windows} (
                scan_id TEXT NOT NULL,
                fifa_match_id BIGINT NOT NULL,
                market_id TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                outcome_label TEXT NOT NULL,
                clob_token_id TEXT NOT NULL,
                window_start_ms BIGINT NOT NULL,
                window_end_ms BIGINT NOT NULL,
                depth INTEGER NOT NULL CHECK (depth >= 0),
                status TEXT NOT NULL CHECK (
                    status IN (
                        'pending', 'split', 'loaded', 'empty', 'failed'
                    )
                ),
                api_attempt_count INTEGER NOT NULL DEFAULT 0,
                snapshot_count INTEGER NOT NULL DEFAULT 0,
                content_sha256 TEXT CHECK (
                    content_sha256 IS NULL
                    OR regexp_full_match(content_sha256, '[0-9a-f]{{64}}')
                ),
                snapshot_hashes_json TEXT NOT NULL DEFAULT '[]' CHECK (
                    json_valid(snapshot_hashes_json)
                ),
                updated_at TIMESTAMP NOT NULL,
                error_type TEXT,
                error_message TEXT CHECK (
                    error_message IS NULL OR length(error_message) <= 500
                ),
                CHECK (window_start_ms <= window_end_ms),
                PRIMARY KEY (
                    scan_id, clob_token_id, window_start_ms, window_end_ms
                )
            )
            """
        )
        polygon_fill_columns = """
                scan_id TEXT NOT NULL,
                chain_id INTEGER NOT NULL CHECK (chain_id = 137),
                exchange_address TEXT NOT NULL CHECK (
                    lower(exchange_address) IN (
                        '0xe111180000d2663c0091e4f400237545b87b996b',
                        '0xe2222d279d744050d28e00520010520000310f59'
                    )
                ),
                chunk_from_block BIGINT NOT NULL CHECK (chunk_from_block >= 0),
                chunk_to_block BIGINT NOT NULL CHECK (chunk_to_block >= 0),
                block_number BIGINT NOT NULL CHECK (block_number >= 0),
                block_hash TEXT NOT NULL CHECK (
                    regexp_full_match(block_hash, '0x[0-9a-f]{64}')
                ),
                block_timestamp TIMESTAMP NOT NULL,
                transaction_hash TEXT NOT NULL CHECK (
                    regexp_full_match(transaction_hash, '0x[0-9a-f]{64}')
                ),
                transaction_index BIGINT NOT NULL CHECK (transaction_index >= 0),
                passive_log_index BIGINT NOT NULL CHECK (passive_log_index >= 0),
                active_log_index BIGINT NOT NULL CHECK (active_log_index >= 0),
                matched_log_index BIGINT NOT NULL CHECK (matched_log_index >= 0),
                normalized_leg_ordinal SMALLINT NOT NULL CHECK (
                    normalized_leg_ordinal IN (0, 1)
                ),
                proposition_id TEXT NOT NULL,
                condition_id TEXT NOT NULL CHECK (
                    regexp_full_match(condition_id, '0x[0-9a-f]{64}')
                ),
                token_id TEXT NOT NULL CHECK (
                    regexp_full_match(token_id, '[1-9][0-9]{0,77}')
                ),
                outcome_side TEXT NOT NULL CHECK (outcome_side IN ('yes', 'no')),
                order_side TEXT NOT NULL CHECK (order_side IN ('BUY', 'SELL')),
                source_token_id TEXT NOT NULL CHECK (
                    regexp_full_match(source_token_id, '[1-9][0-9]{0,77}')
                ),
                source_maker_amount TEXT NOT NULL CHECK (
                    regexp_full_match(source_maker_amount, '[1-9][0-9]{0,77}')
                ),
                source_taker_amount TEXT NOT NULL CHECK (
                    regexp_full_match(source_taker_amount, '[1-9][0-9]{0,77}')
                ),
                share_volume DECIMAL(38, 6) NOT NULL CHECK (
                    share_volume > 0
                    AND share_volume <= 340282366920938.463374
                ),
                gross_collateral_volume DECIMAL(38, 6) NOT NULL CHECK (
                    gross_collateral_volume > 0
                    AND gross_collateral_volume <= 340282366920938.463374
                ),
                price DECIMAL(38, 18) NOT NULL CHECK (price BETWEEN 0 AND 1),
                normalization_kind TEXT NOT NULL CHECK (
                    normalization_kind IN ('complementary', 'mint', 'merge')
                ),
                is_derived BOOLEAN NOT NULL,
                segment_sha256 TEXT NOT NULL CHECK (
                    regexp_full_match(segment_sha256, '[0-9a-f]{64}')
                ),
                decoder_version TEXT NOT NULL,
                ingested_at TIMESTAMP NOT NULL,
                CHECK (chunk_from_block <= block_number),
                CHECK (block_number <= chunk_to_block),
                CHECK (passive_log_index < active_log_index),
                CHECK (active_log_index < matched_log_index),
                CHECK (gross_collateral_volume <= share_volume),
                CHECK (
                    (normalization_kind = 'complementary'
                        AND NOT is_derived AND normalized_leg_ordinal = 0)
                    OR (normalization_kind IN ('mint', 'merge')
                        AND (
                            (NOT is_derived AND normalized_leg_ordinal = 0)
                            OR (is_derived AND normalized_leg_ordinal = 1)
                        ))
                )
        """
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {polygon_fills} (
                {polygon_fill_columns},
                PRIMARY KEY (
                    chain_id, exchange_address, transaction_hash,
                    passive_log_index, normalized_leg_ordinal
                )
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {polygon_stage} (
                {polygon_fill_columns},
                PRIMARY KEY (
                    scan_id, chain_id, exchange_address, transaction_hash,
                    passive_log_index, normalized_leg_ordinal
                )
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {polygon_runs} (
                scan_id TEXT PRIMARY KEY,
                manifest_version TEXT NOT NULL,
                manifest_sha256 TEXT NOT NULL CHECK (
                    regexp_full_match(manifest_sha256, '[0-9a-f]{{64}}')
                ),
                normalizer_version TEXT NOT NULL,
                chain_id INTEGER NOT NULL CHECK (chain_id = 137),
                provider_label TEXT NOT NULL CHECK (
                    length(provider_label) BETWEEN 1 AND 100
                ),
                provider_origin TEXT NOT NULL,
                finalized_head_number BIGINT NOT NULL CHECK (
                    finalized_head_number >= 0
                ),
                finalized_head_hash TEXT NOT NULL CHECK (
                    regexp_full_match(finalized_head_hash, '0x[0-9a-f]{{64}}')
                ),
                target_ranges_json TEXT NOT NULL,
                boundary_blocks_sha256 TEXT NOT NULL CHECK (
                    regexp_full_match(boundary_blocks_sha256, '[0-9a-f]{{64}}')
                ),
                status TEXT NOT NULL CHECK (status IN ('running', 'failed', 'published')),
                raw_published BOOLEAN NOT NULL DEFAULT FALSE,
                verification_status TEXT NOT NULL DEFAULT 'not_requested' CHECK (
                    verification_status IN (
                        'not_requested', 'matched', 'mismatched', 'error'
                    )
                ),
                verification_provider_label TEXT,
                verification_provider_origin TEXT,
                started_at TIMESTAMP NOT NULL,
                finished_at TIMESTAMP,
                published_at TIMESTAMP,
                error_type TEXT,
                error_message TEXT CHECK (
                    error_message IS NULL OR length(error_message) <= 500
                )
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {polygon_chunks} (
                scan_id TEXT NOT NULL,
                exchange_address TEXT NOT NULL CHECK (
                    lower(exchange_address) IN (
                        '0xe111180000d2663c0091e4f400237545b87b996b',
                        '0xe2222d279d744050d28e00520010520000310f59'
                    )
                ),
                from_block BIGINT NOT NULL CHECK (from_block >= 0),
                to_block BIGINT NOT NULL CHECK (to_block >= 0),
                from_block_hash TEXT,
                to_block_hash TEXT,
                status TEXT NOT NULL CHECK (status IN ('success', 'failed')),
                event_count BIGINT NOT NULL CHECK (event_count >= 0),
                scoped_event_count BIGINT NOT NULL CHECK (scoped_event_count >= 0),
                normalized_fill_count BIGINT NOT NULL CHECK (
                    normalized_fill_count >= 0
                ),
                scoped_event_sha256 TEXT,
                duration_ms BIGINT NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
                http_request_count BIGINT NOT NULL DEFAULT 0 CHECK (http_request_count >= 0),
                log_rpc_call_count BIGINT NOT NULL DEFAULT 0 CHECK (log_rpc_call_count >= 0),
                receipt_rpc_call_count BIGINT NOT NULL DEFAULT 0 CHECK (
                    receipt_rpc_call_count >= 0
                ),
                header_rpc_call_count BIGINT NOT NULL DEFAULT 0 CHECK (
                    header_rpc_call_count >= 0
                ),
                discovery_count BIGINT NOT NULL DEFAULT 0 CHECK (discovery_count >= 0),
                eligible_discovery_count BIGINT NOT NULL DEFAULT 0 CHECK (
                    eligible_discovery_count >= 0
                ),
                filtered_discovery_count BIGINT NOT NULL DEFAULT 0 CHECK (
                    filtered_discovery_count >= 0
                ),
                receipt_transaction_count BIGINT NOT NULL DEFAULT 0 CHECK (
                    receipt_transaction_count >= 0
                ),
                receipt_log_count BIGINT NOT NULL DEFAULT 0 CHECK (receipt_log_count >= 0),
                retry_count BIGINT NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
                adaptive_split_count BIGINT NOT NULL DEFAULT 0 CHECK (
                    adaptive_split_count >= 0
                ),
                completed_at TIMESTAMP,
                error_type TEXT,
                error_message TEXT CHECK (
                    error_message IS NULL OR length(error_message) <= 500
                ),
                CHECK (from_block <= to_block),
                CHECK (scoped_event_count <= event_count),
                CHECK (
                    status = 'failed'
                    OR (
                        regexp_full_match(from_block_hash, '0x[0-9a-f]{{64}}')
                        AND regexp_full_match(to_block_hash, '0x[0-9a-f]{{64}}')
                        AND regexp_full_match(scoped_event_sha256, '[0-9a-f]{{64}}')
                    )
                ),
                PRIMARY KEY (scan_id, exchange_address, from_block, to_block)
            )
            """
        )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tod} (
            clobTokenId TEXT,
            odds_date_utc DATE,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE,
            avg_price DOUBLE,
            observed_points BIGINT,
            first_timestamp BIGINT,
            last_timestamp BIGINT,
            refreshed_at TIMESTAMP,
            PRIMARY KEY (clobTokenId, odds_date_utc)
        )
        """
    )
    conn.execute(f"ALTER TABLE {tod} ADD COLUMN IF NOT EXISTS refreshed_at TIMESTAMP")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {led} (
            clobTokenId TEXT PRIMARY KEY,
            last_sync_timestamp BIGINT,
            fully_checked BOOLEAN DEFAULT FALSE,
            last_checked_at TIMESTAMP,
            next_check_at TIMESTAMP,
            empty_run_streak INTEGER DEFAULT 0
        )
        """
    )
    conn.execute(
        f"ALTER TABLE {led} ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMP"
    )
    conn.execute(f"ALTER TABLE {led} ADD COLUMN IF NOT EXISTS next_check_at TIMESTAMP")
    _add_column_if_missing(
        conn,
        led,
        "empty_run_streak",
        "empty_run_streak INTEGER DEFAULT 0",
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {skip} (
            clobTokenId TEXT PRIMARY KEY,
            reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {mmu} (
            market_id TEXT,
            field_name TEXT,
            reason TEXT,
            attempts INTEGER DEFAULT 0,
            last_checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            next_retry_at TIMESTAMP,
            PRIMARY KEY (market_id, field_name)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {pre} (
            {polymarket_raw_ddl_body("ingestion_run_events")},
            PRIMARY KEY (run_id)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {srm} (
            task_name TEXT PRIMARY KEY,
            recorded_at TIMESTAMP NOT NULL,
            metrics_json TEXT NOT NULL,
            history_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {scope_reg} (
            {polymarket_raw_ddl_body("market_scope_registry")},
            PRIMARY KEY (scope_name, market_id)
        )
        """
    )


def bootstrap_all_polymarket_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for scope_name in _POLYMARKET_SCOPES:
        bootstrap_polymarket_tables(conn, scope_name=scope_name)


def ensure_all_polymarket_indexes(conn: duckdb.DuckDBPyConnection) -> None:
    for scope_name in _POLYMARKET_SCOPES:
        ensure_polymarket_indexes(conn, scope_name=scope_name)


_MARKETS_TEST_DDL: str = """
    id TEXT PRIMARY KEY,
    question TEXT,
    category TEXT,
    description TEXT,
    market_resolution_source TEXT,
    outcomes TEXT,
    volume DOUBLE,
    active BOOLEAN,
    closed BOOLEAN,
    created_at TIMESTAMP,
    scraped_at TIMESTAMP,
    end_date TIMESTAMP,
    slug TEXT,
    event_slug TEXT,
    event_id TEXT,
    event_title TEXT,
    event_start_time TIMESTAMP,
    event_finished_time TIMESTAMP,
    event_game_id TEXT,
    event_ended BOOLEAN,
    condition_id TEXT,
    sports_market_type TEXT,
    game_start_time TIMESTAMP,
    group_item_title TEXT,
    group_item_threshold TEXT,
    line DOUBLE,
    tags TEXT,
    clob_token_ids TEXT,
    is_resolved BOOLEAN,
    winning_outcome TEXT,
    winning_clob_token_id TEXT,
    neg_risk_market_id TEXT,
    neg_risk_request_id TEXT,
    neg_risk_other BOOLEAN
"""


def create_test_markets_table(
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = SCOPE_WC2026,
) -> None:
    """Empty markets source fixture for dbt source tests and local CI."""
    m = polymarket_raw_tbl(scope_name, "markets")
    conn.execute(f"CREATE TABLE IF NOT EXISTS {m} ({_MARKETS_TEST_DDL})")


def create_test_catalog_markets_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Empty platform-wide catalog markets fixture for dbt CI builds."""
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{POLYMARKET_CATALOG_RAW_SCHEMA}"')
    m = polymarket_q(POLYMARKET_CATALOG_RAW_SCHEMA, "markets")
    conn.execute(f"CREATE TABLE IF NOT EXISTS {m} ({_MARKETS_TEST_DDL})")


def create_all_scope_test_markets_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for scope_name in _POLYMARKET_SCOPES:
        create_test_markets_table(conn, scope_name=scope_name)
    create_test_catalog_markets_table(conn)


def seed_test_ingestion_run_event(conn: duckdb.DuckDBPyConnection) -> None:
    """Healthy sync_odds fixture for dbt observability tests in local CI."""
    pre = polymarket_wc2026_ops_tbl("ingestion_run_events")
    recorded_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    metrics = {
        "noop": False,
        "duration_seconds": 1.0,
        "tokens": 10,
        "windows": 5,
        "rows": 100,
        "empty": 0,
        "errors": 0,
        "permanent_errors": 0,
        "invalid_tokens": 0,
        "planning": {"plans": 10},
        "planning_context": {
            "market_tokens_distinct_tokens": 100,
            "odds_history_distinct_tokens": 96,
            "history_coverage_vs_market_tokens": 0.96,
        },
    }
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {pre} (
            run_id,
            task_name,
            recorded_at,
            metrics_json
        )
        VALUES (?, ?, ?, ?)
        """,
        [
            str(uuid.uuid4()),
            "sync_odds",
            recorded_at,
            json.dumps(metrics, sort_keys=True),
        ],
    )


__all__ = [
    "bootstrap_all_polymarket_tables",
    "bootstrap_polymarket_tables",
    "create_all_scope_test_markets_tables",
    "create_test_catalog_markets_table",
    "create_test_markets_table",
    "ensure_all_polymarket_indexes",
    "ensure_polymarket_indexes",
    "seed_test_ingestion_run_event",
]

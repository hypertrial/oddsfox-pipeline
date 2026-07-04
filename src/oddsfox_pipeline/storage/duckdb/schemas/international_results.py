"""International-results raw DDL."""

from __future__ import annotations

import duckdb

from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    international_results_wc2026_raw_tbl,
)


def bootstrap_international_results_tables(conn: duckdb.DuckDBPyConnection) -> None:
    mr = international_results_wc2026_raw_tbl("match_results")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {mr} (
            match_id TEXT PRIMARY KEY,
            match_date DATE NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            home_score INTEGER,
            away_score INTEGER,
            tournament TEXT NOT NULL,
            city TEXT,
            country TEXT,
            neutral BOOLEAN,
            match_status TEXT NOT NULL,
            source_url TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            source_row_hash TEXT NOT NULL,
            source_loaded_at TIMESTAMP NOT NULL
        )
        """
    )


__all__ = ["bootstrap_international_results_tables"]

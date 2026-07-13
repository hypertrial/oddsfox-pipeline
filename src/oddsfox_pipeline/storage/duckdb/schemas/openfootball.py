"""OpenFootball raw fixture DDL."""

from __future__ import annotations

import duckdb

from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    openfootball_wc2026_raw_tbl,
)


def bootstrap_openfootball_tables(conn: duckdb.DuckDBPyConnection) -> None:
    fixtures = openfootball_wc2026_raw_tbl("knockout_fixtures")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {fixtures} (
            fifa_match_id INTEGER PRIMARY KEY,
            stage_key TEXT NOT NULL,
            stage_rank SMALLINT NOT NULL,
            kickoff_at_utc TIMESTAMP NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            venue TEXT NOT NULL,
            match_status TEXT NOT NULL,
            source_url TEXT NOT NULL,
            source_line_number INTEGER NOT NULL,
            source_line_hash TEXT NOT NULL,
            source_loaded_at TIMESTAMP NOT NULL
        )
        """
    )


__all__ = ["bootstrap_openfootball_tables"]

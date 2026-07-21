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
            source_revision TEXT NOT NULL CHECK (
                regexp_full_match(source_revision, '[0-9a-f]{{40}}')
                AND position(source_revision IN source_url) > 0
            ),
            source_payload_sha256 TEXT NOT NULL CHECK (
                regexp_full_match(source_payload_sha256, '[0-9a-f]{{64}}')
            ),
            source_loaded_at TIMESTAMP NOT NULL
        )
        """
    )
    matches = international_results_wc2026_raw_tbl("historical_matches")
    shootouts = international_results_wc2026_raw_tbl("historical_shootouts")
    goalscorers = international_results_wc2026_raw_tbl("historical_goalscorers")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {matches} (
            match_id TEXT PRIMARY KEY,
            match_date DATE NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            home_score INTEGER,
            away_score INTEGER,
            tournament TEXT NOT NULL,
            city TEXT,
            country TEXT,
            is_neutral_site BOOLEAN NOT NULL,
            source_url TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            source_row_hash TEXT NOT NULL,
            source_loaded_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {shootouts} (
            match_id TEXT PRIMARY KEY,
            shootout_winner TEXT,
            shootout_first_shooter TEXT,
            source_url TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            source_loaded_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {goalscorers} (
            goal_event_id TEXT PRIMARY KEY,
            match_id TEXT NOT NULL,
            scoring_team TEXT,
            scorer TEXT,
            goal_minute TEXT,
            is_own_goal BOOLEAN NOT NULL,
            is_penalty_goal BOOLEAN NOT NULL,
            source_url TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            source_loaded_at TIMESTAMPTZ NOT NULL
        )
        """
    )


__all__ = ["bootstrap_international_results_tables"]

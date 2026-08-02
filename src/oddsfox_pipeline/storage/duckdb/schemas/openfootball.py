"""OpenFootball raw fixture DDL."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb

from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    OPENFOOTBALL_WC2026_RAW_SCHEMA,
    openfootball_wc2026_raw_tbl,
)

_STAGE_BY_MATCH_ID = {
    **{match_id: ("group_stage", 0) for match_id in range(1, 73)},
    **{match_id: ("round_of_32", 1) for match_id in range(73, 89)},
    **{match_id: ("round_of_16", 2) for match_id in range(89, 97)},
    **{match_id: ("quarterfinal", 3) for match_id in range(97, 101)},
    **{match_id: ("semifinal", 4) for match_id in range(101, 103)},
    103: ("third_place", 0),
    104: ("final", 5),
}


def bootstrap_openfootball_tables(conn: duckdb.DuckDBPyConnection) -> None:
    fixtures = openfootball_wc2026_raw_tbl("schedule_fixtures")
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{OPENFOOTBALL_WC2026_RAW_SCHEMA}"')
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {fixtures} (
            fifa_match_id INTEGER PRIMARY KEY,
            stage_key TEXT NOT NULL,
            stage_rank SMALLINT NOT NULL,
            group_label TEXT,
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
    conn.execute(f"ALTER TABLE {fixtures} ADD COLUMN IF NOT EXISTS group_label TEXT")


def seed_test_openfootball_schedule_fixtures(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Complete FIFA 1..104 schedule fixture for disposable dbt CI builds."""
    bootstrap_openfootball_tables(conn)
    fixtures = openfootball_wc2026_raw_tbl("schedule_fixtures")
    loaded_at = datetime(2026, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None)
    kickoff = datetime(2026, 6, 11, 16, 0, 0)
    rows = []
    for match_id in range(1, 105):
        stage_key, stage_rank = _STAGE_BY_MATCH_ID[match_id]
        group_label = "ABCDEFGHIJKL"[(match_id - 1) // 6] if match_id <= 72 else None
        rows.append(
            (
                match_id,
                stage_key,
                stage_rank,
                group_label,
                kickoff,
                f"Home {match_id}",
                f"Away {match_id}",
                f"Venue {match_id}",
                "scheduled",
                "https://example.com/openfootball/schedule",
                match_id,
                f"{'0' * 63}{match_id:x}"[-64:],
                loaded_at,
            )
        )
    conn.execute(f"DELETE FROM {fixtures}")
    conn.executemany(
        f"""
        INSERT INTO {fixtures} (
            fifa_match_id,
            stage_key,
            stage_rank,
            group_label,
            kickoff_at_utc,
            home_team,
            away_team,
            venue,
            match_status,
            source_url,
            source_line_number,
            source_line_hash,
            source_loaded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


__all__ = [
    "bootstrap_openfootball_tables",
    "seed_test_openfootball_schedule_fixtures",
]

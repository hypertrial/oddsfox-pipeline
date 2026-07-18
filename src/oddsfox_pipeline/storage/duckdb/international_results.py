"""Persistence helpers for international-results match data."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import duckdb

from oddsfox_pipeline.storage.duckdb.connection import _use_conn
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    international_results_wc2026_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.international_results import (
    bootstrap_international_results_tables,
)

_MATCH_RESULT_COLUMNS = (
    "match_id",
    "match_date",
    "home_team",
    "away_team",
    "home_score",
    "away_score",
    "tournament",
    "city",
    "country",
    "neutral",
    "match_status",
    "source_url",
    "source_row_number",
    "source_row_hash",
    "source_loaded_at",
)
_HISTORICAL_MATCH_COLUMNS = (
    "match_id",
    "match_date",
    "home_team",
    "away_team",
    "home_score",
    "away_score",
    "tournament",
    "city",
    "country",
    "is_neutral_site",
    "source_url",
    "source_row_number",
    "source_row_hash",
    "source_loaded_at",
)
_HISTORICAL_SHOOTOUT_COLUMNS = (
    "match_id",
    "shootout_winner",
    "shootout_first_shooter",
    "source_url",
    "source_row_number",
    "source_loaded_at",
)
_HISTORICAL_GOALSCORER_COLUMNS = (
    "goal_event_id",
    "match_id",
    "scoring_team",
    "scorer",
    "goal_minute",
    "is_own_goal",
    "is_penalty_goal",
    "source_url",
    "source_row_number",
    "source_loaded_at",
)


def replace_wc2026_match_results(
    rows: Sequence[Mapping[str, object]],
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, int]:
    """Replace the WC2026 raw match-results slice."""
    table = international_results_wc2026_raw_tbl("match_results")
    with _use_conn(conn) as active:
        bootstrap_international_results_tables(active)
        active.execute("BEGIN TRANSACTION")
        try:
            deleted = active.execute(f"DELETE FROM {table}").fetchone()[0]
            if rows:
                placeholders = ", ".join(["?"] * len(_MATCH_RESULT_COLUMNS))
                active.executemany(
                    f"""
                    INSERT INTO {table} ({", ".join(_MATCH_RESULT_COLUMNS)})
                    VALUES ({placeholders})
                    """,
                    [
                        tuple(row.get(column) for column in _MATCH_RESULT_COLUMNS)
                        for row in rows
                    ],
                )
            active.execute("COMMIT")
        except Exception:
            active.execute("ROLLBACK")
            raise
    return {"deleted_rows": int(deleted), "inserted_rows": len(rows)}


def _replace_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    table: str,
    columns: tuple[str, ...],
    rows: Sequence[Mapping[str, object]],
) -> int:
    deleted = conn.execute(f"DELETE FROM {table}").fetchone()[0]
    if rows:
        placeholders = ", ".join(["?"] * len(columns))
        conn.executemany(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            [tuple(row.get(column) for column in columns) for row in rows],
        )
    return int(deleted)


def replace_historical_international_results(
    *,
    matches: Sequence[Mapping[str, object]],
    shootouts: Sequence[Mapping[str, object]],
    goalscorers: Sequence[Mapping[str, object]],
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, int]:
    """Atomically replace the audited 2006+ public international-results snapshot."""
    match_table = international_results_wc2026_raw_tbl("historical_matches")
    shootout_table = international_results_wc2026_raw_tbl("historical_shootouts")
    goalscorer_table = international_results_wc2026_raw_tbl("historical_goalscorers")
    with _use_conn(conn) as active:
        bootstrap_international_results_tables(active)
        active.execute("BEGIN TRANSACTION")
        try:
            deleted_goals = _replace_rows(
                active,
                table=goalscorer_table,
                columns=_HISTORICAL_GOALSCORER_COLUMNS,
                rows=goalscorers,
            )
            deleted_shootouts = _replace_rows(
                active,
                table=shootout_table,
                columns=_HISTORICAL_SHOOTOUT_COLUMNS,
                rows=shootouts,
            )
            deleted_matches = _replace_rows(
                active,
                table=match_table,
                columns=_HISTORICAL_MATCH_COLUMNS,
                rows=matches,
            )
            active.execute("COMMIT")
        except Exception:
            active.execute("ROLLBACK")
            raise
    return {
        "deleted_matches": deleted_matches,
        "deleted_shootouts": deleted_shootouts,
        "deleted_goalscorers": deleted_goals,
        "inserted_matches": len(matches),
        "inserted_shootouts": len(shootouts),
        "inserted_goalscorers": len(goalscorers),
    }


__all__ = [
    "replace_historical_international_results",
    "replace_wc2026_match_results",
]

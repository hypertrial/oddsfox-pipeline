"""Tests for the WC2026 match-minute Parquet export."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import duckdb


def test_export_and_summarize_match_minute_odds(tmp_path: Path) -> None:
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    from export_polymarket_wc2026_match_minute_odds import (
        export_polymarket_wc2026_match_minute_odds,
        summarize_parquet,
    )

    output = tmp_path / "match_minute.parquet"
    with duckdb.connect() as conn:
        conn.execute("create schema polymarket_wc2026_marts")
        conn.execute(
            """
            create table polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds as
            select * from (values
                (timestamp '2026-06-12 18:00:00', 1, 'm1', true, true, true),
                (timestamp '2026-06-12 18:01:00', 1, 'm1', true, false, false),
                (timestamp '2026-07-19 22:00:00', 104, 'm2', false, true, false)
            ) as rows (
                odds_minute_utc, fifa_match_id, market_id,
                yes_observed, no_observed, minute_complete
            )
            """
        )

        export_polymarket_wc2026_match_minute_odds(conn, output)
        summary = summarize_parquet(conn, output)

    assert summary == {
        "rows": 3,
        "fifa_matches": 2,
        "markets": 2,
        "first_minute_utc": datetime(2026, 6, 12, 18),
        "last_minute_utc": datetime(2026, 7, 19, 22),
        "yes_observed_minutes": 2,
        "no_observed_minutes": 2,
        "complete_minutes": 1,
        "minute_completeness_pct": 33.33,
        "file_bytes": output.stat().st_size,
    }

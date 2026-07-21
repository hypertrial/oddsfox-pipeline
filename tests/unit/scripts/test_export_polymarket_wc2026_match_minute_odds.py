"""Tests for the WC2026 match-minute Parquet export."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pytest


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
            select
                timestamp '2026-06-11 19:00:00' + market_number * interval '1 minute'
                    as odds_minute_utc,
                1781204400 + market_number * 60 as odds_minute_epoch,
                0::bigint as elapsed_window_minute,
                case
                    when market_number <= 216 then ((market_number - 1) // 3) + 1
                    else market_number - 144
                end as fifa_match_id,
                'market-' || market_number as market_id,
                'yes-' || market_number as yes_clob_token_id,
                'no-' || market_number as no_clob_token_id,
                'result-' || case
                    when market_number <= 216 then ((market_number - 1) // 3) + 1
                    else market_number - 144
                end as international_results_match_id,
                case
                    when market_number <= 216 then 'moneyline'
                    else 'soccer_team_to_advance'
                end as sports_market_type,
                case
                    when market_number <= 216 and (market_number - 1) % 3 = 0
                        then 'home_win'
                    when market_number <= 216 and (market_number - 1) % 3 = 1
                        then 'draw'
                    when market_number <= 216 then 'away_win'
                    when market_number <= 246 then 'home_advances'
                    when market_number = 247 then 'home_win_third_place'
                    else 'home_wins_final'
                end as proposition_type,
                true as yes_observed,
                true as no_observed,
                true as minute_complete,
                false as is_game_start_minute,
                false as is_game_finish_minute,
                'complete' as minute_status,
                market_number = 1 as pair_price_anomaly,
                case when market_number = 1 then 0.1 else 0.0 end
                    as yes_no_close_deviation,
                timestamp '2026-06-11 18:55:00' as scheduled_kickoff_at_utc,
                timestamp '2026-06-11 19:00:00'
                    + market_number * interval '1 minute' as game_started_at_utc,
                timestamp '2026-06-11 19:00:00'
                    + market_number * interval '1 minute' as game_finished_at_utc,
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                    as results_source_revision,
                'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
                    as results_source_payload_sha256,
                timestamp '2026-07-21 10:00:00' as results_source_loaded_at
            from range(1, 249) as markets(market_number)
            """
        )

        summary = export_polymarket_wc2026_match_minute_odds(conn, output)
        assert summary == summarize_parquet(conn, output)

        previous_export = output.read_bytes()
        conn.execute(
            """
            update polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds
            set elapsed_window_minute = 1
            where market_id = 'market-1'
            """
        )
        with pytest.raises(ValueError, match="elapsed_axis_issue_markets"):
            export_polymarket_wc2026_match_minute_odds(conn, output)
        assert output.read_bytes() == previous_export

    assert summary["rows"] == 248
    assert summary["grain_rows"] == 248
    assert summary["fifa_matches"] == 104
    assert summary["markets"] == 248
    assert summary["tokens"] == 496
    assert summary["min_elapsed_window_minute"] == 0
    assert summary["max_elapsed_window_minute"] == 0
    assert summary["games_over_120_elapsed_minutes"] == 0
    assert summary["elapsed_axis_issue_markets"] == 0
    assert summary["group_moneyline_markets"] == 216
    assert summary["knockout_markets"] == 32
    assert summary["proposition_inventory"]["home_advances"] == 30
    assert summary["first_minute_utc"] == datetime(2026, 6, 11, 19, 1)
    assert summary["complete_minutes"] == 248
    assert summary["minute_completeness_pct"] == 100.0
    assert summary["non_finish_completeness_pct"] == 100.0
    assert summary["pair_price_anomaly_minutes"] == 1
    assert len(summary["sha256"]) == 64
    assert summary["file_bytes"] == output.stat().st_size


def test_invalid_export_preserves_previous_file(tmp_path: Path) -> None:
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    from export_polymarket_wc2026_match_minute_odds import (
        export_polymarket_wc2026_match_minute_odds,
    )

    output = tmp_path / "match_minute.parquet"
    output.write_bytes(b"previous-good-export")
    with duckdb.connect() as conn:
        conn.execute("create schema polymarket_wc2026_marts")
        conn.execute(
            """
            create table polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds as
            select
                timestamp '2026-06-11 19:00:00' as odds_minute_utc,
                1::bigint as odds_minute_epoch,
                0::bigint as elapsed_window_minute,
                1 as fifa_match_id,
                'market-1' as market_id,
                'yes-1' as yes_clob_token_id,
                'no-1' as no_clob_token_id,
                'result-1' as international_results_match_id,
                'moneyline' as sports_market_type,
                'home_win' as proposition_type,
                true as yes_observed,
                true as no_observed,
                true as minute_complete,
                false as is_game_start_minute,
                false as is_game_finish_minute,
                'complete' as minute_status,
                false as pair_price_anomaly,
                0.0 as yes_no_close_deviation,
                timestamp '2026-06-11 18:55:00' as scheduled_kickoff_at_utc,
                timestamp '2026-06-11 19:00:00' as game_started_at_utc,
                timestamp '2026-06-11 20:40:00' as game_finished_at_utc,
                'bad' as results_source_revision,
                'bad' as results_source_payload_sha256,
                timestamp '2026-07-21 10:00:00' as results_source_loaded_at
            """
        )
        with pytest.raises(ValueError, match="Invalid match-minute mart"):
            export_polymarket_wc2026_match_minute_odds(conn, output)

    assert output.read_bytes() == b"previous-good-export"

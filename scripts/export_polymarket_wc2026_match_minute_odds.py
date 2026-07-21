#!/usr/bin/env python3
"""Export and summarize the WC2026 match-minute odds mart as Parquet."""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path
from typing import Any, Final
from uuid import uuid4

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path
from _export_common import mart_exists as _mart_exists
from _export_common import qualified_mart_name

REPO_ROOT: Final[Path] = ensure_src_on_path()
from oddsfox_pipeline.storage.duckdb.schemas.dbt_schemas import (  # noqa: E402
    POLYMARKET_WC2026_MARTS_SCHEMA,
)

MART_SCHEMA: Final = POLYMARKET_WC2026_MARTS_SCHEMA
MART_NAME: Final = "polymarket_wc2026_match_minute_odds"
DEFAULT_OUTPUT: Final = (
    REPO_ROOT / "artifacts" / "polymarket_wc2026_exports" / f"{MART_NAME}.parquet"
)


def mart_exists(conn: duckdb.DuckDBPyConnection) -> bool:
    return _mart_exists(conn, MART_SCHEMA, MART_NAME)


def export_polymarket_wc2026_match_minute_odds(
    conn: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> dict[str, Any]:
    if not mart_exists(conn):
        raise LookupError(f"Missing {MART_SCHEMA}.{MART_NAME}. Run dbt build first.")
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(
        f".{output_path.name}.{uuid4().hex}.tmp.parquet"
    )
    relation = qualified_mart_name(MART_SCHEMA, MART_NAME)
    try:
        conn.execute(
            f"copy (select * from {relation}) to ? (format parquet)",
            [str(temporary_path)],
        )
        summary = summarize_parquet(conn, temporary_path)
        validate_summary(summary)
        temporary_path.replace(output_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    summary["file_bytes"] = output_path.stat().st_size
    return summary


def summarize_parquet(
    conn: duckdb.DuckDBPyConnection,
    parquet_path: Path,
) -> dict[str, Any]:
    result = conn.execute(
        """
        with mart as (
            select * from read_parquet(?)
        ),
        tokens as (
            select yes_clob_token_id as clob_token_id from mart
            union
            select no_clob_token_id from mart
        ),
        elapsed_axis_by_market as (
            select
                market_id,
                count(*) as row_count,
                count(distinct elapsed_window_minute) as distinct_minute_count,
                min(elapsed_window_minute) as first_elapsed_minute,
                max(elapsed_window_minute) as final_elapsed_minute,
                max(
                    date_diff(
                        'minute',
                        date_trunc('minute', game_started_at_utc),
                        date_trunc('minute', game_finished_at_utc)
                    )
                ) as expected_final_elapsed_minute,
                count(*) filter (
                    where
                        elapsed_window_minute is null
                        or elapsed_window_minute < 0
                        or elapsed_window_minute <> date_diff(
                            'minute',
                            date_trunc('minute', game_started_at_utc),
                            odds_minute_utc
                        )
                ) as invalid_row_count
            from mart
            group by market_id
        ),
        games as (
            select
                fifa_match_id,
                max(elapsed_window_minute) as final_elapsed_minute
            from mart
            group by fifa_match_id
        )
        select
            count(*) as rows,
            count(distinct (odds_minute_epoch, market_id)) as grain_rows,
            count(distinct fifa_match_id) as fifa_matches,
            min(fifa_match_id) as first_fifa_match_id,
            max(fifa_match_id) as last_fifa_match_id,
            count(distinct market_id) as markets,
            (select count(clob_token_id) from tokens) as tokens,
            min(elapsed_window_minute) as min_elapsed_window_minute,
            max(elapsed_window_minute) as max_elapsed_window_minute,
            (
                select count(*) from games where final_elapsed_minute > 120
            ) as games_over_120_elapsed_minutes,
            (
                select count(*)
                from elapsed_axis_by_market
                where
                    invalid_row_count > 0
                    or first_elapsed_minute <> 0
                    or final_elapsed_minute <> row_count - 1
                    or distinct_minute_count <> row_count
                    or final_elapsed_minute <> expected_final_elapsed_minute
            ) as elapsed_axis_issue_markets,
            count(*) filter (
                where yes_clob_token_id is null or no_clob_token_id is null
            ) as missing_token_identity_rows,
            count(distinct market_id) filter (
                where sports_market_type = 'moneyline'
            ) as group_moneyline_markets,
            count(distinct market_id) filter (
                where sports_market_type = 'soccer_team_to_advance'
            ) as knockout_markets,
            min(odds_minute_utc) as first_minute_utc,
            max(odds_minute_utc) as last_minute_utc,
            count(*) filter (where yes_observed) as yes_observed_minutes,
            count(*) filter (where no_observed) as no_observed_minutes,
            count(*) filter (where minute_complete) as complete_minutes,
            count(*) filter (where not is_game_finish_minute)
                as non_finish_minutes,
            count(*) filter (
                where not is_game_finish_minute and minute_complete
            ) as non_finish_complete_minutes,
            count(*) filter (where minute_status = 'interior_incomplete')
                as interior_incomplete_minutes,
            count(*) filter (
                where is_game_start_minute and not minute_complete
            ) as start_boundary_incomplete_minutes,
            count(*) filter (
                where is_game_finish_minute and not minute_complete
            ) as finish_boundary_incomplete_minutes,
            count(*) filter (where pair_price_anomaly)
                as pair_price_anomaly_minutes,
            max(yes_no_close_deviation) as max_pair_price_deviation,
            quantile_cont(yes_no_close_deviation, 0.95)
                as p95_pair_price_deviation,
            count(distinct international_results_match_id)
                as international_results_matches,
            count(*) filter (where international_results_match_id is null)
                as missing_international_results_match_rows,
            count(distinct results_source_revision) as results_source_revisions,
            min(results_source_revision) as results_source_revision,
            count(distinct results_source_payload_sha256) as results_payload_hashes,
            min(results_source_payload_sha256) as results_source_payload_sha256,
            count(*) filter (where results_source_loaded_at is null)
                as missing_results_source_loaded_at_rows,
            min(results_source_loaded_at) as results_source_loaded_at,
            count(*) filter (
                where
                    scheduled_kickoff_at_utc is null
                    or game_started_at_utc is null
                    or game_finished_at_utc is null
            ) as missing_timing_rows
        from mart
        """,
        [str(parquet_path.resolve())],
    )
    row = result.fetchone()
    if row is None:
        raise RuntimeError(f"Could not summarize {parquet_path}")
    summary = dict(zip((column[0] for column in result.description), row, strict=True))
    integer_fields = (
        "rows",
        "grain_rows",
        "fifa_matches",
        "first_fifa_match_id",
        "last_fifa_match_id",
        "markets",
        "tokens",
        "min_elapsed_window_minute",
        "max_elapsed_window_minute",
        "games_over_120_elapsed_minutes",
        "elapsed_axis_issue_markets",
        "missing_token_identity_rows",
        "group_moneyline_markets",
        "knockout_markets",
        "yes_observed_minutes",
        "no_observed_minutes",
        "complete_minutes",
        "non_finish_minutes",
        "non_finish_complete_minutes",
        "interior_incomplete_minutes",
        "start_boundary_incomplete_minutes",
        "finish_boundary_incomplete_minutes",
        "pair_price_anomaly_minutes",
        "international_results_matches",
        "missing_international_results_match_rows",
        "results_source_revisions",
        "results_payload_hashes",
        "missing_results_source_loaded_at_rows",
        "missing_timing_rows",
    )
    summary.update({key: int(summary[key]) for key in integer_fields})
    proposition_inventory = conn.execute(
        """
        select proposition_type, count(distinct market_id)
        from read_parquet(?)
        group by proposition_type
        order by proposition_type
        """,
        [str(parquet_path.resolve())],
    ).fetchall()
    summary["proposition_inventory"] = dict(proposition_inventory)
    rows = summary["rows"]
    non_finish_minutes = summary["non_finish_minutes"]
    summary.update(
        {
            "minute_completeness_pct": round(
                100 * summary["complete_minutes"] / rows, 2
            )
            if rows
            else 0.0,
            "non_finish_completeness_pct": round(
                100 * summary["non_finish_complete_minutes"] / non_finish_minutes,
                2,
            )
            if non_finish_minutes
            else 0.0,
            "file_bytes": parquet_path.stat().st_size,
            "sha256": hashlib.sha256(parquet_path.read_bytes()).hexdigest(),
        }
    )
    return summary


def validate_summary(summary: dict[str, Any]) -> None:
    expected = {
        "fifa_matches": 104,
        "first_fifa_match_id": 1,
        "last_fifa_match_id": 104,
        "markets": 248,
        "tokens": 496,
        "min_elapsed_window_minute": 0,
        "elapsed_axis_issue_markets": 0,
        "missing_token_identity_rows": 0,
        "group_moneyline_markets": 216,
        "knockout_markets": 32,
        "international_results_matches": 104,
        "missing_international_results_match_rows": 0,
        "results_source_revisions": 1,
        "results_payload_hashes": 1,
        "missing_results_source_loaded_at_rows": 0,
        "missing_timing_rows": 0,
    }
    failures = [
        f"{key}={summary.get(key)!r}, expected {value}"
        for key, value in expected.items()
        if summary.get(key) != value
    ]
    if summary.get("grain_rows") != summary.get("rows"):
        failures.append("duplicate (odds_minute_epoch, market_id) grain")
    expected_propositions = {
        "away_win": 72,
        "draw": 72,
        "home_advances": 30,
        "home_win": 72,
        "home_win_third_place": 1,
        "home_wins_final": 1,
    }
    if summary.get("proposition_inventory") != expected_propositions:
        failures.append(
            f"invalid proposition_inventory={summary.get('proposition_inventory')!r}"
        )
    for key in ("results_source_revision", "results_source_payload_sha256"):
        value = str(summary.get(key) or "")
        length = 40 if key.endswith("revision") else 64
        if len(value) != length or any(
            char not in "0123456789abcdef" for char in value
        ):
            failures.append(f"invalid {key}")
    if failures:
        raise ValueError("Invalid match-minute mart: " + "; ".join(failures))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb-path", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--read-only", action=argparse.BooleanOptionalAction, default=True
    )
    args = parser.parse_args(argv)

    from oddsfox_pipeline.config import settings
    from oddsfox_pipeline.storage.duckdb.connection import open_duckdb_connection

    duckdb_path = Path(args.duckdb_path or settings.DUCKDB_PATH).resolve()
    output_path = args.output.resolve()
    conn = open_duckdb_connection(duckdb_path, read_only=args.read_only)
    try:
        summary = export_polymarket_wc2026_match_minute_odds(conn, output_path)
    except (duckdb.Error, LookupError, RuntimeError, ValueError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    finally:
        conn.close()

    print(f"Exported {summary['rows']:,} rows to {output_path}")
    for key, value in summary.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

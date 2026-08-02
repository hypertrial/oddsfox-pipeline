"""Incremental/full-refresh equivalence for every incremental odds model."""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pytest
from tests.integration.conftest import write_dbt_profile

REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_ROOT = REPO_ROOT / "dbt"


@dataclass(frozen=True)
class IncrementalCase:
    model: str
    relation: str
    kind: str
    key_column: str
    retention: bool


CASES = (
    IncrementalCase(
        "int_polymarket_wc2026_token_hourly_odds",
        '"polymarket_wc2026_intermediate"."int_polymarket_wc2026_token_hourly_odds"',
        "polymarket_wc2026_token",
        "clob_token_id",
        True,
    ),
    IncrementalCase(
        "int_polymarket_wc2026_match_hourly_odds",
        '"polymarket_wc2026_intermediate"."int_polymarket_wc2026_match_hourly_odds"',
        "polymarket_wc2026_match",
        "clob_token_id",
        False,
    ),
    IncrementalCase(
        "int_polymarket_us_midterms_2026_token_hourly_odds",
        '"polymarket_us_midterms_2026_intermediate".'
        '"int_polymarket_us_midterms_2026_token_hourly_odds"',
        "polymarket_us_midterms_2026_token",
        "clob_token_id",
        True,
    ),
    IncrementalCase(
        "int_kalshi_wc2026_market_hourly_odds",
        '"kalshi_wc2026_intermediate"."int_kalshi_wc2026_market_hourly_odds"',
        "kalshi_wc2026_market",
        "market_ticker",
        True,
    ),
    IncrementalCase(
        "int_kalshi_wc2026_match_hourly_odds",
        '"kalshi_wc2026_intermediate"."int_kalshi_wc2026_match_hourly_odds"',
        "kalshi_wc2026_match",
        "market_ticker",
        False,
    ),
)


def _run_dbt(args: list[str], *, profiles_dir: Path, env: dict[str, str]) -> None:
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            *args,
            "--project-dir",
            str(DBT_ROOT),
            "--profiles-dir",
            str(profiles_dir),
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def _create_polymarket_inputs(
    conn: duckdb.DuckDBPyConnection,
    case: IncrementalCase,
) -> None:
    scope = "us_midterms_2026" if "us_midterms_2026" in case.kind else "wc2026"
    staging = f"polymarket_{scope}_staging"
    intermediate = f"polymarket_{scope}_intermediate"
    conn.execute(f'create schema "{staging}"')
    conn.execute(f'create schema "{intermediate}"')
    conn.execute(
        f"""
        create table "{staging}"."stg_polymarket_{scope}_odds" (
            clob_token_id varchar,
            odds_timestamp timestamp,
            odds_timestamp_epoch bigint,
            price double,
            ingested_at timestamp
        )
        """
    )
    if case.retention:
        conn.execute(
            f"""
            create table "{staging}"."polymarket_{scope}_pipeline_policy" (
                scope_name varchar,
                hourly_window_days integer
            )
            """
        )
        conn.execute(
            f'insert into "{staging}"."polymarket_{scope}_pipeline_policy" values (?, 30)',
            [scope],
        )
    else:
        conn.execute(
            f"""
            create table "{intermediate}".
            "int_polymarket_wc2026_match_advance_tokens" (
                clob_token_id varchar,
                is_ambiguous_mapping boolean
            )
            """
        )
        conn.execute(
            f"""
            insert into "{intermediate}".
            "int_polymarket_wc2026_match_advance_tokens"
            values ('token-a', false)
            """
        )
    conn.executemany(
        f"""
        insert into "{staging}"."stg_polymarket_{scope}_odds"
        values (?, ?, ?, ?, ?)
        """,
        [
            ("token-a", "2099-01-01 10:05:00", 4070945100, 0.2, "2099-01-01 11:00:00"),
            ("token-a", "2099-01-01 10:40:00", 4070947200, 0.6, "2099-01-01 11:00:00"),
            ("token-old", "2000-01-01 10:05:00", 946721100, 0.1, "2000-01-01 11:00:00"),
        ],
    )
    if case.retention:
        conn.execute(
            f"""
            insert into "{staging}"."stg_polymarket_{scope}_odds"
            select
                'token-retained',
                cast(current_timestamp - interval '29 day' as timestamp),
                cast(epoch(current_timestamp - interval '29 day') as bigint),
                0.3,
                cast(current_timestamp as timestamp)
            """
        )


def _change_polymarket_inputs(
    conn: duckdb.DuckDBPyConnection,
    case: IncrementalCase,
) -> None:
    scope = "us_midterms_2026" if "us_midterms_2026" in case.kind else "wc2026"
    staging = f"polymarket_{scope}_staging"
    if not case.retention:
        conn.execute(
            """
            insert into "polymarket_wc2026_intermediate".
            "int_polymarket_wc2026_match_advance_tokens"
            values ('token-null', false), ('token-new', false)
            """
        )
    conn.executemany(
        f"""
        insert into "{staging}"."stg_polymarket_{scope}_odds"
        values (?, ?, ?, ?, ?)
        """,
        [
            ("token-a", "2099-01-01 10:50:00", 4070947800, 0.9, "2099-01-01 14:00:00"),
            ("token-null", "2099-01-01 12:05:00", 4070952300, 0.4, None),
            (
                "token-new",
                "2099-01-01 13:05:00",
                4070955900,
                0.5,
                "2000-01-01 01:00:00" if not case.retention else "2099-01-01 14:00:00",
            ),
        ],
    )


def _create_kalshi_inputs(
    conn: duckdb.DuckDBPyConnection,
    case: IncrementalCase,
) -> None:
    conn.execute('create schema "kalshi_wc2026_staging"')
    conn.execute('create schema "kalshi_wc2026_intermediate"')
    conn.execute(
        """
        create table "kalshi_wc2026_staging".
        "stg_kalshi_wc2026_market_candlesticks_hourly" (
            market_ticker varchar,
            hour_start_utc timestamp,
            odds_hour_epoch bigint,
            open_price double,
            high_price double,
            low_price double,
            close_price double,
            avg_price double,
            volume bigint,
            refreshed_at timestamp
        )
        """
    )
    if case.retention:
        conn.execute(
            """
            create table "kalshi_wc2026_staging"."kalshi_wc2026_pipeline_policy" (
                scope_name varchar,
                hourly_window_days integer
            )
            """
        )
        conn.execute(
            """
            insert into "kalshi_wc2026_staging"."kalshi_wc2026_pipeline_policy"
            values ('wc2026', 30)
            """
        )
    else:
        conn.execute(
            """
            create table "kalshi_wc2026_intermediate".
            "int_kalshi_wc2026_match_advance_markets" (
                market_ticker varchar,
                is_ambiguous_mapping boolean
            )
            """
        )
        conn.execute(
            """
            insert into "kalshi_wc2026_intermediate".
            "int_kalshi_wc2026_match_advance_markets"
            values ('ticker-a', false)
            """
        )
    conn.executemany(
        """
        insert into "kalshi_wc2026_staging".
        "stg_kalshi_wc2026_market_candlesticks_hourly"
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "ticker-a",
                "2099-01-01 10:00:00",
                4070944800,
                0.2,
                0.6,
                0.2,
                0.6,
                0.4,
                3,
                "2099-01-01 11:00:00",
            ),
            (
                "ticker-old",
                "2000-01-01 10:00:00",
                946720800,
                0.1,
                0.1,
                0.1,
                0.1,
                0.1,
                1,
                "2000-01-01 11:00:00",
            ),
        ],
    )
    if case.retention:
        conn.execute(
            """
            insert into "kalshi_wc2026_staging".
            "stg_kalshi_wc2026_market_candlesticks_hourly"
            select
                'ticker-retained',
                cast(date_trunc('hour', current_timestamp - interval '29 day')
                    as timestamp),
                cast(epoch(date_trunc(
                    'hour', current_timestamp - interval '29 day'
                )) as bigint),
                0.3,
                0.3,
                0.3,
                0.3,
                0.3,
                1,
                cast(current_timestamp as timestamp)
            """
        )


def _change_kalshi_inputs(
    conn: duckdb.DuckDBPyConnection,
    case: IncrementalCase,
) -> None:
    if not case.retention:
        conn.execute(
            """
            insert into "kalshi_wc2026_intermediate".
            "int_kalshi_wc2026_match_advance_markets"
            values ('ticker-null', false), ('ticker-new', false)
            """
        )
    conn.execute(
        """
        update "kalshi_wc2026_staging".
        "stg_kalshi_wc2026_market_candlesticks_hourly"
        set close_price = 0.9, avg_price = 0.55,
            refreshed_at = timestamp '2099-01-01 14:00:00'
        where market_ticker = 'ticker-a'
        """
    )
    conn.executemany(
        """
        insert into "kalshi_wc2026_staging".
        "stg_kalshi_wc2026_market_candlesticks_hourly"
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "ticker-null",
                "2099-01-01 12:00:00",
                4070952000,
                0.4,
                0.4,
                0.4,
                0.4,
                0.4,
                1,
                None,
            ),
            (
                "ticker-new",
                "2099-01-01 13:00:00",
                4070955600,
                0.5,
                0.5,
                0.5,
                0.5,
                0.5,
                1,
                "2099-01-01 14:00:00" if case.retention else "2000-01-01 01:00:00",
            ),
        ],
    )


def _rows(db_path: Path, case: IncrementalCase) -> list[tuple]:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        return conn.execute(
            f"select * from {case.relation} order by {case.key_column}, odds_hour_epoch"
        ).fetchall()


def test_incremental_model_inventory_is_complete() -> None:
    configured = {case.model for case in CASES}
    discovered = {
        path.stem
        for path in (DBT_ROOT / "models").rglob("*.sql")
        if "materialized='incremental'" in path.read_text()
    }
    assert configured == discovered


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.model)
def test_incremental_output_matches_full_refresh(
    case: IncrementalCase,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dbt_profiles_dir: Path,
) -> None:
    db_path = tmp_path / f"{case.model}.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    write_dbt_profile(dbt_profiles_dir, db_path)
    with duckdb.connect(str(db_path)) as conn:
        if case.kind.startswith("polymarket"):
            _create_polymarket_inputs(conn, case)
        else:
            _create_kalshi_inputs(conn, case)

    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(db_path)
    env["DUCKDB_NAME"] = str(db_path)
    env["DBT_PROFILES_DIR"] = str(dbt_profiles_dir)
    _run_dbt(
        ["run", "--full-refresh", "--select", case.model],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    with duckdb.connect(str(db_path)) as conn:
        if case.kind.startswith("polymarket"):
            _change_polymarket_inputs(conn, case)
        else:
            _change_kalshi_inputs(conn, case)
    _run_dbt(
        ["run", "--select", case.model],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    incremental_rows = _rows(db_path, case)

    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert conn.execute(
            f"""
            select count(*) = count(distinct ({case.key_column}, odds_hour_epoch))
            from {case.relation}
            """
        ).fetchone()[0]
        keys = {
            row[0]
            for row in conn.execute(
                f"select {case.key_column} from {case.relation}"
            ).fetchall()
        }
    assert {"token-a", "token-null", "token-new"} <= keys or {
        "ticker-a",
        "ticker-null",
        "ticker-new",
    } <= keys
    if case.retention:
        assert any(key.endswith("-retained") for key in keys)
        assert not any(key.endswith("-old") for key in keys)

    _run_dbt(
        ["run", "--full-refresh", "--select", case.model],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    assert incremental_rows == _rows(db_path, case)

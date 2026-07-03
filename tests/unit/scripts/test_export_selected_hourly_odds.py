"""Tests for scripts/export_selected_hourly_odds.py."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


def _load_export_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    from export_selected_hourly_odds import (
        LIVE_MART_NAME,
        _select_mart_name,
        export_hourly_odds_parquet,
        mart_exists,
    )

    return export_hourly_odds_parquet, mart_exists, LIVE_MART_NAME, _select_mart_name


def _create_hourly_mart(
    conn: duckdb.DuckDBPyConnection,
    mart_name: str = "selected_token_hourly_odds",
) -> None:
    conn.execute("create schema if not exists polymarket_marts")
    conn.execute(
        f"""
        create table polymarket_marts.{mart_name} (
            market_id varchar,
            outcome_index integer,
            clob_token_id varchar,
            question varchar,
            outcome_label varchar,
            event_slug varchar,
            is_active boolean,
            is_closed boolean,
            market_volume_usd double,
            odds_hour_utc timestamp,
            odds_hour_epoch bigint,
            open_price double,
            high_price double,
            low_price double,
            close_price double,
            avg_price double,
            observed_points integer,
            first_timestamp bigint,
            first_observed_at timestamp,
            last_timestamp bigint,
            last_observed_at timestamp
        )
        """
    )


def test_export_hourly_odds_parquet_round_trip(tmp_path: Path) -> None:
    export_hourly_odds_parquet, mart_exists, _, _ = _load_export_module()

    db_path = tmp_path / "test.duckdb"
    out_path = tmp_path / "selected_token_hourly_odds.parquet"

    conn = duckdb.connect(str(db_path))
    try:
        _create_hourly_mart(conn)
        conn.executemany(
            """
            insert into polymarket_marts.selected_token_hourly_odds values
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "m1",
                    0,
                    "tok-a",
                    "Q1",
                    "Yes",
                    "market_scope-a",
                    True,
                    False,
                    1000.0,
                    "2026-06-01 12:00:00",
                    1780833600,
                    0.40,
                    0.45,
                    0.39,
                    0.42,
                    0.415,
                    3,
                    1780833600,
                    "2026-06-01 12:00:00",
                    1780837140,
                    "2026-06-01 12:59:00",
                ),
                (
                    "m1",
                    1,
                    "tok-b",
                    "Q1",
                    None,
                    "market_scope-a",
                    True,
                    False,
                    1000.0,
                    "2026-06-01 12:00:00",
                    1780833600,
                    0.60,
                    0.61,
                    0.55,
                    0.58,
                    0.585,
                    3,
                    1780833600,
                    "2026-06-01 12:00:00",
                    1780837140,
                    "2026-06-01 12:59:00",
                ),
            ],
        )
        assert mart_exists(conn) is True
        row_count, spec_path = export_hourly_odds_parquet(conn, out_path)
    finally:
        conn.close()

    assert row_count == 2
    assert out_path.is_file()
    assert spec_path is not None
    assert spec_path.is_file()
    spec_text = spec_path.read_text(encoding="utf-8")
    assert "selected_token_hourly_odds" in spec_text
    assert "observed_points" in spec_text
    assert "one row per `(clob_token_id, odds_hour_utc)`" in spec_text
    assert "| Null outcome_label rows | 1 |" in spec_text
    assert "| (null) | 1 |" in spec_text

    verify = duckdb.connect()
    try:
        got = verify.execute(
            "select count(*), min(open_price), max(close_price) from read_parquet(?)",
            [str(out_path)],
        ).fetchone()
    finally:
        verify.close()

    assert got == (2, 0.4, 0.58)


def test_export_hourly_odds_skips_spec_when_disabled(tmp_path: Path) -> None:
    export_hourly_odds_parquet, _, _, _ = _load_export_module()

    db_path = tmp_path / "test.duckdb"
    out_path = tmp_path / "selected_token_hourly_odds.parquet"

    conn = duckdb.connect(str(db_path))
    try:
        _create_hourly_mart(conn)
        conn.execute(
            """
            insert into polymarket_marts.selected_token_hourly_odds values
            ('m1', 0, 'tok-a', 'Q1', 'Yes', 'market_scope-a', true, false,
             1.0, '2026-06-01 12:00:00', 1780833600, 0.40, 0.45, 0.39,
             0.42, 0.415, 3, 1780833600, '2026-06-01 12:00:00',
             1780837140, '2026-06-01 12:59:00')
            """
        )
        _, spec_path = export_hourly_odds_parquet(conn, out_path, write_spec=False)
    finally:
        conn.close()

    assert spec_path is None
    assert not out_path.with_suffix(".md").exists()


def test_export_missing_hourly_mart_raises(tmp_path: Path) -> None:
    export_hourly_odds_parquet, _, _, _ = _load_export_module()

    db_path = tmp_path / "empty.duckdb"
    out_path = tmp_path / "missing.parquet"

    conn = duckdb.connect(str(db_path))
    try:
        try:
            export_hourly_odds_parquet(conn, out_path)
        except LookupError as exc:
            assert "selected_token_hourly_odds" in str(exc)
        else:
            raise AssertionError("expected LookupError for missing mart")
    finally:
        conn.close()


def test_export_live_current_hourly_mart(tmp_path: Path) -> None:
    (
        export_hourly_odds_parquet,
        mart_exists,
        live_mart_name,
        select_mart_name,
    ) = _load_export_module()

    db_path = tmp_path / "test.duckdb"
    out_path = tmp_path / f"{live_mart_name}.parquet"

    conn = duckdb.connect(str(db_path))
    try:
        _create_hourly_mart(conn, live_mart_name)
        conn.execute(
            f"""
            insert into polymarket_marts.{live_mart_name} values
            ('m1', 0, 'tok-a', 'Q1', 'Yes', 'market_scope-a', true, false,
             1.0, '2026-06-01 12:00:00', 1780833600, 0.40, 0.45, 0.39,
             0.42, 0.415, 3, 1780833600, '2026-06-01 12:00:00',
             1780837140, '2026-06-01 12:59:00')
            """
        )
        assert mart_exists(conn, mart_name=live_mart_name) is True
        row_count, spec_path = export_hourly_odds_parquet(
            conn,
            out_path,
            mart_name=select_mart_name(live_current=True),
        )
    finally:
        conn.close()

    assert row_count == 1
    assert out_path.is_file()
    assert spec_path is not None
    spec_text = spec_path.read_text(encoding="utf-8")
    assert live_mart_name in spec_text
    assert "live-current" in spec_text

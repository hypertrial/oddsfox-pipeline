"""Fast unit coverage for the minute-odds dbt benchmark harness."""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock


def _load_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    import benchmark_polymarket_wc2026_minute_odds_dbt as benchmark

    return importlib.reload(benchmark)


def test_tier_sizes_cover_documented_scales():
    benchmark = _load_module()
    assert benchmark.TIER_SIZES["smoke"] == (8, 64)
    assert benchmark.TIER_SIZES["performance"] == (200, 50_000)
    tokens, rows = benchmark.TIER_SIZES["production-shaped"]
    assert tokens * rows == 60_000 * 6_284


def test_write_profile_embeds_path_threads_and_temp(tmp_path):
    benchmark = _load_module()
    profiles = tmp_path / "profiles"
    db = tmp_path / "warehouse.duckdb"
    temp = tmp_path / "duckdb-temp"
    benchmark._write_profile(profiles, db, threads=3, temp_dir=temp, memory_limit="16GB")
    text = (profiles / "profiles.yml").read_text(encoding="utf-8")
    assert db.as_posix() in text
    assert "threads: 3" in text
    assert temp.as_posix() in text
    assert 'memory_limit: "16GB"' in text
    assert "preserve_insertion_order: false" in text


def test_main_report_gates_without_running_dbt(tmp_path, monkeypatch):
    """Unit path: mock seed/dbt; prove return codes and report fields."""
    benchmark = _load_module()
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    monkeypatch.setattr(benchmark.connection, "reset_duckdb_connection_state", lambda: None)
    monkeypatch.setattr(benchmark.connection, "init_duck_db", lambda: None)
    monkeypatch.setattr(benchmark, "seed_match_minute_contract", lambda _conn: None)
    monkeypatch.setattr(benchmark, "backfill_primary_ohlc_table", lambda *_a, **_k: 0)
    monkeypatch.setattr(
        benchmark,
        "_seed_futures",
        lambda _conn, *, tokens, rows_per_token: {
            "futures_markets": tokens,
            "raw_tokens": tokens * 2,
            "raw_rows": tokens * 2 * rows_per_token,
            "rows_per_token": rows_per_token,
        },
    )
    monkeypatch.setattr(
        benchmark.subprocess,
        "run",
        lambda *a, **k: MagicMock(returncode=0, stdout="", stderr=""),
    )
    monkeypatch.setattr(benchmark, "seed_wc2026_schedule_matches", lambda _conn: None)
    monkeypatch.setattr(
        benchmark,
        "_run_dbt",
        lambda *_a, **_k: {
            "returncode": 0,
            "elapsed_seconds": 1.5,
            "stdout_tail": "",
            "stderr_tail": "",
        },
    )

    def _norm(sql: str) -> str:
        return " ".join(sql.lower().split())

    class _FakeConn:
        def execute(self, sql):
            q = _norm(sql)
            if "count(distinct clobtokenid)" in q:
                return MagicMock(fetchone=lambda: (16,))
            if "minute_source = 'futures'" in q:
                return MagicMock(fetchone=lambda: (8,))
            if "blocking_issue_keys" in q:
                return MagicMock(fetchone=lambda: (True, True, None, 8))
            if "count(*)" in q:
                return MagicMock(fetchone=lambda: (100,))
            return MagicMock(fetchone=lambda: (0,))

        def close(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(
        benchmark.duckdb,
        "connect",
        lambda *_a, **_k: _FakeConn(),
    )

    output = tmp_path / "report.json"
    rc = benchmark.main(["--tier", "smoke", "--output", str(output), "--threads", "1"])
    assert rc == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["dbt"]["returncode"] == 0
    assert report["primary_token_ok"] is True
    assert report["all_token_raw_ok"] is True
    assert report["dq"]["blocking_issue_keys"] is None
    assert report["mart_futures_tokens"] == 8
    assert report["raw_futures_tokens"] == 16


def test_main_returns_2_when_dq_blocked(tmp_path, monkeypatch):
    benchmark = _load_module()
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    monkeypatch.setattr(benchmark.connection, "reset_duckdb_connection_state", lambda: None)
    monkeypatch.setattr(benchmark.connection, "init_duck_db", lambda: None)
    monkeypatch.setattr(benchmark, "seed_match_minute_contract", lambda _conn: None)
    monkeypatch.setattr(benchmark, "backfill_primary_ohlc_table", lambda *_a, **_k: 0)
    monkeypatch.setattr(
        benchmark,
        "_seed_futures",
        lambda _conn, *, tokens, rows_per_token: {
            "futures_markets": tokens,
            "raw_tokens": tokens * 2,
            "raw_rows": 1,
            "rows_per_token": rows_per_token,
        },
    )
    monkeypatch.setattr(
        benchmark.subprocess,
        "run",
        lambda *a, **k: MagicMock(returncode=0, stdout="", stderr=""),
    )
    monkeypatch.setattr(benchmark, "seed_wc2026_schedule_matches", lambda _conn: None)
    monkeypatch.setattr(
        benchmark,
        "_run_dbt",
        lambda *_a, **_k: {
            "returncode": 0,
            "elapsed_seconds": 0.1,
            "stdout_tail": "",
            "stderr_tail": "",
        },
    )

    def _norm(sql: str) -> str:
        return " ".join(sql.lower().split())

    class _FakeConn:
        def execute(self, sql):
            q = _norm(sql)
            if "blocking_issue_keys" in q:
                return MagicMock(fetchone=lambda: (True, False, "futures_rows", 0))
            if "minute_source = 'futures'" in q:
                return MagicMock(fetchone=lambda: (0,))
            if "count(distinct clobtokenid)" in q:
                return MagicMock(fetchone=lambda: (16,))
            return MagicMock(fetchone=lambda: (0,))

        def close(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(benchmark.duckdb, "connect", lambda *_a, **_k: _FakeConn())
    output = tmp_path / "blocked.json"
    rc = benchmark.main(["--tier", "smoke", "--output", str(output), "--threads", "1"])
    assert rc == 2

"""End-to-end contract test for the isolated Polymarket soccer dbt graph."""

from __future__ import annotations

import hashlib
import json
import os
import resource
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import duckdb
from tests.integration.conftest import dbt_subprocess_env, write_dbt_profile
from tests.integration.dbt_cli import run_dbt

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.ingestion.polymarket.match_minute import MatchMinuteTokenPlan
from oddsfox_pipeline.ingestion.polymarket.odds.minute_batch import (
    cleanup_minute_odds_publish_cache,
    fetch_and_write_minute_history_parquet_shards,
)
from oddsfox_pipeline.orchestration.assets_soccer import (
    polymarket_soccer_minute_mart_check,
)


def _rows_sha256(rows: list[tuple]) -> str:
    return hashlib.sha256(repr(rows).encode("utf-8")).hexdigest()


def _seed_soccer_contract(
    conn: duckdb.DuckDBPyConnection,
    minute_prices: tuple[tuple[int, float], ...] = ((0, 0.2), (3, 0.4)),
) -> None:
    started = datetime(2025, 1, 2, 12, 0, 30)
    finished = started + timedelta(minutes=4)
    observed = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(
        """
        insert into polymarket_soccer_raw.event_snapshots (
            event_id, event_slug, event_title, created_at, observed_at,
            series_slugs_json, tags_json, candidate_sources_json,
            source_market_count, source_endpoint
        ) values (
            'event-1', 'alpha-beta', 'Alpha FC vs. Beta United',
            timestamp '2025-01-01', ?, '["league-one"]', '["soccer"]',
            '["exact_soccer_tag"]', 3, '/events/keyset'
        )
        """,
        [observed],
    )
    conn.execute(
        "insert into polymarket_soccer_raw.events "
        "select * from polymarket_soccer_raw.event_snapshots"
    )
    roles = ("home_win", "draw", "away_win")
    registry_rows = []
    audit_rows = []
    raw_rows = []
    primary_rows = []
    for index, role in enumerate(roles):
        market_id = f"market-{index}"
        yes_token = f"yes-{index}"
        no_token = f"no-{index}"
        registry_rows.append(
            (
                "event-1",
                market_id,
                role,
                "Alpha FC",
                "Beta United",
                yes_token,
                no_token,
                started,
                finished,
                "market_game_start_time",
                "explicit_finish",
                "high",
                "guaranteed_tag_era",
                observed,
            )
        )
        for token_id in (yes_token, no_token):
            audit_rows.append(
                (
                    "run-1",
                    market_id,
                    token_id,
                    "success",
                    True,
                    1,
                    started,
                    finished,
                    int(started.replace(tzinfo=timezone.utc).timestamp()),
                    int(finished.replace(tzinfo=timezone.utc).timestamp()),
                    len(minute_prices),
                    len(minute_prices),
                    "a" * 64,
                    "https://clob.polymarket.com/prices-history",
                    observed,
                    observed,
                )
            )
            for offset, base_price in minute_prices:
                price = base_price + index * 0.1
                at = started + timedelta(minutes=offset)
                raw_rows.append(
                    (
                        market_id,
                        token_id,
                        int(at.replace(tzinfo=timezone.utc).timestamp()),
                        price if token_id == yes_token else 1 - price,
                        1,
                        started,
                        finished,
                        observed,
                    )
                )
        for offset, base_price in minute_prices:
            price = base_price + index * 0.1
            at = started + timedelta(minutes=offset)
            minute_at = at.replace(second=0, microsecond=0)
            primary_rows.append(
                (
                    market_id,
                    yes_token,
                    int(minute_at.replace(tzinfo=timezone.utc).timestamp()),
                    minute_at,
                    price,
                    price,
                    price,
                    price,
                    price,
                    1,
                    at,
                    at,
                )
            )
    conn.executemany(
        "insert into polymarket_soccer_ops.match_result_registry values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        registry_rows,
    )
    # A later forced retry may fail without invalidating the last successfully
    # published exact window.
    audit_rows.append(
        (
            "run-2",
            "market-0",
            "yes-0",
            "error",
            False,
            1,
            started,
            finished,
            int(started.replace(tzinfo=timezone.utc).timestamp()),
            int(finished.replace(tzinfo=timezone.utc).timestamp()),
            0,
            0,
            None,
            "https://clob.polymarket.com/prices-history",
            observed + timedelta(hours=1),
            observed + timedelta(hours=1),
        )
    )
    conn.executemany(
        """
        insert into polymarket_soccer_ops.match_minute_odds_fetch_audit (
            fetch_run_id, market_id, clobTokenId, fetch_status, raw_published,
            fidelity_minutes, exact_window_start_at, exact_window_end_at,
            request_start_epoch, request_end_epoch, source_row_count,
            in_game_row_count, in_game_history_sha256, source_endpoint,
            fetch_started_at, fetch_finished_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        audit_rows,
    )
    conn.executemany(
        """
        insert into polymarket_soccer_raw.match_minute_odds_history values
        (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        raw_rows,
    )
    conn.executemany(
        """
        insert into polymarket_soccer_raw.match_primary_minute_ohlc values
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        primary_rows,
    )
    conn.execute(
        """
        insert into polymarket_soccer_ops.pipeline_runs (
            dagster_run_id, job_name, started_at, heartbeat_at, finished_at,
            status, terminal_step, warning_count, critical_count, metrics_json
        ) values (
            'pipeline-1', 'polymarket_soccer_full_pipeline', ?, ?, ?,
            'success', 'dbt_build', 0, 0, '{}'
        )
        """,
        [observed - timedelta(minutes=5), observed, observed],
    )
    step_rows = [
        ("event_catalog", '{"events": 1, "unique_markets": 3, "elapsed_seconds": 1}'),
        ("match_result_registry", '{"matches": 1, "elapsed_seconds": 1}'),
        (
            "match_minute_odds",
            '{"raw_published_tokens": 6, "elapsed_seconds": 1}',
        ),
        (
            "dbt_build",
            '{"elapsed_seconds": 1, "peak_rss_bytes": 1024, '
            '"disk_free_bytes": 21474836480, "warehouse_bytes": 104857600, '
            '"observed_minute_coverage_percent": 100.0, '
            '"dense_minute_coverage_percent": 100.0}',
        ),
    ]
    conn.executemany(
        """
        insert into polymarket_soccer_ops.pipeline_step_runs (
            dagster_run_id, step_name, attempt_number, phase, started_at,
            heartbeat_at, finished_at, status, metrics_json
        ) values ('pipeline-1', ?, 0, 'complete', ?, ?, ?, 'success', ?)
        """,
        [
            (step, observed - timedelta(minutes=5), observed, observed, metrics)
            for step, metrics in step_rows
        ],
    )


def test_soccer_modeling_mart_publishes_fully_priced_games(
    tmp_path, monkeypatch, dbt_profiles_dir, dbt_target_dir
):
    db_path = tmp_path / "soccer_modeling.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    with duckdb.connect(str(db_path)) as conn:
        _seed_soccer_contract(
            conn,
            minute_prices=((0, 0.2), (1, 0.25), (2, 0.3), (3, 0.35), (4, 0.4)),
        )

    write_dbt_profile(dbt_profiles_dir, db_path, threads=1)
    env = dbt_subprocess_env(
        db_path=db_path,
        profiles_dir=dbt_profiles_dir,
        target_dir=dbt_target_dir,
        dbt_threads=1,
    )
    run_dbt(
        [
            "build",
            "--select",
            "+polymarket_soccer_match_result_minute_odds_modeling",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    with duckdb.connect(str(db_path), read_only=True) as conn:
        result = conn.execute(
            """
            select
                count(*),
                count(distinct market_id),
                count(*) filter (
                    where open_odds is null or high_odds is null
                        or low_odds is null or close_odds is null
                        or avg_odds is null
                ),
                min(observed_minute_coverage_percent),
                max(maximum_consecutive_gap_minutes)
            from polymarket_soccer_marts.
                polymarket_soccer_match_result_minute_odds_modeling
            """
        ).fetchone()

    assert result == (15, 3, 0, 100.0, 0)


def test_soccer_minute_graph_publishes_sparse_and_dense_contracts(
    tmp_path, monkeypatch, dbt_profiles_dir, dbt_target_dir
):
    db_path = tmp_path / "soccer_minute.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    with duckdb.connect(str(db_path)) as conn:
        _seed_soccer_contract(conn)

    write_dbt_profile(dbt_profiles_dir, db_path, threads=1)
    env = dbt_subprocess_env(
        db_path=db_path,
        profiles_dir=dbt_profiles_dir,
        target_dir=dbt_target_dir,
        dbt_threads=1,
    )
    cold_started = time.perf_counter()
    run_dbt(
        ["build", "--select", "+tag:soccer"],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    cold_seconds = time.perf_counter() - cold_started

    with duckdb.connect(str(db_path), read_only=True) as conn:
        cold_rows = conn.execute(
            "select * from polymarket_soccer_marts.polymarket_soccer_match_result_minute_odds "
            "order by market_id, odds_minute_epoch"
        ).fetchall()
        cold_market_zero_rows = conn.execute(
            "select count(*) from polymarket_soccer_marts."
            "polymarket_soccer_match_result_minute_odds where market_id = 'market-0'"
        ).fetchone()[0]
    warm_started = time.perf_counter()
    run_dbt(
        ["build", "--select", "+tag:soccer"],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    warm_seconds = time.perf_counter() - warm_started

    with duckdb.connect(str(db_path)) as conn:
        matches = conn.execute(
            "select count(*), count(home_win_market_id), count(draw_market_id), count(away_win_market_id) from polymarket_soccer_marts.polymarket_soccer_matches"
        ).fetchone()
        observed_rows = conn.execute(
            "select count(*), count(distinct market_id), count(*) - count(distinct (market_id, odds_minute_epoch)) from polymarket_soccer_marts.polymarket_soccer_match_result_minute_odds_observed"
        ).fetchone()
        dense_rows = conn.execute(
            "select count(*), count(*) filter (where is_observed), count(*) filter (where not is_observed and close_odds is not null), count(*) filter (where close_odds is null) from polymarket_soccer_marts.polymarket_soccer_match_result_minute_odds"
        ).fetchone()
        modeling_rows = conn.execute(
            "select count(*) from polymarket_soccer_marts."
            "polymarket_soccer_match_result_minute_odds_modeling"
        ).fetchone()[0]
        carried = conn.execute(
            "select open_odds, high_odds, low_odds, close_odds, minutes_since_observation from polymarket_soccer_marts.polymarket_soccer_match_result_minute_odds where market_id = 'market-0' and odds_minute_utc = timestamp '2025-01-02 12:02:00'"
        ).fetchone()
        raw_sides = conn.execute(
            "select count(distinct clobTokenId) from polymarket_soccer_raw.match_minute_odds_history"
        ).fetchone()[0]
        retry = conn.execute(
            "select fetch_status, raw_published, is_retry_backlog from polymarket_soccer_observability.polymarket_soccer_match_result_token_fetch_status where clob_token_id = 'yes-0'"
        ).fetchone()
        quality = conn.execute(
            "select terminal_unavailable_tokens, published_tokens from polymarket_soccer_observability.polymarket_soccer_match_result_data_quality"
        ).fetchone()
        health = conn.execute(
            """
            select health_status, warning_count, critical_count
            from polymarket_soccer_observability.polymarket_soccer_pipeline_health
            """
        ).fetchone()
        trend = conn.execute(
            """
            select catalog_events, catalog_markets, mapped_matches, published_tokens
            from polymarket_soccer_observability.polymarket_soccer_pipeline_trends
            """
        ).fetchone()
        warm_rows = conn.execute(
            "select * from polymarket_soccer_marts.polymarket_soccer_match_result_minute_odds "
            "order by market_id, odds_minute_epoch"
        ).fetchall()
        dirty = conn.execute(
            "select dirty_observed_markets, dirty_dense_markets "
            "from polymarket_soccer_observability.polymarket_soccer_match_result_data_quality"
        ).fetchone()

    assert matches == (1, 1, 1, 1)
    assert observed_rows == (6, 3, 0)
    assert dense_rows == (15, 6, 9, 0)
    assert modeling_rows == 0
    assert carried == (0.2, 0.2, 0.2, 0.2, 2)
    assert raw_sides == 6
    assert retry == ("error", True, True)
    assert quality == (0, 6)
    assert health == ("healthy", 0, 0)
    assert trend == (1, 3, 1, 6)
    assert warm_rows == cold_rows
    assert dirty == (0, 0)

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            "update polymarket_soccer_ops.match_result_registry "
            "set window_end_at = timestamp '2025-01-02 12:02:30' "
            "where market_id = 'market-0'"
        )
        conn.execute(
            "update polymarket_soccer_ops.match_minute_odds_fetch_audit "
            "set exact_window_end_at = timestamp '2025-01-02 12:02:30' "
            "where market_id = 'market-0' and clobTokenId = 'yes-0' "
            "and fetch_status = 'success'"
        )
    run_dbt(
        ["build", "--select", "+int_polymarket_soccer_match_result_minute_odds"],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert conn.execute(
            "select count(*), min(odds_minute_utc), max(odds_minute_utc), "
            "count(distinct source_revision) from polymarket_soccer_intermediate."
            "int_polymarket_soccer_match_result_minute_odds "
            "where market_id = 'market-0'"
        ).fetchone() == (
            3,
            datetime(2025, 1, 2, 12, 0),
            datetime(2025, 1, 2, 12, 2),
            1,
        )

    with duckdb.connect(str(db_path)) as conn:
        terminalized_at = conn.execute(
            "select finished_at from polymarket_soccer_ops.pipeline_runs "
            "where dagster_run_id = 'pipeline-1'"
        ).fetchone()[0] + timedelta(minutes=1)
        conn.execute(
            "update polymarket_soccer_ops.pipeline_runs set finished_at = ?, "
            "heartbeat_at = ? where dagster_run_id = 'pipeline-1'",
            [terminalized_at, terminalized_at],
        )
        assert conn.execute(
            "select last_full_success_at from polymarket_soccer_observability."
            "polymarket_soccer_match_result_data_quality"
        ).fetchone() == (terminalized_at.replace(tzinfo=None),)

    report_path = os.getenv("SOCCER_MINUTE_PERFORMANCE_REPORT_PATH")
    if report_path:
        benchmark_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        benchmark_plans = [
            MatchMinuteTokenPlan(
                market_id=f"benchmark-market-{index}",
                token_id=f"benchmark-token-{index}",
                started_at=benchmark_start,
                finished_at=benchmark_start + timedelta(minutes=1),
            )
            for index in range(100)
        ]
        ingestion_started = time.perf_counter()
        benchmark_results, benchmark_shards, fetch_metrics = (
            fetch_and_write_minute_history_parquet_shards(
                benchmark_plans,
                fetch_run_id="soccer-performance-benchmark",
                ingested_at=benchmark_start,
                asset_name="soccer-performance-benchmark",
                workers=2,
                requests_per_second=1000,
                batch_group_size=1,
                auto_tune_rps=False,
                client_factory=object,
                fetch_window_fn=lambda _client, token, *_args, **_kwargs: [
                    (token, int(benchmark_start.timestamp()), 0.5)
                ],
            )
        )
        ingestion_seconds = time.perf_counter() - ingestion_started
        snapshot_bytes = sum(path.stat().st_size for path in benchmark_shards)
        report = {
            "cold_dbt_seconds": round(cold_seconds, 6),
            "warm_incremental_dbt_seconds": round(warm_seconds, 6),
            "ingestion_duration_seconds": round(ingestion_seconds, 6),
            "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "warehouse_bytes": db_path.stat().st_size,
            "snapshot_bytes": snapshot_bytes,
            "dense_rows": len(warm_rows),
            "warm_dirty_observed_markets": dirty[0],
            "warm_dirty_dense_markets": dirty[1],
            "attempted_tokens": len(benchmark_results),
            "audit_amplification": len(benchmark_results) / len(benchmark_plans),
            "output_equal": warm_rows == cold_rows,
            "cold_output_sha256": _rows_sha256(cold_rows),
            "warm_output_sha256": _rows_sha256(warm_rows),
            **fetch_metrics,
        }
        output = Path(report_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        cleanup_minute_odds_publish_cache("soccer-performance-benchmark")
    mart_check = polymarket_soccer_minute_mart_check.node_def.compute_fn.decorated_fn(
        SimpleNamespace(run=SimpleNamespace(run_id="fixture-run"))
    )
    assert mart_check.passed

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            "create table check_sparse_backup as select * from "
            "polymarket_soccer_intermediate.int_polymarket_soccer_match_result_observed "
            "where market_id = 'market-0' limit 1"
        )
        conn.execute(
            "delete from polymarket_soccer_intermediate."
            "int_polymarket_soccer_match_result_observed where (market_id, "
            "odds_minute_epoch) in (select market_id, odds_minute_epoch "
            "from check_sparse_backup)"
        )
    missing_sparse = (
        polymarket_soccer_minute_mart_check.node_def.compute_fn.decorated_fn(
            SimpleNamespace(run=SimpleNamespace(run_id="fixture-run"))
        )
    )
    assert not missing_sparse.passed
    assert missing_sparse.metadata["missing_sparse_observations"].value == 1
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            "insert into polymarket_soccer_intermediate."
            "int_polymarket_soccer_match_result_observed "
            "select * from check_sparse_backup"
        )
        conn.execute("drop table check_sparse_backup")
        conn.execute(
            "create table check_dense_backup as select * from "
            "polymarket_soccer_intermediate.int_polymarket_soccer_match_result_minute_odds "
            "where market_id = 'market-0'"
        )
        conn.execute(
            "delete from polymarket_soccer_intermediate."
            "int_polymarket_soccer_match_result_minute_odds "
            "where market_id = 'market-0'"
        )
    missing_market = (
        polymarket_soccer_minute_mart_check.node_def.compute_fn.decorated_fn(
            SimpleNamespace(run=SimpleNamespace(run_id="fixture-run"))
        )
    )
    assert not missing_market.passed
    assert missing_market.metadata["invalid_spines"].value == 1
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            "insert into polymarket_soccer_intermediate."
            "int_polymarket_soccer_match_result_minute_odds "
            "select * from check_dense_backup"
        )
        conn.execute("drop table check_dense_backup")

    health_result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve().parents[2] / "scripts" / "run_health.py"),
            "--scope",
            "polymarket:soccer",
            "--fail-on",
            "critical",
            "--format",
            "json",
            "--duckdb-path",
            str(db_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert health_result.returncode == 0, health_result.stderr
    assert json.loads(health_result.stdout)["health_status"] == "healthy"

    with duckdb.connect(str(db_path)) as conn:
        later = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=10)
        conn.execute(
            """
            insert into polymarket_soccer_ops.pipeline_runs (
                dagster_run_id, job_name, started_at, heartbeat_at, finished_at,
                status, terminal_step, warning_count, critical_count, metrics_json
            ) values (
                'pipeline-2', 'polymarket_soccer_full_pipeline', ?, ?, ?,
                'success', 'dbt_build', 0, 0, '{}'
            )
            """,
            [later - timedelta(minutes=5), later, later],
        )
        regression_rows = [
            ("event_catalog", '{"events": 1, "unique_markets": 3}'),
            ("match_result_registry", '{"matches": 1}'),
            ("match_minute_odds", '{"raw_published_tokens": 6}'),
            (
                "dbt_build",
                '{"elapsed_seconds": 1, "peak_rss_bytes": 1024, '
                '"disk_free_bytes": 1073741824, "warehouse_bytes": 2254857830, '
                '"observed_minute_coverage_percent": 90.0, '
                '"dense_minute_coverage_percent": 100.0}',
            ),
        ]
        conn.executemany(
            """
            insert into polymarket_soccer_ops.pipeline_step_runs (
                dagster_run_id, step_name, attempt_number, phase, started_at,
                heartbeat_at, finished_at, status, metrics_json
            ) values ('pipeline-2', ?, 0, 'complete', ?, ?, ?, 'success', ?)
            """,
            [
                (step, later - timedelta(minutes=5), later, later, metrics)
                for step, metrics in regression_rows
            ],
        )
        alerts = dict(
            conn.execute(
                """
                select alert_code, severity
                from polymarket_soccer_observability.polymarket_soccer_pipeline_alerts
                where alert_code in (
                    'low_free_disk', 'observed_coverage_drop',
                    'warehouse_storage_regression'
                )
                """
            ).fetchall()
        )

    assert alerts == {
        "low_free_disk": "critical",
        "observed_coverage_drop": "warning",
        "warehouse_storage_regression": "warning",
    }

    with duckdb.connect(str(db_path)) as conn:
        stable_before = conn.execute(
            "select * from polymarket_soccer_intermediate."
            "int_polymarket_soccer_match_result_minute_odds "
            "where market_id <> 'market-0' order by market_id, odds_minute_epoch"
        ).fetchall()
        window = conn.execute(
            "select window_start_at, window_end_at from "
            "polymarket_soccer_ops.match_result_registry where market_id = 'market-0'"
        ).fetchone()
        changed_at = later + timedelta(minutes=1)
        conn.execute(
            "update polymarket_soccer_raw.match_primary_minute_ohlc "
            "set open_price = 0.77, high_price = 0.77, low_price = 0.77, "
            "close_price = 0.77, avg_price = 0.77 "
            "where market_id = 'market-0' and odds_minute_epoch = "
            "(select min(odds_minute_epoch) from "
            "polymarket_soccer_raw.match_primary_minute_ohlc where market_id = 'market-0')"
        )
        conn.execute(
            """
            insert into polymarket_soccer_ops.match_minute_odds_fetch_audit (
                fetch_run_id, market_id, clobTokenId, fetch_status, raw_published,
                fidelity_minutes, exact_window_start_at, exact_window_end_at,
                request_start_epoch, request_end_epoch, source_row_count,
                in_game_row_count, in_game_history_sha256, source_endpoint,
                fetch_started_at, fetch_finished_at
            ) values (
                'run-3', 'market-0', 'yes-0', 'success', true, 1, ?, ?,
                epoch(?), epoch(?), 2, 2, ?,
                'https://clob.polymarket.com/prices-history', ?, ?
            )
            """,
            [*window, *window, "b" * 64, changed_at, changed_at],
        )

    run_dbt(
        ["build", "--select", "+tag:soccer"],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    with duckdb.connect(str(db_path), read_only=True) as conn:
        incremental_rows = conn.execute(
            "select * from polymarket_soccer_marts."
            "polymarket_soccer_match_result_minute_odds "
            "order by market_id, odds_minute_epoch"
        ).fetchall()
        stable_after = conn.execute(
            "select * from polymarket_soccer_intermediate."
            "int_polymarket_soccer_match_result_minute_odds "
            "where market_id <> 'market-0' order by market_id, odds_minute_epoch"
        ).fetchall()
        incremental_market_zero_rows = conn.execute(
            "select count(*) from polymarket_soccer_marts."
            "polymarket_soccer_match_result_minute_odds where market_id = 'market-0'"
        ).fetchone()[0]
    assert stable_after == stable_before

    run_dbt(
        ["build", "--full-refresh", "--select", "+tag:soccer"],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    with duckdb.connect(str(db_path), read_only=True) as conn:
        full_refresh_rows = conn.execute(
            "select * from polymarket_soccer_marts."
            "polymarket_soccer_match_result_minute_odds "
            "order by market_id, odds_minute_epoch"
        ).fetchall()
    assert incremental_rows == full_refresh_rows
    if report_path:
        output = Path(report_path)
        report = json.loads(output.read_text())
        report.update(
            incremental_full_refresh_equal=incremental_rows == full_refresh_rows,
            incremental_output_sha256=_rows_sha256(incremental_rows),
            full_refresh_output_sha256=_rows_sha256(full_refresh_rows),
            rebuilt_markets=1,
            rows_deleted=cold_market_zero_rows,
            rows_inserted=incremental_market_zero_rows,
        )
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


def test_soccer_data_quality_separates_due_and_recoverable_coverage(
    tmp_path, monkeypatch, dbt_profiles_dir, dbt_target_dir
):
    db_path = tmp_path / "soccer_coverage.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    now = datetime.now(timezone.utc).replace(tzinfo=None, second=0, microsecond=0)
    due_start = now - timedelta(hours=3)
    due_end = now - timedelta(hours=2)
    future_start = now + timedelta(days=1)
    future_end = future_start + timedelta(hours=1)
    with duckdb.connect(str(db_path)) as conn:
        _seed_soccer_contract(conn)
        registry_rows = []
        terminal_audits = []
        terminal_rows = []
        for event_id, prefix, started, finished in (
            ("event-terminal", "terminal", due_start, due_end),
            ("event-future", "future", future_start, future_end),
        ):
            for index, role in enumerate(("home_win", "draw", "away_win")):
                market_id = f"{prefix}-market-{index}"
                yes_token = f"{prefix}-yes-{index}"
                no_token = f"{prefix}-no-{index}"
                registry_rows.append(
                    (
                        event_id,
                        market_id,
                        role,
                        "Alpha FC",
                        "Beta United",
                        yes_token,
                        no_token,
                        started,
                        finished,
                        "market_game_start_time",
                        "explicit_finish",
                        "high",
                        "guaranteed_tag_era",
                        now,
                    )
                )
                if prefix == "terminal":
                    terminal_audits.append(
                        (
                            "terminal-run",
                            market_id,
                            yes_token,
                            "empty",
                            False,
                            1,
                            started,
                            finished,
                            int(started.replace(tzinfo=timezone.utc).timestamp()),
                            int(finished.replace(tzinfo=timezone.utc).timestamp()),
                            0,
                            0,
                            None,
                            "https://clob.polymarket.com/prices-history",
                            now,
                            now,
                        )
                    )
                    terminal_rows.append(
                        (market_id, yes_token, started, finished, 72, now)
                    )
        conn.executemany(
            "insert into polymarket_soccer_ops.match_result_registry values "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            registry_rows,
        )
        conn.executemany(
            """
            insert into polymarket_soccer_ops.match_minute_odds_fetch_audit (
                fetch_run_id, market_id, clobTokenId, fetch_status, raw_published,
                fidelity_minutes, exact_window_start_at, exact_window_end_at,
                request_start_epoch, request_end_epoch, source_row_count,
                in_game_row_count, in_game_history_sha256, source_endpoint,
                fetch_started_at, fetch_finished_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            terminal_audits,
        )
        conn.executemany(
            "insert into polymarket_soccer_ops."
            "match_minute_odds_terminal_unavailable values (?, ?, ?, ?, ?, ?)",
            terminal_rows,
        )

    write_dbt_profile(dbt_profiles_dir, db_path, threads=1)
    env = dbt_subprocess_env(
        db_path=db_path,
        profiles_dir=dbt_profiles_dir,
        target_dir=dbt_target_dir,
        dbt_threads=1,
    )
    run_dbt(
        [
            "build",
            "--select",
            "+polymarket_soccer_match_result_data_quality",
        ],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    with duckdb.connect(str(db_path), read_only=True) as conn:
        quality = conn.execute(
            """
            select
                due_markets,
                not_due_markets,
                terminal_unavailable_markets,
                publishable_due_markets,
                expected_dense_minutes,
                expected_due_dense_minutes,
                expected_recoverable_dense_minutes,
                dense_minute_coverage_percent,
                due_dense_minute_coverage_percent,
                recoverable_dense_minute_coverage_percent
            from polymarket_soccer_observability
                .polymarket_soccer_match_result_data_quality
            """
        ).fetchone()

    assert quality == (6, 3, 3, 3, 381, 198, 15, 3.937, 7.576, 100.0)

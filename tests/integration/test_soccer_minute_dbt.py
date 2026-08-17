"""End-to-end contract test for the isolated Polymarket soccer dbt graph."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import duckdb
from tests.integration.conftest import dbt_subprocess_env, write_dbt_profile
from tests.integration.dbt_cli import run_dbt

import oddsfox_pipeline.storage.duckdb.connection as connection


def _seed_soccer_contract(conn: duckdb.DuckDBPyConnection) -> None:
    started = datetime(2025, 1, 2, 12, 0, 30)
    finished = started + timedelta(minutes=4)
    observed = datetime(2025, 1, 3)
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
                    2,
                    2,
                    "a" * 64,
                    "https://clob.polymarket.com/prices-history",
                    observed,
                    observed,
                )
            )
            for offset, price in ((0, 0.2 + index * 0.1), (3, 0.4 + index * 0.1)):
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
        for offset, price in ((0, 0.2 + index * 0.1), (3, 0.4 + index * 0.1)):
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
    run_dbt(
        ["build", "--select", "+tag:soccer"],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

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
        carried = conn.execute(
            "select open_odds, high_odds, low_odds, close_odds, minutes_since_observation from polymarket_soccer_marts.polymarket_soccer_match_result_minute_odds where market_id = 'market-0' and odds_minute_utc = timestamp '2025-01-02 12:02:00'"
        ).fetchone()
        raw_sides = conn.execute(
            "select count(distinct clobTokenId) from polymarket_soccer_raw.match_minute_odds_history"
        ).fetchone()[0]
        retry = conn.execute(
            "select fetch_status, is_retry_backlog from polymarket_soccer_observability.polymarket_soccer_match_result_token_fetch_status where clob_token_id = 'yes-0'"
        ).fetchone()
        quality = conn.execute(
            "select terminal_unavailable_tokens from polymarket_soccer_observability.polymarket_soccer_match_result_data_quality"
        ).fetchone()[0]

    assert matches == (1, 1, 1, 1)
    assert observed_rows == (6, 3, 0)
    assert dense_rows == (15, 6, 9, 0)
    assert carried == (0.2, 0.2, 0.2, 0.2, 2)
    assert raw_sides == 6
    assert retry == ("error", True)
    assert quality == 0

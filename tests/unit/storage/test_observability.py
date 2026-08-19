import json

import duckdb
import pytest

from oddsfox_pipeline.storage.duckdb import observability as obs
from oddsfox_pipeline.storage.duckdb.connection import init_duck_db
from oddsfox_pipeline.storage.duckdb.observability import (
    delta_dbt_models,
    delta_raw_layer,
    format_dbt_snapshot_log,
    format_raw_snapshot_log,
    snapshot_dbt_models,
    snapshot_raw_layer,
)
from oddsfox_pipeline.storage.duckdb.schemas.kalshi import create_test_kalshi_raw_tables
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    create_test_markets_table,
    seed_test_ingestion_run_event,
)


def test_snapshot_raw_layer_counts_polymarket_tables(
    tmp_path, monkeypatch, isolated_env
):
    import oddsfox_pipeline.storage.duckdb.connection as conn_mod

    db_path = tmp_path / "obs.duckdb"
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    conn_mod.reset_duckdb_connection_state()
    init_duck_db()

    with duckdb.connect(str(db_path)) as conn:
        create_test_markets_table(conn)
        conn.execute(
            """
            insert into polymarket_wc2026_raw.markets (
                id, question, category, description, outcomes, volume, active,
                closed, created_at, scraped_at, end_date, slug, event_slug, event_id
            )
            values (
                'm1', 'q', 'cat', 'desc', '[]', 1.0, true, false,
                current_timestamp, current_timestamp, current_timestamp,
                'slug', 'event', 'event-id'
            )
            """
        )

        snapshot = snapshot_raw_layer(conn=conn, level="basic")

    assert snapshot["markets_rows"] == 1
    assert snapshot["markets_missing"] is False
    assert snapshot["polymarket_wc2026_raw.markets_rows"] == 1
    assert snapshot["polymarket_wc2026_raw.markets_missing"] is False
    assert snapshot["polymarket_wc2026_raw.match_minute_odds_history_rows"] == 0
    assert "market_scope_registry_rows" in snapshot
    assert "market_tokens_distinct_tokens" not in snapshot


def test_snapshot_raw_layer_counts_kalshi_tables(tmp_path, monkeypatch, isolated_env):
    import oddsfox_pipeline.storage.duckdb.connection as conn_mod

    db_path = tmp_path / "kalshi-obs.duckdb"
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    conn_mod.reset_duckdb_connection_state()
    init_duck_db()

    with duckdb.connect(str(db_path)) as conn:
        create_test_kalshi_raw_tables(conn)
        conn.execute(
            """
            insert into kalshi_wc2026_raw.events (
                event_ticker, series_ticker, title, sub_title, category, status,
                open_time, close_time, scraped_at
            )
            values (
                'KXWCUP-26', 'KXWCUP', 'Winner', 'sub', 'sports', 'open',
                current_timestamp, current_timestamp, current_timestamp
            )
            """
        )

        snapshot = snapshot_raw_layer(conn=conn, level="basic")

    assert snapshot["kalshi_wc2026_raw.events_rows"] == 1
    assert snapshot["kalshi_wc2026_raw.events_missing"] is False
    assert snapshot["kalshi_wc2026_raw.market_candlesticks_hourly_rows"] == 0
    assert snapshot["kalshi_wc2026_ops.candlestick_sync_ledger_rows"] == 0
    assert "events_rows" not in snapshot


def test_seed_test_ingestion_run_event_inserts_sync_odds_row(tmp_path, monkeypatch):
    import oddsfox_pipeline.storage.duckdb.connection as conn_mod

    db_path = tmp_path / "seed.duckdb"
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    conn_mod.reset_duckdb_connection_state()
    init_duck_db()

    with duckdb.connect(str(db_path)) as conn:
        seed_test_ingestion_run_event(conn)
        row = conn.execute(
            """
            select task_name, metrics_json
            from polymarket_wc2026_ops.ingestion_run_events
            """
        ).fetchone()

    assert row is not None
    assert row[0] == "sync_odds"
    assert '"errors": 0' in row[1]
    assert '"history_coverage_vs_market_tokens": 0.96' in row[1]


def test_delta_raw_layer_ignores_missing_flags():
    assert delta_raw_layer(
        {"markets_rows": 1, "markets_missing": True},
        {"markets_rows": 2, "markets_missing": False},
    ) == {"markets_rows": {"before": 1, "after": 2}}


def test_snapshot_dbt_models_reports_missing_relations(tmp_path):
    with duckdb.connect(str(tmp_path / "dbt.duckdb")) as conn:
        snapshot = snapshot_dbt_models(
            conn=conn,
            dbt_select="+tag:kalshi",
            dbt_exclude="tag:cross_domain",
        )

    assert "polymarket_wc2026_staging.stg_polymarket_wc2026_markets" not in snapshot
    assert snapshot["kalshi_wc2026_marts.kalshi_wc2026_stage_markets"] == {
        "exists": False,
        "rows": None,
    }


def test_scoped_dbt_relations_filters_kalshi_scope(monkeypatch):
    # Isolate tag filtering from live-manifest ancestor expansion.
    monkeypatch.setattr(obs, "_dbt_model_parent_names", lambda: {})
    relations = obs._scoped_dbt_relations(
        dbt_select="+tag:kalshi",
        dbt_exclude="tag:polymarket",
    )
    assert relations
    assert all(schema.startswith("kalshi_wc2026_") for schema, _ in relations)
    assert not any(schema.startswith("polymarket_wc2026_") for schema, _ in relations)


def test_dbt_model_parent_names_does_not_sticky_cache_empty_miss(tmp_path, monkeypatch):
    missing = tmp_path / "missing" / "manifest.json"
    present = tmp_path / "present" / "manifest.json"
    present.parent.mkdir(parents=True)
    present.write_text(
        json.dumps(
            {
                "nodes": {
                    "model.oddsfox.child": {
                        "name": "child",
                        "depends_on": {"nodes": ["model.oddsfox.parent"]},
                    },
                    "model.oddsfox.parent": {
                        "name": "parent",
                        "depends_on": {"nodes": []},
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(obs, "_resolve_dbt_manifest_path", lambda: missing)
    assert obs._dbt_model_parent_names() == {}
    monkeypatch.setattr(obs, "_resolve_dbt_manifest_path", lambda: present)
    parents = obs._dbt_model_parent_names()
    assert parents["child"] == frozenset({"parent"})


def test_scoped_dbt_relations_expands_plus_selector_ancestors(monkeypatch):
    monkeypatch.setattr(
        obs,
        "_dbt_model_parent_names",
        lambda: {
            "polymarket_wc2026_market_hourly_odds": frozenset(
                {"int_polymarket_wc2026_token_hourly_odds"}
            ),
            "int_polymarket_wc2026_token_hourly_odds": frozenset(
                {"stg_polymarket_wc2026_odds"}
            ),
        },
    )
    exact_names = {
        model
        for _, model in obs._scoped_dbt_relations(
            dbt_select="polymarket_wc2026_market_hourly_odds"
        )
    }
    assert "polymarket_wc2026_market_hourly_odds" in exact_names
    assert "stg_polymarket_wc2026_odds" not in exact_names

    plus_names = {
        model
        for _, model in obs._scoped_dbt_relations(
            dbt_select="+polymarket_wc2026_market_hourly_odds"
        )
    }
    assert "polymarket_wc2026_market_hourly_odds" in plus_names
    assert "int_polymarket_wc2026_token_hourly_odds" in plus_names
    assert "stg_polymarket_wc2026_odds" in plus_names


def test_order_book_scope_excludes_trade_only_models():
    relations = obs._scoped_dbt_relations(
        dbt_select="+tag:pmxt_order_book",
        dbt_exclude=None,
    )
    names = {model for _, model in relations}
    assert "polymarket_wc2026_match_order_book" in names
    assert "polymarket_wc2026_match_trades" not in names
    assert "int_polymarket_wc2026_match_trade_publication_gate" not in names
    assert "polymarket_wc2026_match_order_book_states" in names


def test_match_minute_exclude_covers_working_set_and_token_minute_models():
    names = {model for _, model in obs._scoped_dbt_relations(None, "tag:match_minute")}
    assert "int_polymarket_wc2026_match_working_set" not in names
    assert "int_polymarket_wc2026_match_token_minute_odds" not in names
    assert "polymarket_wc2026_match_minute_odds" not in names


def test_market_portrait_scope_includes_trade_models():
    relations = obs._scoped_dbt_relations(
        dbt_select="+tag:pmxt_order_book +tag:market_portrait",
        dbt_exclude=None,
    )
    names = {model for _, model in relations}
    assert "polymarket_wc2026_match_trades" in names
    assert "int_polymarket_wc2026_match_trade_publication_gate" in names


def test_batch_table_row_counts_reports_missing_and_existing_tables(tmp_path):
    with duckdb.connect(str(tmp_path / "batch.duckdb")) as conn:
        conn.execute("CREATE SCHEMA polymarket_wc2026_raw")
        conn.execute("CREATE TABLE polymarket_wc2026_raw.markets (id VARCHAR)")
        conn.execute("INSERT INTO polymarket_wc2026_raw.markets VALUES ('m1')")
        counts = obs._batch_table_row_counts(
            conn,
            (
                ("polymarket_wc2026_raw", "markets"),
                ("polymarket_wc2026_raw", "missing_table"),
            ),
        )

    assert counts[("polymarket_wc2026_raw", "markets")] == (True, 1)
    assert counts[("polymarket_wc2026_raw", "missing_table")] == (False, None)


def test_dbt_delta_and_formatters():
    before = {
        "polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds": {
            "exists": False,
            "rows": None,
        }
    }
    after = {
        "polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds": {
            "exists": True,
            "rows": 3,
        }
    }

    assert delta_dbt_models(before, after) == {
        "polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds": {
            "before": {"exists": False, "rows": None},
            "after": {"exists": True, "rows": 3},
        }
    }
    assert "markets=2" in format_raw_snapshot_log({"markets_rows": 2})
    assert "kalshi_wc2026_raw.events=1" in format_raw_snapshot_log(
        {"kalshi_wc2026_raw.events_rows": 1}
    )
    assert (
        "polymarket_wc2026_market_hourly_odds:exists=True,rows=3"
        in format_dbt_snapshot_log(after)
    )


def test_observability_scalar_and_row_count_error_branches(caplog):
    class NoneRowConn:
        def execute(self, *_args, **_kwargs):
            return self

        def fetchone(self):
            return None

    class BadValueConn(NoneRowConn):
        def fetchone(self):
            return ("bad-int",)

    class DuckErrorConn(NoneRowConn):
        def execute(self, *_args, **_kwargs):
            raise duckdb.Error("boom")

    assert obs._scalar_int(NoneRowConn(), "select 1") is None
    assert obs._table_row_count(NoneRowConn(), "x") == (True, 0)
    assert obs._scalar_int(DuckErrorConn(), "select 1") is None
    assert obs._table_row_count(DuckErrorConn(), "x") == (False, None)

    caplog.set_level("WARNING")
    assert obs._scalar_int(BadValueConn(), "select 1") is None
    assert obs._table_row_count(BadValueConn(), "x") == (False, None)
    assert "unexpected value" in caplog.text


def test_observability_dict_rows_and_datetime_format_branches(caplog):
    class RowsConn:
        def execute(self, *_args, **_kwargs):
            return self

        def fetchall(self):
            return [(None, 1), ("missing", None), ("ok", 2)]

    class DuckErrorConn(RowsConn):
        def execute(self, *_args, **_kwargs):
            raise duckdb.Error("boom")

    class RuntimeErrorConn(RowsConn):
        def execute(self, *_args, **_kwargs):
            raise RuntimeError("boom")

    assert obs._dict_rows(RowsConn(), "select") == {"ok": 2}
    assert obs._dict_rows(DuckErrorConn(), "select") is None

    caplog.set_level("WARNING")
    assert obs._dict_rows(RuntimeErrorConn(), "select") is None
    assert "unexpected error" in caplog.text

    assert obs._normalize_dt(None) is None
    assert obs._normalize_dt(" ") is None
    assert "T" in obs._normalize_dt(obs.datetime(2026, 1, 1))


def test_snapshot_raw_layer_rejects_invalid_level():
    with pytest.raises(ValueError, match="snapshot_raw_layer level"):
        snapshot_raw_layer(conn=object(), level="deep")


def test_snapshot_dbt_models_handles_unexpected_count_value(caplog):
    class BadCountConn:
        def execute(self, sql, *_args, **_kwargs):
            class _Result:
                def fetchall(self_inner):
                    if "information_schema.tables" in str(sql):
                        return [
                            (
                                "polymarket_wc2026_staging",
                                "stg_polymarket_wc2026_markets",
                                True,
                            )
                        ]
                    return [
                        (
                            "polymarket_wc2026_staging",
                            "stg_polymarket_wc2026_markets",
                            "bad-int",
                        )
                    ]

                def fetchone(self_inner):
                    return ("bad-int",)

            return _Result()

    caplog.set_level("WARNING")
    snapshot = snapshot_dbt_models(
        conn=BadCountConn(),
        dbt_select="stg_polymarket_wc2026_markets",
    )

    assert snapshot["polymarket_wc2026_staging.stg_polymarket_wc2026_markets"] == {
        "exists": False,
        "rows": None,
    }
    assert "unexpected value" in caplog.text


def test_formatters_render_skip_reasons_and_plain_values():
    raw = format_raw_snapshot_log(
        {
            "markets_rows": 1,
            "odds_history_max_ts": "123",
            "token_sync_skips_by_reason": {"empty": 2, "error": 1},
        }
    )
    dbt = format_dbt_snapshot_log({"plain": 3})

    assert "token_sync_skips_by_reason={empty:2,error:1}" in raw
    assert "odds_history_max_ts=123" in raw
    assert dbt == "plain=3"


def test_snapshot_raw_layer_full_tolerates_missing_odds_history(
    tmp_path, monkeypatch, isolated_env
):
    import oddsfox_pipeline.storage.duckdb.connection as conn_mod

    db_path = tmp_path / "obs-missing-oh.duckdb"
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    conn_mod.reset_duckdb_connection_state()
    init_duck_db()

    with duckdb.connect(str(db_path)) as conn:
        conn.execute("DROP TABLE IF EXISTS polymarket_wc2026_raw.odds_history")
        snapshot = snapshot_raw_layer(conn=conn, level="full")

    assert snapshot["polymarket_wc2026_raw.odds_history_missing"] is True
    assert snapshot["odds_history_max_ts"] is None


def test_market_tokens_without_history_casts_token_ids(
    tmp_path, monkeypatch, isolated_env
):
    import oddsfox_pipeline.storage.duckdb.connection as conn_mod

    db_path = tmp_path / "obs-token-cast.duckdb"
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    conn_mod.reset_duckdb_connection_state()
    init_duck_db()

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            insert into polymarket_wc2026_raw.market_tokens
                (market_id, clobTokenIds, updated_at)
            values
                ('1', '["111","222","333"]', current_timestamp)
            """
        )
        # Insert odds history with numeric-looking token ids (string column).
        conn.execute(
            """
            insert into polymarket_wc2026_raw.odds_history
                (clobTokenId, timestamp, price, ingested_at)
            values
                ('111', 1, 0.5, current_timestamp),
                ('222', 2, 0.6, current_timestamp)
            """
        )
        snapshot = snapshot_raw_layer(conn=conn, level="full")

    assert snapshot["market_tokens_distinct_tokens"] == 3
    assert snapshot["odds_history_distinct_tokens"] == 2
    assert snapshot["market_tokens_without_history"] == 1
    assert snapshot["history_tokens_without_market_tokens"] == 0


def test_observability_batch_count_fallbacks_and_empty_input():
    assert obs._batch_table_row_counts(object(), ()) == {}

    class ExistsQueryFails:
        def execute(self, *_args, **_kwargs):
            raise duckdb.Error("metadata query failed")

    assert obs._batch_table_row_counts(ExistsQueryFails(), (("schema", "table"),)) == {
        ("schema", "table"): (False, None)
    }

    class CountQueryFails:
        calls = 0

        def execute(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                return self
            raise duckdb.Error("count query failed")

        def fetchall(self):
            return [("schema", "table", True)]

    assert obs._batch_table_row_counts(CountQueryFails(), (("schema", "table"),)) == {
        ("schema", "table"): (False, None)
    }


def test_observability_timestamp_manifest_and_selector_edge_branches(
    tmp_path, monkeypatch
):
    class NoneTimestamp:
        def execute(self, *_args, **_kwargs):
            return self

        def fetchone(self):
            return None

    assert obs._scalar_max_timestamp(NoneTimestamp(), "select") is None

    monkeypatch.delenv("DBT_TARGET_PATH", raising=False)
    assert obs._resolve_dbt_manifest_path() == (
        obs.DBT_PROJECT_DIR / "target" / "manifest.json"
    )

    monkeypatch.setenv("DBT_TARGET_PATH", "relative-target")
    assert (
        obs._resolve_dbt_manifest_path()
        == (obs.DBT_PROJECT_DIR / "relative-target").resolve() / "manifest.json"
    )

    bad_nodes = tmp_path / "bad-nodes.json"
    bad_nodes.write_text('{"nodes": []}', encoding="utf-8")
    assert (
        obs._dbt_model_parent_names_cached(str(bad_nodes), bad_nodes.stat().st_mtime)
        == {}
    )

    mixed_nodes = tmp_path / "mixed-nodes.json"
    mixed_nodes.write_text(
        json.dumps(
            {
                "nodes": {
                    "source.project.name": {},
                    "model.project.bad": [],
                    "model.project.good": {"depends_on": {"nodes": []}},
                }
            }
        ),
        encoding="utf-8",
    )
    assert obs._dbt_model_parent_names_cached(
        str(mixed_nodes), mixed_nodes.stat().st_mtime
    ) == {"good": frozenset()}

    malformed = tmp_path / "malformed.json"
    malformed.write_text("{", encoding="utf-8")
    monkeypatch.setattr(obs, "_resolve_dbt_manifest_path", lambda: malformed)
    assert obs._dbt_model_parent_names() == {}

    assert obs._selector_groups("+ ,,") == ()

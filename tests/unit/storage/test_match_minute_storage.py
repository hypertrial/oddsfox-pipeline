from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from oddsfox_pipeline.naming import SCOPE_SOCCER
from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    load_match_minute_fetch_audit,
    load_match_minute_odds_history_stage,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_all_polymarket_tables,
)


def test_match_minute_raw_table_is_bootstrapped_for_shipped_minute_scopes():
    with duckdb.connect(":memory:") as conn:
        conn.execute("create schema polymarket_wc2026_raw")
        conn.execute("create schema polymarket_wc2026_ops")
        conn.execute("create schema polymarket_soccer_raw")
        conn.execute("create schema polymarket_soccer_ops")
        bootstrap_all_polymarket_tables(conn)

        rows = conn.execute(
            """
            select table_schema
            from information_schema.tables
            where table_name = 'match_minute_odds_history'
            order by table_schema
            """
        ).fetchall()

    assert rows == [("polymarket_soccer_raw",), ("polymarket_wc2026_raw",)]


def test_match_minute_raw_replace_is_exact_idempotent_and_isolated(duck):
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    row = {
        "market_id": "market",
        "clobTokenId": "token",
        "timestamp": 100,
        "price": 0.4,
        "fidelity_minutes": 1,
        "window_start_at": now,
        "window_end_at": now,
        "ingested_at": now,
    }

    def audit(run_id: str) -> dict[str, object]:
        return {
            "fetch_run_id": run_id,
            "market_id": "market",
            "clobTokenId": "token",
            "fetch_status": "success",
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": now,
            "exact_window_end_at": now,
            "request_start_epoch": 100,
            "request_end_epoch": 100,
            "source_row_count": 1,
            "in_game_row_count": 1,
            "in_game_history_sha256": "a" * 64,
            "source_endpoint": "https://clob.polymarket.com/prices-history",
            "fetch_started_at": now,
            "fetch_finished_at": now,
            "error_type": None,
            "error_message": None,
        }

    with duck.get_connection() as conn:
        load_match_minute_fetch_audit([audit("run-1")], conn)
        load_match_minute_odds_history_stage(
            [row, {**row, "timestamp": 101}], conn, fetch_run_id="run-1"
        )
        load_match_minute_fetch_audit([audit("run-2")], conn)
        load_match_minute_odds_history_stage(
            [{**row, "price": 0.5}], conn, fetch_run_id="run-2"
        )
        try:
            load_match_minute_odds_history_stage(
                [{**row, "price": 0.9}], conn, fetch_run_id="missing-audit"
            )
        except RuntimeError as exc:
            assert "Fetch audit inventory" in str(exc)
        else:  # pragma: no cover - assertion helper
            raise AssertionError("missing audit must block raw publication")
        load_match_minute_fetch_audit([audit("run-3")], conn)
        from oddsfox_pipeline.storage.minute_odds_snapshots import (
            MinuteOddsSnapshotError,
        )

        with pytest.raises(MinuteOddsSnapshotError, match="fidelity_minutes"):
            load_match_minute_odds_history_stage(
                [{**row, "price": 0.9, "fidelity_minutes": 2}],
                conn,
                fetch_run_id="run-3",
            )
        minute_rows = conn.execute(
            "select clobTokenId, timestamp, price "
            "from polymarket_wc2026_raw.match_minute_odds_history"
        ).fetchall()
        hourly_rows = conn.execute(
            "select count(*) from polymarket_wc2026_raw.odds_history"
        ).fetchone()[0]
        ledger_rows = conn.execute(
            "select count(*) from polymarket_wc2026_ops.token_sync_ledger"
        ).fetchone()[0]
        published = conn.execute(
            "select count(*) from polymarket_wc2026_ops.match_minute_odds_fetch_audit "
            "where raw_published"
        ).fetchone()[0]
        unpublished_run_3 = conn.execute(
            "select count(*) "
            "from polymarket_wc2026_ops.match_minute_odds_fetch_audit "
            "where fetch_run_id = 'run-3' and not raw_published"
        ).fetchone()[0]

    assert minute_rows == [("token", 100, 0.5)]
    assert hourly_rows == 0
    assert ledger_rows == 0
    assert published == 1
    assert unpublished_run_3 == 1


def test_soccer_late_publication_supersedes_newer_fetch_audit(
    duck, tmp_path, monkeypatch
):
    from oddsfox_pipeline.storage.minute_odds_snapshots import (
        active_snapshot_dir,
        load_latest_published_token_windows,
        minute_odds_snapshot_root,
        validate_minute_odds_snapshot,
    )

    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    start = datetime(2026, 8, 1, tzinfo=timezone.utc)
    end = start + timedelta(hours=2)

    def audit(run_id: str, finished: datetime, history_hash: str) -> dict:
        return {
            "fetch_run_id": run_id,
            "market_id": "market",
            "clobTokenId": "yes",
            "fetch_status": "success",
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": start,
            "exact_window_end_at": end,
            "request_start_epoch": int(start.timestamp()),
            "request_end_epoch": int(end.timestamp()),
            "source_row_count": 1,
            "in_game_row_count": 1,
            "in_game_history_sha256": history_hash,
            "source_endpoint": "https://clob.polymarket.com/prices-history",
            "fetch_started_at": finished,
            "fetch_finished_at": finished,
            "error_type": None,
            "error_message": None,
        }

    def row(price: float) -> dict:
        return {
            "market_id": "market",
            "clobTokenId": "yes",
            "timestamp": int(start.timestamp()),
            "price": price,
            "fidelity_minutes": 1,
            "window_start_at": start,
            "window_end_at": end,
            "ingested_at": start,
        }

    with duck.get_connection() as conn:
        conn.execute(
            "insert into polymarket_soccer_raw.markets "
            "(id, outcomes, clob_token_ids, observed_at) "
            'values (\'market\', \'["Yes","No"]\', \'["yes","no"]\', ?)',
            [start],
        )
        load_match_minute_fetch_audit(
            [audit("run-a", start + timedelta(days=2), "a" * 64)],
            conn,
            scope_name=SCOPE_SOCCER,
        )
        load_match_minute_odds_history_stage(
            [row(0.1)],
            conn,
            fetch_run_id="run-a",
            scope_name=SCOPE_SOCCER,
            audit_mode="success_only",
        )
        load_match_minute_fetch_audit(
            [audit("run-b", start + timedelta(days=1), "b" * 64)],
            conn,
            scope_name=SCOPE_SOCCER,
        )
        load_match_minute_odds_history_stage(
            [row(0.9)],
            conn,
            fetch_run_id="run-b",
            scope_name=SCOPE_SOCCER,
            audit_mode="success_only",
        )

        assert conn.execute(
            "select price from polymarket_soccer_raw.match_minute_odds_history"
        ).fetchone() == (0.9,)
        published = load_latest_published_token_windows(
            conn, leg="match", scope_name=SCOPE_SOCCER
        )
        assert published["yes"].history_sha256 == "b" * 64
        assert conn.execute(
            "select fetch_run_id from "
            "polymarket_soccer_ops.match_minute_odds_fetch_audit "
            "where raw_published"
        ).fetchall() == [("run-b",)]

    snapshot = validate_minute_odds_snapshot(
        active_snapshot_dir(
            minute_odds_snapshot_root(leg="match", scope_name=SCOPE_SOCCER)
        )
    )
    assert snapshot.manifest["window_hashes"]["yes"] == "b" * 64


def test_match_minute_fetch_audit_append_is_atomic(duck):
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)

    def row(token: str, error_message: str | None = None):
        return {
            "fetch_run_id": "run",
            "market_id": "market",
            "clobTokenId": token,
            "fetch_status": "error",
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": now,
            "exact_window_end_at": now,
            "request_start_epoch": 100,
            "request_end_epoch": 100,
            "source_row_count": 0,
            "in_game_row_count": 0,
            "in_game_history_sha256": None,
            "source_endpoint": "https://clob.polymarket.com/prices-history",
            "fetch_started_at": now,
            "fetch_finished_at": now,
            "error_type": "RuntimeError",
            "error_message": error_message,
        }

    with duck.get_connection() as conn:
        load_match_minute_fetch_audit([], conn)
        with pytest.raises(duckdb.ConstraintException):
            load_match_minute_fetch_audit(
                [row("valid", "ok"), row("invalid", "x" * 501)], conn
            )
        assert (
            conn.execute(
                "select count(*) "
                "from polymarket_wc2026_ops.match_minute_odds_fetch_audit"
            ).fetchone()[0]
            == 0
        )


def test_match_minute_raw_replace_accepts_arrow_table(duck):
    import pyarrow as pa

    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    table = pa.table(
        {
            "market_id": pa.array(["market"], type=pa.string()),
            "clob_token_id": pa.array(["token"], type=pa.string()),
            "timestamp": pa.array([100], type=pa.int64()),
            "price": pa.array([0.4], type=pa.float64()),
            "fidelity_minutes": pa.array([1], type=pa.int32()),
            "window_start_at": pa.array([now], type=pa.timestamp("us", tz="UTC")),
            "window_end_at": pa.array([now], type=pa.timestamp("us", tz="UTC")),
            "ingested_at": pa.array([now], type=pa.timestamp("us", tz="UTC")),
            "row_order": pa.array([0], type=pa.int64()),
        }
    )
    audit = {
        "fetch_run_id": "run-arrow",
        "market_id": "market",
        "clobTokenId": "token",
        "fetch_status": "success",
        "raw_published": False,
        "fidelity_minutes": 1,
        "exact_window_start_at": now,
        "exact_window_end_at": now,
        "request_start_epoch": 100,
        "request_end_epoch": 100,
        "source_row_count": 1,
        "in_game_row_count": 1,
        "in_game_history_sha256": "a" * 64,
        "source_endpoint": "https://clob.polymarket.com/prices-history",
        "fetch_started_at": now,
        "fetch_finished_at": now,
        "error_type": None,
        "error_message": None,
    }

    with duck.get_connection() as conn:
        load_match_minute_fetch_audit([audit], conn)
        load_match_minute_odds_history_stage(table, conn, fetch_run_id="run-arrow")
        minute_rows = conn.execute(
            "select clobTokenId, timestamp, price "
            "from polymarket_wc2026_raw.match_minute_odds_history"
        ).fetchall()
        published = conn.execute(
            "select count(*) from polymarket_wc2026_ops.match_minute_odds_fetch_audit "
            "where fetch_run_id = 'run-arrow' and raw_published"
        ).fetchone()[0]

    assert minute_rows == [("token", 100, 0.4)]
    assert published == 1


def test_match_minute_publish_preserves_prior_snapshot_on_invalid_fidelity(
    duck, tmp_path, monkeypatch
):
    from oddsfox_pipeline.storage.minute_odds_snapshots import MinuteOddsSnapshotError

    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    good = {
        "market_id": "market",
        "clobTokenId": "token",
        "timestamp": 100,
        "price": 0.4,
        "fidelity_minutes": 1,
        "window_start_at": now,
        "window_end_at": now,
        "ingested_at": now,
    }

    def audit(run_id: str) -> dict[str, object]:
        return {
            "fetch_run_id": run_id,
            "market_id": "market",
            "clobTokenId": "token",
            "fetch_status": "success",
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": now,
            "exact_window_end_at": now,
            "request_start_epoch": 100,
            "request_end_epoch": 100,
            "source_row_count": 1,
            "in_game_row_count": 1,
            "in_game_history_sha256": "a" * 64,
            "source_endpoint": "https://clob.polymarket.com/prices-history",
            "fetch_started_at": now,
            "fetch_finished_at": now,
            "error_type": None,
            "error_message": None,
        }

    with duck.get_connection() as conn:
        load_match_minute_fetch_audit([audit("run-1")], conn)
        load_match_minute_odds_history_stage([good], conn, fetch_run_id="run-1")
        load_match_minute_fetch_audit([audit("run-2")], conn)
        with pytest.raises(MinuteOddsSnapshotError, match="fidelity_minutes"):
            load_match_minute_odds_history_stage(
                [{**good, "fidelity_minutes": 5}],
                conn,
                fetch_run_id="run-2",
            )
        assert conn.execute(
            "select price from polymarket_wc2026_raw.match_minute_odds_history"
        ).fetchall() == [(0.4,)]
        assert (
            conn.execute(
                """
                select raw_published
                from polymarket_wc2026_ops.match_minute_odds_fetch_audit
                where fetch_run_id = 'run-2'
                """
            ).fetchone()[0]
            is False
        )


def test_match_minute_register_failure_rolls_current_back(duck, tmp_path, monkeypatch):
    from oddsfox_pipeline.storage import minute_odds_snapshots as snapshots
    from oddsfox_pipeline.storage.minute_odds_snapshots import (
        active_snapshot_id,
        minute_odds_snapshot_root,
    )

    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    row = {
        "market_id": "market",
        "clobTokenId": "token",
        "timestamp": 100,
        "price": 0.4,
        "fidelity_minutes": 1,
        "window_start_at": now,
        "window_end_at": now,
        "ingested_at": now,
    }

    def audit(run_id: str) -> dict[str, object]:
        return {
            "fetch_run_id": run_id,
            "market_id": "market",
            "clobTokenId": "token",
            "fetch_status": "success",
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": now,
            "exact_window_end_at": now,
            "request_start_epoch": 100,
            "request_end_epoch": 100,
            "source_row_count": 1,
            "in_game_row_count": 1,
            "in_game_history_sha256": "a" * 64,
            "source_endpoint": "https://clob.polymarket.com/prices-history",
            "fetch_started_at": now,
            "fetch_finished_at": now,
            "error_type": None,
            "error_message": None,
        }

    with duck.get_connection() as conn:
        load_match_minute_fetch_audit([audit("run-1")], conn)
        load_match_minute_odds_history_stage([row], conn, fetch_run_id="run-1")
        root = minute_odds_snapshot_root(leg="match")
        first_id = active_snapshot_id(root)
        assert first_id is not None

        def boom(_conn, _snapshot, **_kwargs):
            raise RuntimeError("register failed")

        monkeypatch.setattr(snapshots, "register_snapshot_views", boom)
        load_match_minute_fetch_audit([audit("run-2")], conn)
        with pytest.raises(RuntimeError, match="register failed"):
            load_match_minute_odds_history_stage(
                [{**row, "price": 0.5}],
                conn,
                fetch_run_id="run-2",
            )
        assert active_snapshot_id(root) == first_id
        assert conn.execute(
            "select price from polymarket_wc2026_raw.match_minute_odds_history"
        ).fetchall() == [(0.4,)]
        assert (
            conn.execute(
                """
                select raw_published
                from polymarket_wc2026_ops.match_minute_odds_fetch_audit
                where fetch_run_id = 'run-2'
                """
            ).fetchone()[0]
            is False
        )


def test_resolve_primary_token_ids_reuse_only_picks_one_per_market():
    from oddsfox_pipeline.storage.duckdb.dlt_batch import _resolve_primary_token_ids

    with duckdb.connect(":memory:") as conn:
        # No markets table: reuse-only must still pick one primary per market
        # (Yes tip when present in the token id, else lowest id).
        primary = _resolve_primary_token_ids(
            conn,
            [],
            extra_token_market_rows=[
                ("m1", "m1-no"),
                ("m1", "m1-yes"),
                ("m2", "aaa"),
                ("m2", "zzz"),
            ],
        )
    assert primary == {"m1-yes", "aaa"}


def test_resolve_primary_token_ids_soccer_keeps_native_no_tokens():
    from oddsfox_pipeline.storage.duckdb.dlt_batch import _resolve_primary_token_ids

    with duckdb.connect(":memory:") as conn:
        conn.execute("create schema polymarket_soccer_ops")
        conn.execute(
            """
            create table polymarket_soccer_ops.match_result_registry (
                yes_token_id text,
                no_token_id text
            )
            """
        )
        conn.execute(
            "insert into polymarket_soccer_ops.match_result_registry "
            "values ('yes-0', 'no-0')"
        )
        primary = _resolve_primary_token_ids(
            conn,
            [],
            extra_token_market_rows=[
                ("market-0", "yes-0"),
                ("market-0", "no-0"),
                ("market-0", "other"),
            ],
            scope_name=SCOPE_SOCCER,
        )
    assert primary == {"yes-0", "no-0"}

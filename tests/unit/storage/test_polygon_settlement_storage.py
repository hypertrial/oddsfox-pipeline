from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import duckdb
import pytest

from oddsfox_pipeline.ingestion.polymarket.polygon_rpc import PolygonRPCError
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    NEG_RISK_V2_EXCHANGE,
    STANDARD_V2_EXCHANGE,
)
from oddsfox_pipeline.storage.duckdb.polygon_settlement import (
    CHUNKS_TABLE,
    FILL_COLUMNS,
    FILLS_TABLE,
    RUNS_TABLE,
    STAGE_TABLE,
    completed_polygon_chunk_ranges,
    load_polygon_settlement_release_provenance,
    publish_polygon_settlement_scan,
    record_polygon_settlement_chunk,
    record_polygon_settlement_failure,
    set_polygon_verification_status,
    start_polygon_settlement_scan,
)

_HASH_A = "0x" + "a" * 64
_HASH_B = "0x" + "b" * 64
_TARGETS = [
    {
        "exchange_address": STANDARD_V2_EXCHANGE.casefold(),
        "from_block": 10,
        "to_block": 12,
        "from_block_hash": _HASH_A,
        "to_block_hash": _HASH_B,
    },
    {
        "exchange_address": NEG_RISK_V2_EXCHANGE.casefold(),
        "from_block": 10,
        "to_block": 12,
        "from_block_hash": _HASH_A,
        "to_block_hash": _HASH_B,
    },
]


def _start(conn, scan_id: str, *, manifest_sha256: str = "1" * 64) -> bool:
    return start_polygon_settlement_scan(
        conn,
        scan_id=scan_id,
        manifest_version="1.0.0",
        manifest_sha256=manifest_sha256,
        normalizer_version="polygon-v2-settlement-v4",
        chain_id=137,
        provider_label="test-provider",
        provider_origin="https://rpc.example",
        finalized_head_number=99,
        finalized_head_hash="0x" + "9" * 64,
        target_ranges=_TARGETS,
        boundary_blocks_sha256="2" * 64,
    )


def _fill(scan_id: str, address: str = STANDARD_V2_EXCHANGE.casefold()):
    return {
        "scan_id": scan_id,
        "chain_id": 137,
        "exchange_address": address,
        "chunk_from_block": 10,
        "chunk_to_block": 12,
        "block_number": 11,
        "block_hash": "0x" + "3" * 64,
        "block_timestamp": datetime(2026, 6, 1, tzinfo=timezone.utc),
        "transaction_hash": "0x" + "4" * 64,
        "transaction_index": 2,
        "passive_log_index": 3,
        "active_log_index": 4,
        "matched_log_index": 5,
        "normalized_leg_ordinal": 0,
        "proposition_id": "m001-home_win",
        "condition_id": "0x" + "5" * 64,
        "token_id": "123",
        "outcome_side": "yes",
        "order_side": "BUY",
        "source_token_id": "123",
        "source_maker_amount": "600000",
        "source_taker_amount": "1000000",
        "share_volume": Decimal("1.000000"),
        "gross_collateral_volume": Decimal("0.600000"),
        "price": Decimal("0.600000000000000000"),
        "normalization_kind": "complementary",
        "is_derived": False,
        "segment_sha256": "6" * 64,
        "decoder_version": "polygon-v2-settlement-v4",
        "ingested_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
    }


def _metrics(event_count: int, scoped_event_count: int) -> dict[str, int]:
    return {
        "duration_ms": 1,
        "http_request_count": 1,
        "log_rpc_call_count": 1,
        "receipt_rpc_call_count": 0,
        "header_rpc_call_count": 0,
        "discovery_count": 0,
        "eligible_discovery_count": 0,
        "filtered_discovery_count": 0,
        "receipt_transaction_count": 0,
        "receipt_log_count": event_count,
        "retry_count": 0,
        "adaptive_split_count": 0,
    }


def _record(conn, scan_id: str, address: str, rows) -> None:
    event_count = 3 if rows else 0
    record_polygon_settlement_chunk(
        conn,
        scan_id=scan_id,
        exchange_address=address,
        from_block=10,
        to_block=12,
        from_block_hash=_HASH_A,
        to_block_hash=_HASH_B,
        event_count=event_count,
        scoped_event_count=event_count,
        scoped_event_sha256="7" * 64,
        rows=rows,
        metrics=_metrics(event_count, event_count),
    )


def test_chunk_commit_ignores_unregister_cleanup_error(duck) -> None:
    class UnregisterFailureConnection:
        def __init__(self, conn) -> None:
            self.conn = conn

        def unregister(self, _name):
            raise duckdb.Error("synthetic unregister failure")

        def __getattr__(self, name):
            return getattr(self.conn, name)

    with duck.get_connection() as conn:
        _start(conn, "unregister-cleanup")
        proxy = UnregisterFailureConnection(conn)
        _record(
            proxy,
            "unregister-cleanup",
            STANDARD_V2_EXCHANGE.casefold(),
            [_fill("unregister-cleanup")],
        )
        assert (
            conn.execute(
                f"SELECT count(*) FROM {CHUNKS_TABLE} "
                "WHERE scan_id = 'unregister-cleanup' AND status = 'success'"
            ).fetchone()[0]
            == 1
        )


class _FailingRestartConnection:
    def __init__(self, conn) -> None:
        self._conn = conn

    def execute(self, query, *args, **kwargs):
        if "verification_status = 'not_requested'" in query:
            raise RuntimeError("synthetic restart failure")
        return self._conn.execute(query, *args, **kwargs)


def test_polygon_tables_exclude_participant_and_raw_event_fields(duck) -> None:
    with duck.get_connection() as conn:
        columns = {
            row[0]
            for row in conn.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'polymarket_wc2026_raw'
                  AND table_name = 'polygon_settlement_fills'
                """
            ).fetchall()
        }
    assert "transaction_hash" in columns
    assert (
        not {
            "maker",
            "taker",
            "wallet_address",
            "order_hash",
            "topics",
            "data",
            "calldata",
            "signature",
            "rpc_url",
        }
        & columns
    )


def test_fill_timestamps_are_stored_as_utc_in_a_non_utc_session(duck) -> None:
    with duck.get_connection() as conn:
        conn.execute("SET TimeZone = 'Europe/Warsaw'")
        assert conn.execute("SELECT current_setting('TimeZone')").fetchone()[0] == (
            "Europe/Warsaw"
        )
        _start(conn, "timezone")
        row = {
            **_fill("timezone"),
            "block_timestamp": datetime(
                2026, 6, 1, 14, tzinfo=timezone(timedelta(hours=2))
            ),
            "ingested_at": datetime(
                2026, 8, 1, 9, tzinfo=timezone(timedelta(hours=-4))
            ),
        }

        _record(conn, "timezone", STANDARD_V2_EXCHANGE, [row])
        _record(conn, "timezone", NEG_RISK_V2_EXCHANGE, [])

        expected = (
            datetime(2026, 6, 1, 12),
            datetime(2026, 8, 1, 13),
        )
        assert (
            conn.execute(
                f"SELECT block_timestamp, ingested_at FROM {STAGE_TABLE} "
                "WHERE scan_id = 'timezone'"
            ).fetchone()
            == expected
        )

        publish_polygon_settlement_scan(
            conn,
            scan_id="timezone",
            target_ranges=_TARGETS,
        )
        assert (
            conn.execute(
                f"SELECT block_timestamp, ingested_at FROM {FILLS_TABLE} "
                "WHERE scan_id = 'timezone'"
            ).fetchone()
            == expected
        )


@pytest.mark.parametrize("field", ["block_timestamp", "ingested_at"])
def test_chunk_rejects_naive_fill_timestamps(duck, field: str) -> None:
    scan_id = f"naive-{field}"
    with duck.get_connection() as conn:
        _start(conn, scan_id)
        row = {**_fill(scan_id), field: datetime(2026, 6, 1, 12)}

        with pytest.raises(
            ValueError,
            match=rf"{field} must be a timezone-aware datetime",
        ):
            _record(conn, scan_id, STANDARD_V2_EXCHANGE, [row])

        assert (
            conn.execute(
                f"SELECT count(*) FROM {STAGE_TABLE} WHERE scan_id = ?", [scan_id]
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                f"SELECT count(*) FROM {CHUNKS_TABLE} WHERE scan_id = ?", [scan_id]
            ).fetchone()[0]
            == 0
        )


def test_resumable_chunks_publish_atomically_and_expose_safe_provenance(duck) -> None:
    with duck.get_connection() as conn:
        assert _start(conn, "scan-1") is False
        _record(conn, "scan-1", STANDARD_V2_EXCHANGE, [_fill("scan-1")])
        _record(conn, "scan-1", NEG_RISK_V2_EXCHANGE, [])

        completed = completed_polygon_chunk_ranges(conn, "scan-1")
        assert completed == {
            STANDARD_V2_EXCHANGE.casefold(): [(10, 12)],
            NEG_RISK_V2_EXCHANGE.casefold(): [(10, 12)],
        }
        assert (
            publish_polygon_settlement_scan(
                conn,
                scan_id="scan-1",
                target_ranges=_TARGETS,
            )
            == 1
        )
        assert conn.execute(f"SELECT count(*) FROM {STAGE_TABLE}").fetchone()[0] == 0
        assert _start(conn, "scan-1") is True

        provenance = load_polygon_settlement_release_provenance(conn)
        assert provenance["scan_id"] == "scan-1"
        assert provenance["seed_sha256"] == "1" * 64
        assert provenance["seed_version"] == "1.0.0"
        assert provenance["chain_id"] == 137
        assert provenance["rpc_provider_origin"] == "https://rpc.example"
        assert len(provenance["block_ranges"]) == 2
        assert "rpc_url" not in provenance


def test_incomplete_new_scan_and_failed_stage_preserve_previous_snapshot(duck) -> None:
    with duck.get_connection() as conn:
        _start(conn, "scan-good")
        _record(conn, "scan-good", STANDARD_V2_EXCHANGE, [_fill("scan-good")])
        _record(conn, "scan-good", NEG_RISK_V2_EXCHANGE, [])
        publish_polygon_settlement_scan(
            conn,
            scan_id="scan-good",
            target_ranges=_TARGETS,
        )

        _start(conn, "scan-bad", manifest_sha256="8" * 64)
        _record(conn, "scan-bad", STANDARD_V2_EXCHANGE, [_fill("scan-bad")])
        with pytest.raises(RuntimeError, match="every target exchange"):
            publish_polygon_settlement_scan(
                conn,
                scan_id="scan-bad",
                target_ranges=_TARGETS,
            )
        record_polygon_settlement_failure(
            conn,
            scan_id="scan-bad",
            error=RuntimeError("provider https://secret.example/key failed"),
            exchange_address=NEG_RISK_V2_EXCHANGE,
            from_block=10,
            to_block=12,
        )

        assert conn.execute(
            f"SELECT DISTINCT scan_id FROM {FILLS_TABLE}"
        ).fetchall() == [("scan-good",)]
        run = conn.execute(
            f"SELECT status, error_message FROM {RUNS_TABLE} WHERE scan_id='scan-bad'"
        ).fetchone()
        assert run[0] == "failed"
        assert "secret.example" not in run[1]
        assert "<redacted-url>" in run[1]


def test_published_scan_survives_late_concurrent_failure(duck) -> None:
    with duck.get_connection() as conn:
        _start(conn, "concurrent")
        _record(conn, "concurrent", STANDARD_V2_EXCHANGE, [_fill("concurrent")])
        _record(conn, "concurrent", NEG_RISK_V2_EXCHANGE, [])
        assert (
            publish_polygon_settlement_scan(
                conn,
                scan_id="concurrent",
                target_ranges=_TARGETS,
            )
            == 1
        )

        record_polygon_settlement_failure(
            conn,
            scan_id="concurrent",
            error=PolygonRPCError("bare-provider-secret"),
            exchange_address=STANDARD_V2_EXCHANGE,
            from_block=10,
            to_block=12,
        )

        assert conn.execute(
            f"SELECT status, raw_published, error_message FROM {RUNS_TABLE} "
            "WHERE scan_id='concurrent'"
        ).fetchone() == ("published", True, None)
        assert (
            conn.execute(
                f"SELECT count(*) FROM {CHUNKS_TABLE} "
                "WHERE scan_id='concurrent' AND status='failed'"
            ).fetchone()[0]
            == 0
        )
        assert _start(conn, "concurrent") is True
        assert (
            publish_polygon_settlement_scan(
                conn,
                scan_id="concurrent",
                target_ranges=_TARGETS,
            )
            == 1
        )


def test_polygon_rpc_failure_persistence_discards_bare_provider_message(duck) -> None:
    with duck.get_connection() as conn:
        _start(conn, "rpc-error")
        record_polygon_settlement_failure(
            conn,
            scan_id="rpc-error",
            error=PolygonRPCError("api_key_live_bare_secret"),
        )
        error_type, error_message = conn.execute(
            f"SELECT error_type, error_message FROM {RUNS_TABLE} "
            "WHERE scan_id='rpc-error'"
        ).fetchone()
        assert error_type == "PolygonRPCError"
        assert error_message == "Polygon RPC failure"
        assert "api_key_live_bare_secret" not in error_message


def test_replacement_marks_old_run_noncanonical_and_old_id_forces_replay(duck) -> None:
    with duck.get_connection() as conn:
        _start(conn, "old-scan")
        _record(conn, "old-scan", STANDARD_V2_EXCHANGE, [_fill("old-scan")])
        _record(conn, "old-scan", NEG_RISK_V2_EXCHANGE, [])
        publish_polygon_settlement_scan(
            conn,
            scan_id="old-scan",
            target_ranges=_TARGETS,
        )
        set_polygon_verification_status(
            conn,
            "old-scan",
            "matched",
            provider_label="verification-provider",
            provider_origin="https://verify.example",
        )

        _start(conn, "new-scan", manifest_sha256="8" * 64)
        _record(conn, "new-scan", STANDARD_V2_EXCHANGE, [_fill("new-scan")])
        _record(conn, "new-scan", NEG_RISK_V2_EXCHANGE, [])
        publish_polygon_settlement_scan(
            conn,
            scan_id="new-scan",
            target_ranges=_TARGETS,
        )

        assert conn.execute(
            f"SELECT scan_id, raw_published FROM {RUNS_TABLE} "
            "WHERE status='published' ORDER BY scan_id"
        ).fetchall() == [("new-scan", True), ("old-scan", False)]
        assert conn.execute(
            f"SELECT DISTINCT scan_id FROM {FILLS_TABLE}"
        ).fetchall() == [("new-scan",)]

        assert _start(conn, "old-scan") is False
        assert conn.execute(
            f"SELECT status, raw_published, verification_status "
            f"FROM {RUNS_TABLE} WHERE scan_id='old-scan'"
        ).fetchone() == ("running", False, "not_requested")
        assert (
            conn.execute(
                f"SELECT count(*) FROM {CHUNKS_TABLE} WHERE scan_id='old-scan'"
            ).fetchone()[0]
            == 0
        )
        assert conn.execute(
            f"SELECT DISTINCT scan_id FROM {FILLS_TABLE}"
        ).fetchall() == [("new-scan",)]


def test_restarting_a_superseded_scan_rolls_back_on_persistence_failure(duck) -> None:
    with duck.get_connection() as conn:
        _start(conn, "rollback-old")
        _record(
            conn,
            "rollback-old",
            STANDARD_V2_EXCHANGE,
            [_fill("rollback-old")],
        )
        _record(conn, "rollback-old", NEG_RISK_V2_EXCHANGE, [])
        publish_polygon_settlement_scan(
            conn,
            scan_id="rollback-old",
            target_ranges=_TARGETS,
        )

        _start(conn, "rollback-new", manifest_sha256="8" * 64)
        _record(
            conn,
            "rollback-new",
            STANDARD_V2_EXCHANGE,
            [_fill("rollback-new")],
        )
        _record(conn, "rollback-new", NEG_RISK_V2_EXCHANGE, [])
        publish_polygon_settlement_scan(
            conn,
            scan_id="rollback-new",
            target_ranges=_TARGETS,
        )

        with pytest.raises(RuntimeError, match="synthetic restart failure"):
            _start(_FailingRestartConnection(conn), "rollback-old")

        assert conn.execute(
            f"SELECT status, raw_published FROM {RUNS_TABLE} "
            "WHERE scan_id = 'rollback-old'"
        ).fetchone() == ("published", False)
        assert (
            conn.execute(
                f"SELECT count(*) FROM {CHUNKS_TABLE} WHERE scan_id = 'rollback-old'"
            ).fetchone()[0]
            == 2
        )


def test_chunk_insert_rolls_back_rows_when_primary_key_conflicts(duck) -> None:
    with duck.get_connection() as conn:
        _start(conn, "scan-conflict")
        row = _fill("scan-conflict")
        with pytest.raises(Exception):
            _record(conn, "scan-conflict", STANDARD_V2_EXCHANGE, [row, row])
        assert (
            conn.execute(
                f"SELECT count(*) FROM {STAGE_TABLE} WHERE scan_id='scan-conflict'"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                f"SELECT count(*) FROM {CHUNKS_TABLE} WHERE scan_id='scan-conflict'"
            ).fetchone()[0]
            == 0
        )


def test_scan_start_rejects_labels_provenance_and_empty_published_state(duck) -> None:
    with duck.get_connection() as conn:
        with pytest.raises(RuntimeError, match="does not exist"):
            record_polygon_settlement_failure(
                conn,
                scan_id="missing",
                error=RuntimeError("failure"),
            )
        with pytest.raises(RuntimeError, match="does not exist"):
            publish_polygon_settlement_scan(
                conn,
                scan_id="missing",
                target_ranges=_TARGETS,
            )

        with pytest.raises(ValueError, match="both fixed V2 exchanges"):
            start_polygon_settlement_scan(
                conn,
                scan_id="missing-exchange",
                manifest_version="1.0.0",
                manifest_sha256="1" * 64,
                normalizer_version="polygon-v2-settlement-v4",
                chain_id=137,
                provider_label="provider",
                provider_origin="https://rpc.example",
                finalized_head_number=99,
                finalized_head_hash=_HASH_A,
                target_ranges=_TARGETS[:1],
                boundary_blocks_sha256="2" * 64,
            )

        with pytest.raises(ValueError, match="target ranges are malformed"):
            start_polygon_settlement_scan(
                conn,
                scan_id="malformed-targets",
                manifest_version="1.0.0",
                manifest_sha256="1" * 64,
                normalizer_version="polygon-v2-settlement-v4",
                chain_id=137,
                provider_label="provider",
                provider_origin="https://rpc.example",
                finalized_head_number=99,
                finalized_head_hash=_HASH_A,
                target_ranges=[{"unexpected": 1}],
                boundary_blocks_sha256="2" * 64,
            )

        for label in (
            "",
            "x" * 65,
            "https://rpc.example/key",
            "provider?api_key=secret",
            "provider\nsecret",
        ):
            with pytest.raises(ValueError, match="provider_label"):
                start_polygon_settlement_scan(
                    conn,
                    scan_id=f"bad-{len(label)}",
                    manifest_version="1.0.0",
                    manifest_sha256="1" * 64,
                    normalizer_version="normalizer",
                    chain_id=137,
                    provider_label=label,
                    provider_origin="https://rpc.example",
                    finalized_head_number=99,
                    finalized_head_hash=_HASH_A,
                    target_ranges=_TARGETS,
                    boundary_blocks_sha256="2" * 64,
                )

        for index, origin in enumerate(
            (
                "https://user:secret@rpc.example",
                "https://rpc.example/secret",
                "https://rpc.example?key=secret",
                "https://rpc.example:bad",
                "https://RPC.EXAMPLE",
                "http://rpc.example",
            )
        ):
            with pytest.raises(ValueError, match="sanitized HTTPS origin"):
                start_polygon_settlement_scan(
                    conn,
                    scan_id=f"bad-origin-{index}",
                    manifest_version="1.0.0",
                    manifest_sha256="1" * 64,
                    normalizer_version="normalizer",
                    chain_id=137,
                    provider_label="provider",
                    provider_origin=origin,
                    finalized_head_number=99,
                    finalized_head_hash=_HASH_A,
                    target_ranges=_TARGETS,
                    boundary_blocks_sha256="2" * 64,
                )

        assert (
            start_polygon_settlement_scan(
                conn,
                scan_id="ipv6-origin",
                manifest_version="1.0.0",
                manifest_sha256="1" * 64,
                normalizer_version="normalizer",
                chain_id=137,
                provider_label="provider",
                provider_origin="https://[2001:4860:4860::8888]",
                finalized_head_number=99,
                finalized_head_hash=_HASH_A,
                target_ranges=_TARGETS,
                boundary_blocks_sha256="2" * 64,
            )
            is False
        )

        _start(conn, "resume")
        record_polygon_settlement_failure(
            conn, scan_id="resume", error=RuntimeError("temporary")
        )
        assert _start(conn, "resume") is False
        assert conn.execute(
            f"SELECT status, finalized_head_number FROM {RUNS_TABLE} "
            "WHERE scan_id='resume'"
        ).fetchone() == ("running", 99)
        with pytest.raises(RuntimeError, match="incompatible provenance"):
            _start(conn, "resume", manifest_sha256="f" * 64)
        with pytest.raises(
            RuntimeError, match="target-range provenance is incompatible"
        ):
            publish_polygon_settlement_scan(
                conn,
                scan_id="resume",
                target_ranges=[
                    {**_TARGETS[0], "to_block": 11},
                    _TARGETS[1],
                ],
            )

        _start(conn, "invalid-stored-targets")
        conn.execute(
            f"UPDATE {RUNS_TABLE} SET target_ranges_json = ?::JSON "
            "WHERE scan_id = 'invalid-stored-targets'",
            [json.dumps([{"unexpected": 1}])],
        )
        with pytest.raises(RuntimeError, match="target-range provenance is invalid"):
            publish_polygon_settlement_scan(
                conn,
                scan_id="invalid-stored-targets",
                target_ranges=_TARGETS,
            )

        _start(conn, "empty-published")
        conn.execute(
            f"UPDATE {RUNS_TABLE} SET status='published', raw_published=TRUE "
            "WHERE scan_id='empty-published'"
        )
        with pytest.raises(RuntimeError, match="no canonical rows"):
            _start(conn, "empty-published")
        with pytest.raises(RuntimeError, match="no canonical rows"):
            publish_polygon_settlement_scan(
                conn,
                scan_id="empty-published",
                target_ranges=_TARGETS,
            )


def test_chunk_validation_is_idempotent_and_rejects_conflicts_and_overlap(duck) -> None:
    with duck.get_connection() as conn:
        _start(conn, "chunks")
        invalid = [
            {"from_block": 13, "to_block": 12, "event_count": 0, "scoped": 0},
            {"from_block": 10, "to_block": 12, "event_count": 1, "scoped": 2},
            {"from_block": 10, "to_block": 12, "event_count": 0, "scoped": -1},
        ]
        for values in invalid:
            with pytest.raises(ValueError, match="Invalid Polygon chunk"):
                record_polygon_settlement_chunk(
                    conn,
                    scan_id="chunks",
                    exchange_address=STANDARD_V2_EXCHANGE,
                    from_block=values["from_block"],
                    to_block=values["to_block"],
                    from_block_hash=_HASH_A,
                    to_block_hash=_HASH_B,
                    event_count=values["event_count"],
                    scoped_event_count=values["scoped"],
                    scoped_event_sha256="7" * 64,
                    rows=[],
                    metrics=_metrics(values["event_count"], values["scoped"]),
                )

        missing_metric = _metrics(0, 0)
        missing_metric.pop("duration_ms")
        negative_metric = {**_metrics(0, 0), "retry_count": -1}
        inconsistent_metric = {
            **_metrics(0, 0),
            "eligible_discovery_count": 1,
        }
        for metrics, message in (
            (missing_metric, "Invalid Polygon chunk metrics"),
            (negative_metric, "Invalid Polygon chunk metrics"),
            (inconsistent_metric, "Inconsistent Polygon chunk metrics"),
        ):
            with pytest.raises(ValueError, match=message):
                record_polygon_settlement_chunk(
                    conn,
                    scan_id="chunks",
                    exchange_address=STANDARD_V2_EXCHANGE,
                    from_block=10,
                    to_block=12,
                    from_block_hash=_HASH_A,
                    to_block_hash=_HASH_B,
                    event_count=0,
                    scoped_event_count=0,
                    scoped_event_sha256="7" * 64,
                    rows=[],
                    metrics=metrics,
                )

        bad_row = {**_fill("chunks"), "scan_id": "other"}
        with pytest.raises(ValueError, match="do not belong"):
            _record(conn, "chunks", STANDARD_V2_EXCHANGE, [bad_row])

        row = _fill("chunks")
        _record(conn, "chunks", STANDARD_V2_EXCHANGE, [row])
        _record(conn, "chunks", STANDARD_V2_EXCHANGE, [row])
        with pytest.raises(RuntimeError, match="conflicting payload"):
            record_polygon_settlement_chunk(
                conn,
                scan_id="chunks",
                exchange_address=STANDARD_V2_EXCHANGE,
                from_block=10,
                to_block=12,
                from_block_hash=_HASH_A,
                to_block_hash=_HASH_B,
                event_count=3,
                scoped_event_count=3,
                scoped_event_sha256="8" * 64,
                rows=[row],
                metrics=_metrics(3, 3),
            )
        with pytest.raises(RuntimeError, match="must not overlap"):
            record_polygon_settlement_chunk(
                conn,
                scan_id="chunks",
                exchange_address=STANDARD_V2_EXCHANGE,
                from_block=11,
                to_block=13,
                from_block_hash=_HASH_A,
                to_block_hash=_HASH_B,
                event_count=0,
                scoped_event_count=0,
                scoped_event_sha256="7" * 64,
                rows=[],
                metrics=_metrics(0, 0),
            )
        record_polygon_settlement_failure(
            conn,
            scan_id="chunks",
            error=RuntimeError("failed after commit"),
            exchange_address=STANDARD_V2_EXCHANGE,
            from_block=10,
            to_block=12,
        )
        assert (
            conn.execute(
                f"SELECT status FROM {CHUNKS_TABLE} WHERE scan_id='chunks' "
                "AND lower(exchange_address)=lower(?) AND from_block=10 AND to_block=12",
                [STANDARD_V2_EXCHANGE],
            ).fetchone()[0]
            == "success"
        )


def test_publish_removes_superseded_parent_failure_after_leaf_resume(duck) -> None:
    with duck.get_connection() as conn:
        _start(conn, "adaptive-resume")
        first_leaf = {
            **_fill("adaptive-resume"),
            "chunk_from_block": 10,
            "chunk_to_block": 10,
            "block_number": 10,
        }
        record_polygon_settlement_chunk(
            conn,
            scan_id="adaptive-resume",
            exchange_address=STANDARD_V2_EXCHANGE,
            from_block=10,
            to_block=10,
            from_block_hash=_HASH_A,
            to_block_hash=_HASH_A,
            event_count=1,
            scoped_event_count=1,
            scoped_event_sha256="7" * 64,
            rows=[first_leaf],
            metrics=_metrics(1, 1),
        )
        record_polygon_settlement_failure(
            conn,
            scan_id="adaptive-resume",
            error=RuntimeError("later adaptive sibling failed"),
            exchange_address=STANDARD_V2_EXCHANGE,
            from_block=10,
            to_block=12,
        )
        assert (
            conn.execute(
                f"SELECT count(*) FROM {CHUNKS_TABLE} "
                "WHERE scan_id='adaptive-resume' AND status='failed'"
            ).fetchone()[0]
            == 1
        )

        assert _start(conn, "adaptive-resume") is False
        _empty_range(conn, "adaptive-resume", STANDARD_V2_EXCHANGE, 11, 12)
        _empty_range(conn, "adaptive-resume", NEG_RISK_V2_EXCHANGE, 10, 12)
        assert (
            publish_polygon_settlement_scan(
                conn,
                scan_id="adaptive-resume",
                target_ranges=_TARGETS,
            )
            == 1
        )

        assert (
            conn.execute(
                f"SELECT count(*) FROM {CHUNKS_TABLE} "
                "WHERE scan_id='adaptive-resume' AND status <> 'success'"
            ).fetchone()[0]
            == 0
        )
        assert conn.execute(
            f"SELECT status, raw_published FROM {RUNS_TABLE} "
            "WHERE scan_id='adaptive-resume'"
        ).fetchone() == ("published", True)


def _empty_range(conn, scan_id: str, address: str, start: int, end: int) -> None:
    record_polygon_settlement_chunk(
        conn,
        scan_id=scan_id,
        exchange_address=address,
        from_block=start,
        to_block=end,
        from_block_hash=_HASH_A,
        to_block_hash=_HASH_B,
        event_count=0,
        scoped_event_count=0,
        scoped_event_sha256="7" * 64,
        rows=[],
        metrics=_metrics(0, 0),
    )


def test_publication_rejects_gap_extra_empty_drift_and_invalid_run_state(duck) -> None:
    with duck.get_connection() as conn:
        _start(conn, "gap")
        _empty_range(conn, "gap", STANDARD_V2_EXCHANGE, 10, 10)
        _empty_range(conn, "gap", STANDARD_V2_EXCHANGE, 12, 12)
        _empty_range(conn, "gap", NEG_RISK_V2_EXCHANGE, 10, 12)
        with pytest.raises(RuntimeError, match="gap or overlap"):
            publish_polygon_settlement_scan(
                conn,
                scan_id="gap",
                target_ranges=_TARGETS,
            )

        _start(conn, "partial")
        _empty_range(conn, "partial", STANDARD_V2_EXCHANGE, 10, 11)
        _empty_range(conn, "partial", NEG_RISK_V2_EXCHANGE, 10, 12)
        with pytest.raises(RuntimeError, match="fully cover"):
            publish_polygon_settlement_scan(
                conn,
                scan_id="partial",
                target_ranges=_TARGETS,
            )

        _start(conn, "extra")
        for address in (STANDARD_V2_EXCHANGE, NEG_RISK_V2_EXCHANGE):
            _empty_range(conn, "extra", address, 10, 12)
        _empty_range(conn, "extra", STANDARD_V2_EXCHANGE, 13, 13)
        with pytest.raises(RuntimeError, match="outside target ranges"):
            publish_polygon_settlement_scan(
                conn,
                scan_id="extra",
                target_ranges=_TARGETS,
            )

        _start(conn, "empty")
        for address in (STANDARD_V2_EXCHANGE, NEG_RISK_V2_EXCHANGE):
            _empty_range(conn, "empty", address, 10, 12)
        with pytest.raises(RuntimeError, match="stage must be non-empty"):
            publish_polygon_settlement_scan(
                conn,
                scan_id="empty",
                target_ranges=_TARGETS,
            )

        _start(conn, "drift")
        _record(conn, "drift", STANDARD_V2_EXCHANGE, [_fill("drift")])
        _record(conn, "drift", NEG_RISK_V2_EXCHANGE, [])
        conn.execute(
            f"UPDATE {CHUNKS_TABLE} SET normalized_fill_count=2 "
            "WHERE scan_id='drift' AND normalized_fill_count=1"
        )
        with pytest.raises(RuntimeError, match="1/2"):
            publish_polygon_settlement_scan(
                conn,
                scan_id="drift",
                target_ranges=_TARGETS,
            )

        _start(conn, "bad-state")
        _record(conn, "bad-state", STANDARD_V2_EXCHANGE, [_fill("bad-state")])
        _record(conn, "bad-state", NEG_RISK_V2_EXCHANGE, [])
        conn.execute(
            f"UPDATE {RUNS_TABLE} SET status='published' WHERE scan_id='bad-state'"
        )
        with pytest.raises(RuntimeError, match="not publishable"):
            publish_polygon_settlement_scan(
                conn,
                scan_id="bad-state",
                target_ranges=_TARGETS,
            )
        assert (
            conn.execute(
                f"SELECT count(*) FROM {STAGE_TABLE} WHERE scan_id='bad-state'"
            ).fetchone()[0]
            == 1
        )


def test_verification_and_release_provenance_fail_closed(duck) -> None:
    with duck.get_connection() as conn:
        with pytest.raises(RuntimeError, match="exactly one canonical"):
            load_polygon_settlement_release_provenance(conn)
        with pytest.raises(ValueError, match="Invalid Polygon verification"):
            set_polygon_verification_status(conn, "missing", "unknown")
        with pytest.raises(ValueError, match="provider_label"):
            set_polygon_verification_status(conn, "missing", "error", provider_label="")
        with pytest.raises(ValueError, match="provider_label"):
            set_polygon_verification_status(
                conn,
                "missing",
                "error",
                provider_label="https://verify.example/api_key=secret",
            )
        with pytest.raises(ValueError, match="sanitized HTTPS origin"):
            set_polygon_verification_status(
                conn,
                "missing",
                "error",
                provider_label="verify",
                provider_origin="https://verify.example/secret",
            )

        _start(conn, "unpublished")
        set_polygon_verification_status(conn, "unpublished", "not_requested")
        _record(conn, "unpublished", STANDARD_V2_EXCHANGE, [_fill("unpublished")])
        columns = ", ".join(FILL_COLUMNS)
        conn.execute(
            f"INSERT INTO {FILLS_TABLE} ({columns}) "
            f"SELECT {columns} FROM {STAGE_TABLE} WHERE scan_id='unpublished'"
        )
        with pytest.raises(RuntimeError, match="not published"):
            load_polygon_settlement_release_provenance(conn)
        set_polygon_verification_status(
            conn,
            "unpublished",
            "mismatched",
            provider_label="verify",
            provider_origin="https://verify.example",
        )
        assert (
            conn.execute(
                f"SELECT verification_status FROM {RUNS_TABLE} "
                "WHERE scan_id='unpublished'"
            ).fetchone()[0]
            == "mismatched"
        )

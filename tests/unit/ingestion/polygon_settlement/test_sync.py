from __future__ import annotations

import json
from datetime import timedelta
from threading import Lock, get_ident
from time import sleep

import pytest
from tests.unit.ingestion.polygon_settlement.conftest import (
    _raw_log,
    polygon_settlement_module,
)
from tests.unit.ingestion.polygon_settlement.conftest import (
    build_manifest as _manifest,
)

import oddsfox_pipeline.ingestion.polymarket.polygon_settlement_sync as polygon_settlement_sync_module
from oddsfox_pipeline.ingestion.polymarket.polygon_rpc import (
    EVENT_TOPICS,
    PolygonBlock,
    PolygonReceipt,
    PolygonRPCError,
    PolygonRPCSizeLimitError,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    NEG_RISK_V2_EXCHANGE,
    STANDARD_V2_EXCHANGE,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_settlement import (
    NORMALIZER_VERSION,
    PolygonSettlementSyncConfig,
    sync_polygon_settlement_fills,
    verify_polygon_settlement_scan,
)
from oddsfox_pipeline.resources.http import RateLimiter


def test_status_root_is_under_pipeline_base_dir() -> None:
    assert polygon_settlement_module._STATUS_ROOT == (
        polygon_settlement_module.BASE_DIR / ".cache" / "polygon_settlement" / "status"
    )


def test_status_json_is_atomic_allowlisted_and_rejects_endpoint_fields(
    tmp_path,
) -> None:
    path = tmp_path / "status.json"
    polygon_settlement_module._write_status(
        path,
        {"scan_id": "scan", "version": NORMALIZER_VERSION, "status": "running"},
    )
    assert json.loads(path.read_text(encoding="utf-8")) == {
        "scan_id": "scan",
        "status": "running",
        "version": NORMALIZER_VERSION,
    }
    assert not list(tmp_path.glob("*.tmp"))
    with pytest.raises(ValueError, match="prohibited field"):
        polygon_settlement_module._write_status(
            path, {"scan_id": "scan", "rpc_url": "https://secret.invalid/key"}
        )


@pytest.mark.parametrize("already_missing", [False, True])
def test_status_json_removes_temporary_file_after_atomic_replace_failure(
    tmp_path, monkeypatch, already_missing
) -> None:
    path = tmp_path / "status.json"
    monkeypatch.setattr(
        polygon_settlement_module.os,
        "replace",
        lambda *_args: (_ for _ in ()).throw(OSError("synthetic replace failure")),
    )
    if already_missing:
        monkeypatch.setattr(
            polygon_settlement_module.os,
            "unlink",
            lambda *_args: (_ for _ in ()).throw(FileNotFoundError()),
        )
    with pytest.raises(OSError, match="synthetic replace failure"):
        polygon_settlement_module._write_status(path, {"scan_id": "scan"})
    if not already_missing:
        assert not list(tmp_path.iterdir())


class _SyncRPC:
    def __init__(
        self,
        manifest,
        *,
        fail_neg_risk=False,
        collateral=600_000,
        origin="https://rpc.example",
        hash_overrides=None,
    ):
        self.manifest = manifest
        self.fail_neg_risk = fail_neg_risk
        self.collateral = collateral
        self.origin = origin
        self.hash_overrides = hash_overrides or {}

    def chain_id(self):
        return 137

    def finalized_head(self):
        return PolygonBlock(
            200,
            f"0x{200:064x}",
            self.manifest.markets[0].window_end_at_utc + timedelta(days=1),
        )

    def first_block_at_or_after(self, timestamp, *, finalized_head, low=0):
        del finalized_head, low
        return 100 if timestamp == self.manifest.markets[0].window_start_at_utc else 101

    def block(self, number):
        if number in self.hash_overrides:
            block_hash = self.hash_overrides[number]
            timestamp = self.manifest.markets[0].window_start_at_utc + timedelta(
                seconds=number - 100
            )
        elif number == 100:
            block_hash = "0x" + "1" * 64
            timestamp = self.manifest.markets[0].window_start_at_utc
        else:
            block_hash = f"0x{number:064x}"
            timestamp = self.manifest.markets[0].window_start_at_utc + timedelta(
                seconds=number - 100
            )
        return PolygonBlock(number, block_hash, timestamp)

    def blocks(self, numbers):
        return {number: self.block(number) for number in dict.fromkeys(numbers)}

    def _settlement_rows(self):
        market = self.manifest.markets[0]
        return [
            _raw_log(
                "order_filled",
                1,
                int(market.yes_token_id),
                1_000_000,
                self.collateral,
                1,
            ),
            _raw_log(
                "order_filled",
                0,
                int(market.yes_token_id),
                self.collateral,
                1_000_000,
                2,
            ),
            _raw_log(
                "orders_matched",
                0,
                int(market.yes_token_id),
                self.collateral,
                1_000_000,
                3,
            ),
        ]

    def logs(self, address, start, end, *, event_topics=EVENT_TOPICS):
        if self.fail_neg_risk and address.casefold() != STANDARD_V2_EXCHANGE.casefold():
            raise PolygonRPCError("secondary provider range failure")
        if (
            address.casefold() != STANDARD_V2_EXCHANGE.casefold()
            or not start <= 100 <= end
        ):
            return []
        allowed = set(event_topics)
        return [row for row in self._settlement_rows() if row["topics"][0] in allowed]

    def transaction_receipts(self, transaction_hashes):
        rows = self._settlement_rows()
        return {
            transaction_hash: PolygonReceipt(
                transaction_hash=transaction_hash,
                block_number=100,
                block_hash="0x" + "1" * 64,
                transaction_index=0,
                logs=tuple(rows),
            )
            for transaction_hash in dict.fromkeys(transaction_hashes)
        }


class _MainThreadConnection:
    """DuckDB proxy that fails if a worker thread reaches persistence."""

    def __init__(self, conn, owner: int) -> None:
        self._conn = conn
        self._owner = owner

    def execute(self, *args, **kwargs):
        assert get_ident() == self._owner
        return self._conn.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        assert get_ident() == self._owner
        return self._conn.executemany(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _FailingExecuteConnection:
    def __init__(self, conn, marker: str) -> None:
        self._conn = conn
        self._marker = marker

    def execute(self, query, *args, **kwargs):
        if self._marker in query:
            raise RuntimeError("synthetic persistence failure")
        return self._conn.execute(query, *args, **kwargs)


def test_concurrent_sync_shares_one_limiter_and_keeps_duckdb_on_main(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    main_thread = get_ident()
    creations: list[tuple[int, object, int]] = []
    lock = Lock()
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "load_polygon_market_seed",
        lambda _path: manifest,
    )

    def rpc_factory(_url, **kwargs):
        result = _SyncRPC(manifest)
        with lock:
            creations.append((get_ident(), kwargs.get("rate_limiter"), id(result)))
        return result

    monkeypatch.setattr(polygon_settlement_sync_module, "PolygonRPC", rpc_factory)
    with duck.get_connection() as conn:
        summary = sync_polygon_settlement_fills(
            _MainThreadConnection(conn, main_thread),
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            config=PolygonSettlementSyncConfig(initial_block_chunk_size=250),
        )

    assert summary["published"] is True
    primary = [row for row in creations if row[0] == main_thread]
    workers = [row for row in creations if row[0] != main_thread]
    assert len(primary) == 1
    assert workers
    limiter = primary[0][1]
    assert isinstance(limiter, RateLimiter)
    assert all(row[1] is limiter for row in workers)
    assert len({row[2] for row in creations}) == len(creations)


def test_concurrent_sync_adaptively_splits_worker_rpc_error(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    main_thread = get_ident()
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "load_polygon_market_seed",
        lambda _path: manifest,
    )

    class PrimaryRPC(_SyncRPC):
        def finalized_head(self):
            return PolygonBlock(
                1_000,
                f"0x{1_000:064x}",
                manifest.markets[0].window_end_at_utc + timedelta(days=1),
            )

        def first_block_at_or_after(self, timestamp, **_kwargs):
            return 100 if timestamp == manifest.markets[0].window_start_at_utc else 600

        def logs(self, address, start, end, *, event_topics=EVENT_TOPICS):
            if address.casefold() == STANDARD_V2_EXCHANGE.casefold() and (
                start,
                end,
            ) == (99, 600):
                raise PolygonRPCSizeLimitError("provider range limit")
            return super().logs(address, start, end, event_topics=event_topics)

    class WorkerRPC(_SyncRPC):
        def logs(self, address, start, end, *, event_topics=EVENT_TOPICS):
            if address.casefold() == STANDARD_V2_EXCHANGE.casefold() and (
                start,
                end,
            ) == (99, 600):
                raise PolygonRPCSizeLimitError("provider range limit")
            return super().logs(address, start, end, event_topics=event_topics)

    def rpc_factory(_url, **_kwargs):
        cls = PrimaryRPC if get_ident() == main_thread else WorkerRPC
        return cls(manifest)

    monkeypatch.setattr(polygon_settlement_sync_module, "PolygonRPC", rpc_factory)
    with duck.get_connection() as conn:
        summary = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            config=PolygonSettlementSyncConfig(initial_block_chunk_size=600),
        )
        standard_chunks = conn.execute(
            f"""
            SELECT from_block, to_block, event_count
            FROM {polygon_settlement_module.CHUNKS_TABLE}
            WHERE exchange_address = ? AND status = 'success'
            ORDER BY from_block
            """,
            [STANDARD_V2_EXCHANGE.casefold()],
        ).fetchall()

    assert summary["fill_count"] == 1
    assert standard_chunks == [(99, 349, 4), (350, 600, 0)]


def test_concurrent_sync_cancels_unsubmitted_work_and_attributes_failure(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    main_thread = get_ident()
    started: list[tuple[str, int, int]] = []
    lock = Lock()
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "load_polygon_market_seed",
        lambda _path: manifest,
    )

    class WorkerRPC(_SyncRPC):
        def logs(self, address, start, end, *, event_topics=EVENT_TOPICS):
            with lock:
                started.append((address.casefold(), start, end))
            if address.casefold() == STANDARD_V2_EXCHANGE.casefold() and (
                start,
                end,
            ) == (99, 101):
                raise RuntimeError("synthetic worker failure")
            sleep(0.02)
            return super().logs(address, start, end, event_topics=event_topics)

    def rpc_factory(_url, **_kwargs):
        if get_ident() == main_thread:
            return _SyncRPC(manifest)
        return WorkerRPC(manifest)

    monkeypatch.setattr(polygon_settlement_sync_module, "PolygonRPC", rpc_factory)
    with duck.get_connection() as conn:
        with pytest.raises(RuntimeError, match="synthetic worker failure"):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example/key",
                provider_label="primary",
                config=PolygonSettlementSyncConfig(initial_block_chunk_size=250),
            )
        failed = conn.execute(
            f"""
            SELECT exchange_address, from_block, to_block, error_type
            FROM {polygon_settlement_module.CHUNKS_TABLE}
            WHERE status = 'failed'
            """
        ).fetchall()

    assert {address for address, _start, _end in started} == {
        STANDARD_V2_EXCHANGE.casefold()
    }
    assert failed == [(STANDARD_V2_EXCHANGE.casefold(), 99, 101, "RuntimeError")]


def test_sync_preflight_requires_credentials_chain_ranges_and_constructs_client(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "load_polygon_market_seed",
        lambda _path: manifest,
    )
    with duck.get_connection() as conn:
        for rpc_url, label in (("", "provider"), ("https://rpc.example", "")):
            with pytest.raises(ValueError, match="are required"):
                sync_polygon_settlement_fills(
                    conn,
                    seed_path=tmp_path / "unused.csv",
                    rpc_url=rpc_url,
                    provider_label=label,
                    client=_SyncRPC(manifest),
                )

        with pytest.raises(ValueError, match="safe 1-64 character"):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example",
                provider_label="https://rpc.example/api_key=secret",
                client=_SyncRPC(manifest),
            )

        wrong_chain = _SyncRPC(manifest)
        wrong_chain.chain_id = lambda: 1
        with pytest.raises(PolygonRPCError, match="chain ID 137"):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example",
                provider_label="provider",
                client=wrong_chain,
            )

        monkeypatch.setattr(
            polygon_settlement_sync_module,
            "build_polygon_scan_plan",
            lambda *_args: polygon_settlement_module.PolygonScanPlan((), {}),
        )
        with pytest.raises(RuntimeError, match="no target block ranges"):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example",
                provider_label="provider",
                client=_SyncRPC(manifest),
            )

        created = {}

        def rpc_factory(url, **kwargs):
            created.update(url=url, **kwargs)
            result = _SyncRPC(manifest)
            result.chain_id = lambda: 1
            return result

        monkeypatch.setattr(polygon_settlement_sync_module, "PolygonRPC", rpc_factory)
        with pytest.raises(PolygonRPCError, match="chain ID 137"):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example/key",
                provider_label="provider",
            )
        assert created["url"] == "https://rpc.example/key"
        assert created["retries"] == 4


@pytest.mark.parametrize("disappears", [False, True])
def test_sync_rechecks_scan_that_storage_reports_as_published(
    duck, monkeypatch, tmp_path, disappears
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "load_polygon_market_seed",
        lambda _path: manifest,
    )
    expected = None if disappears else {"scan_id": "published", "published": True}
    summaries = iter((None, expected))
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "_offline_published_summary",
        lambda *_args: next(summaries),
    )
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "start_polygon_settlement_scan",
        lambda *_args, **_kwargs: True,
    )
    with duck.get_connection() as conn:
        if disappears:
            with pytest.raises(RuntimeError, match="disappeared during startup"):
                sync_polygon_settlement_fills(
                    conn,
                    seed_path=tmp_path / "unused.csv",
                    rpc_url="https://rpc.example/key",
                    provider_label="primary",
                    client=_SyncRPC(manifest),
                )
        else:
            assert (
                sync_polygon_settlement_fills(
                    conn,
                    seed_path=tmp_path / "unused.csv",
                    rpc_url="https://rpc.example/key",
                    provider_label="primary",
                    client=_SyncRPC(manifest),
                )
                == expected
            )


def test_sync_worker_rpc_reports_activity_and_requires_shared_limiter(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "load_polygon_market_seed",
        lambda _path: manifest,
    )
    activity = []

    class CallbackRPC(_SyncRPC):
        def __init__(self, callback=None):
            super().__init__(manifest)
            self.callback = callback

        def logs(self, *args, **kwargs):
            if self.callback is not None:
                self.callback("eth_getLogs")
                activity.append("eth_getLogs")
            return super().logs(*args, **kwargs)

    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "PolygonRPC",
        lambda _url, **kwargs: CallbackRPC(kwargs.get("activity_callback")),
    )
    with duck.get_connection() as conn:
        summary = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
        )
        for table in (
            polygon_settlement_module.FILLS_TABLE,
            polygon_settlement_module.STAGE_TABLE,
            polygon_settlement_module.CHUNKS_TABLE,
            polygon_settlement_module.RUNS_TABLE,
        ):
            conn.execute(f"DELETE FROM {table}")
    assert summary["published"] is True
    assert activity

    monkeypatch.setattr(
        polygon_settlement_sync_module, "RateLimiter", lambda _rps: None
    )
    with duck.get_connection() as conn:
        with pytest.raises(RuntimeError, match="shared limiter"):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example/key",
                provider_label="primary",
            )


def test_sync_resumes_successful_leaves_publishes_and_short_circuits(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.polygon_settlement_sync.load_polygon_market_seed",
        lambda _path: manifest,
    )
    config = PolygonSettlementSyncConfig(initial_block_chunk_size=250)

    class WideRPC(_SyncRPC):
        def finalized_head(self):
            return PolygonBlock(
                1_000,
                f"0x{1_000:064x}",
                manifest.markets[0].window_end_at_utc + timedelta(days=1),
            )

        def first_block_at_or_after(self, timestamp, **_kwargs):
            return 100 if timestamp == manifest.markets[0].window_start_at_utc else 600

    class InterruptedRPC(WideRPC):
        def logs(self, address, start, end, *, event_topics=EVENT_TOPICS):
            if start >= 349:
                raise PolygonRPCError("synthetic interruption")
            return super().logs(address, start, end, event_topics=event_topics)

    with duck.get_connection() as conn:
        with pytest.raises(PolygonRPCError):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example/key",
                provider_label="primary",
                config=config,
                client=InterruptedRPC(manifest),
            )
        summary = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            config=config,
            client=WideRPC(manifest),
        )
        assert summary["published"] is True
        assert summary["resumed_chunk_count"] == 2
        assert summary["fill_count"] == 1

        repeated = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="",
            provider_label="",
            config=config,
        )
        assert repeated["short_circuited"] is True
        assert repeated["offline"] is True
        assert repeated["fill_count"] == 1


@pytest.mark.parametrize(
    ("corruption", "message"),
    [
        ("provenance", "provenance is inconsistent"),
        ("missing_exchange", "incomplete exchange coverage"),
        ("gap", "gap or overlap"),
        ("incomplete", "incomplete coverage"),
        ("outside", "extends outside"),
        ("canonical", "canonical fills are inconsistent"),
    ],
)
def test_offline_published_scan_revalidates_all_local_invariants(
    duck, monkeypatch, tmp_path, corruption, message
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "load_polygon_market_seed",
        lambda _path: manifest,
    )
    with duck.get_connection() as conn:
        summary = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            client=_SyncRPC(manifest),
        )
        scan_id = summary["scan_id"]
        if corruption == "provenance":
            conn.execute(
                f"UPDATE {polygon_settlement_module.RUNS_TABLE} "
                "SET boundary_blocks_sha256 = ? WHERE scan_id = ?",
                ["0" * 64, scan_id],
            )
        elif corruption == "missing_exchange":
            conn.execute(
                f"DELETE FROM {polygon_settlement_module.CHUNKS_TABLE} "
                "WHERE scan_id = ?",
                [scan_id],
            )
        elif corruption == "gap":
            conn.execute(
                f"UPDATE {polygon_settlement_module.CHUNKS_TABLE} "
                "SET from_block = 100 WHERE scan_id = ?",
                [scan_id],
            )
        elif corruption == "incomplete":
            conn.execute(
                f"UPDATE {polygon_settlement_module.CHUNKS_TABLE} "
                "SET to_block = 100 WHERE scan_id = ?",
                [scan_id],
            )
        elif corruption == "outside":
            conn.execute(
                f"""
                INSERT INTO {polygon_settlement_module.CHUNKS_TABLE} (
                    scan_id, exchange_address, from_block, to_block,
                    from_block_hash, to_block_hash, status, event_count,
                    scoped_event_count, normalized_fill_count, scoped_event_sha256
                ) VALUES (?, ?, 102, 102, ?, ?, 'success', 0, 0, 0, ?)
                """,
                [
                    scan_id,
                    STANDARD_V2_EXCHANGE.casefold(),
                    "0x" + "1" * 64,
                    "0x" + "1" * 64,
                    "1" * 64,
                ],
            )
        else:
            conn.execute(f"DELETE FROM {polygon_settlement_module.FILLS_TABLE}")

        with pytest.raises(RuntimeError, match=message):
            polygon_settlement_module._offline_published_summary(conn, manifest)


def test_sync_rejects_and_discards_stale_resumed_leaf_boundary_hash(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "load_polygon_market_seed",
        lambda _path: manifest,
    )
    config = PolygonSettlementSyncConfig(initial_block_chunk_size=250)

    class WideRPC(_SyncRPC):
        def finalized_head(self):
            return PolygonBlock(
                1_000,
                f"0x{1_000:064x}",
                manifest.markets[0].window_end_at_utc + timedelta(days=1),
            )

        def first_block_at_or_after(self, timestamp, **_kwargs):
            return 100 if timestamp == manifest.markets[0].window_start_at_utc else 600

    class InterruptedRPC(WideRPC):
        def logs(self, address, start, end, *, event_topics=EVENT_TOPICS):
            if start >= 349:
                raise PolygonRPCError("synthetic interruption")
            return super().logs(address, start, end, event_topics=event_topics)

    with duck.get_connection() as conn:
        with pytest.raises(PolygonRPCError):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example/key",
                provider_label="primary",
                config=config,
                client=InterruptedRPC(manifest),
            )
        scan_id = conn.execute(
            f"SELECT scan_id FROM {polygon_settlement_module.RUNS_TABLE}"
        ).fetchone()[0]

        with pytest.raises(RuntimeError, match="leaf boundary hash changed"):
            sync_polygon_settlement_fills(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://rpc.example/key",
                provider_label="primary",
                config=config,
                client=WideRPC(
                    manifest,
                    hash_overrides={348: "0x" + "8" * 64},
                ),
            )

        assert (
            conn.execute(
                f"""
            SELECT count(*) FROM {polygon_settlement_module.CHUNKS_TABLE}
            WHERE scan_id = ? AND exchange_address = ?
              AND from_block = 99 AND to_block = 100 AND status = 'success'
            """,
                [scan_id, STANDARD_V2_EXCHANGE.casefold()],
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                f"""
            SELECT count(*) FROM {polygon_settlement_module.STAGE_TABLE}
            WHERE scan_id = ? AND exchange_address = ?
              AND chunk_from_block = 99 AND chunk_to_block = 100
            """,
                [scan_id, STANDARD_V2_EXCHANGE.casefold()],
            ).fetchone()[0]
            == 0
        )

        recovered = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            config=config,
            client=_SyncRPC(manifest),
        )
        assert recovered["published"] is True
        assert recovered["fill_count"] == 1


def test_published_scan_ignores_stale_header_cleanup(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "load_polygon_market_seed",
        lambda _path: manifest,
    )
    with duck.get_connection() as conn:
        summary = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            client=_SyncRPC(manifest),
        )

        completed = polygon_settlement_module._revalidate_resumed_chunk_headers(
            conn,
            _SyncRPC(manifest, hash_overrides={99: "0x" + "8" * 64}),
            summary["scan_id"],
        )

        assert completed == {
            STANDARD_V2_EXCHANGE.casefold(): [(99, 101)],
            NEG_RISK_V2_EXCHANGE.casefold(): [(99, 101)],
        }
        assert (
            conn.execute(
                f"SELECT count(*) FROM {polygon_settlement_module.CHUNKS_TABLE} "
                "WHERE scan_id = ? AND status = 'success'",
                [summary["scan_id"]],
            ).fetchone()[0]
            == 2
        )


def _create_interrupted_scan(conn, manifest, tmp_path) -> str:
    class InterruptedRPC(_SyncRPC):
        def logs(self, *_args, **_kwargs):
            raise PolygonRPCError("synthetic interruption")

    with pytest.raises(PolygonRPCError):
        sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            client=InterruptedRPC(manifest),
        )
    return str(
        conn.execute(
            f"SELECT scan_id FROM {polygon_settlement_module.RUNS_TABLE}"
        ).fetchone()[0]
    )


def test_stale_header_cleanup_rolls_back_on_persistence_failure(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "load_polygon_market_seed",
        lambda _path: manifest,
    )
    with duck.get_connection() as conn:
        scan_id = _create_interrupted_scan(conn, manifest, tmp_path)
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {polygon_settlement_module.CHUNKS_TABLE} (
                scan_id, exchange_address, from_block, to_block,
                from_block_hash, to_block_hash, status, event_count,
                scoped_event_count, normalized_fill_count, scoped_event_sha256
            ) VALUES (?, ?, 99, 101, ?, ?, 'success', 0, 0, 0, ?)
            """,
            [
                scan_id,
                STANDARD_V2_EXCHANGE.casefold(),
                "0x" + "9" * 64,
                f"0x{101:064x}",
                "7" * 64,
            ],
        )
        proxy = _FailingExecuteConnection(
            conn,
            f"DELETE FROM {polygon_settlement_module.CHUNKS_TABLE}",
        )

        with pytest.raises(RuntimeError, match="synthetic persistence failure"):
            polygon_settlement_module._revalidate_resumed_chunk_headers(
                proxy,
                _SyncRPC(manifest, hash_overrides={99: "0x" + "8" * 64}),
                scan_id,
            )

        assert (
            conn.execute(
                f"SELECT count(*) FROM {polygon_settlement_module.CHUNKS_TABLE} "
                "WHERE scan_id = ? AND status = 'success'",
                [scan_id],
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                f"SELECT count(*) FROM {polygon_settlement_module.STAGE_TABLE} "
                "WHERE scan_id = ?",
                [scan_id],
            ).fetchone()[0]
            == 0
        )


def test_sync_accepts_a_parent_chunk_iterator_without_close(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    primary = _SyncRPC(manifest)
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "load_polygon_market_seed",
        lambda _path: manifest,
    )
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "PolygonRPC",
        lambda _url, **_kwargs: primary,
    )

    concurrent_results = polygon_settlement_module._concurrent_leaf_results

    def results_without_close(*args, **kwargs):
        return iter(list(concurrent_results(*args, **kwargs)))

    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "_concurrent_leaf_results",
        results_without_close,
    )
    with duck.get_connection() as conn:
        summary = sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            config=PolygonSettlementSyncConfig(initial_block_chunk_size=250),
        )

    assert summary["published"] is True
    assert summary["fill_count"] == 1


def test_secondary_verification_reports_match_mismatch_and_error(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.polygon_settlement_sync.load_polygon_market_seed",
        lambda _path: manifest,
    )
    with duck.get_connection() as conn:
        sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/key",
            provider_label="primary",
            client=_SyncRPC(manifest),
        )
        with pytest.raises(ValueError, match="safe 1-64 character"):
            verify_polygon_settlement_scan(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://verify.example/key",
                provider_label="verify?api_key=secret",
                client=_SyncRPC(manifest, origin="https://verify.example"),
            )
        matched = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://verify.example/key",
            provider_label="verify",
            client=_SyncRPC(manifest, origin="https://verify.example"),
        )
        assert matched["verification_status"] == "matched"

        mismatched = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://verify.example/key",
            provider_label="verify",
            client=_SyncRPC(
                manifest,
                collateral=500_000,
                origin="https://verify.example",
            ),
        )
        assert mismatched["verification_status"] == "mismatched"
        assert mismatched["mismatched_chunks"]

        class ErrorRPC(_SyncRPC):
            def logs(self, *_args, **_kwargs):
                raise PolygonRPCError("synthetic verification failure")

        errored = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://verify.example/key",
            provider_label="verify",
            client=ErrorRPC(manifest, origin="https://verify.example"),
        )
        assert errored["verification_status"] == "error"

        non_independent = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example/other-key",
            provider_label="verify",
            client=_SyncRPC(manifest),
        )
        assert non_independent == {
            "scan_id": non_independent["scan_id"],
            "verification_status": "error",
            "error_type": "NonIndependentVerificationProvider",
        }
        assert conn.execute(
            f"""
            select verification_status, verification_provider_label,
                   verification_provider_origin
            from {polygon_settlement_module.RUNS_TABLE}
            where scan_id = ?
            """,
            [non_independent["scan_id"]],
        ).fetchone() == ("error", "verify", "https://rpc.example")

        same_label = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://verify.example/key",
            provider_label="primary",
            client=_SyncRPC(manifest, origin="https://verify.example"),
        )
        assert same_label["error_type"] == "NonIndependentVerificationProvider"


def test_secondary_verification_requires_canonical_and_handles_optional_and_chain(
    duck, monkeypatch, tmp_path
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        polygon_settlement_sync_module,
        "load_polygon_market_seed",
        lambda _path: manifest,
    )
    with duck.get_connection() as conn:
        with pytest.raises(RuntimeError, match="one canonical"):
            verify_polygon_settlement_scan(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://verify.example",
                provider_label="verify",
                client=_SyncRPC(manifest),
            )
        sync_polygon_settlement_fills(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://rpc.example",
            provider_label="primary",
            client=_SyncRPC(manifest),
        )
        not_requested = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="",
            provider_label="",
            client=_SyncRPC(manifest),
        )
        assert not_requested["verification_status"] == "not_requested"

        for rpc_url, provider_label in (
            ("https://verify.example", ""),
            ("", "verify"),
        ):
            misconfigured = verify_polygon_settlement_scan(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url=rpc_url,
                provider_label=provider_label,
                client=_SyncRPC(manifest, origin="https://verify.example"),
            )
            assert misconfigured == {
                "scan_id": misconfigured["scan_id"],
                "verification_status": "error",
                "error_type": "VerificationConfigurationError",
            }
            assert conn.execute(
                f"""
                select verification_status, verification_provider_label,
                       verification_provider_origin
                from {polygon_settlement_module.RUNS_TABLE}
                where scan_id = ?
                """,
                [misconfigured["scan_id"]],
            ).fetchone() == ("error", None, None)

        wrong_chain = _SyncRPC(manifest, origin="https://verify.example")
        wrong_chain.chain_id = lambda: 1
        errored = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://verify.example",
            provider_label="verify",
            client=wrong_chain,
        )
        assert errored == {
            "scan_id": errored["scan_id"],
            "verification_status": "error",
            "error_type": "PolygonRPCError",
        }

        monkeypatch.setattr(
            polygon_settlement_sync_module,
            "PolygonRPC",
            lambda _url: wrong_chain,
        )
        constructed = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://verify.example",
            provider_label="verify",
        )
        assert constructed["error_type"] == "PolygonRPCError"

        working_verifier = _SyncRPC(manifest, origin="https://verify.example")
        monkeypatch.setattr(
            polygon_settlement_sync_module,
            "PolygonRPC",
            lambda _url: working_verifier,
        )
        constructed_match = verify_polygon_settlement_scan(
            conn,
            seed_path=tmp_path / "unused.csv",
            rpc_url="https://verify.example",
            provider_label="verify",
        )
        assert constructed_match["verification_status"] == "matched"

        conn.execute(f"delete from {polygon_settlement_module.RUNS_TABLE}")
        with pytest.raises(RuntimeError, match="not published"):
            verify_polygon_settlement_scan(
                conn,
                seed_path=tmp_path / "unused.csv",
                rpc_url="https://verify.example",
                provider_label="verify",
                client=wrong_chain,
            )

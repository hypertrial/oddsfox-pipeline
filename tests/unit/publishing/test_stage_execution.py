from __future__ import annotations

import hashlib
import importlib.util
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oddsfox_pipeline.contracts.schema import schema_fingerprint
from oddsfox_pipeline.publishing import stage_execution as subject
from oddsfox_pipeline.publishing import stage_execution_archive as archive
from oddsfox_pipeline.publishing._bundle_io import sha256_file


def _file_metadata(path: Path) -> dict[str, object]:
    value: dict[str, object] = {
        "sha256": sha256_file(path),
        "byte_size": path.stat().st_size,
    }
    if path.suffix == ".parquet":
        parquet = pq.ParquetFile(path)
        value.update(
            row_count=parquet.metadata.num_rows,
            schema_fingerprint=schema_fingerprint(parquet.schema_arrow),
        )
    return value


def _finish_bundle(directory: Path, manifest: dict[str, object]) -> None:
    files = [
        path
        for path in directory.iterdir()
        if path.name not in {"MANIFEST.json", "CHECKSUMS.sha256"}
    ]
    manifest["files"] = {path.name: _file_metadata(path) for path in files}
    (directory / "MANIFEST.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    checksums = [
        f"{sha256_file(path)}  {path.name}"
        for path in sorted(directory.iterdir())
        if path.name != "CHECKSUMS.sha256"
    ]
    (directory / "CHECKSUMS.sha256").write_text(
        "\n".join(checksums) + "\n", encoding="utf-8"
    )


def _inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    release = tmp_path / "minute"
    report = tmp_path / "report"
    release.mkdir(parents=True)
    report.mkdir(parents=True)
    outcomes = [
        {
            "market_id": "m-source",
            "condition_id": "0x" + "1" * 64,
            "outcome_label": "Yes",
            "clob_token_id": "10",
        },
        {
            "market_id": "m-source",
            "condition_id": "0x" + "1" * 64,
            "outcome_label": "No",
            "clob_token_id": "11",
        },
        {
            "market_id": "m-target",
            "condition_id": "0x" + "2" * 64,
            "outcome_label": "Yes",
            "clob_token_id": "22",
        },
        {
            "market_id": "m-target",
            "condition_id": "0x" + "2" * 64,
            "outcome_label": "No",
            "clob_token_id": "23",
        },
    ]
    pq.write_table(pa.Table.from_pylist(outcomes), release / "outcomes.parquet")
    implications = [
        {
            "implication_id": implication,
            "source_clob_token_id": "10",
            "target_clob_token_id": "22",
            "team_name": "Test Team",
            "source_stage_key": "semifinals",
            "target_stage_key": "quarterfinals",
            "rule_id": "wc.stage_monotonicity",
        }
        for implication in ("edge-a", "edge-b")
    ]
    pq.write_table(pa.Table.from_pylist(implications), release / "implications.parquet")
    for name in ("token_minute_ohlc.parquet", "coverage.parquet"):
        pq.write_table(
            pa.table({"placeholder": pa.array([], type=pa.string())}), release / name
        )
    (release / "SCHEMA.json").write_text("{}\n", encoding="utf-8")
    _finish_bundle(
        release,
        {
            "contract_version": "oddsfox.polymarket_wc2026.stage_minute.v1",
            "dataset_version": "1.0.0",
        },
    )
    release_hash = sha256_file(release / "MANIFEST.json")
    monkeypatch.setattr(subject, "STAGE_MINUTE_MANIFEST_SHA256", release_hash)

    opportunities = []
    for implication in ("edge-a", "edge-b"):
        opportunities.append(
            {
                "implication_id": implication,
                "team_name": "Test Team",
                "rule_id": "wc.stage_monotonicity",
                "source_stage_key": "semifinals",
                "target_stage_key": "quarterfinals",
                "source_no_token_id": "11",
                "target_yes_token_id": "22",
                "signal_minute_epoch": 60,
                "signal_minute_utc": datetime(1970, 1, 1, 0, 1, tzinfo=timezone.utc),
                "source_no_close_price": 0.4,
                "target_yes_close_price": 0.5,
                "source_no_signal_fee": 0.0072,
                "target_yes_signal_fee": 0.0075,
                "signal_net_edge": 0.0853,
                "scenario_id": "primary_high_3pct",
            }
        )
    pq.write_table(
        pa.Table.from_pylist(opportunities), report / "opportunity_minutes.parquet"
    )
    for name in ("opportunity_episodes.parquet", "primary_entries.parquet"):
        pq.write_table(
            pa.table({"placeholder": pa.array([], type=pa.string())}), report / name
        )
    for name in ("scenario_summary.csv", "period_summary.csv"):
        (report / name).write_text("placeholder\n", encoding="utf-8")
    (report / "REPORT.md").write_text("# report\n", encoding="utf-8")
    _finish_bundle(
        report,
        {
            "report_contract": subject.OHLC_REPORT_CONTRACT,
            "strategy_revision": subject.OHLC_STRATEGY_SHA,
            "input": {"manifest_sha256": release_hash},
            "configuration": {"fee_rate": 0.03, "min_net_edge": 0.01},
            "periods": [
                {"day_count": 20},
                {"day_count": 10},
                {"day_count": 10},
            ],
        },
    )
    return release, report


def test_plan_validates_inputs_and_coalesces_same_token_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release, report = _inputs(tmp_path, monkeypatch)
    plan = subject.build_execution_plan(release, report, request_budget=4)
    assert plan.summary() == {
        "signals": 2,
        "legs": 4,
        "tokens": 2,
        "windows": 2,
        "minimum_requests": 4,
        "request_budget": 4,
        "within_budget": True,
        "estimated_storage_bytes": 32_768,
    }
    assert len({row["window_id"] for row in plan.legs}) == 2


def test_plan_rejects_tampering_and_absolute_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release, report = _inputs(tmp_path, monkeypatch)
    (report / "REPORT.md").write_text("tampered\n", encoding="utf-8")
    with pytest.raises(subject.StageExecutionError, match="checksum"):
        subject.build_execution_plan(release, report)

    release, report = _inputs(tmp_path / "second", monkeypatch)
    manifest_path = report / "MANIFEST.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["source_path"] = "/private/operator"
    _finish_bundle(report, manifest)
    with pytest.raises(subject.StageExecutionError, match="absolute path"):
        subject.build_execution_plan(release, report)

    release_link = tmp_path / "minute-link"
    release_link.symlink_to(release, target_is_directory=True)
    with pytest.raises(subject.StageExecutionError, match="symlink"):
        subject.build_execution_plan(release_link, report)


def test_budget_rejection_makes_no_state_or_network_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release, report = _inputs(tmp_path, monkeypatch)
    plan = subject.build_execution_plan(release, report, request_budget=3)
    called = False

    def fetch(*_args: object) -> list[dict[str, object]]:
        nonlocal called
        called = True
        return []

    state = tmp_path / "state.duckdb"
    with pytest.raises(subject.StageExecutionError, match="no network"):
        subject.acquire_execution_evidence(
            plan,
            state,
            book_fetch=fetch,
            trade_fetch=fetch,
            client=object(),
            credit_ledger_path=tmp_path / "credits.duckdb",
        )
    assert not called
    assert not state.exists()


def test_transient_fetch_retries_are_counted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release, report = _inputs(tmp_path, monkeypatch)
    plan = subject.build_execution_plan(release, report, request_budget=5)
    calls = 0

    class TransientError(RuntimeError):
        retryable = True

    def books(*_args: object) -> list[dict[str, object]]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TransientError
        return []

    connection = subject.acquire_execution_evidence(
        plan,
        tmp_path / "retry.duckdb",
        book_fetch=books,
        trade_fetch=lambda *_args: [],
        client=object(),
        sleep_fn=lambda _delay: None,
        credit_ledger_path=tmp_path / "credits.duckdb",
    )
    attempts = connection.execute(
        "SELECT sum(book_attempts), sum(trade_attempts) FROM execution_windows"
    ).fetchone()
    connection.close()
    assert attempts == (3, 2)


def test_acquisition_and_atomic_release_include_empty_trade_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release, report = _inputs(tmp_path, monkeypatch)
    plan = subject.build_execution_plan(release, report, request_budget=4)

    def books(_client: object, window: dict[str, object]) -> list[dict[str, object]]:
        return [
            {
                "timestamp": (
                    int(window["window_start_ms"]) + int(window["window_end_ms"])
                )
                // 2,
                "bids": [{"price": "0.40", "size": "2"}],
                "asks": [{"price": "0.42", "size": "3"}],
            }
        ]

    connection = subject.acquire_execution_evidence(
        plan,
        tmp_path / "state.duckdb",
        book_fetch=books,
        trade_fetch=lambda _client, _window: [],
        client=object(),
        credit_ledger_path=tmp_path / "credits.duckdb",
    )
    output_root = tmp_path / "output"
    published = subject.publish_execution_release(
        plan,
        connection,
        output_root,
        generator_commit="a" * 40,
    )
    connection.close()
    assert {path.name for path in published.iterdir()} == subject.OUTPUT_FILES
    manifest = json.loads((published / "MANIFEST.json").read_text())
    assert manifest["counts"] == {
        "targets": 2,
        "legs": 4,
        "windows": 2,
        "book_snapshots": 2,
        "book_levels": 4,
        "trades": 0,
        "empty_book_windows": 0,
        "empty_trade_windows": 2,
    }
    assert pq.read_table(published / "trades.parquet").num_rows == 0
    with pytest.raises(FileExistsError):
        subject.publish_execution_release(
            plan,
            duckdb.connect(str(tmp_path / "state.duckdb")),
            output_root,
            generator_commit="a" * 40,
        )


def test_saturated_ranges_split_and_resume(tmp_path: Path) -> None:
    (tmp_path / "MANIFEST.json").write_text("{}\n", encoding="utf-8")
    window = {
        "window_id": hashlib.sha256(b"root").hexdigest(),
        "clob_token_id": "11",
        "market_id": "m",
        "condition_id": "0x" + "1" * 64,
        "window_start_ms": 0,
        "window_end_ms": 10,
    }
    plan = subject.ExecutionPlan(
        tmp_path,
        tmp_path,
        {},
        {},
        (),
        (),
        (window,),
        10,
        5,
    )

    def books(_client: object, value: dict[str, object]) -> list[dict[str, object]]:
        if int(value["depth"]) == 0:
            return [
                {"timestamp": 5, "bids": [], "asks": []}
                for _ in range(subject.PMXT_MAX_RANGE_SNAPSHOTS)
            ]
        return [
            {
                "timestamp": int(value["window_start_ms"]),
                "bids": [{"price": "0.40", "size": "2"}],
                "asks": [{"price": "0.42", "size": "3"}],
            }
        ]

    connection = subject.acquire_execution_evidence(
        plan,
        tmp_path / "resume.duckdb",
        book_fetch=books,
        trade_fetch=lambda _client, _window: [],
        client=object(),
        credit_ledger_path=tmp_path / "credits.duckdb",
    )
    statuses = dict(
        connection.execute(
            "SELECT status, count(*) FROM execution_windows GROUP BY status"
        ).fetchall()
    )
    assert statuses == {"split": 1, "complete": 2}
    published = subject.publish_execution_release(
        plan, connection, tmp_path / "split-output", generator_commit="a" * 40
    )
    connection.close()
    assert set(
        pq.read_table(published / "book_snapshots.parquet")["window_id"].to_pylist()
    ) == {window["window_id"]}


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("implication_id", "unknown", "unknown implication"),
        ("team_name", "Wrong Team", "pinned implication"),
        ("source_stage_key", "final", "pinned implication"),
        ("source_no_token_id", "23", "pinned implication"),
        ("signal_minute_epoch", 0, "signal time"),
        ("signal_net_edge", 0.03, "signal economics"),
    ],
)
def test_plan_rejects_self_consistent_semantic_report_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
    message: str,
) -> None:
    release, report = _inputs(tmp_path, monkeypatch)
    path = report / "opportunity_minutes.parquet"
    table = pq.read_table(path)
    rows = table.to_pylist()
    rows[0][field] = value
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
    manifest = json.loads((report / "MANIFEST.json").read_text())
    _finish_bundle(report, manifest)

    with pytest.raises(subject.StageExecutionError, match=message):
        subject.build_execution_plan(release, report)


def test_locked_book_is_valid_but_crossed_book_is_rejected() -> None:
    window = {
        "clob_token_id": "11",
        "window_start_ms": 0,
        "window_end_ms": 10,
    }
    locked = {
        "timestamp": 5,
        "bids": [{"price": "0.40", "size": "2"}],
        "asks": [{"price": "0.40", "size": "3"}],
    }
    assert subject._canonical_snapshot(window, locked)["timestamp"] == 5
    crossed = {**locked, "asks": [{"price": "0.39", "size": "3"}]}
    with pytest.raises(subject.StageExecutionError, match="crossed"):
        subject._canonical_snapshot(window, crossed)


def test_shared_monthly_budget_applies_across_independent_checkpoints(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release, report = _inputs(tmp_path, monkeypatch)
    plan = subject.build_execution_plan(release, report, request_budget=4)
    ledger = tmp_path / "shared-credits.duckdb"
    first = subject.acquire_execution_evidence(
        plan,
        tmp_path / "first.duckdb",
        book_fetch=lambda *_args: [],
        trade_fetch=lambda *_args: [],
        client=object(),
        credit_ledger_path=ledger,
    )
    first.close()
    called = False

    def unexpected(*_args: object) -> list[dict[str, object]]:
        nonlocal called
        called = True
        return []

    with pytest.raises(subject.StageExecutionError, match="shared monthly"):
        subject.acquire_execution_evidence(
            plan,
            tmp_path / "second.duckdb",
            book_fetch=unexpected,
            trade_fetch=unexpected,
            client=object(),
            credit_ledger_path=ledger,
        )
    assert not called


def test_deduplicated_and_resumed_evidence_drives_coverage_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release, report = _inputs(tmp_path, monkeypatch)
    plan = subject.build_execution_plan(release, report, request_budget=8)

    def books(_client: object, window: dict[str, object]) -> list[dict[str, object]]:
        row = {
            "timestamp": int(window["window_start_ms"]),
            "bids": [{"price": "0.40", "size": "2"}],
            "asks": [{"price": "0.42", "size": "3"}],
        }
        return [row, row]

    def trades(_client: object, window: dict[str, object]) -> list[dict[str, object]]:
        row = {
            "id": f"trade-{window['clob_token_id']}",
            "timestamp": int(window["window_start_ms"]),
            "outcomeId": window["clob_token_id"],
            "price": "0.41",
            "amount": "1",
        }
        return [row, row]

    state = tmp_path / "resume-counts.duckdb"
    ledger = tmp_path / "credits.duckdb"
    connection = subject.acquire_execution_evidence(
        plan,
        state,
        book_fetch=books,
        trade_fetch=trades,
        client=object(),
        credit_ledger_path=ledger,
    )
    assert connection.execute(
        "SELECT snapshot_count, trade_count FROM execution_windows ORDER BY window_id"
    ).fetchall() == [(1, 1), (1, 1)]
    connection.execute(
        "UPDATE execution_windows SET status='pending', snapshot_count=0, trade_count=0"
    )
    connection.close()

    resumed = subject.acquire_execution_evidence(
        plan,
        state,
        book_fetch=lambda *_args: [],
        trade_fetch=lambda *_args: [],
        client=object(),
        credit_ledger_path=ledger,
    )
    published = subject.publish_execution_release(
        plan, resumed, tmp_path / "dedup-output", generator_commit="a" * 40
    )
    resumed.close()
    coverage = pq.read_table(published / "coverage.parquet").to_pylist()
    assert [(row["snapshot_count"], row["trade_count"]) for row in coverage] == [
        (1, 1),
        (1, 1),
    ]
    assert all(not row["empty_book"] and not row["empty_trades"] for row in coverage)


def test_nondefault_window_is_recorded_in_release_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release, report = _inputs(tmp_path, monkeypatch)
    plan = subject.build_execution_plan(
        release, report, request_budget=4, window_seconds=2
    )
    connection = subject.acquire_execution_evidence(
        plan,
        tmp_path / "state.duckdb",
        book_fetch=lambda *_args: [],
        trade_fetch=lambda *_args: [],
        client=object(),
        credit_ledger_path=tmp_path / "credits.duckdb",
    )
    published = subject.publish_execution_release(
        plan, connection, tmp_path / "output", generator_commit="a" * 40
    )
    connection.close()
    manifest = json.loads((published / "MANIFEST.json").read_text())
    assert plan.window_seconds == 2
    assert manifest["configuration"]["window_seconds"] == 2


def test_publication_lock_and_late_target_creation_prevent_overwrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release, report = _inputs(tmp_path, monkeypatch)
    plan = subject.build_execution_plan(release, report, request_budget=4)
    connection = subject.acquire_execution_evidence(
        plan,
        tmp_path / "state.duckdb",
        book_fetch=lambda *_args: [],
        trade_fetch=lambda *_args: [],
        client=object(),
        credit_ledger_path=tmp_path / "credits.duckdb",
    )
    output = tmp_path / "output"
    target = output / "releases" / subject.DATASET_VERSION
    original = subject._validate_release_tables

    def create_racing_target(directory: Path, value: subject.ExecutionPlan) -> None:
        original(directory, value)
        target.mkdir()

    monkeypatch.setattr(subject, "_validate_release_tables", create_racing_target)
    with pytest.raises(FileExistsError):
        subject.publish_execution_release(
            plan, connection, output, generator_commit="a" * 40
        )
    connection.close()
    assert target.is_dir() and not any(target.iterdir())
    assert not (target.parent / ".1.0.0.publication-lock").exists()
    assert not list(target.parent.glob(".1.0.0.*"))


@pytest.mark.parametrize(
    "failure",
    [
        ValueError("invalid dataset version"),
        FileExistsError("existing release"),
        RuntimeError("dirty tree"),
    ],
)
def test_cli_preflight_failure_occurs_before_acquisition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: Exception
) -> None:
    script = Path("scripts/build_polymarket_wc2026_stage_execution_release.py")
    spec = importlib.util.spec_from_file_location("stage_execution_cli", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Plan:
        def summary(self) -> dict[str, object]:
            return {"within_budget": True}

    acquired = False

    def acquire(*_args: object, **_kwargs: object) -> None:
        nonlocal acquired
        acquired = True

    monkeypatch.setattr(module, "build_execution_plan", lambda *_a, **_k: Plan())
    monkeypatch.setattr(
        module,
        "preflight_execution_release",
        lambda *_a, **_k: (_ for _ in ()).throw(failure),
    )
    monkeypatch.setattr(module, "acquire_execution_evidence", acquire)
    result = module.main(
        [
            "release",
            "--stage-minute-release",
            str(tmp_path / "minute"),
            "--ohlc-report",
            str(tmp_path / "report"),
            "--output-root",
            str(tmp_path / "output"),
            "--source",
            "api-range",
        ]
    )
    assert result == 2
    assert not acquired


def test_generator_revision_requires_clean_untracked_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options: dict[str, object] = {}

    def clean_commit(path: Path, **kwargs: object) -> str:
        options.update(kwargs)
        assert path == subject.BASE_DIR
        return "a" * 40

    monkeypatch.setattr(subject, "current_clean_commit", clean_commit)
    assert subject.current_generator_commit() == "a" * 40
    assert options == {}


def _archive_table(tokens: tuple[str, ...]) -> pa.Table:
    rows: list[dict[str, object]] = []
    for token in tokens:
        condition = ("0x" + ("1" if token == "11" else "2") * 64).encode()
        common = {"market": condition, "asset_id": token}
        rows.extend(
            [
                {
                    **common,
                    "timestamp_received": datetime.fromtimestamp(119, timezone.utc),
                    "timestamp": datetime.fromtimestamp(118.9, timezone.utc),
                    "event_type": "book",
                    "bids": '[["0.40","2"]]',
                    "asks": '[["0.42","3"]]',
                },
                {
                    **common,
                    "timestamp_received": datetime.fromtimestamp(120, timezone.utc),
                    "timestamp": datetime.fromtimestamp(119.9, timezone.utc),
                    "event_type": "price_change",
                    "price": Decimal("0.41"),
                    "size": Decimal("4"),
                    "side": "BUY",
                    "best_bid": Decimal("0.41"),
                    "best_ask": Decimal("0.42"),
                },
                {
                    **common,
                    "timestamp_received": datetime.fromtimestamp(121, timezone.utc),
                    "timestamp": datetime.fromtimestamp(120.9, timezone.utc),
                    "event_type": "last_trade_price",
                    "price": Decimal("0.415"),
                    "size": Decimal("1.5"),
                    "side": "BUY",
                    "fee_rate_bps": 0,
                    "transaction_hash": f"tx-{token}",
                },
            ]
        )
    schema = pa.schema(
        [
            ("timestamp_received", pa.timestamp("ms", tz="UTC")),
            ("timestamp", pa.timestamp("ms", tz="UTC")),
            ("market", pa.binary(66)),
            ("event_type", pa.string()),
            ("asset_id", pa.string()),
            ("bids", pa.string()),
            ("asks", pa.string()),
            ("price", pa.decimal128(9, 4)),
            ("size", pa.decimal128(18, 6)),
            ("side", pa.string()),
            ("best_bid", pa.decimal128(9, 4)),
            ("best_ask", pa.decimal128(9, 4)),
            ("fee_rate_bps", pa.uint16()),
            ("transaction_hash", pa.string()),
            ("old_tick_size", pa.decimal128(9, 4)),
            ("new_tick_size", pa.decimal128(9, 4)),
        ]
    )
    return pa.Table.from_pylist(rows, schema=schema)


def test_archive_plan_counts_token_hours_and_cross_hour_windows(tmp_path: Path) -> None:
    window = {
        "window_id": "window",
        "clob_token_id": "11",
        "market_id": "m",
        "condition_id": "0x" + "1" * 64,
        "window_start_ms": archive.HOUR_MS - 1,
        "window_end_ms": archive.HOUR_MS + 1,
    }
    plan = subject.ExecutionPlan(
        tmp_path, tmp_path, {}, {}, (), (), (window,), 20_000, 5
    )
    summary = archive.archive_plan_summary(plan)
    assert summary["archive_hours"] == 2
    assert summary["token_hours"] == 2
    assert summary["minimum_requests"] == 2
    assert set(archive.archive_work(plan)) == {0, archive.HOUR_MS}


def test_archive_acquisition_reconstructs_receipt_timed_books_and_trades(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release, report = _inputs(tmp_path, monkeypatch)
    plan = subject.build_execution_plan(release, report, request_budget=4)
    seed_calls: list[tuple[str, int]] = []

    def seed(_client: object, token: str, hour_ms: int) -> dict[str, object]:
        seed_calls.append((token, hour_ms))
        return {
            "timestamp": hour_ms,
            "bids": [{"price": "0.39", "size": "1"}],
            "asks": [{"price": "0.43", "size": "1"}],
        }

    def download(_url: str, destination: Path) -> dict[str, object]:
        pq.write_table(_archive_table(("11", "22")), destination)
        return {
            "status": "downloaded",
            "byte_size": destination.stat().st_size,
            "sha256": sha256_file(destination),
            "etag": "fixture",
        }

    connection = archive.acquire_archive_execution_evidence(
        plan,
        tmp_path / "archive-state.duckdb",
        seed_fetch=seed,
        download=download,
        client=object(),
        credit_ledger_path=tmp_path / "credits.duckdb",
    )
    assert seed_calls == [("11", 0), ("22", 0)]
    assert connection.execute(
        "SELECT count(*), min(received_timestamp_ms), max(received_timestamp_ms) "
        "FROM execution_book_snapshots"
    ).fetchone() == (4, 119_000, 120_000)
    assert connection.execute(
        "SELECT count(*), min(received_timestamp_ms) FROM execution_trades"
    ).fetchone() == (2, 121_000)
    published = subject.publish_execution_release(
        plan, connection, tmp_path / "archive-output", generator_commit="a" * 40
    )
    connection.close()
    snapshots = pq.read_table(published / "book_snapshots.parquet")
    assert "received_timestamp_ms" in snapshots.column_names
    assert snapshots["received_timestamp_ms"].to_pylist() == [
        119_000,
        120_000,
        119_000,
        120_000,
    ]
    manifest = json.loads((published / "MANIFEST.json").read_text())
    assert manifest["configuration"]["source_mode"] == "archive-v2"
    assert manifest["request_audit"] == {
        "api_attempt_count": 2,
        "archive_http_attempt_count": 1,
    }
    assert manifest["archive_objects"][0]["sha256"]


def test_archive_missing_hour_is_valid_negative_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release, report = _inputs(tmp_path, monkeypatch)
    plan = subject.build_execution_plan(release, report, request_budget=4)
    connection = archive.acquire_archive_execution_evidence(
        plan,
        tmp_path / "missing.duckdb",
        seed_fetch=lambda _client, _token, hour: {
            "timestamp": hour,
            "bids": [],
            "asks": [],
        },
        download=lambda _url, _path: {"status": "missing", "etag": None},
        client=object(),
        credit_ledger_path=tmp_path / "credits.duckdb",
    )
    assert connection.execute(
        "SELECT status, event_count FROM execution_archive_objects"
    ).fetchone() == ("missing", 0)
    assert (
        connection.execute("SELECT count(*) FROM execution_book_snapshots").fetchone()[
            0
        ]
        == 0
    )
    connection.close()

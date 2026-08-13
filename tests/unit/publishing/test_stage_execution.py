from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oddsfox_pipeline.contracts.raw_snapshots import schema_fingerprint
from oddsfox_pipeline.publishing import stage_execution as subject
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
            "outcome_label": "No",
            "clob_token_id": "11",
        },
        {
            "market_id": "m-target",
            "condition_id": "0x" + "2" * 64,
            "outcome_label": "Yes",
            "clob_token_id": "22",
        },
    ]
    pq.write_table(pa.Table.from_pylist(outcomes), release / "outcomes.parquet")
    for name in (
        "token_minute_ohlc.parquet",
        "implications.parquet",
        "coverage.parquet",
    ):
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
                "signal_minute_utc": datetime(2026, 6, 11, 0, 1),
                "signal_net_edge": 0.02,
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
            plan, state, book_fetch=fetch, trade_fetch=fetch, client=object()
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

"""Targeted PMXT execution evidence for frozen WC2026 stage signals."""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Callable, Final, Iterable, Mapping

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from oddsfox_pipeline.config.settings import PMXT_API_KEY
from oddsfox_pipeline.config.settings_warehouse import BASE_DIR, DUCKDB_PATH
from oddsfox_pipeline.contracts.schema import schema_fingerprint
from oddsfox_pipeline.ingestion.polymarket.match_order_book import (
    PMXT_MAX_RANGE_SNAPSHOTS,
    PMXT_ORDER_BOOK_ENDPOINT,
    PMXT_ORDER_BOOK_SOURCE,
    _normalize_levels,
    _pmxt_books,
    build_pmxt_client,
)
from oddsfox_pipeline.ingestion.polymarket.match_trades import (
    ENDPOINT as PMXT_TRADES_ENDPOINT,
)
from oddsfox_pipeline.ingestion.polymarket.match_trades import (
    SOURCE as PMXT_TRADES_SOURCE,
)
from oddsfox_pipeline.publishing._bundle_io import (
    COMMIT_RE,
    current_clean_commit,
    sha256_file,
    validate_dataset_version,
    write_checksums,
    write_json,
)
from oddsfox_pipeline.resources.http_retry import (
    exponential_backoff_seconds,
    is_transient_status,
)
from oddsfox_pipeline.storage.duckdb.match_order_book import METADATA

CONTRACT_VERSION: Final = "oddsfox.polymarket_wc2026.stage_execution.v1"
DATASET_VERSION: Final = "1.0.0"
STAGE_MINUTE_MANIFEST_SHA256: Final = (
    "a6eddbbd2cc1693689fc0a0b32e0da8ea98979624d37de3fca29ae140d613a7d"
)
OHLC_REPORT_CONTRACT: Final = "oddsfox.wc2026.stage_minute_locked_edge.v1"
OHLC_STRATEGY_SHA: Final = "f2d71d6ffef3c912372939bf30b3af8ef8311517"
PRIMARY_SCENARIO: Final = "primary_high_3pct"
DEFAULT_OUTPUT_ROOT: Final = (
    BASE_DIR / "artifacts" / "strategy-inputs" / "polymarket_wc2026_stage_execution"
)
INPUT_RELEASE_FILES: Final = frozenset(
    {
        "token_minute_ohlc.parquet",
        "outcomes.parquet",
        "implications.parquet",
        "coverage.parquet",
        "SCHEMA.json",
        "MANIFEST.json",
        "CHECKSUMS.sha256",
    }
)
INPUT_REPORT_FILES: Final = frozenset(
    {
        "opportunity_minutes.parquet",
        "opportunity_episodes.parquet",
        "primary_entries.parquet",
        "scenario_summary.csv",
        "period_summary.csv",
        "REPORT.md",
        "MANIFEST.json",
        "CHECKSUMS.sha256",
    }
)
OUTPUT_FILES: Final = frozenset(
    {
        "execution_targets.parquet",
        "target_legs.parquet",
        "book_snapshots.parquet",
        "book_levels.parquet",
        "trades.parquet",
        "coverage.parquet",
        "SCHEMA.json",
        "MANIFEST.json",
        "CHECKSUMS.sha256",
    }
)


class StageExecutionError(RuntimeError):
    """Raised when execution evidence cannot be planned or published safely."""


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    stage_minute_release: Path
    ohlc_report: Path
    stage_minute_manifest: Mapping[str, Any]
    report_manifest: Mapping[str, Any]
    targets: tuple[Mapping[str, Any], ...]
    legs: tuple[Mapping[str, Any], ...]
    windows: tuple[Mapping[str, Any], ...]
    request_budget: int
    window_seconds: int

    @property
    def minimum_requests(self) -> int:
        return len(self.windows) * 2

    def summary(self) -> dict[str, Any]:
        tokens = {str(row["clob_token_id"]) for row in self.legs}
        return {
            "signals": len(self.targets),
            "legs": len(self.legs),
            "tokens": len(tokens),
            "windows": len(self.windows),
            "minimum_requests": self.minimum_requests,
            "request_budget": self.request_budget,
            "within_budget": self.minimum_requests <= self.request_budget,
            "estimated_storage_bytes": len(self.windows) * 16_384,
        }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StageExecutionError(f"invalid JSON: {path.name}") from exc
    if not isinstance(value, dict):
        raise StageExecutionError(f"invalid JSON object: {path.name}")
    return value


def _reject_absolute_paths(value: Any) -> None:
    if isinstance(value, Mapping):
        for item in value.values():
            _reject_absolute_paths(item)
    elif isinstance(value, list):
        for item in value:
            _reject_absolute_paths(item)
    elif isinstance(value, str) and (
        PurePosixPath(value).is_absolute() or PureWindowsPath(value).is_absolute()
    ):
        raise StageExecutionError("bundle provenance contains an absolute path")


def _verify_bundle(directory: Path, expected: frozenset[str]) -> dict[str, Any]:
    directory = directory.expanduser()
    if directory.is_symlink() or not directory.is_dir():
        raise StageExecutionError(f"invalid bundle directory: {directory}")
    directory = directory.resolve()
    entries = {path.name for path in directory.iterdir()}
    if entries != expected or any(path.is_symlink() for path in directory.iterdir()):
        raise StageExecutionError("bundle inventory or symlink contract mismatch")
    checksums: dict[str, str] = {}
    for line in (
        (directory / "CHECKSUMS.sha256").read_text(encoding="utf-8").splitlines()
    ):
        parts = line.split("  ")
        if len(parts) != 2 or len(parts[0]) != 64 or parts[1] in checksums:
            raise StageExecutionError("malformed checksum inventory")
        checksums[parts[1]] = parts[0]
    if set(checksums) != expected - {"CHECKSUMS.sha256"}:
        raise StageExecutionError("checksum inventory mismatch")
    for name, digest in checksums.items():
        if sha256_file(directory / name) != digest:
            raise StageExecutionError(f"checksum mismatch: {name}")
    manifest = _read_json(directory / "MANIFEST.json")
    _reject_absolute_paths(manifest)
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise StageExecutionError("manifest file inventory is missing")
    if set(files) != expected - {"MANIFEST.json", "CHECKSUMS.sha256"}:
        raise StageExecutionError("manifest file inventory mismatch")
    for name, metadata in files.items():
        if name not in expected or not isinstance(metadata, dict):
            raise StageExecutionError("manifest file inventory mismatch")
        path = directory / name
        if metadata.get("sha256") != sha256_file(path):
            raise StageExecutionError(f"manifest hash mismatch: {name}")
        if metadata.get("byte_size") != path.stat().st_size:
            raise StageExecutionError(f"manifest byte size mismatch: {name}")
        if name.endswith(".parquet"):
            parquet = pq.ParquetFile(path)
            if metadata.get("row_count") != parquet.metadata.num_rows:
                raise StageExecutionError(f"manifest row count mismatch: {name}")
            if metadata.get("schema_fingerprint") != schema_fingerprint(
                parquet.schema_arrow
            ):
                raise StageExecutionError(f"manifest schema mismatch: {name}")
    return manifest


def _window_id(token_id: str, start_ms: int, end_ms: int) -> str:
    return hashlib.sha256(f"{token_id}\0{start_ms}\0{end_ms}".encode()).hexdigest()


def _validate_signal_economics(row: Mapping[str, Any], fee_rate: float) -> float:
    try:
        source_price = float(row["source_no_close_price"])
        target_price = float(row["target_yes_close_price"])
        source_fee = float(row["source_no_signal_fee"])
        target_fee = float(row["target_yes_signal_fee"])
        signal_edge = float(row["signal_net_edge"])
    except (KeyError, TypeError, ValueError) as exc:
        raise StageExecutionError("invalid report signal economics") from exc
    values = (source_price, target_price, source_fee, target_fee, signal_edge)
    if (
        not all(math.isfinite(value) for value in values)
        or not 0 <= source_price <= 1
        or not 0 <= target_price <= 1
    ):
        raise StageExecutionError("invalid report signal economics")
    expected_source_fee = max(round(fee_rate * source_price * (1 - source_price), 5), 0)
    expected_target_fee = max(round(fee_rate * target_price * (1 - target_price), 5), 0)
    expected_edge = (
        1 - source_price - target_price - expected_source_fee - expected_target_fee
    )
    if (
        not math.isclose(source_fee, expected_source_fee, rel_tol=0, abs_tol=1e-12)
        or not math.isclose(target_fee, expected_target_fee, rel_tol=0, abs_tol=1e-12)
        or not math.isclose(signal_edge, expected_edge, rel_tol=0, abs_tol=1e-12)
    ):
        raise StageExecutionError("invalid report signal economics")
    return signal_edge


def build_execution_plan(
    stage_minute_release: Path,
    ohlc_report: Path,
    *,
    request_budget: int = 20_000,
    window_seconds: int = 5,
) -> ExecutionPlan:
    if request_budget <= 0 or window_seconds <= 0:
        raise ValueError("request_budget and window_seconds must be positive")
    release = stage_minute_release.expanduser()
    report = ohlc_report.expanduser()
    if release.is_symlink() or report.is_symlink():
        raise StageExecutionError("bundle directory must not be a symlink")
    release = release.resolve()
    report = report.resolve()
    release_manifest = _verify_bundle(release, INPUT_RELEASE_FILES)
    report_manifest = _verify_bundle(report, INPUT_REPORT_FILES)
    if sha256_file(release / "MANIFEST.json") != STAGE_MINUTE_MANIFEST_SHA256:
        raise StageExecutionError("stage-minute manifest is not the pinned release")
    if (
        release_manifest.get("contract_version")
        != ("oddsfox.polymarket_wc2026.stage_minute.v1")
        or release_manifest.get("dataset_version") != DATASET_VERSION
    ):
        raise StageExecutionError("stage-minute contract drift")
    if (
        report_manifest.get("report_contract") != OHLC_REPORT_CONTRACT
        or report_manifest.get("strategy_revision") != OHLC_STRATEGY_SHA
        or report_manifest.get("input", {}).get("manifest_sha256")
        != STAGE_MINUTE_MANIFEST_SHA256
        or report_manifest.get("configuration")
        != {"fee_rate": 0.03, "min_net_edge": 0.01}
        or [period.get("day_count") for period in report_manifest.get("periods", [])]
        != [20, 10, 10]
    ):
        raise StageExecutionError("OHLC report contract drift")

    outcomes = {
        str(row["clob_token_id"]): row
        for row in pq.read_table(release / "outcomes.parquet").to_pylist()
    }
    implications = {
        str(row["implication_id"]): row
        for row in pq.read_table(release / "implications.parquet").to_pylist()
    }
    no_tokens_by_market = {
        str(row["market_id"]): str(row["clob_token_id"])
        for row in outcomes.values()
        if row.get("outcome_label") == "No"
    }
    opportunities = pq.read_table(
        report / "opportunity_minutes.parquet",
        filters=[("scenario_id", "=", PRIMARY_SCENARIO)],
    ).to_pylist()
    targets: list[dict[str, Any]] = []
    legs: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    padding_ms = window_seconds * 1_000
    configuration = report_manifest["configuration"]
    fee_rate = float(configuration["fee_rate"])
    minimum_edge = float(configuration["min_net_edge"])
    for row in opportunities:
        key = (str(row["implication_id"]), int(row["signal_minute_epoch"]))
        if key in seen:
            raise StageExecutionError("duplicate execution target")
        seen.add(key)
        implication = implications.get(key[0])
        if implication is None:
            raise StageExecutionError("report contains an unknown implication")
        source_yes = outcomes.get(str(implication["source_clob_token_id"]))
        target_yes = outcomes.get(str(implication["target_clob_token_id"]))
        if source_yes is None or target_yes is None:
            raise StageExecutionError("implication endpoint is absent from outcomes")
        expected = {
            "team_name": implication["team_name"],
            "rule_id": implication["rule_id"],
            "source_stage_key": implication["source_stage_key"],
            "target_stage_key": implication["target_stage_key"],
            "source_no_token_id": no_tokens_by_market.get(str(source_yes["market_id"])),
            "target_yes_token_id": implication["target_clob_token_id"],
        }
        if any(str(row.get(field)) != str(value) for field, value in expected.items()):
            raise StageExecutionError(
                "report target does not match the pinned implication"
            )
        signal_utc = row.get("signal_minute_utc")
        if not isinstance(
            signal_utc, datetime
        ) or signal_utc.utcoffset() != timezone.utc.utcoffset(signal_utc):
            raise StageExecutionError("invalid signal minute timestamp")
        signal_edge = _validate_signal_economics(row, fee_rate)
        if (
            int(signal_utc.timestamp()) != key[1]
            or key[1] % 60 != 0
            or signal_edge < minimum_edge
        ):
            raise StageExecutionError("invalid report signal time or edge")
        decision_ms = (key[1] + 60) * 1_000
        target_id = hashlib.sha256(f"{key[0]}\0{key[1]}".encode()).hexdigest()
        target = {
            "target_id": target_id,
            "implication_id": key[0],
            "team_name": str(row["team_name"]),
            "rule_id": str(row["rule_id"]),
            "source_stage_key": str(row["source_stage_key"]),
            "target_stage_key": str(row["target_stage_key"]),
            "signal_minute_epoch": key[1],
            "signal_minute_utc": row["signal_minute_utc"],
            "decision_timestamp_ms": decision_ms,
            "signal_net_edge": float(row["signal_net_edge"]),
        }
        targets.append(target)
        for role, field in (
            ("source_no", "source_no_token_id"),
            ("target_yes", "target_yes_token_id"),
        ):
            token_id = str(row[field])
            outcome = outcomes.get(token_id)
            if outcome is None:
                raise StageExecutionError(
                    f"target token is absent from outcomes: {token_id}"
                )
            expected_label = "No" if role == "source_no" else "Yes"
            if outcome.get("outcome_label") != expected_label:
                raise StageExecutionError(
                    f"{role} token is not the {expected_label} outcome"
                )
            legs.append(
                {
                    "target_id": target_id,
                    "leg_role": role,
                    "clob_token_id": token_id,
                    "market_id": str(outcome["market_id"]),
                    "condition_id": str(outcome["condition_id"]),
                    "outcome_label": str(outcome["outcome_label"]),
                    "decision_timestamp_ms": decision_ms,
                    "window_start_ms": decision_ms - padding_ms,
                    "window_end_ms": decision_ms + padding_ms,
                }
            )

    by_token: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for leg in legs:
        by_token[str(leg["clob_token_id"])].append(leg)
    windows: list[dict[str, Any]] = []
    leg_windows: dict[tuple[str, str], str] = {}
    for token_id, token_legs in sorted(by_token.items()):
        active: dict[str, Any] | None = None
        members: list[dict[str, Any]] = []
        for leg in sorted(
            token_legs,
            key=lambda value: (
                int(value["window_start_ms"]),
                int(value["window_end_ms"]),
                str(value["target_id"]),
                str(value["leg_role"]),
            ),
        ):
            if active is None or int(leg["window_start_ms"]) > int(
                active["window_end_ms"]
            ):
                if active is not None:
                    windows.append(active)
                    for member in members:
                        leg_windows[(member["target_id"], member["leg_role"])] = str(
                            active["window_id"]
                        )
                active = {
                    "window_id": "",
                    "clob_token_id": token_id,
                    "market_id": leg["market_id"],
                    "condition_id": leg["condition_id"],
                    "window_start_ms": int(leg["window_start_ms"]),
                    "window_end_ms": int(leg["window_end_ms"]),
                }
                members = [leg]
            else:
                active["window_end_ms"] = max(
                    int(active["window_end_ms"]), int(leg["window_end_ms"])
                )
                members.append(leg)
            active["window_id"] = _window_id(
                token_id,
                int(active["window_start_ms"]),
                int(active["window_end_ms"]),
            )
        if active is not None:
            windows.append(active)
            for member in members:
                leg_windows[(member["target_id"], member["leg_role"])] = str(
                    active["window_id"]
                )
    legs = [
        {
            **leg,
            "window_id": leg_windows[(str(leg["target_id"]), str(leg["leg_role"]))],
        }
        for leg in legs
    ]
    return ExecutionPlan(
        release,
        report,
        release_manifest,
        report_manifest,
        tuple(
            sorted(
                targets,
                key=lambda row: (row["implication_id"], row["signal_minute_epoch"]),
            )
        ),
        tuple(sorted(legs, key=lambda row: (row["target_id"], row["leg_role"]))),
        tuple(
            sorted(
                windows, key=lambda row: (row["clob_token_id"], row["window_start_ms"])
            )
        ),
        request_budget,
        window_seconds,
    )


def _state_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS execution_windows(
          root_window_id VARCHAR, window_id VARCHAR PRIMARY KEY, clob_token_id VARCHAR,
          market_id VARCHAR, condition_id VARCHAR, window_start_ms BIGINT,
          window_end_ms BIGINT, depth INTEGER, status VARCHAR,
          book_attempts INTEGER DEFAULT 0, trade_attempts INTEGER DEFAULT 0,
          snapshot_count BIGINT DEFAULT 0, trade_count BIGINT DEFAULT 0,
          updated_at TIMESTAMP)
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS execution_book_snapshots(
          window_id VARCHAR, clob_token_id VARCHAR, snapshot_timestamp_ms BIGINT,
          received_timestamp_ms BIGINT, snapshot_sha256 VARCHAR,
          bids_json VARCHAR, asks_json VARCHAR,
          ingested_at TIMESTAMP,
          PRIMARY KEY(clob_token_id, snapshot_timestamp_ms, snapshot_sha256))
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS execution_trades(
          window_id VARCHAR, clob_token_id VARCHAR, trade_id VARCHAR,
          trade_timestamp_ms BIGINT, received_timestamp_ms BIGINT,
          event_sequence BIGINT, price DOUBLE, amount DOUBLE, ingested_at TIMESTAMP,
          PRIMARY KEY(clob_token_id, trade_id))
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS execution_audit(
          key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL)
    """)
    conn.execute(
        "ALTER TABLE execution_book_snapshots ADD COLUMN IF NOT EXISTS "
        "received_timestamp_ms BIGINT"
    )
    conn.execute(
        "ALTER TABLE execution_trades ADD COLUMN IF NOT EXISTS "
        "received_timestamp_ms BIGINT"
    )


def _reserve_shared_pmxt_attempt(
    ledger_path: Path, budget: int, *, now: datetime | None = None
) -> bool:
    """Atomically reserve one request from the operator-wide UTC-month cap."""
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    key = f"pmxt_api_attempts_{date(current.year, current.month, 1)}"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger = duckdb.connect(str(ledger_path))
    try:
        ledger.execute('CREATE SCHEMA IF NOT EXISTS "polymarket_wc2026_ops"')
        ledger.execute(
            f"CREATE TABLE IF NOT EXISTS {METADATA} "
            "(key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL)"
        )
        ledger.execute("BEGIN TRANSACTION")
        row = ledger.execute(
            f"SELECT value FROM {METADATA} WHERE key=?", [key]
        ).fetchone()
        if row is not None and int(row[0]) >= budget:
            ledger.execute("COMMIT")
            return False
        ledger.execute(
            f"INSERT INTO {METADATA} VALUES (?, '1') "
            f"ON CONFLICT (key) DO UPDATE SET value=CAST(CAST({METADATA}.value "
            "AS BIGINT) + 1 AS VARCHAR)",
            [key],
        )
        ledger.execute("COMMIT")
        return True
    except BaseException:
        try:
            ledger.execute("ROLLBACK")
        except duckdb.Error:
            pass
        raise
    finally:
        ledger.close()


def _default_book_fetch(client: Any, window: Mapping[str, Any]) -> list[dict[str, Any]]:
    payload = client.post(
        PMXT_ORDER_BOOK_ENDPOINT,
        headers={"Authorization": f"Bearer {PMXT_API_KEY}"},
        json={
            "args": [
                str(window["condition_id"]),
                None,
                {
                    "since": int(window["window_start_ms"]),
                    "until": int(window["window_end_ms"]),
                    "outcome": str(window["clob_token_id"]),
                    "limit": PMXT_MAX_RANGE_SNAPSHOTS,
                },
            ]
        },
    )
    return _pmxt_books(payload)


def _default_trade_fetch(
    client: Any, window: Mapping[str, Any]
) -> list[dict[str, Any]]:
    def iso(value: int) -> str:
        return (
            datetime.fromtimestamp(value / 1_000, timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )

    payload = client.get(
        PMXT_TRADES_ENDPOINT,
        headers={"Authorization": f"Bearer {PMXT_API_KEY}"},
        params={
            "outcomeId": str(window["clob_token_id"]),
            "start": iso(int(window["window_start_ms"])),
            "end": iso(int(window["window_end_ms"])),
            "limit": PMXT_MAX_RANGE_SNAPSHOTS,
        },
    )
    return _pmxt_books(payload)


def _canonical_snapshot(
    window: Mapping[str, Any], raw: Mapping[str, Any]
) -> dict[str, Any]:
    try:
        timestamp_decimal = Decimal(str(raw.get("timestamp")))
        timestamp = int(timestamp_decimal)
    except (ArithmeticError, ValueError) as exc:
        raise StageExecutionError("invalid PMXT book timestamp") from exc
    if timestamp_decimal != timestamp:
        raise StageExecutionError("invalid PMXT book timestamp")
    if not int(window["window_start_ms"]) <= timestamp <= int(window["window_end_ms"]):
        raise StageExecutionError("book timestamp is outside its requested window")
    bids = _normalize_levels(raw.get("bids"), side="bids")
    asks = _normalize_levels(raw.get("asks"), side="asks")
    if bids and asks and Decimal(bids[0]["price"]) > Decimal(asks[0]["price"]):
        raise StageExecutionError("crossed execution book")
    canonical = {
        "clob_token_id": str(window["clob_token_id"]),
        "timestamp": timestamp,
        "received_timestamp": int(raw.get("received_timestamp", timestamp)),
        "bids": bids,
        "asks": asks,
    }
    if canonical["received_timestamp"] < timestamp:
        raise StageExecutionError("book receipt timestamp precedes source timestamp")
    return {
        **canonical,
        "snapshot_sha256": hashlib.sha256(
            json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }


def _canonical_trade(
    window: Mapping[str, Any], raw: Mapping[str, Any]
) -> dict[str, Any]:
    try:
        trade_id = str(raw.get("id") or "").strip()
        timestamp_decimal = Decimal(str(raw.get("timestamp")))
        timestamp = int(timestamp_decimal)
        token = str(raw.get("outcomeId") or window["clob_token_id"])
        price = float(Decimal(str(raw.get("price"))))
        amount = float(Decimal(str(raw.get("amount"))))
        received_timestamp = int(raw.get("received_timestamp", timestamp))
    except (ArithmeticError, ValueError) as exc:
        raise StageExecutionError("invalid PMXT trade") from exc
    if (
        not trade_id
        or timestamp_decimal != timestamp
        or token != window["clob_token_id"]
        or not int(window["window_start_ms"])
        <= timestamp
        <= int(window["window_end_ms"])
        or not math.isfinite(price)
        or not 0 <= price <= 1
        or not math.isfinite(amount)
        or amount <= 0
        or received_timestamp < timestamp
    ):
        raise StageExecutionError("invalid PMXT trade")
    return {
        "trade_id": trade_id,
        "timestamp": timestamp,
        "received_timestamp": received_timestamp,
        "price": price,
        "amount": amount,
    }


def acquire_execution_evidence(
    plan: ExecutionPlan,
    state_path: Path,
    *,
    book_fetch: Callable[
        [Any, Mapping[str, Any]], list[dict[str, Any]]
    ] = _default_book_fetch,
    trade_fetch: Callable[
        [Any, Mapping[str, Any]], list[dict[str, Any]]
    ] = _default_trade_fetch,
    client: Any | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    credit_ledger_path: Path = DUCKDB_PATH,
) -> duckdb.DuckDBPyConnection:
    if plan.minimum_requests > plan.request_budget:
        raise StageExecutionError(
            f"planned minimum requests {plan.minimum_requests} exceed budget "
            f"{plan.request_budget}; no network requests were made"
        )
    if client is None and not PMXT_API_KEY.strip():
        raise StageExecutionError("PMXT_API_KEY is required")
    state_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(state_path))
    _state_schema(conn)
    input_key = hashlib.sha256(
        (
            STAGE_MINUTE_MANIFEST_SHA256
            + sha256_file(plan.ohlc_report / "MANIFEST.json")
        ).encode()
    ).hexdigest()
    prior = conn.execute(
        "SELECT value FROM execution_audit WHERE key='input_sha256'"
    ).fetchone()
    if prior and prior[0] != input_key:
        conn.close()
        raise StageExecutionError("checkpoint belongs to different immutable inputs")
    conn.execute(
        "INSERT OR REPLACE INTO execution_audit VALUES ('input_sha256', ?)",
        [input_key],
    )
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for window in plan.windows:
        conn.execute(
            """
            INSERT INTO execution_windows VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'pending',
              0, 0, 0, 0, ?) ON CONFLICT DO NOTHING
            """,
            [
                window["window_id"],
                window["window_id"],
                window["clob_token_id"],
                window["market_id"],
                window["condition_id"],
                window["window_start_ms"],
                window["window_end_ms"],
                now,
            ],
        )
    http = client or build_pmxt_client(requests_per_minute=50)
    while True:
        cursor = conn.execute("""
            SELECT root_window_id, window_id, clob_token_id, market_id, condition_id,
                   window_start_ms, window_end_ms, depth
            FROM execution_windows WHERE status='pending'
            ORDER BY depth, clob_token_id, window_start_ms LIMIT 1
        """)
        row = cursor.fetchone()
        if row is None:
            break
        window = dict(zip([item[0] for item in cursor.description], row, strict=True))

        def fetch_with_retry(
            fetch: Callable[[Any, Mapping[str, Any]], list[dict[str, Any]]],
            attempt_column: str,
        ) -> list[dict[str, Any]]:
            for retry_number in range(5):
                used = int(
                    conn.execute(
                        "SELECT coalesce(sum(book_attempts + trade_attempts), 0) "
                        "FROM execution_windows"
                    ).fetchone()[0]
                )
                if used >= plan.request_budget:
                    raise StageExecutionError(
                        "PMXT request budget exhausted; checkpoint preserved"
                    )
                if not _reserve_shared_pmxt_attempt(
                    credit_ledger_path, plan.request_budget
                ):
                    raise StageExecutionError(
                        "shared monthly PMXT request budget exhausted; "
                        "checkpoint preserved"
                    )
                conn.execute(
                    f"UPDATE execution_windows SET {attempt_column}="
                    f"{attempt_column}+1 WHERE window_id=?",
                    [window["window_id"]],
                )
                try:
                    return fetch(http, window)
                except requests.RequestException as exc:
                    status = exc.response.status_code if exc.response is not None else 0
                    retryable = is_transient_status(status)
                    caught: BaseException = exc
                except Exception as exc:
                    retryable = getattr(exc, "retryable", False)
                    caught = exc
                if not retryable or retry_number == 4:
                    raise caught
                sleep_fn(max(1.0, exponential_backoff_seconds(retry_number + 1)))
            raise AssertionError("unreachable")  # pragma: no cover

        books = fetch_with_retry(book_fetch, "book_attempts")
        trades = fetch_with_retry(trade_fetch, "trade_attempts")
        if (
            len(books) > PMXT_MAX_RANGE_SNAPSHOTS
            or len(trades) > PMXT_MAX_RANGE_SNAPSHOTS
        ):
            conn.close()
            raise StageExecutionError("PMXT returned more than the requested limit")
        if (
            len(books) == PMXT_MAX_RANGE_SNAPSHOTS
            or len(trades) == PMXT_MAX_RANGE_SNAPSHOTS
        ):
            start, end = int(window["window_start_ms"]), int(window["window_end_ms"])
            if end - start <= 1:
                conn.close()
                raise StageExecutionError("saturated PMXT range cannot be split")
            midpoint = (start + end) // 2
            conn.execute(
                "UPDATE execution_windows SET status='split' WHERE window_id=?",
                [window["window_id"]],
            )
            for child_start, child_end in ((start, midpoint), (midpoint + 1, end)):
                child_id = _window_id(
                    str(window["clob_token_id"]), child_start, child_end
                )
                conn.execute(
                    """
                    INSERT INTO execution_windows VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending',
                      0, 0, 0, 0, ?) ON CONFLICT DO NOTHING
                    """,
                    [
                        window["root_window_id"],
                        child_id,
                        window["clob_token_id"],
                        window["market_id"],
                        window["condition_id"],
                        child_start,
                        child_end,
                        int(window["depth"]) + 1,
                        now,
                    ],
                )
            continue
        normalized_books = [_canonical_snapshot(window, raw) for raw in books]
        normalized_trades = [_canonical_trade(window, raw) for raw in trades]
        for snapshot in normalized_books:
            existing = conn.execute(
                "SELECT snapshot_sha256 FROM execution_book_snapshots "
                "WHERE clob_token_id=? AND snapshot_timestamp_ms=?",
                [window["clob_token_id"], snapshot["timestamp"]],
            ).fetchall()
            if existing and snapshot["snapshot_sha256"] not in {
                value[0] for value in existing
            }:
                conn.close()
                raise StageExecutionError("contradictory book snapshot")
            conn.execute(
                "INSERT INTO execution_book_snapshots "
                "(window_id, clob_token_id, snapshot_timestamp_ms, "
                "received_timestamp_ms, snapshot_sha256, bids_json, asks_json, "
                "ingested_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT DO NOTHING",
                [
                    window["window_id"],
                    window["clob_token_id"],
                    snapshot["timestamp"],
                    snapshot["received_timestamp"],
                    snapshot["snapshot_sha256"],
                    json.dumps(snapshot["bids"]),
                    json.dumps(snapshot["asks"]),
                    now,
                ],
            )
        for sequence, trade in enumerate(
            sorted(normalized_trades, key=lambda x: (x["timestamp"], x["trade_id"]))
        ):
            existing = conn.execute(
                "SELECT trade_timestamp_ms, price, amount FROM execution_trades "
                "WHERE clob_token_id=? AND trade_id=?",
                [window["clob_token_id"], trade["trade_id"]],
            ).fetchone()
            identity = (trade["timestamp"], trade["price"], trade["amount"])
            if existing and tuple(existing) != identity:
                conn.close()
                raise StageExecutionError("contradictory trade identity")
            conn.execute(
                "INSERT INTO execution_trades "
                "(window_id, clob_token_id, trade_id, trade_timestamp_ms, "
                "received_timestamp_ms, event_sequence, price, amount, ingested_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT DO NOTHING",
                [
                    window["window_id"],
                    window["clob_token_id"],
                    trade["trade_id"],
                    trade["timestamp"],
                    trade["received_timestamp"],
                    sequence,
                    trade["price"],
                    trade["amount"],
                    now,
                ],
            )
        snapshot_count = conn.execute(
            "SELECT count(*) FROM execution_book_snapshots WHERE window_id=?",
            [window["window_id"]],
        ).fetchone()[0]
        trade_count = conn.execute(
            "SELECT count(*) FROM execution_trades WHERE window_id=?",
            [window["window_id"]],
        ).fetchone()[0]
        conn.execute(
            "UPDATE execution_windows SET status='complete', snapshot_count=?, "
            "trade_count=?, updated_at=? WHERE window_id=?",
            [snapshot_count, trade_count, now, window["window_id"]],
        )
    return conn


_TARGET_SCHEMA = pa.schema(
    [
        ("target_id", pa.string()),
        ("implication_id", pa.string()),
        ("team_name", pa.string()),
        ("rule_id", pa.string()),
        ("source_stage_key", pa.string()),
        ("target_stage_key", pa.string()),
        ("signal_minute_epoch", pa.int64()),
        ("signal_minute_utc", pa.timestamp("us")),
        ("decision_timestamp_ms", pa.int64()),
        ("signal_net_edge", pa.float64()),
    ]
)
_LEG_SCHEMA = pa.schema(
    [
        ("target_id", pa.string()),
        ("leg_role", pa.string()),
        ("clob_token_id", pa.string()),
        ("market_id", pa.string()),
        ("condition_id", pa.string()),
        ("outcome_label", pa.string()),
        ("decision_timestamp_ms", pa.int64()),
        ("window_start_ms", pa.int64()),
        ("window_end_ms", pa.int64()),
        ("window_id", pa.string()),
    ]
)
_SNAPSHOT_SCHEMA = pa.schema(
    [
        ("window_id", pa.string()),
        ("clob_token_id", pa.string()),
        ("snapshot_timestamp_ms", pa.int64()),
        ("received_timestamp_ms", pa.int64()),
        ("snapshot_sha256", pa.string()),
        ("best_bid_price", pa.float64()),
        ("best_ask_price", pa.float64()),
        ("spread", pa.float64()),
        ("bid_levels", pa.int64()),
        ("ask_levels", pa.int64()),
        ("ingested_at", pa.timestamp("us")),
    ]
)
_LEVEL_SCHEMA = pa.schema(
    [
        ("snapshot_sha256", pa.string()),
        ("clob_token_id", pa.string()),
        ("snapshot_timestamp_ms", pa.int64()),
        ("book_side", pa.string()),
        ("level_rank", pa.int64()),
        ("price", pa.float64()),
        ("size", pa.float64()),
        ("notional", pa.float64()),
        ("cumulative_size", pa.float64()),
        ("cumulative_notional", pa.float64()),
    ]
)
_TRADE_SCHEMA = pa.schema(
    [
        ("window_id", pa.string()),
        ("clob_token_id", pa.string()),
        ("trade_id", pa.string()),
        ("trade_timestamp_ms", pa.int64()),
        ("received_timestamp_ms", pa.int64()),
        ("event_sequence", pa.int64()),
        ("price", pa.float64()),
        ("amount", pa.float64()),
        ("ingested_at", pa.timestamp("us")),
    ]
)
_COVERAGE_SCHEMA = pa.schema(
    [
        ("window_id", pa.string()),
        ("clob_token_id", pa.string()),
        ("window_start_ms", pa.int64()),
        ("window_end_ms", pa.int64()),
        ("status", pa.string()),
        ("api_attempt_count", pa.int64()),
        ("snapshot_count", pa.int64()),
        ("trade_count", pa.int64()),
        ("empty_book", pa.bool_()),
        ("empty_trades", pa.bool_()),
        ("nearest_asof_age_ms", pa.int64()),
    ]
)


def _table(rows: Iterable[Mapping[str, Any]], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=schema)


def _validate_release_tables(directory: Path, plan: ExecutionPlan) -> None:
    connection = duckdb.connect()
    try:
        for view, name in (
            ("targets", "execution_targets.parquet"),
            ("legs", "target_legs.parquet"),
            ("snapshots", "book_snapshots.parquet"),
            ("levels", "book_levels.parquet"),
            ("trades", "trades.parquet"),
            ("coverage", "coverage.parquet"),
        ):
            path = str(directory / name).replace("'", "''")
            connection.execute(
                f"CREATE VIEW {view} AS SELECT * FROM read_parquet('{path}')"
            )
        checks = {
            "execution target keys": (
                "SELECT count(*) != ? OR count(*) != count(DISTINCT target_id) "
                "OR count(*) != count(DISTINCT (implication_id, signal_minute_epoch)) "
                "FROM targets",
                [len(plan.targets)],
            ),
            "target leg relationships": (
                "SELECT count(*) != ? OR count(*) != count(DISTINCT (target_id, leg_role)) "
                "OR count_if(leg_role NOT IN ('source_no', 'target_yes')) > 0 "
                "OR count_if(t.target_id IS NULL OR c.window_id IS NULL "
                "OR c.clob_token_id != l.clob_token_id) > 0 FROM legs l "
                "LEFT JOIN targets t USING(target_id) "
                "LEFT JOIN coverage c USING(window_id)",
                [len(plan.legs)],
            ),
            "target leg cardinality": (
                "SELECT count(*) > 0 FROM (SELECT target_id FROM legs "
                "GROUP BY target_id HAVING count(*) != 2 "
                "OR count(DISTINCT leg_role) != 2)",
                [],
            ),
            "coverage keys": (
                "SELECT count(*) != ? OR count(*) != count(DISTINCT window_id) "
                "OR count_if(status != 'complete') > 0 FROM coverage",
                [len(plan.windows)],
            ),
            "snapshot relationships": (
                "SELECT count(*) != count(DISTINCT (s.clob_token_id, "
                "s.snapshot_timestamp_ms, s.snapshot_sha256)) "
                "OR count_if(c.window_id IS NULL OR c.clob_token_id != s.clob_token_id) > 0 "
                "OR count_if(s.received_timestamp_ms < s.snapshot_timestamp_ms) > 0 "
                "OR count_if(s.received_timestamp_ms < c.window_start_ms "
                "OR s.received_timestamp_ms > c.window_end_ms) > 0 "
                "FROM snapshots s LEFT JOIN coverage c USING(window_id)",
                [],
            ),
            "level relationships": (
                "SELECT count(*) != count(DISTINCT (l.snapshot_sha256, l.book_side, "
                "l.level_rank)) OR count_if(s.snapshot_sha256 IS NULL) > 0 "
                "OR count_if(l.book_side NOT IN ('bid', 'ask') OR l.level_rank <= 0 "
                "OR l.price < 0 OR l.price > 1 OR l.size <= 0) > 0 "
                "FROM levels l LEFT JOIN snapshots s USING(snapshot_sha256)",
                [],
            ),
            "trade relationships": (
                "SELECT count(*) != count(DISTINCT (t.clob_token_id, t.trade_id)) "
                "OR count_if(c.window_id IS NULL OR c.clob_token_id != t.clob_token_id) > 0 "
                "OR count_if(t.received_timestamp_ms < t.trade_timestamp_ms) > 0 "
                "OR count_if(t.received_timestamp_ms < c.window_start_ms "
                "OR t.received_timestamp_ms > c.window_end_ms) > 0 "
                "OR count_if(t.price < 0 OR t.price > 1 OR t.amount <= 0) > 0 "
                "FROM trades t LEFT JOIN coverage c USING(window_id)",
                [],
            ),
            "coverage evidence counts": (
                "WITH s AS (SELECT window_id, count(*) AS n FROM snapshots "
                "GROUP BY window_id), t AS (SELECT window_id, count(*) AS n "
                "FROM trades GROUP BY window_id) SELECT count(*) > 0 FROM coverage c "
                "LEFT JOIN s USING(window_id) LEFT JOIN t USING(window_id) "
                "WHERE c.snapshot_count != coalesce(s.n, 0) "
                "OR c.trade_count != coalesce(t.n, 0) "
                "OR c.empty_book != (coalesce(s.n, 0) = 0) "
                "OR c.empty_trades != (coalesce(t.n, 0) = 0)",
                [],
            ),
        }
        for label, (query, parameters) in checks.items():
            if connection.execute(query, parameters).fetchone()[0]:
                raise StageExecutionError(f"invalid {label}")
        bad_depth = connection.execute("""
            WITH ranked AS (
              SELECT *, row_number() OVER (
                PARTITION BY snapshot_sha256, book_side ORDER BY level_rank
              ) AS expected_rank,
              sum(size) OVER (
                PARTITION BY snapshot_sha256, book_side ORDER BY level_rank
              ) AS expected_size,
              sum(notional) OVER (
                PARTITION BY snapshot_sha256, book_side ORDER BY level_rank
              ) AS expected_notional,
              lag(price) OVER (
                PARTITION BY snapshot_sha256, book_side ORDER BY level_rank
              ) AS previous_price
              FROM levels
            )
            SELECT count(*) FROM ranked
            WHERE level_rank != expected_rank
               OR abs(cumulative_size - expected_size) > 1e-9
               OR abs(cumulative_notional - expected_notional) > 1e-9
               OR (book_side = 'bid' AND price >= previous_price)
               OR (book_side = 'ask' AND price <= previous_price)
        """).fetchone()[0]
        if bad_depth:
            raise StageExecutionError("invalid cumulative book depth")
        bad_quotes = connection.execute("""
            WITH depth AS (
              SELECT snapshot_sha256,
                     count_if(book_side = 'bid') AS bid_count,
                     count_if(book_side = 'ask') AS ask_count,
                     max(price) FILTER (WHERE book_side = 'bid') AS best_bid,
                     min(price) FILTER (WHERE book_side = 'ask') AS best_ask
              FROM levels GROUP BY snapshot_sha256
            )
            SELECT count(*) FROM snapshots s
            LEFT JOIN depth d USING(snapshot_sha256)
            WHERE s.bid_levels != coalesce(d.bid_count, 0)
               OR s.ask_levels != coalesce(d.ask_count, 0)
               OR s.best_bid_price IS DISTINCT FROM d.best_bid
               OR s.best_ask_price IS DISTINCT FROM d.best_ask
               OR s.spread IS DISTINCT FROM (d.best_ask - d.best_bid)
               OR (d.best_bid IS NOT NULL AND d.best_ask IS NOT NULL
                   AND d.best_bid > d.best_ask)
        """).fetchone()[0]
        if bad_quotes:
            raise StageExecutionError("invalid book summary quotes")
    finally:
        connection.close()


def publish_execution_release(
    plan: ExecutionPlan,
    conn: duckdb.DuckDBPyConnection,
    output_root: Path,
    *,
    generator_commit: str,
    dataset_version: str = DATASET_VERSION,
) -> Path:
    validate_dataset_version(dataset_version)
    if dataset_version != DATASET_VERSION or not COMMIT_RE.fullmatch(generator_commit):
        raise StageExecutionError("invalid dataset version or pipeline revision")
    incomplete = conn.execute(
        "SELECT count(*) FROM execution_windows "
        "WHERE status NOT IN ('complete', 'split')"
    ).fetchone()[0]
    if incomplete:
        raise StageExecutionError("execution acquisition is incomplete")
    release_dir = output_root.resolve() / "releases" / dataset_version
    if release_dir.exists() or release_dir.is_symlink():
        raise FileExistsError(release_dir)
    release_dir.parent.mkdir(parents=True, exist_ok=True)
    lock_dir = release_dir.parent / f".{dataset_version}.publication-lock"
    try:
        lock_dir.mkdir()
    except FileExistsError as exc:
        raise StageExecutionError("another release publication is in progress") from exc
    temporary: Path | None = None
    try:
        if release_dir.exists() or release_dir.is_symlink():
            raise FileExistsError(release_dir)
        temporary = Path(
            tempfile.mkdtemp(prefix=f".{dataset_version}.", dir=release_dir.parent)
        )
        pq.write_table(
            _table(plan.targets, _TARGET_SCHEMA),
            temporary / "execution_targets.parquet",
            compression="zstd",
        )
        pq.write_table(
            _table(plan.legs, _LEG_SCHEMA),
            temporary / "target_legs.parquet",
            compression="zstd",
        )
        snapshots: list[dict[str, Any]] = []
        levels: list[dict[str, Any]] = []
        rows = conn.execute("""
            SELECT w.root_window_id, s.clob_token_id, s.snapshot_timestamp_ms,
                   s.received_timestamp_ms, s.snapshot_sha256, s.bids_json,
                   s.asks_json, s.ingested_at
            FROM execution_book_snapshots s
            JOIN execution_windows w USING(window_id)
            ORDER BY clob_token_id, snapshot_timestamp_ms, snapshot_sha256
        """).fetchall()
        for (
            window_id,
            token,
            timestamp,
            received_timestamp,
            digest,
            bids_json,
            asks_json,
            ingested_at,
        ) in rows:
            bids, asks = json.loads(bids_json), json.loads(asks_json)
            best_bid = float(bids[0]["price"]) if bids else None
            best_ask = float(asks[0]["price"]) if asks else None
            snapshots.append(
                {
                    "window_id": window_id,
                    "clob_token_id": token,
                    "snapshot_timestamp_ms": timestamp,
                    "received_timestamp_ms": received_timestamp,
                    "snapshot_sha256": digest,
                    "best_bid_price": best_bid,
                    "best_ask_price": best_ask,
                    "spread": best_ask - best_bid
                    if best_bid is not None and best_ask is not None
                    else None,
                    "bid_levels": len(bids),
                    "ask_levels": len(asks),
                    "ingested_at": ingested_at,
                }
            )
            for side, book in (("bid", bids), ("ask", asks)):
                cumulative_size = cumulative_notional = 0.0
                for rank, level in enumerate(book, 1):
                    price, size = float(level["price"]), float(level["size"])
                    cumulative_size += size
                    cumulative_notional += price * size
                    levels.append(
                        {
                            "snapshot_sha256": digest,
                            "clob_token_id": token,
                            "snapshot_timestamp_ms": timestamp,
                            "book_side": side,
                            "level_rank": rank,
                            "price": price,
                            "size": size,
                            "notional": price * size,
                            "cumulative_size": cumulative_size,
                            "cumulative_notional": cumulative_notional,
                        }
                    )
        pq.write_table(
            _table(snapshots, _SNAPSHOT_SCHEMA),
            temporary / "book_snapshots.parquet",
            compression="zstd",
        )
        pq.write_table(
            _table(levels, _LEVEL_SCHEMA),
            temporary / "book_levels.parquet",
            compression="zstd",
        )
        trade_columns = [field.name for field in _TRADE_SCHEMA]
        trade_rows = [
            dict(zip(trade_columns, row, strict=True))
            for row in conn.execute("""
                SELECT w.root_window_id, t.clob_token_id, t.trade_id,
                       t.trade_timestamp_ms, t.received_timestamp_ms,
                       t.event_sequence, t.price, t.amount, t.ingested_at
                FROM execution_trades t
                JOIN execution_windows w USING(window_id)
                ORDER BY t.clob_token_id, t.trade_timestamp_ms, t.trade_id
            """).fetchall()
        ]
        pq.write_table(
            _table(trade_rows, _TRADE_SCHEMA),
            temporary / "trades.parquet",
            compression="zstd",
        )
        decisions: dict[str, list[int]] = defaultdict(list)
        for leg in plan.legs:
            decisions[str(leg["window_id"])].append(int(leg["decision_timestamp_ms"]))
        coverage: list[dict[str, Any]] = []
        for window in plan.windows:
            window_id = str(window["window_id"])
            descendants = conn.execute(
                "SELECT status, book_attempts + trade_attempts "
                "FROM execution_windows WHERE root_window_id=?",
                [window_id],
            ).fetchall()
            timestamps = [
                (row[0], row[1])
                for row in conn.execute(
                    """
                SELECT s.snapshot_timestamp_ms, s.received_timestamp_ms
                FROM execution_book_snapshots s
                JOIN execution_windows w USING(window_id) WHERE w.root_window_id=?
                """,
                    [window_id],
                ).fetchall()
            ]
            trades_count = int(
                conn.execute(
                    """
                    SELECT count(*) FROM execution_trades t
                    JOIN execution_windows w USING(window_id)
                    WHERE w.root_window_id=?
                    """,
                    [window_id],
                ).fetchone()[0]
            )
            ages = [
                decision
                - max(
                    (source for source, receipt in timestamps if receipt <= decision),
                    default=decision + 1,
                )
                for decision in decisions[window_id]
            ]
            valid_ages = [age for age in ages if age >= 0]
            snapshots_count = len(timestamps)
            coverage.append(
                {
                    "window_id": window_id,
                    "clob_token_id": window["clob_token_id"],
                    "window_start_ms": window["window_start_ms"],
                    "window_end_ms": window["window_end_ms"],
                    "status": "complete"
                    if descendants
                    and all(row[0] in {"complete", "split"} for row in descendants)
                    else "incomplete",
                    "api_attempt_count": sum(int(row[1]) for row in descendants),
                    "snapshot_count": snapshots_count,
                    "trade_count": trades_count,
                    "empty_book": snapshots_count == 0,
                    "empty_trades": trades_count == 0,
                    "nearest_asof_age_ms": max(valid_ages) if valid_ages else None,
                }
            )
        if any(row["status"] != "complete" for row in coverage):
            raise StageExecutionError("execution acquisition is incomplete")
        pq.write_table(
            _table(coverage, _COVERAGE_SCHEMA),
            temporary / "coverage.parquet",
            compression="zstd",
        )
        schema_payload = {
            "contract_version": CONTRACT_VERSION,
            "files": {
                name: [
                    {
                        "name": field.name,
                        "type": str(field.type),
                        "nullable": field.nullable,
                    }
                    for field in pq.ParquetFile(temporary / name).schema_arrow
                ]
                for name in sorted(OUTPUT_FILES)
                if name.endswith(".parquet")
            },
        }
        write_json(temporary / "SCHEMA.json", schema_payload)
        _validate_release_tables(temporary, plan)
        source_mode_row = conn.execute(
            "SELECT value FROM execution_audit WHERE key='source_mode'"
        ).fetchone()
        source_mode = source_mode_row[0] if source_mode_row else "api-range"
        archive_objects: list[dict[str, Any]] = []
        archive_table = conn.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_name='execution_archive_objects'"
        ).fetchone()[0]
        if archive_table:
            archive_objects = [
                {
                    "object_key": row[0],
                    "source_url": row[1],
                    "status": row[2],
                    "http_attempts": row[3],
                    "byte_size": row[4],
                    "sha256": row[5],
                    "etag": row[6],
                    "event_count": row[7],
                }
                for row in conn.execute(
                    "SELECT object_key, source_url, status, http_attempts, byte_size, "
                    "sha256, etag, event_count FROM execution_archive_objects "
                    "ORDER BY hour_start_ms"
                ).fetchall()
            ]
        audit_attempts = conn.execute(
            "SELECT value FROM execution_audit WHERE key='api_attempt_count'"
        ).fetchone()
        manifest: dict[str, Any] = {
            "contract_version": CONTRACT_VERSION,
            "dataset_version": dataset_version,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "pipeline_revision": generator_commit,
            "source_labels": {
                "books": (
                    "archive.pmxt.dev/Polymarket/v2"
                    if source_mode == "archive-v2"
                    else PMXT_ORDER_BOOK_SOURCE
                ),
                "trades": (
                    "archive.pmxt.dev/Polymarket/v2"
                    if source_mode == "archive-v2"
                    else PMXT_TRADES_SOURCE
                ),
            },
            "source_license": "CC-BY-4.0" if source_mode == "archive-v2" else None,
            "inputs": {
                "stage_minute_manifest_sha256": STAGE_MINUTE_MANIFEST_SHA256,
                "ohlc_report_manifest_sha256": sha256_file(
                    plan.ohlc_report / "MANIFEST.json"
                ),
                "strategy_revision": OHLC_STRATEGY_SHA,
            },
            "configuration": {
                "window_seconds": plan.window_seconds,
                "request_budget": plan.request_budget,
                "primary_scenario": PRIMARY_SCENARIO,
                "source_mode": source_mode,
            },
            "planning": plan.summary(),
            "request_audit": {
                "api_attempt_count": (
                    int(audit_attempts[0])
                    if audit_attempts
                    else sum(row["api_attempt_count"] for row in coverage)
                ),
                "archive_http_attempt_count": sum(
                    int(row["http_attempts"]) for row in archive_objects
                ),
            },
            "archive_objects": archive_objects,
            "exclusions": [],
            "counts": {
                "targets": len(plan.targets),
                "legs": len(plan.legs),
                "windows": len(plan.windows),
                "book_snapshots": len(snapshots),
                "book_levels": len(levels),
                "trades": len(trade_rows),
                "empty_book_windows": sum(row["empty_book"] for row in coverage),
                "empty_trade_windows": sum(row["empty_trades"] for row in coverage),
            },
            "files": {},
        }
        for name in sorted(OUTPUT_FILES - {"MANIFEST.json", "CHECKSUMS.sha256"}):
            path = temporary / name
            metadata: dict[str, Any] = {
                "sha256": sha256_file(path),
                "byte_size": path.stat().st_size,
            }
            if name.endswith(".parquet"):
                parquet = pq.ParquetFile(path)
                metadata.update(
                    row_count=parquet.metadata.num_rows,
                    schema_fingerprint=schema_fingerprint(parquet.schema_arrow),
                )
            manifest["files"][name] = metadata
        write_json(temporary / "MANIFEST.json", manifest)
        write_checksums(temporary, file_names=set(OUTPUT_FILES))
        if {path.name for path in temporary.iterdir()} != OUTPUT_FILES:
            raise StageExecutionError("execution release inventory mismatch")
        verified = _verify_bundle(temporary, OUTPUT_FILES)
        if (
            verified.get("contract_version") != CONTRACT_VERSION
            or verified.get("dataset_version") != dataset_version
        ):
            raise StageExecutionError("execution release contract drift")
        if release_dir.exists() or release_dir.is_symlink():
            raise FileExistsError(release_dir)
        os.replace(temporary, release_dir)
    except BaseException:
        if temporary is not None:
            shutil.rmtree(temporary, ignore_errors=True)
        raise
    finally:
        lock_dir.rmdir()
    return release_dir


def current_generator_commit() -> str:
    return current_clean_commit(BASE_DIR)


def preflight_execution_release(
    output_root: Path, *, dataset_version: str = DATASET_VERSION
) -> tuple[Path, str]:
    """Resolve all deterministic release blockers before paid acquisition."""
    validate_dataset_version(dataset_version)
    if dataset_version != DATASET_VERSION:
        raise StageExecutionError("invalid dataset version")
    release_dir = output_root.expanduser().resolve() / "releases" / dataset_version
    lock_dir = release_dir.parent / f".{dataset_version}.publication-lock"
    if release_dir.exists() or release_dir.is_symlink():
        raise FileExistsError(release_dir)
    if lock_dir.exists() or lock_dir.is_symlink():
        raise StageExecutionError("another release publication is in progress")
    return release_dir, current_generator_commit()


__all__ = [
    "CONTRACT_VERSION",
    "DATASET_VERSION",
    "DEFAULT_OUTPUT_ROOT",
    "ExecutionPlan",
    "StageExecutionError",
    "acquire_execution_evidence",
    "build_execution_plan",
    "current_generator_commit",
    "preflight_execution_release",
    "publish_execution_release",
]

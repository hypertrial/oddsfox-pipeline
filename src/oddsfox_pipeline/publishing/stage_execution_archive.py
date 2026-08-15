"""Reconstruct targeted stage-execution books from the public PMXT v2 archive."""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Final, Mapping

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from oddsfox_pipeline.config.settings import PMXT_API_KEY
from oddsfox_pipeline.config.settings_warehouse import DUCKDB_PATH
from oddsfox_pipeline.ingestion.polymarket.match_order_book import (
    PMXT_ORDER_BOOK_ENDPOINT,
    build_pmxt_client,
)
from oddsfox_pipeline.publishing._bundle_io import sha256_file
from oddsfox_pipeline.publishing.stage_execution import (
    STAGE_MINUTE_MANIFEST_SHA256,
    ExecutionPlan,
    StageExecutionError,
    _normalize_levels,
    _reserve_shared_pmxt_attempt,
    _state_schema,
)
from oddsfox_pipeline.resources.http_retry import (
    exponential_backoff_seconds,
    is_transient_status,
)

ARCHIVE_SOURCE: Final = "archive.pmxt.dev/Polymarket/v2"
ARCHIVE_BASE_URL: Final = "https://r2v2.pmxt.dev"
ARCHIVE_LICENSE: Final = "CC-BY-4.0"
ARCHIVE_COLUMNS: Final = (
    "timestamp_received",
    "timestamp",
    "market",
    "event_type",
    "asset_id",
    "bids",
    "asks",
    "price",
    "size",
    "side",
    "best_bid",
    "best_ask",
    "fee_rate_bps",
    "transaction_hash",
    "old_tick_size",
    "new_tick_size",
)
HOUR_MS: Final = 3_600_000


def _hour_start(value_ms: int) -> int:
    return value_ms - value_ms % HOUR_MS


def archive_object_key(hour_ms: int) -> str:
    value = datetime.fromtimestamp(hour_ms / 1_000, timezone.utc)
    return f"polymarket_orderbook_{value:%Y-%m-%dT%H}.parquet"


def archive_work(
    plan: ExecutionPlan,
) -> dict[int, dict[str, tuple[Mapping[str, Any], ...]]]:
    """Map UTC archive hours to token windows, including cross-hour windows."""
    result: dict[int, dict[str, list[Mapping[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for window in plan.windows:
        start = _hour_start(int(window["window_start_ms"]))
        end = _hour_start(int(window["window_end_ms"]))
        hour = start
        while hour <= end:
            result[hour][str(window["clob_token_id"])].append(window)
            hour += HOUR_MS
    return {
        hour: {
            token: tuple(sorted(windows, key=lambda row: str(row["window_id"])))
            for token, windows in sorted(tokens.items())
        }
        for hour, tokens in sorted(result.items())
    }


def archive_plan_summary(plan: ExecutionPlan) -> dict[str, Any]:
    work = archive_work(plan)
    token_hours = sum(len(tokens) for tokens in work.values())
    summary = plan.summary()
    summary.pop("estimated_storage_bytes", None)
    return {
        **summary,
        "source": "archive-v2",
        "archive_hours": len(work),
        "token_hours": token_hours,
        "minimum_requests": token_hours,
        "within_budget": token_hours <= plan.request_budget,
        "estimated_storage_bytes_min": len(work) * 100_000_000,
        "estimated_storage_bytes_max": len(work) * 400_000_000,
    }


def _archive_state_schema(conn: duckdb.DuckDBPyConnection) -> None:
    _state_schema(conn)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS execution_archive_seeds(
          hour_start_ms BIGINT, clob_token_id VARCHAR, source_timestamp_ms BIGINT,
          bids_json VARCHAR, asks_json VARCHAR, api_attempts INTEGER,
          updated_at TIMESTAMP,
          PRIMARY KEY(hour_start_ms, clob_token_id))
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS execution_archive_objects(
          hour_start_ms BIGINT PRIMARY KEY, object_key VARCHAR, source_url VARCHAR,
          status VARCHAR, http_attempts INTEGER, byte_size BIGINT, sha256 VARCHAR,
          etag VARCHAR, event_count BIGINT, updated_at TIMESTAMP)
    """)


def _default_seed_fetch(
    client: Any, token_id: str, hour_start_ms: int
) -> Mapping[str, Any]:
    payload = client.post(
        PMXT_ORDER_BOOK_ENDPOINT,
        headers={"Authorization": f"Bearer {PMXT_API_KEY}"},
        json={"args": [token_id, None, {"since": hour_start_ms}]},
    )
    if not isinstance(payload, dict) or payload.get("success") is not True:
        error = payload.get("error") if isinstance(payload, dict) else None
        exc = StageExecutionError("PMXT seed request failed")
        exc.retryable = isinstance(error, dict) and error.get("retryable") is True
        raise exc
    data = payload.get("data")
    if not isinstance(data, dict):
        raise StageExecutionError("PMXT seed response is not one order book")
    return data


def _default_download(url: str, destination: Path) -> Mapping[str, Any]:
    response = requests.get(url, stream=True, timeout=(15, 120))
    if response.status_code == 404:
        return {"status": "missing", "etag": response.headers.get("ETag")}
    response.raise_for_status()
    digest = hashlib.sha256()
    byte_size = 0
    with destination.open("xb") as handle:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            if chunk:
                handle.write(chunk)
                digest.update(chunk)
                byte_size += len(chunk)
    return {
        "status": "downloaded",
        "etag": response.headers.get("ETag"),
        "byte_size": byte_size,
        "sha256": digest.hexdigest(),
    }


def _timestamp_ms(value: Any, label: str) -> int:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise StageExecutionError(f"archive {label} is not a timezone-aware timestamp")
    return int(value.timestamp() * 1_000)


def _condition(value: Any) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("ascii")
        except UnicodeDecodeError as exc:
            raise StageExecutionError("invalid archive condition id") from exc
    return str(value)


def _archive_levels(value: Any, *, side: str) -> list[dict[str, str]]:
    try:
        raw = json.loads(value) if isinstance(value, str) else value
    except json.JSONDecodeError as exc:
        raise StageExecutionError("invalid archive depth JSON") from exc
    if not isinstance(raw, list):
        raise StageExecutionError("invalid archive depth")
    return _normalize_levels(
        [
            {"price": item[0], "size": item[1]}
            if isinstance(item, list) and len(item) == 2
            else item
            for item in raw
        ],
        side=side,
    )


def _level_map(levels: list[dict[str, str]]) -> dict[Decimal, Decimal]:
    return {Decimal(row["price"]): Decimal(row["size"]) for row in levels}


def _levels_json(levels: Mapping[Decimal, Decimal], *, bids: bool) -> str:
    rows = [
        {"price": str(price), "size": str(size)}
        for price, size in sorted(levels.items(), reverse=bids)
        if size > 0
    ]
    return json.dumps(rows, sort_keys=True, separators=(",", ":"))


def _matching_window(
    windows: tuple[Mapping[str, Any], ...], received_ms: int
) -> Mapping[str, Any] | None:
    matches = [
        window
        for window in windows
        if int(window["window_start_ms"]) <= received_ms <= int(window["window_end_ms"])
    ]
    if len(matches) > 1:
        raise StageExecutionError("coalesced windows overlap unexpectedly")
    return matches[0] if matches else None


def _validate_archive_table(table: pa.Table) -> None:
    if tuple(table.column_names) != ARCHIVE_COLUMNS:
        raise StageExecutionError("PMXT archive schema drift")
    allowed = {"book", "price_change", "last_trade_price", "tick_size_change"}
    if any(value not in allowed for value in table["event_type"].to_pylist()):
        raise StageExecutionError("unknown PMXT archive event type")


def _insert_snapshot(
    conn: duckdb.DuckDBPyConnection,
    window: Mapping[str, Any],
    token: str,
    source_ms: int,
    received_ms: int,
    bids: Mapping[Decimal, Decimal],
    asks: Mapping[Decimal, Decimal],
    ingested_at: datetime,
) -> None:
    bids_json = _levels_json(bids, bids=True)
    asks_json = _levels_json(asks, bids=False)
    best_bid = max(bids, default=None)
    best_ask = min(asks, default=None)
    if best_bid is not None and best_ask is not None and best_bid > best_ask:
        raise StageExecutionError("crossed reconstructed archive book")
    canonical = {
        "clob_token_id": token,
        "timestamp": source_ms,
        "bids": json.loads(bids_json),
        "asks": json.loads(asks_json),
    }
    digest = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    existing = conn.execute(
        "SELECT received_timestamp_ms, bids_json, asks_json FROM "
        "execution_book_snapshots WHERE clob_token_id=? AND "
        "snapshot_timestamp_ms=? AND snapshot_sha256=?",
        [token, source_ms, digest],
    ).fetchone()
    if existing:
        if existing[1:] != (bids_json, asks_json):
            raise StageExecutionError("contradictory archive book identity")
        return
    conn.execute(
        "INSERT INTO execution_book_snapshots "
        "(window_id, clob_token_id, snapshot_timestamp_ms, received_timestamp_ms, "
        "snapshot_sha256, bids_json, asks_json, ingested_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            window["window_id"],
            token,
            source_ms,
            received_ms,
            digest,
            bids_json,
            asks_json,
            ingested_at,
        ],
    )


def _insert_trade(
    conn: duckdb.DuckDBPyConnection,
    window: Mapping[str, Any],
    row: Mapping[str, Any],
    token: str,
    source_ms: int,
    received_ms: int,
    sequence: int,
    ingested_at: datetime,
) -> None:
    try:
        price = float(Decimal(str(row["price"])))
        amount = float(Decimal(str(row["size"])))
    except (ArithmeticError, KeyError, ValueError) as exc:
        raise StageExecutionError("invalid archive trade") from exc
    if not math.isfinite(price) or not 0 <= price <= 1 or not amount > 0:
        raise StageExecutionError("invalid archive trade")
    identity = {
        "token": token,
        "source_ms": source_ms,
        "received_ms": received_ms,
        "price": price,
        "amount": amount,
        "side": row.get("side"),
        "transaction_hash": row.get("transaction_hash"),
    }
    trade_id = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    conn.execute(
        "INSERT INTO execution_trades "
        "(window_id, clob_token_id, trade_id, trade_timestamp_ms, "
        "received_timestamp_ms, event_sequence, price, amount, ingested_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        [
            window["window_id"],
            token,
            trade_id,
            source_ms,
            received_ms,
            sequence,
            price,
            amount,
            ingested_at,
        ],
    )


def _process_object(
    conn: duckdb.DuckDBPyConnection,
    path: Path,
    hour_ms: int,
    token_windows: Mapping[str, tuple[Mapping[str, Any], ...]],
    ingested_at: datetime,
) -> int:
    tokens = list(token_windows)
    table = pq.read_table(path, filters=[("asset_id", "in", tokens)])
    _validate_archive_table(table)
    rows = sorted(
        table.to_pylist(),
        key=lambda row: (
            str(row["asset_id"]),
            _timestamp_ms(row["timestamp_received"], "receipt timestamp"),
            _timestamp_ms(row["timestamp"], "source timestamp"),
        ),
    )
    states: dict[str, tuple[dict[Decimal, Decimal], dict[Decimal, Decimal]]] = {}
    for token in tokens:
        seed = conn.execute(
            "SELECT bids_json, asks_json FROM execution_archive_seeds "
            "WHERE hour_start_ms=? AND clob_token_id=?",
            [hour_ms, token],
        ).fetchone()
        if seed is None:
            raise StageExecutionError("archive seed is missing")
        states[token] = (
            _level_map(_archive_levels(seed[0], side="bids")),
            _level_map(_archive_levels(seed[1], side="asks")),
        )
    for sequence, row in enumerate(rows):
        token = str(row["asset_id"])
        windows = token_windows[token]
        condition_ids = {str(window["condition_id"]) for window in windows}
        if _condition(row["market"]) not in condition_ids:
            raise StageExecutionError("archive token/condition mismatch")
        received_ms = _timestamp_ms(row["timestamp_received"], "receipt timestamp")
        source_ms = _timestamp_ms(row["timestamp"], "source timestamp")
        if received_ms < source_ms or not hour_ms <= received_ms < hour_ms + HOUR_MS:
            raise StageExecutionError("invalid archive event timestamps")
        bids, asks = states[token]
        event_type = str(row["event_type"])
        changed = False
        if event_type == "book":
            bids.clear()
            bids.update(_level_map(_archive_levels(row["bids"], side="bids")))
            asks.clear()
            asks.update(_level_map(_archive_levels(row["asks"], side="asks")))
            changed = True
        elif event_type == "price_change":
            try:
                price = Decimal(str(row["price"]))
                size = Decimal(str(row["size"]))
            except ArithmeticError as exc:
                raise StageExecutionError("invalid archive price change") from exc
            side = str(row["side"])
            if side not in {"BUY", "SELL"} or not 0 <= price <= 1 or size < 0:
                raise StageExecutionError("invalid archive price change")
            levels = bids if side == "BUY" else asks
            if size == 0:
                levels.pop(price, None)
            else:
                levels[price] = size
            changed = True
        window = _matching_window(windows, received_ms)
        if window is not None and changed:
            _insert_snapshot(
                conn,
                window,
                token,
                source_ms,
                received_ms,
                bids,
                asks,
                ingested_at,
            )
        if window is not None and event_type == "last_trade_price":
            _insert_trade(
                conn,
                window,
                row,
                token,
                source_ms,
                received_ms,
                sequence,
                ingested_at,
            )
    return len(rows)


def acquire_archive_execution_evidence(
    plan: ExecutionPlan,
    state_path: Path,
    *,
    credit_ledger_path: Path = DUCKDB_PATH,
    seed_fetch: Callable[[Any, str, int], Mapping[str, Any]] = _default_seed_fetch,
    download: Callable[[str, Path], Mapping[str, Any]] = _default_download,
    client: Any | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> duckdb.DuckDBPyConnection:
    """Acquire targeted evidence with one API seed per token-hour."""
    work = archive_work(plan)
    token_hours = sum(len(tokens) for tokens in work.values())
    if token_hours > plan.request_budget:
        raise StageExecutionError(
            f"planned seed requests {token_hours} exceed budget {plan.request_budget}; "
            "no network requests were made"
        )
    if client is None and not PMXT_API_KEY.strip():
        raise StageExecutionError("PMXT_API_KEY is required for archive seeds")
    state_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(state_path))
    _archive_state_schema(conn)
    input_key = hashlib.sha256(
        (
            STAGE_MINUTE_MANIFEST_SHA256
            + sha256_file(plan.ohlc_report / "MANIFEST.json")
            + "archive-v2"
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
    conn.execute(
        "INSERT OR REPLACE INTO execution_audit VALUES ('source_mode', 'archive-v2')"
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
    for hour_ms, token_windows in work.items():
        complete = conn.execute(
            "SELECT status FROM execution_archive_objects WHERE hour_start_ms=?",
            [hour_ms],
        ).fetchone()
        if complete and complete[0] in {"complete", "missing"}:
            continue
        for token in token_windows:
            if conn.execute(
                "SELECT 1 FROM execution_archive_seeds WHERE hour_start_ms=? "
                "AND clob_token_id=?",
                [hour_ms, token],
            ).fetchone():
                continue
            seed_attempts = 0
            for retry in range(5):
                seed_attempts += 1
                if not _reserve_shared_pmxt_attempt(
                    credit_ledger_path, plan.request_budget
                ):
                    conn.close()
                    raise StageExecutionError(
                        "shared monthly PMXT request budget exhausted; checkpoint preserved"
                    )
                try:
                    seed = seed_fetch(http, token, hour_ms)
                    break
                except requests.RequestException as exc:
                    retryable = is_transient_status(
                        exc.response.status_code if exc.response is not None else 0
                    )
                    caught: BaseException = exc
                except Exception as exc:
                    retryable = getattr(exc, "retryable", False)
                    caught = exc
                if not retryable or retry == 4:
                    conn.close()
                    raise caught
                sleep_fn(max(1.0, exponential_backoff_seconds(retry + 1)))
            try:
                source_ms = int(Decimal(str(seed["timestamp"])))
                bids = _archive_levels(seed.get("bids"), side="bids")
                asks = _archive_levels(seed.get("asks"), side="asks")
            except (ArithmeticError, KeyError, ValueError) as exc:
                conn.close()
                raise StageExecutionError("invalid PMXT seed book") from exc
            if source_ms > hour_ms:
                conn.close()
                raise StageExecutionError("PMXT seed is from the future")
            conn.execute(
                "INSERT INTO execution_archive_seeds VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    hour_ms,
                    token,
                    source_ms,
                    json.dumps(bids, sort_keys=True, separators=(",", ":")),
                    json.dumps(asks, sort_keys=True, separators=(",", ":")),
                    seed_attempts,
                    now,
                ],
            )
        object_key = archive_object_key(hour_ms)
        url = f"{ARCHIVE_BASE_URL}/{object_key}"
        archive_path: Path | None = None
        attempts = 0
        try:
            for retry in range(5):
                attempts += 1
                descriptor = None
                fd, raw_path = tempfile.mkstemp(prefix="pmxt-v2-", suffix=".parquet")
                os.close(fd)
                archive_path = Path(raw_path)
                archive_path.unlink()
                try:
                    descriptor = download(url, archive_path)
                    break
                except requests.RequestException as exc:
                    retryable = is_transient_status(
                        exc.response.status_code if exc.response is not None else 0
                    )
                    caught = exc
                except Exception as exc:
                    retryable = getattr(exc, "retryable", False)
                    caught = exc
                if archive_path.exists():
                    archive_path.unlink()
                if not retryable or retry == 4:
                    conn.close()
                    raise caught
                sleep_fn(max(1.0, exponential_backoff_seconds(retry + 1)))
            if descriptor is None or descriptor.get("status") not in {
                "downloaded",
                "missing",
            }:
                raise StageExecutionError("invalid archive download result")
            event_count = 0
            if descriptor["status"] == "downloaded":
                if archive_path is None or not archive_path.is_file():
                    raise StageExecutionError("archive download did not create a file")
                actual_bytes = archive_path.stat().st_size
                actual_sha = sha256_file(archive_path)
                if (
                    descriptor.get("byte_size") != actual_bytes
                    or descriptor.get("sha256") != actual_sha
                ):
                    raise StageExecutionError("archive download integrity mismatch")
                conn.execute("BEGIN TRANSACTION")
                try:
                    event_count = _process_object(
                        conn, archive_path, hour_ms, token_windows, now
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO execution_archive_objects VALUES "
                        "(?, ?, ?, 'complete', ?, ?, ?, ?, ?, ?)",
                        [
                            hour_ms,
                            object_key,
                            url,
                            attempts,
                            actual_bytes,
                            actual_sha,
                            descriptor.get("etag"),
                            event_count,
                            now,
                        ],
                    )
                    conn.execute("COMMIT")
                except BaseException:
                    conn.execute("ROLLBACK")
                    raise
            else:
                conn.execute(
                    "INSERT OR REPLACE INTO execution_archive_objects VALUES "
                    "(?, ?, ?, 'missing', ?, 0, NULL, ?, 0, ?)",
                    [hour_ms, object_key, url, attempts, descriptor.get("etag"), now],
                )
        finally:
            if archive_path is not None and archive_path.exists():
                archive_path.unlink()
    for window in plan.windows:
        window_id = str(window["window_id"])
        snapshots = conn.execute(
            "SELECT count(*) FROM execution_book_snapshots WHERE window_id=?",
            [window_id],
        ).fetchone()[0]
        trades = conn.execute(
            "SELECT count(*) FROM execution_trades WHERE window_id=?", [window_id]
        ).fetchone()[0]
        seed_hours = (
            _hour_start(int(window["window_end_ms"]))
            - _hour_start(int(window["window_start_ms"]))
        ) // HOUR_MS + 1
        conn.execute(
            "UPDATE execution_windows SET status='complete', book_attempts=?, "
            "trade_attempts=0, snapshot_count=?, trade_count=?, updated_at=? "
            "WHERE window_id=?",
            [seed_hours, snapshots, trades, now, window_id],
        )
    api_attempts = conn.execute(
        "SELECT coalesce(sum(api_attempts), 0) FROM execution_archive_seeds"
    ).fetchone()[0]
    conn.execute(
        "INSERT OR REPLACE INTO execution_audit VALUES ('api_attempt_count', ?)",
        [str(api_attempts)],
    )
    return conn


__all__ = [
    "ARCHIVE_BASE_URL",
    "ARCHIVE_LICENSE",
    "ARCHIVE_SOURCE",
    "acquire_archive_execution_evidence",
    "archive_object_key",
    "archive_plan_summary",
    "archive_work",
]

"""Resumable PMXT historical trade ingestion for a published portrait scan."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import requests

from oddsfox_pipeline.config.settings import PMXT_API_KEY
from oddsfox_pipeline.ingestion.polymarket.match_order_book import (
    MatchOrderBookPaused,
    MatchOrderBookSyncError,
    _decimal_string,
    _pmxt_books,
    build_pmxt_client,
    load_order_book_manifest,
)
from oddsfox_pipeline.resources.http_retry import (
    exponential_backoff_seconds,
    is_transient_status,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_wc2026_ops_tbl,
    polymarket_wc2026_raw_tbl,
)

ENDPOINT = "/api/polymarket/fetchTrades"
SOURCE = "api.pmxt.dev/api/polymarket/fetchTrades"
LIMIT = 1_000
RUNS = polymarket_wc2026_ops_tbl("match_trade_scan_runs")
WINDOWS = polymarket_wc2026_ops_tbl("match_trade_scan_windows")
TRADES = polymarket_wc2026_raw_tbl("match_trades")
BOOK_RUNS = polymarket_wc2026_ops_tbl("match_order_book_scan_runs")
METADATA = polymarket_wc2026_ops_tbl("scrape_metadata")


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _reserve_credit(conn: Any, scan_id: str, budget: int) -> bool:
    utc_month = datetime.now(timezone.utc).date().replace(day=1)
    key = f"pmxt_api_attempts_{utc_month}"
    conn.execute("BEGIN TRANSACTION")
    try:
        value = conn.execute(
            f"SELECT value FROM {METADATA} WHERE key=?", [key]
        ).fetchone()
        if value and int(value[0]) >= budget:
            conn.execute("COMMIT")
            return False
        conn.execute(
            f"""
            INSERT INTO {METADATA} (key, value) VALUES (?, '1')
            ON CONFLICT (key) DO UPDATE
            SET value=CAST(CAST({METADATA}.value AS BIGINT)+1 AS VARCHAR)
            """,
            [key],
        )
        conn.execute(f"UPDATE {RUNS} SET status='running' WHERE scan_id=?", [scan_id])
        conn.execute("COMMIT")
        return True
    except Exception:  # pragma: no cover - defensive transaction boundary
        conn.execute("ROLLBACK")
        raise


def _normalize(
    trade: Any,
    *,
    scan_id: str,
    manifest_sha256: str,
    target: Any,
    outcome: Any,
    ordinal: int,
    window_start_ms: int,
    window_end_ms: int,
) -> dict[str, Any]:
    if not isinstance(trade, dict):
        raise ValueError("PMXT trade must be an object")
    trade_id = str(trade.get("id") or "").strip()
    if not trade_id:
        raise ValueError("PMXT trade id must not be blank")
    raw_timestamp = Decimal(str(trade.get("timestamp")))
    timestamp = int(raw_timestamp)
    if raw_timestamp != timestamp:
        raise ValueError("PMXT trade timestamp must be an integer")
    if not window_start_ms <= timestamp <= window_end_ms:
        raise ValueError("PMXT trade timestamp is outside the requested range")
    outcome_id = str(trade.get("outcomeId") or outcome.clob_token_id)
    if outcome_id != outcome.clob_token_id:
        raise ValueError("PMXT trade outcomeId changed")
    return {
        "scan_id": scan_id,
        "manifest_sha256": manifest_sha256,
        "fifa_match_id": target.fifa_match_id,
        "market_id": target.market_id,
        "clob_token_id": outcome.clob_token_id,
        "landscape_role": outcome.role,
        "trade_id": trade_id,
        "trade_timestamp_ms": timestamp,
        "event_sequence": ordinal,
        "price": _decimal_string(
            trade.get("price"),
            field="trade.price",
            minimum=Decimal("0"),
            maximum=Decimal("1"),
        ),
        "amount": _decimal_string(
            trade.get("amount"), field="trade.amount", strictly_positive=True
        ),
        "source_endpoint": SOURCE,
        "ingested_at": _now(),
    }


def _request(
    client: Any,
    *,
    api_key: str,
    token_id: str,
    start: int,
    end: int,
    retries: int,
    backoff: float,
    sleep_fn: Callable[[float], None],
) -> list[dict[str, Any]]:
    start_iso = (
        datetime.fromtimestamp(start / 1_000, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )
    end_iso = (
        datetime.fromtimestamp(end / 1_000, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )
    for attempt in range(retries + 1):
        try:
            payload = client.get(
                ENDPOINT,
                headers={"Authorization": f"Bearer {api_key}"},
                params={
                    "outcomeId": token_id,
                    "start": start_iso,
                    "end": end_iso,
                    "limit": LIMIT,
                },
            )
            return _pmxt_books(payload)
        except requests.RequestException as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if is_transient_status(status) and attempt < retries:
                sleep_fn(max(backoff, exponential_backoff_seconds(attempt + 1)))
                continue
            if status == 429:
                raise MatchOrderBookPaused(
                    "PMXT rate or credit limit remained exhausted",
                    {"status": "paused", "reason": "upstream_429"},
                ) from exc
            raise
        except Exception as exc:
            if getattr(exc, "retryable", False) and attempt < retries:
                sleep_fn(max(backoff, exponential_backoff_seconds(attempt + 1)))
                continue
            raise
    raise AssertionError("unreachable")  # pragma: no cover


def sync_match_trades(
    conn: Any,
    *,
    manifest_path: Path | None = None,
    api_key: str | None = None,
    requests_per_minute: int = 50,
    monthly_credit_budget: int = 20_000,
    transient_retries: int = 4,
    transient_backoff_seconds: float = 1.0,
    pmxt_client: Any | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Resume saturated ranges and publish trades only after all tokens finish."""
    manifest = load_order_book_manifest(manifest_path)
    book_run = conn.execute(
        f"""
        SELECT scan_id FROM {BOOK_RUNS}
        WHERE manifest_sha256=? AND status='published' AND raw_published
        ORDER BY finished_at DESC LIMIT 1
        """,
        [manifest.sha256],
    ).fetchone()
    if not book_run:
        raise ValueError("order-book scan must publish before trade acquisition")
    scan_id = str(book_run[0])
    existing = conn.execute(
        f"SELECT status, trade_count FROM {RUNS} WHERE scan_id=?", [scan_id]
    ).fetchone()
    if existing and existing[0] == "published":
        return {"scan_id": scan_id, "trade_count": int(existing[1]), "noop": True}
    if not existing:
        now = _now()
        conn.execute(
            f"INSERT INTO {RUNS} VALUES (?, ?, 'running', 0, NULL, ?, NULL, NULL, NULL)",
            [scan_id, manifest.sha256, now],
        )
        for target in manifest.targets:
            for outcome in target.outcomes:
                conn.execute(
                    f"""
                    INSERT INTO {WINDOWS}
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'pending', 0, 0, NULL, ?, NULL, NULL)
                    """,
                    [
                        scan_id,
                        target.fifa_match_id,
                        target.market_id,
                        outcome.clob_token_id,
                        outcome.role,
                        target.window_start_ms,
                        target.window_end_ms,
                        now,
                    ],
                )
    key = (api_key if api_key is not None else PMXT_API_KEY).strip()
    if not key:
        raise ValueError("PMXT_API_KEY is required for trade acquisition")
    client = pmxt_client or build_pmxt_client(requests_per_minute=requests_per_minute)
    targets = {
        outcome.clob_token_id: (target, outcome)
        for target in manifest.targets
        for outcome in target.outcomes
    }
    try:
        while True:
            cursor = conn.execute(
                f"""
                SELECT fifa_match_id, market_id, clob_token_id, landscape_role,
                       window_start_ms, window_end_ms, depth
                FROM {WINDOWS}
                WHERE scan_id=? AND status='pending'
                ORDER BY depth, clob_token_id, window_start_ms
                LIMIT 1
                """,
                [scan_id],
            )
            row = cursor.fetchone()
            if row is None:
                break
            window = dict(
                zip([column[0] for column in cursor.description], row, strict=True)
            )
            if not _reserve_credit(conn, scan_id, monthly_credit_budget):
                conn.execute(
                    f"UPDATE {RUNS} SET status='paused' WHERE scan_id=?", [scan_id]
                )
                raise MatchOrderBookPaused(
                    "PMXT local monthly credit budget reached",
                    {"scan_id": scan_id, "status": "paused"},
                )
            conn.execute(
                f"""
                UPDATE {WINDOWS}
                SET api_attempt_count=api_attempt_count+1, updated_at=?
                WHERE scan_id=? AND clob_token_id=?
                  AND window_start_ms=? AND window_end_ms=?
                """,
                [
                    _now(),
                    scan_id,
                    window["clob_token_id"],
                    window["window_start_ms"],
                    window["window_end_ms"],
                ],
            )
            payload = _request(
                client,
                api_key=key,
                token_id=str(window["clob_token_id"]),
                start=int(window["window_start_ms"]),
                end=int(window["window_end_ms"]),
                retries=transient_retries,
                backoff=transient_backoff_seconds,
                sleep_fn=sleep_fn,
            )
            if len(payload) > LIMIT:
                raise ValueError("PMXT returned more than the requested trade limit")
            if len(payload) == LIMIT:
                start, end = (
                    int(window["window_start_ms"]),
                    int(window["window_end_ms"]),
                )
                if end - start <= 1:
                    raise RuntimeError("saturated PMXT trade range cannot be split")
                midpoint = (start + end) // 2
                conn.execute("BEGIN TRANSACTION")
                try:
                    conn.execute(
                        f"""
                        UPDATE {WINDOWS} SET status='split', updated_at=?
                        WHERE scan_id=? AND clob_token_id=?
                          AND window_start_ms=? AND window_end_ms=?
                        """,
                        [_now(), scan_id, window["clob_token_id"], start, end],
                    )
                    for child_start, child_end in (
                        (start, midpoint),
                        (midpoint, end),
                    ):
                        conn.execute(
                            f"""
                            INSERT INTO {WINDOWS}
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0, NULL, ?, NULL, NULL)
                            ON CONFLICT DO NOTHING
                            """,
                            [
                                scan_id,
                                window["fifa_match_id"],
                                window["market_id"],
                                window["clob_token_id"],
                                window["landscape_role"],
                                child_start,
                                child_end,
                                int(window["depth"]) + 1,
                                _now(),
                            ],
                        )
                    conn.execute("COMMIT")
                except Exception:  # pragma: no cover - defensive transaction boundary
                    conn.execute("ROLLBACK")
                    raise
                continue
            target, outcome = targets[str(window["clob_token_id"])]
            normalized = [
                _normalize(
                    trade,
                    scan_id=scan_id,
                    manifest_sha256=manifest.sha256,
                    target=target,
                    outcome=outcome,
                    ordinal=ordinal,
                    window_start_ms=int(window["window_start_ms"]),
                    window_end_ms=int(window["window_end_ms"]),
                )
                for ordinal, trade in enumerate(payload)
            ]
            for trade in normalized:
                existing_trade = conn.execute(
                    f"""
                    SELECT trade_timestamp_ms, price, amount
                    FROM {TRADES}
                    WHERE scan_id=? AND clob_token_id=? AND trade_id=?
                    """,
                    [scan_id, trade["clob_token_id"], trade["trade_id"]],
                ).fetchone()
                if existing_trade and tuple(map(str, existing_trade)) != (
                    str(trade["trade_timestamp_ms"]),
                    trade["price"],
                    trade["amount"],
                ):
                    raise RuntimeError("contradictory PMXT trade ID across windows")
                if not existing_trade:
                    columns = ", ".join(trade)
                    placeholders = ", ".join("?" for _ in trade)
                    conn.execute(
                        f"INSERT INTO {TRADES} ({columns}) VALUES ({placeholders})",
                        list(trade.values()),
                    )
            ids_hash = hashlib.sha256(
                json.dumps(
                    sorted({trade["trade_id"] for trade in normalized}),
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            conn.execute(
                f"""
                UPDATE {WINDOWS}
                SET status=?, trade_count=?, trade_ids_sha256=?, updated_at=?
                WHERE scan_id=? AND clob_token_id=?
                  AND window_start_ms=? AND window_end_ms=?
                """,
                [
                    "loaded" if normalized else "empty",
                    len(normalized),
                    ids_hash,
                    _now(),
                    scan_id,
                    window["clob_token_id"],
                    window["window_start_ms"],
                    window["window_end_ms"],
                ],
            )
        conn.execute(
            f"""
            UPDATE {TRADES} AS trades
            SET event_sequence=sequenced.event_sequence
            FROM (
                SELECT
                    clob_token_id,
                    trade_id,
                    row_number() OVER (
                        PARTITION BY clob_token_id
                        ORDER BY trade_timestamp_ms, trade_id
                    ) - 1 AS event_sequence
                FROM {TRADES}
                WHERE scan_id=?
            ) AS sequenced
            WHERE trades.scan_id=?
              AND trades.clob_token_id=sequenced.clob_token_id
              AND trades.trade_id=sequenced.trade_id
            """,
            [scan_id, scan_id],
        )
        count = int(
            conn.execute(
                f"SELECT count(*) FROM {TRADES} WHERE scan_id=?", [scan_id]
            ).fetchone()[0]
        )
        if count == 0:
            raise RuntimeError("total-zero PMXT trade coverage blocks publication")
        hashes = [
            str(row[0])
            for row in conn.execute(
                f"""
                SELECT trade_id FROM {TRADES}
                WHERE scan_id=?
                ORDER BY clob_token_id, trade_timestamp_ms, event_sequence, trade_id
                """,
                [scan_id],
            ).fetchall()
        ]
        aggregate = hashlib.sha256("\n".join(hashes).encode()).hexdigest()
        conn.execute(
            f"""
            UPDATE {RUNS}
            SET status='published', trade_count=?, aggregate_sha256=?, finished_at=?
            WHERE scan_id=?
            """,
            [count, aggregate, _now(), scan_id],
        )
        empty_roles = [
            str(row[0])
            for row in conn.execute(
                f"""
                SELECT landscape_role
                FROM {WINDOWS}
                WHERE scan_id=?
                GROUP BY landscape_role
                HAVING sum(trade_count)=0
                """,
                [scan_id],
            ).fetchall()
        ]
        return {
            "scan_id": scan_id,
            "trade_count": count,
            "empty_landscape_warnings": empty_roles,
            "aggregate_sha256": aggregate,
            "noop": False,
        }
    except MatchOrderBookPaused:
        conn.execute(f"UPDATE {RUNS} SET status='paused' WHERE scan_id=?", [scan_id])
        raise
    except Exception as exc:
        conn.execute(
            f"""
            UPDATE {RUNS}
            SET status='failed', error_type=?, error_message=?
            WHERE scan_id=?
            """,
            [exc.__class__.__name__, "PMXT trade scan failed", scan_id],
        )
        raise MatchOrderBookSyncError(
            "PMXT trade scan failed", {"scan_id": scan_id, "status": "failed"}
        ) from exc


__all__ = ["sync_match_trades"]

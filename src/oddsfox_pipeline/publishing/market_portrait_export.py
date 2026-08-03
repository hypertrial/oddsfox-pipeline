"""Read-only warehouse fetch and immutable market portrait bundle export."""

from __future__ import annotations

import bisect
import json
import os
import shutil
import tempfile
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from oddsfox_pipeline.publishing.market_portrait_story import (
    _ROLES,
    BUNDLE_CONTRACT_VERSION,
    FootballEvent,
    MatchFacts,
    RenderProfile,
    _canonical,
    _decimal,
    _gzip,
    _iso,
    _landscape_roles,
    _ndjson,
    _sha256,
    _utc,
    _validate_facts,
    build_story,
)

_PORTRAIT_REQUIRED_TABLES = {
    ("polymarket_wc2026_marts", "polymarket_wc2026_match_order_book_states"),
    ("polymarket_wc2026_marts", "polymarket_wc2026_match_trades"),
    ("polymarket_wc2026_ops", "match_order_book_scan_runs"),
    ("polymarket_wc2026_ops", "match_trade_scan_runs"),
}


def _require_portrait_marts(connection: Any) -> None:
    available_tables = {
        (str(schema), str(table))
        for schema, table in connection.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN (
                'polymarket_wc2026_marts', 'polymarket_wc2026_ops'
            )
            """
        ).fetchall()
        if (str(schema), str(table)) in _PORTRAIT_REQUIRED_TABLES
    }
    if available_tables != _PORTRAIT_REQUIRED_TABLES:
        raise ValueError("published PMXT portrait marts are required for export")


def _resolve_published_pmxt_scan(
    connection: Any, fifa_match_id: int
) -> tuple[str, str, str]:
    candidates = connection.execute(
        """
        SELECT DISTINCT scan_id, manifest_sha256
        FROM polymarket_wc2026_marts.polymarket_wc2026_match_order_book_states
        WHERE fifa_match_id=?
        ORDER BY scan_id DESC
        """,
        [fifa_match_id],
    ).fetchall()
    published = []
    for scan_id, manifest_sha256 in candidates:
        valid = connection.execute(
            """
            SELECT aggregate_sha256
            FROM polymarket_wc2026_ops.match_order_book_scan_runs
            WHERE scan_id=? AND manifest_sha256=?
              AND status='published' AND raw_published
            LIMIT 1
            """,
            [scan_id, manifest_sha256],
        ).fetchone()
        if valid and valid[0]:
            published.append((str(scan_id), str(manifest_sha256), str(valid[0])))
    if len(published) != 1:
        raise ValueError(f"no published PMXT scan exists for match {fifa_match_id}")
    return published[0]


def _fetch_portrait_states(
    connection: Any, *, scan_id: str, fifa_match_id: int
) -> list[dict[str, Any]]:
    cursor = connection.execute(
        """
        SELECT market_id, clob_token_id, landscape_role,
               snapshot_timestamp_ms, provider_sequence, snapshot_sha256,
               bids_json, asks_json, last_trade_price_raw, ingested_at
        FROM polymarket_wc2026_marts.polymarket_wc2026_match_order_book_states
        WHERE scan_id=? AND fifa_match_id=?
        ORDER BY snapshot_timestamp_ms, provider_sequence, snapshot_sha256,
                 landscape_role
        """,
        [scan_id, fifa_match_id],
    )
    states = []
    seen = set()
    for row in cursor.fetchall():
        key = (str(row[1]), int(row[3]), str(row[5]))
        if key in seen:
            continue
        seen.add(key)
        states.append(
            {
                "event_sequence": len(states),
                "market_id": str(row[0]),
                "token_id": str(row[1]),
                "role": str(row[2]),
                "timestamp_ms": int(row[3]),
                "provider_sequence": int(row[4]),
                "snapshot_hash": str(row[5]),
                "bids": json.loads(row[6]),
                "asks": json.loads(row[7]),
                "last_trade": _decimal(row[8], "last_trade")
                if row[8] is not None
                else None,
                "receipt_time": _iso(
                    row[9].replace(tzinfo=timezone.utc)
                    if row[9].tzinfo is None
                    else row[9]
                ),
            }
        )
    if not states:  # pragma: no cover - candidate query proves one state exists
        raise ValueError("published PMXT scan contains no states")
    roles = set(_landscape_roles(states))
    if not roles <= _ROLES:  # pragma: no cover - helper validates the exact inventory
        raise ValueError(f"invalid portrait role inventory: {sorted(roles)}")
    return states


def _fetch_portrait_trades(
    connection: Any,
    *,
    scan_id: str,
    manifest_sha256: str,
    fifa_match_id: int,
    states: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], str]:
    roles = {str(state["role"]) for state in states}
    trade_run = connection.execute(
        """
        SELECT trade_count, aggregate_sha256
        FROM polymarket_wc2026_ops.match_trade_scan_runs
        WHERE scan_id=? AND manifest_sha256=? AND status='published'
        """,
        [scan_id, manifest_sha256],
    ).fetchone()
    if trade_run is None or int(trade_run[0]) <= 0 or not trade_run[1]:
        raise ValueError("a non-empty published PMXT trade scan is required")
    rows = connection.execute(
        """
        SELECT trade_id, market_id, clob_token_id, landscape_role,
               trade_timestamp_ms, event_sequence, price, amount
        FROM polymarket_wc2026_marts.polymarket_wc2026_match_trades
        WHERE scan_id=? AND fifa_match_id=?
        ORDER BY trade_timestamp_ms, event_sequence, trade_id
        """,
        [scan_id, fifa_match_id],
    ).fetchall()
    raw_trade_count = int(
        connection.execute(
            """
            SELECT count(*)
            FROM polymarket_wc2026_marts.polymarket_wc2026_match_trades
            WHERE scan_id=?
            """,
            [scan_id],
        ).fetchone()[0]
    )
    if raw_trade_count != int(trade_run[0]):
        raise ValueError("published PMXT trade count does not match portrait mart rows")
    trades: list[dict[str, Any]] = []
    state_by_role = {
        role: [state for state in states if state["role"] == role] for role in roles
    }
    state_timestamps_by_role = {
        role: [int(state["timestamp_ms"]) for state in state_by_role[role]]
        for role in roles
    }
    for row in rows:
        trade = {
            "trade_id": str(row[0]),
            "market_id": str(row[1]),
            "token_id": str(row[2]),
            "role": str(row[3]),
            "timestamp_ms": int(row[4]),
            "event_sequence": int(row[5]),
            "price": _decimal(row[6], "trade.price"),
            "amount": _decimal(row[7], "trade.amount"),
        }
        if trade["role"] not in state_by_role:
            raise ValueError(f"trade has unknown landscape role: {trade['role']}")
        books = state_by_role[trade["role"]]
        timestamps = state_timestamps_by_role[trade["role"]]
        index = bisect.bisect_right(timestamps, trade["timestamp_ms"]) - 1
        side = "unknown"
        if index >= 0:
            book = books[index]
            best_bid = Decimal(book["bids"][0]["price"]) if book["bids"] else None
            best_ask = Decimal(book["asks"][0]["price"]) if book["asks"] else None
            price = Decimal(trade["price"])
            if best_ask is not None and price >= best_ask:
                side = "buy"
            elif best_bid is not None and price <= best_bid:
                side = "sell"
        trade["aggressor_side"] = side
        trades.append(trade)
    return trades, str(trade_run[1])


def _fetch_rows(
    connection: Any, fifa_match_id: int
) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
    _require_portrait_marts(connection)
    scan_id, manifest_sha256, order_book_sha256 = _resolve_published_pmxt_scan(
        connection, fifa_match_id
    )
    states = _fetch_portrait_states(
        connection, scan_id=scan_id, fifa_match_id=fifa_match_id
    )
    trades, trade_aggregate_sha256 = _fetch_portrait_trades(
        connection,
        scan_id=scan_id,
        manifest_sha256=manifest_sha256,
        fifa_match_id=fifa_match_id,
        states=states,
    )
    return (
        scan_id,
        states,
        trades,
        {
            "manifest_sha256": manifest_sha256,
            "order_book_aggregate_sha256": order_book_sha256,
            "trade_aggregate_sha256": trade_aggregate_sha256,
        },
    )


def _as_utc(value: datetime, field: str) -> datetime:
    return _utc(value, field)


def _validate_export_alignment(
    connection: Any,
    *,
    scan_id: str,
    fifa_match_id: int,
    facts: MatchFacts,
    states: Sequence[dict[str, Any]],
) -> None:
    required = {
        (
            "polymarket_wc2026_intermediate",
            "int_polymarket_wc2026_match_working_set",
        ),
        ("polymarket_wc2026_ops", "match_order_book_scan_windows"),
    }
    available = {
        (str(schema), str(table))
        for schema, table in connection.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE (
                table_schema='polymarket_wc2026_intermediate'
                AND table_name='int_polymarket_wc2026_match_working_set'
            ) OR (
                table_schema='polymarket_wc2026_ops'
                AND table_name='match_order_book_scan_windows'
            )
            """
        ).fetchall()
    }
    if available != required:
        raise ValueError(
            "validated match timing and PMXT scan windows are required for export"
        )
    timing_rows = connection.execute(
        """
        SELECT DISTINCT scheduled_kickoff_at_utc, match_started_at_utc
        FROM polymarket_wc2026_intermediate
            .int_polymarket_wc2026_match_working_set
        WHERE fifa_match_id=?
          AND fixture_mapping_count=1
          AND primary_mapping_count=1
        """,
        [fifa_match_id],
    ).fetchall()
    if len(timing_rows) != 1 or any(value is None for value in timing_rows[0]):
        raise ValueError(
            "validated match working set must provide one consistent match timing"
        )
    scheduled = _as_utc(timing_rows[0][0], "scheduled_kickoff_at_utc")
    market_start = _as_utc(timing_rows[0][1], "match_started_at_utc")
    kickoff = _utc(facts.kickoff_at_utc, "kickoff_at_utc")
    if abs((kickoff - scheduled).total_seconds()) > 60:
        raise ValueError(
            "football kickoff does not agree with the validated match working set"
        )
    first_half = _utc(facts.first_half_started_at, "first_half")
    market_delta = first_half - market_start
    if not timedelta(minutes=-2) <= market_delta <= timedelta(minutes=30):
        raise ValueError(
            "football period timing does not agree with the validated market start"
        )

    role_by_token = {str(state["token_id"]): str(state["role"]) for state in states}
    rows = connection.execute(
        """
        SELECT clob_token_id, min(window_start_ms), max(window_end_ms), count(*)
        FROM polymarket_wc2026_ops.match_order_book_scan_windows
        WHERE scan_id=? AND fifa_match_id=? AND depth=0
        GROUP BY clob_token_id
        """,
        [scan_id, fifa_match_id],
    ).fetchall()
    windows = {
        role_by_token[str(token)]: (int(start), int(end))
        for token, start, end, count in rows
        if str(token) in role_by_token and int(count) == 1
    }
    roles = set(role_by_token.values())
    if set(windows) != roles:
        raise ValueError("PMXT root scan windows do not cover every portrait role")
    final_whistle = _utc(
        facts.match_ended_at
        or facts.second_extra_half_ended_at
        or facts.second_half_ended_at,
        "match_ended_at",
    )
    football_start_ms = int(first_half.timestamp() * 1_000)
    football_end_ms = int(final_whistle.timestamp() * 1_000)
    for role, (window_start, window_end) in windows.items():
        if window_start >= football_start_ms or window_end <= football_end_ms:
            raise ValueError(
                f"PMXT root scan window does not cover the football timeline "
                f"for role {role}"
            )


def build_market_portrait_bundle(
    connection: Any,
    *,
    fifa_match_id: int,
    match_facts: MatchFacts,
    football_events: Sequence[FootballEvent],
    output_root: Path,
    render_profile: RenderProfile = RenderProfile(),
    pipeline_revision: str = "unknown",
    scraper_revision: str = "unknown",
) -> dict[str, Any]:
    """Publish and verify one immutable ``oddsfox.market-portrait.v1`` bundle."""
    if match_facts.fifa_match_id != fifa_match_id:
        raise ValueError("fifa_match_id does not match MatchFacts")
    scan_id, states, trades, scan_hashes = _fetch_rows(connection, fifa_match_id)
    _validate_facts(match_facts, football_events)
    _validate_export_alignment(
        connection,
        scan_id=scan_id,
        fifa_match_id=fifa_match_id,
        facts=match_facts,
        states=states,
    )
    story = build_story(match_facts, football_events, states, trades, render_profile)
    book_bytes = _gzip(_ndjson(states))
    trade_bytes = _gzip(_ndjson(trades))
    story_bytes = _canonical(story) + b"\n"
    files = {
        "book_states.ndjson.gz": {
            "sha256": _sha256(book_bytes),
            "record_count": len(states),
        },
        "trades.ndjson.gz": {
            "sha256": _sha256(trade_bytes),
            "record_count": len(trades),
        },
        "story.json": {"sha256": _sha256(story_bytes), "record_count": 1},
    }
    base_manifest = {
        "contract_version": BUNDLE_CONTRACT_VERSION,
        "fifa_match_id": fifa_match_id,
        "stage": match_facts.stage,
        "home_team": match_facts.home_team,
        "away_team": match_facts.away_team,
        "landscape_roles": _landscape_roles(states),
        "source_bounds": {
            "start": _iso(
                min(
                    [match_facts.first_half_started_at]
                    + [
                        datetime.fromtimestamp(
                            row["timestamp_ms"] / 1_000, tz=timezone.utc
                        )
                        for row in [*states, *trades]
                    ]
                )
            ),
            "end": _iso(
                max(
                    [
                        match_facts.match_ended_at
                        or match_facts.second_extra_half_ended_at
                        or match_facts.second_half_ended_at
                    ]
                    + [
                        datetime.fromtimestamp(
                            row["timestamp_ms"] / 1_000, tz=timezone.utc
                        )
                        for row in [*states, *trades]
                    ]
                )
            ),
        },
        "render_defaults": {
            **asdict(render_profile),
            "duration_seconds": story["duration_seconds"],
        },
        "pipeline_revision": pipeline_revision,
        "scraper_revision": scraper_revision,
        "pmxt": {"scan_id": scan_id, **scan_hashes},
        "source_facts": {
            "provenance_sha256": match_facts.source_provenance_sha256,
            "sanitization": match_facts.sanitization,
        },
        "files": files,
    }
    bundle_id = _sha256(_canonical(base_manifest))[:24]
    manifest = {**base_manifest, "bundle_id": bundle_id}
    manifest_bytes = _canonical(manifest) + b"\n"
    destination = output_root / str(fifa_match_id) / bundle_id
    expected = {
        "manifest.json": manifest_bytes,
        "book_states.ndjson.gz": book_bytes,
        "trades.ndjson.gz": trade_bytes,
        "story.json": story_bytes,
    }
    if destination.exists():
        if all(
            (destination / name).is_file() and (destination / name).read_bytes() == raw
            for name, raw in expected.items()
        ):
            return {"bundle_id": bundle_id, "path": str(destination), "noop": True}
        raise RuntimeError(
            f"immutable bundle path already exists with different bytes: {destination}"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix=f".{bundle_id}.", dir=destination.parent))
    try:
        for name, raw in expected.items():
            (temp / name).write_bytes(raw)
        os.replace(temp, destination)
    except Exception:  # pragma: no cover - defensive atomic-publication boundary
        shutil.rmtree(temp, ignore_errors=True)
        raise
    return {"bundle_id": bundle_id, "path": str(destination), "noop": False}

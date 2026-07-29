"""Deterministic private bundle builder for World Cup market portraits.

The public boundary deliberately accepts football facts rather than importing
the private collector.  It performs read-only queries against a completed PMXT
scan and publishes a content-addressed directory.
"""

from __future__ import annotations

import bisect
import gzip
import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from statistics import pstdev
from typing import Any, Iterable, Literal, Sequence

BUNDLE_CONTRACT_VERSION = "oddsfox.market-portrait.v1"
_ROLES = {"home", "away", "home_win", "draw", "away_win"}


@dataclass(frozen=True)
class MatchFacts:
    fifa_match_id: int
    stage: str
    home_team: str
    away_team: str
    kickoff_at_utc: datetime
    first_half_started_at: datetime
    first_half_ended_at: datetime
    second_half_started_at: datetime
    second_half_ended_at: datetime
    first_extra_half_started_at: datetime | None = None
    first_extra_half_ended_at: datetime | None = None
    second_extra_half_started_at: datetime | None = None
    second_extra_half_ended_at: datetime | None = None
    game_ended_at: datetime | None = None
    home_score: int | None = None
    away_score: int | None = None
    penalty_home_score: int | None = None
    penalty_away_score: int | None = None
    source_provenance_sha256: str = ""
    sanitization: str = "deterministic-micro-epsilon-v1"


@dataclass(frozen=True)
class FootballEvent:
    event_id: str
    event_order: int
    event_type: str
    period: Literal["1H", "2H", "ET1", "ET2", "PENS"]
    match_minute: int | None
    stoppage_minute: int | None
    minute_label: str
    team_id: int | None = None
    team_name: str | None = None
    player_id: int | None = None
    player_name: str | None = None
    secondary_player_id: int | None = None
    secondary_player_name: str | None = None
    home_score: int | None = None
    away_score: int | None = None
    is_revoked: bool = False
    var_decision: str | None = None
    card_type: str | None = None
    substitution_direction: str | None = None
    is_penalty_shootout: bool = False
    shootout_sequence: int | None = None
    shootout_score: str | None = None
    time_precision: Literal["minute"] = "minute"
    source_order_available: bool = True


@dataclass(frozen=True)
class RenderProfile:
    width: int = 1920
    height: int = 1080
    fps: int = 60
    regulation_seconds: float = 75.0
    pre_match_seconds: float = 8.0
    halftime_seconds: float = 3.0
    post_match_seconds: float = 6.0


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return (
        _utc(value, "timestamp").isoformat().replace("+00:00", "Z") if value else None
    )


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def _ndjson(rows: Iterable[dict[str, Any]]) -> bytes:
    return b"".join(_canonical(row) + b"\n" for row in rows)


def _gzip(raw: bytes) -> bytes:
    return gzip.compress(raw, compresslevel=9, mtime=0)


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _decimal(value: Any, field: str) -> str:
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError(f"{field} must be finite")
    text = format(number, "f").rstrip("0").rstrip(".")
    return text or "0"


def _priority(event: FootballEvent) -> str:
    event_type = event.event_type.casefold()
    if "goal" in event_type or "penalty" in event_type or "final whistle" in event_type:
        return "P0"
    if (
        event.is_penalty_shootout
        or event.var_decision
        or "red" in (event.card_type or "").casefold()
    ):
        return "P0"
    if (
        "card" in event_type
        or "substitution" in event_type
        or "half" in event_type
        or "added time" in event_type
    ):
        return "P1"
    return "P2"


def _validate_facts(facts: MatchFacts, events: Sequence[FootballEvent]) -> None:
    if facts.sanitization != "deterministic-micro-epsilon-v1":
        raise ValueError(
            "football facts must use deterministic-micro-epsilon-v1 sanitation"
        )
    if len(facts.source_provenance_sha256) != 64 or any(
        character not in "0123456789abcdef"
        for character in facts.source_provenance_sha256
    ):
        raise ValueError("source provenance must be a lowercase SHA-256 commitment")
    kickoff = _utc(facts.kickoff_at_utc, "kickoff_at_utc")
    first_half_start = _utc(facts.first_half_started_at, "first_half")
    kickoff_delta = first_half_start - kickoff
    if not timedelta(minutes=-2) <= kickoff_delta <= timedelta(minutes=30):
        raise ValueError(
            "first_half must begin between two minutes before and "
            "30 minutes after kickoff_at_utc"
        )
    periods = [
        ("first_half", facts.first_half_started_at, facts.first_half_ended_at),
        ("second_half", facts.second_half_started_at, facts.second_half_ended_at),
    ]
    extras = [
        (
            "first_extra_half",
            facts.first_extra_half_started_at,
            facts.first_extra_half_ended_at,
        ),
        (
            "second_extra_half",
            facts.second_extra_half_started_at,
            facts.second_extra_half_ended_at,
        ),
    ]
    if any(start is not None or end is not None for _, start, end in extras):
        periods.extend(extras)
    for name, start, end in periods:
        if start is None or end is None:
            raise ValueError(f"{name} requires both actual period boundaries")
        if _utc(start, name) >= _utc(end, name):
            raise ValueError(f"{name} boundaries are inconsistent")
    ordered = [(_utc(start, name), _utc(end, name)) for name, start, end in periods]
    if any(
        ordered[index][1] > ordered[index + 1][0] for index in range(len(ordered) - 1)
    ):
        raise ValueError("football periods overlap or are out of order")
    final_period_end = ordered[-1][1]
    duration_limits = [
        ("first_half", ordered[0], timedelta(minutes=40), timedelta(minutes=75)),
        ("second_half", ordered[1], timedelta(minutes=40), timedelta(minutes=90)),
    ]
    if len(ordered) == 4:
        duration_limits.extend(
            [
                (
                    "first_extra_half",
                    ordered[2],
                    timedelta(minutes=10),
                    timedelta(minutes=30),
                ),
                (
                    "second_extra_half",
                    ordered[3],
                    timedelta(minutes=10),
                    timedelta(minutes=30),
                ),
            ]
        )
    for name, (start, end), minimum, maximum in duration_limits:
        if not minimum <= end - start <= maximum:
            raise ValueError(f"{name} duration is implausible")
    halftime = ordered[1][0] - ordered[0][1]
    if not timedelta(minutes=5) <= halftime <= timedelta(minutes=30):
        raise ValueError("halftime duration is implausible")
    if len(ordered) == 4:
        full_time_break = ordered[2][0] - ordered[1][1]
        extra_time_break = ordered[3][0] - ordered[2][1]
        if not timedelta(0) <= full_time_break <= timedelta(minutes=30):
            raise ValueError("full-time to extra-time break is implausible")
        if not timedelta(0) <= extra_time_break <= timedelta(minutes=15):
            raise ValueError("extra-time break is implausible")
    if facts.game_ended_at is not None:
        game_ended_at = _utc(facts.game_ended_at, "game_ended_at")
        if game_ended_at < final_period_end:
            raise ValueError("game_ended_at precedes the final period boundary")
        if game_ended_at - final_period_end > timedelta(minutes=45):
            raise ValueError("game_ended_at is implausibly late")
    orders = [event.event_order for event in events]
    if orders != sorted(orders) or len(orders) != len(set(orders)):
        raise ValueError("football event order must be unique and monotonic")
    for event in events:
        if event.time_precision != "minute":
            raise ValueError("market portrait events must be minute-precision")
        if event.period == "PENS":
            if not event.is_penalty_shootout or event.match_minute is not None:
                raise ValueError(
                    "penalty events belong to PENS without an invented minute"
                )
        elif event.match_minute is None:
            raise ValueError("non-penalty events require a football minute")
    if any(event.period == "PENS" for event in events) and (
        facts.game_ended_at is None
        or _utc(facts.game_ended_at, "game_ended_at") <= final_period_end
    ):
        raise ValueError(
            "penalties require an actual game_ended_at after the final period"
        )


def _minute_specs(
    facts: MatchFacts, events: Sequence[FootballEvent]
) -> list[tuple[str, int, int, int, datetime, datetime]]:
    def stoppage(period: str, boundary: int) -> int:
        return max(
            [0]
            + [
                event.stoppage_minute or 0
                for event in events
                if event.period == period and event.match_minute == boundary
            ]
        )

    specs: list[tuple[str, int, int, int, datetime, datetime]] = [
        (
            "1H",
            1,
            45,
            stoppage("1H", 45),
            facts.first_half_started_at,
            facts.first_half_ended_at,
        ),
        (
            "2H",
            46,
            90,
            stoppage("2H", 90),
            facts.second_half_started_at,
            facts.second_half_ended_at,
        ),
    ]
    if facts.first_extra_half_started_at and facts.first_extra_half_ended_at:
        specs.extend(
            [
                (
                    "ET1",
                    91,
                    105,
                    stoppage("ET1", 105),
                    facts.first_extra_half_started_at,
                    facts.first_extra_half_ended_at,
                ),
                (
                    "ET2",
                    106,
                    120,
                    stoppage("ET2", 120),
                    facts.second_extra_half_started_at,
                    facts.second_extra_half_ended_at,
                ),
            ]
        )
    return specs


def _duration(
    facts: MatchFacts, events: Sequence[FootballEvent], profile: RenderProfile
) -> float:
    extra_minutes = 30 if facts.first_extra_half_started_at else 0
    penalties = any(event.period == "PENS" for event in events)
    if not extra_minutes and not penalties:
        return profile.regulation_seconds
    return min(
        90.0, profile.regulation_seconds + 0.5 * extra_minutes + (5 if penalties else 0)
    )


def _landscape_roles(states: Sequence[dict[str, Any]]) -> list[str]:
    roles = {str(state["role"]) for state in states}
    if roles == {"home", "away"}:
        return ["home", "away"]
    if roles == {"home_win", "draw", "away_win"}:
        return ["home_win", "draw", "away_win"]
    raise ValueError(f"invalid portrait role inventory: {sorted(roles)}")


def build_story(
    facts: MatchFacts,
    events: Sequence[FootballEvent],
    states: Sequence[dict[str, Any]],
    trades: Sequence[dict[str, Any]],
    profile: RenderProfile,
) -> dict[str, Any]:
    """Build source/video mapping, minute bands, annotations, and exact-book metrics."""
    _validate_facts(facts, events)
    duration = _duration(facts, events, profile)
    penalty_seconds = 5.0 if any(event.period == "PENS" for event in events) else 0.0
    football_seconds = (
        duration
        - profile.pre_match_seconds
        - profile.halftime_seconds
        - profile.post_match_seconds
        - penalty_seconds
    )
    specs = _minute_specs(facts, events)
    p0_minutes = {
        (event.period, event.match_minute, event.stoppage_minute)
        for event in events
        if event.period != "PENS" and _priority(event) == "P0"
    }
    minute_rows: list[dict[str, Any]] = []
    weights = sum(
        1.5 if (period, minute, added) in p0_minutes else 1.0
        for period, start, end, stoppage_count, _, _ in specs
        for minute, added in (
            [(value, None) for value in range(start, end + 1)]
            + [(end, value) for value in range(1, stoppage_count + 1)]
        )
    )
    source_timestamps = [
        int(row["timestamp_ms"]) for row in [*states, *trades] if "timestamp_ms" in row
    ]
    first_half_start = _utc(facts.first_half_started_at, "first_half")
    final_whistle = _utc(
        facts.game_ended_at
        or facts.second_extra_half_ended_at
        or facts.second_half_ended_at,
        "game_ended_at",
    )
    market_start = (
        datetime.fromtimestamp(min(source_timestamps) / 1_000, tz=timezone.utc)
        if source_timestamps
        else first_half_start
    )
    market_end = (
        datetime.fromtimestamp(max(source_timestamps) / 1_000, tz=timezone.utc)
        if source_timestamps
        else final_whistle
    )
    fixed_segments = [
        {
            "kind": "pre_match",
            "period": "PRE",
            "minute_label": "PRE",
            "source_start": _iso(min(market_start, first_half_start)),
            "source_end": _iso(first_half_start),
            "video_start_seconds": 0.0,
            "video_end_seconds": profile.pre_match_seconds,
        }
    ]
    cursor = profile.pre_match_seconds
    for period, first, last, stoppage_count, source_start, source_end in specs:
        minutes = [(value, None) for value in range(first, last + 1)] + [
            (last, value) for value in range(1, stoppage_count + 1)
        ]
        count = len(minutes)
        source_span = (
            _utc(source_end, period) - _utc(source_start, period)
        ).total_seconds()
        for offset, (minute, added) in enumerate(minutes):
            weight = 1.5 if (period, minute, added) in p0_minutes else 1.0
            video_start = cursor
            cursor += football_seconds * weight / weights
            minute_rows.append(
                {
                    "period": period,
                    "kind": "football_minute",
                    "match_minute": minute,
                    "stoppage_minute": added,
                    "minute_label": (
                        f"{minute}+{added}" if added is not None else str(minute)
                    ),
                    "source_start": _iso(
                        _utc(source_start, period)
                        + timedelta(seconds=source_span * offset / count)
                    ),
                    "source_end": _iso(
                        _utc(source_start, period)
                        + timedelta(seconds=source_span * (offset + 1) / count)
                    ),
                    "video_start_seconds": round(video_start, 9),
                    "video_end_seconds": round(cursor, 9),
                    "weight": weight,
                }
            )
        if period == "1H":
            fixed_segments.append(
                {
                    "kind": "halftime",
                    "period": "HT",
                    "minute_label": "HT",
                    "source_start": _iso(facts.first_half_ended_at),
                    "source_end": _iso(facts.second_half_started_at),
                    "video_start_seconds": round(cursor, 9),
                    "video_end_seconds": round(cursor + profile.halftime_seconds, 9),
                }
            )
            cursor += profile.halftime_seconds
    penalty_events = [event for event in events if event.period == "PENS"]
    if penalty_events:
        source_start = _utc(
            facts.second_extra_half_ended_at or facts.second_half_ended_at,
            "penalties",
        )
        source_end = (
            _utc(facts.game_ended_at, "game_ended_at")
            if facts.game_ended_at
            else source_start
        )
        minute_rows.append(
            {
                "period": "PENS",
                "kind": "football_minute",
                "match_minute": None,
                "stoppage_minute": None,
                "minute_label": "PENS",
                "source_start": _iso(source_start),
                "source_end": _iso(source_end),
                "video_start_seconds": round(cursor, 9),
                "video_end_seconds": round(duration - profile.post_match_seconds, 9),
                "weight": None,
            }
        )
    fixed_segments.append(
        {
            "kind": "post_match",
            "period": "POST",
            "minute_label": "FT",
            "source_start": _iso(final_whistle),
            "source_end": _iso(max(market_end, final_whistle)),
            "video_start_seconds": round(duration - profile.post_match_seconds, 9),
            "video_end_seconds": duration,
        }
    )
    annotations = []
    for event in events:
        band = next(
            (
                row
                for row in minute_rows
                if row["period"] == event.period
                and (
                    event.period == "PENS"
                    or (
                        row["match_minute"] == event.match_minute
                        and row["stoppage_minute"] == event.stoppage_minute
                    )
                )
            ),
            None,
        )
        if band is None:
            raise ValueError(f"event {event.event_id} falls outside its period")
        annotations.append(
            {
                **asdict(event),
                "priority": _priority(event),
                "video_start_seconds": band["video_start_seconds"],
                "video_end_seconds": band["video_end_seconds"],
                "alignment": "minute-aligned",
            }
        )
    annotations.sort(
        key=lambda row: (
            row["video_start_seconds"],
            row["video_end_seconds"],
            row["event_order"],
        )
    )
    annotation_start_by_order = {
        annotation["event_order"]: annotation["video_start_seconds"]
        for annotation in annotations
    }
    score_checkpoints = [
        {
            "event_order": 0,
            "video_start_seconds": 0.0,
            "home_score": 0,
            "away_score": 0,
        }
    ]
    current_score = (0, 0)
    for event in events:
        if event.home_score is None or event.away_score is None:
            continue
        next_score = (event.home_score, event.away_score)
        if next_score == current_score:
            continue
        score_checkpoints.append(
            {
                "event_order": event.event_order,
                "video_start_seconds": annotation_start_by_order[event.event_order],
                "home_score": event.home_score,
                "away_score": event.away_score,
            }
        )
        current_score = next_score
    score_checkpoints.sort(
        key=lambda row: (row["video_start_seconds"], row["event_order"])
    )
    metrics = _market_metrics(states, trades)
    reactions = _reactions(minute_rows, annotations, metrics)
    return {
        "duration_seconds": duration,
        "alignment": "minute-aligned",
        "segments": sorted(
            [*fixed_segments, *minute_rows],
            key=lambda row: (row["video_start_seconds"], row["video_end_seconds"]),
        ),
        "football_minute_bands": minute_rows,
        "annotations": annotations,
        "score_checkpoints": score_checkpoints,
        "market_metrics": metrics,
        "reactions": reactions,
    }


def _market_metrics(
    states: Sequence[dict[str, Any]], trades: Sequence[dict[str, Any]]
) -> list[dict[str, Any]]:
    trades_by_role: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        trades_by_role.setdefault(str(trade["role"]), []).append(trade)
    changes: dict[str, list[tuple[int, float]]] = {}
    previous_midpoint: dict[str, float] = {}
    rows = []
    for state in states:
        role = str(state["role"])
        bids = state["bids"]
        asks = state["asks"]
        best_bid = float(bids[0]["price"]) if bids else None
        best_ask = float(asks[0]["price"]) if asks else None
        midpoint = (
            (best_bid + best_ask) / 2
            if best_bid is not None and best_ask is not None
            else None
        )
        spread = best_ask - best_bid if midpoint is not None else None
        bid_depth = sum(
            float(level["size"])
            for level in bids
            if best_bid is not None and float(level["price"]) >= best_bid - 0.03
        )
        ask_depth = sum(
            float(level["size"])
            for level in asks
            if best_ask is not None and float(level["price"]) <= best_ask + 0.03
        )
        total_depth = bid_depth + ask_depth
        timestamp = int(state["timestamp_ms"])
        if midpoint is not None:
            if role in previous_midpoint:
                changes.setdefault(role, []).append(
                    (timestamp, midpoint - previous_midpoint[role])
                )
            previous_midpoint[role] = midpoint
        recent_changes = [
            change
            for at, change in changes.get(role, [])
            if timestamp - 60_000 < at <= timestamp
        ]
        volume = sum(
            float(trade["amount"])
            for trade in trades_by_role.get(role, [])
            if timestamp - 60_000 < int(trade["timestamp_ms"]) <= timestamp
        )
        rows.append(
            {
                "event_sequence": state["event_sequence"],
                "role": role,
                "timestamp_ms": timestamp,
                "midpoint": midpoint,
                "spread": spread,
                "near_touch_bid_depth": bid_depth,
                "near_touch_ask_depth": ask_depth,
                "imbalance": (bid_depth - ask_depth) / total_depth
                if total_depth
                else None,
                "rolling_volume_60s": volume,
                "rolling_midpoint_change_volatility_60s": pstdev(recent_changes)
                if recent_changes
                else None,
            }
        )
    return rows


def _reactions(
    minute_rows: Sequence[dict[str, Any]],
    annotations: Sequence[dict[str, Any]],
    metrics: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    projected_fields = (
        "timestamp_ms",
        "midpoint",
        "spread",
        "near_touch_bid_depth",
        "near_touch_ask_depth",
        "imbalance",
        "rolling_volume_60s",
        "rolling_midpoint_change_volatility_60s",
    )

    def before(rows: Sequence[dict[str, Any]], boundary: int) -> dict[str, Any] | None:
        timestamps = [int(row["timestamp_ms"]) for row in rows]
        index = bisect.bisect_left(timestamps, boundary) - 1
        if index < 0:
            return None
        return {field: rows[index][field] for field in projected_fields}

    by_role: dict[str, list[dict[str, Any]]] = {}
    for metric in metrics:
        by_role.setdefault(str(metric["role"]), []).append(metric)
    reactions = []
    for annotation in annotations:
        if annotation["period"] == "PENS":
            continue
        band = next(
            row
            for row in minute_rows
            if row["period"] == annotation["period"]
            and row["match_minute"] == annotation["match_minute"]
            and row["stoppage_minute"] == annotation["stoppage_minute"]
        )
        band_index = minute_rows.index(band)
        start = int(
            datetime.fromisoformat(
                str(band["source_start"]).replace("Z", "+00:00")
            ).timestamp()
            * 1000
        )
        end = int(
            datetime.fromisoformat(
                str(band["source_end"]).replace("Z", "+00:00")
            ).timestamp()
            * 1000
        )
        following_end = (
            int(
                datetime.fromisoformat(
                    str(minute_rows[band_index + 1]["source_end"]).replace(
                        "Z", "+00:00"
                    )
                ).timestamp()
                * 1000
            )
            if band_index + 1 < len(minute_rows)
            and minute_rows[band_index + 1]["period"] != "PENS"
            else None
        )
        for role, rows in sorted(by_role.items()):
            reactions.append(
                {
                    "event_id": annotation["event_id"],
                    "role": role,
                    "alignment": "minute-aligned",
                    "label": "minute-aligned market move",
                    "primary": {
                        "source_start_ms": start,
                        "source_end_ms": end,
                        "before": before(rows, start),
                        "after": before(rows, end),
                    },
                    "extended": {
                        "source_start_ms": start,
                        "source_end_ms": following_end,
                        "before": before(rows, start),
                        "after": (
                            before(rows, following_end)
                            if following_end is not None
                            else None
                        ),
                    },
                }
            )
    return reactions


def _fetch_rows(
    connection: Any, fifa_match_id: int
) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
    required_tables = {
        ("polymarket_wc2026_marts", "polymarket_wc2026_match_order_book_states"),
        ("polymarket_wc2026_marts", "polymarket_wc2026_match_trades"),
        ("polymarket_wc2026_ops", "match_order_book_scan_runs"),
        ("polymarket_wc2026_ops", "match_trade_scan_runs"),
    }
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
        if (str(schema), str(table)) in required_tables
    }
    if available_tables != required_tables:
        raise ValueError("published PMXT portrait marts are required for export")
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
    scan_id, manifest_sha256, order_book_sha256 = published[0]
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
        index = (
            bisect.bisect_right(
                [state["timestamp_ms"] for state in books], trade["timestamp_ms"]
            )
            - 1
        )
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
    return (
        scan_id,
        states,
        trades,
        {
            "manifest_sha256": manifest_sha256,
            "order_book_aggregate_sha256": order_book_sha256,
            "trade_aggregate_sha256": str(trade_run[1]),
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
            "int_polymarket_wc2026_match_market_universe",
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
                AND table_name='int_polymarket_wc2026_match_market_universe'
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
        SELECT DISTINCT scheduled_kickoff_at_utc, game_started_at_utc
        FROM polymarket_wc2026_intermediate
            .int_polymarket_wc2026_match_market_universe
        WHERE fifa_match_id=?
          AND fixture_mapping_count=1
          AND primary_mapping_count=1
        """,
        [fifa_match_id],
    ).fetchall()
    if len(timing_rows) != 1 or any(value is None for value in timing_rows[0]):
        raise ValueError(
            "validated match universe must provide one consistent match timing"
        )
    scheduled = _as_utc(timing_rows[0][0], "scheduled_kickoff_at_utc")
    market_start = _as_utc(timing_rows[0][1], "game_started_at_utc")
    kickoff = _utc(facts.kickoff_at_utc, "kickoff_at_utc")
    if abs((kickoff - scheduled).total_seconds()) > 60:
        raise ValueError(
            "football kickoff does not agree with the validated match universe"
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
        facts.game_ended_at
        or facts.second_extra_half_ended_at
        or facts.second_half_ended_at,
        "game_ended_at",
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
            "start": story["segments"][0]["source_start"],
            "end": story["segments"][-1]["source_end"],
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


__all__ = [
    "BUNDLE_CONTRACT_VERSION",
    "FootballEvent",
    "MatchFacts",
    "RenderProfile",
    "build_market_portrait_bundle",
    "build_story",
]

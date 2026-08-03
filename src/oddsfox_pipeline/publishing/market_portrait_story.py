"""Story DTOs, validators, and deterministic market portrait narrative."""

from __future__ import annotations

import bisect
import gzip
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
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
    match_ended_at: datetime | None = None
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
    regulation_seconds: float = 45.0
    pre_match_seconds: float = 0.0
    halftime_seconds: float = 0.0
    post_match_seconds: float = 0.0
    penalty_seconds: float = 5.0


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


def _validate_match_periods(facts: MatchFacts) -> list[tuple[datetime, datetime]]:
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
    if facts.match_ended_at is not None:
        match_ended_at = _utc(facts.match_ended_at, "match_ended_at")
        if match_ended_at + timedelta(microseconds=2) < final_period_end:
            raise ValueError("match_ended_at precedes the final period boundary")
        if match_ended_at - final_period_end > timedelta(minutes=45):
            raise ValueError("match_ended_at is implausibly late")
    return ordered


def _validate_football_events(
    facts: MatchFacts,
    events: Sequence[FootballEvent],
    *,
    final_period_end: datetime,
) -> None:
    orders = [event.event_order for event in events]
    if orders != sorted(orders) or len(orders) != len(set(orders)):
        raise ValueError("football event order must be unique and monotonic")
    event_ids = [event.event_id for event in events]
    if len(event_ids) != len(set(event_ids)):
        raise ValueError("football event IDs must be unique")
    if (facts.home_score is None) != (facts.away_score is None):
        raise ValueError("final match score must provide both teams or neither")
    for event in events:
        if event.time_precision != "minute":
            raise ValueError("market portrait events must be minute-precision")
        if (event.home_score is None) != (event.away_score is None):
            raise ValueError(
                f"event {event.event_id} score must provide both teams or neither"
            )
        if event.period == "PENS":
            if not event.is_penalty_shootout or event.match_minute is not None:
                raise ValueError(
                    "penalty events belong to PENS without an invented minute"
                )
        elif event.match_minute is None:
            raise ValueError("non-penalty events require a football minute")
    if any(event.period == "PENS" for event in events) and (
        facts.match_ended_at is None
        or _utc(facts.match_ended_at, "match_ended_at") <= final_period_end
    ):
        raise ValueError(
            "penalties require an actual match_ended_at after the final period"
        )


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
    ordered = _validate_match_periods(facts)
    _validate_football_events(facts, events, final_period_end=ordered[-1][1])


def _minute_specs(
    facts: MatchFacts, events: Sequence[FootballEvent]
) -> list[tuple[str, int, int, int, datetime, datetime]]:
    def stoppage(
        period: str,
        boundary: int,
        source_start: datetime,
        source_end: datetime,
    ) -> int:
        explicit = max(
            [0]
            + [
                event.stoppage_minute or 0
                for event in events
                if event.period == period and event.match_minute == boundary
            ]
        )
        nominal_minutes = 45 if period in {"1H", "2H"} else 15
        span = _utc(source_end, period) - _utc(source_start, period)
        excess_microseconds = max(
            0,
            span // timedelta(microseconds=1) - nominal_minutes * 60_000_000 - 1_000,
        )
        inferred = (excess_microseconds + 59_999_999) // 60_000_000
        return max(explicit, inferred)

    specs: list[tuple[str, int, int, int, datetime, datetime]] = [
        (
            "1H",
            1,
            45,
            stoppage(
                "1H",
                45,
                facts.first_half_started_at,
                facts.first_half_ended_at,
            ),
            facts.first_half_started_at,
            facts.first_half_ended_at,
        ),
        (
            "2H",
            46,
            90,
            stoppage(
                "2H",
                90,
                facts.second_half_started_at,
                facts.second_half_ended_at,
            ),
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
                    stoppage(
                        "ET1",
                        105,
                        facts.first_extra_half_started_at,
                        facts.first_extra_half_ended_at,
                    ),
                    facts.first_extra_half_started_at,
                    facts.first_extra_half_ended_at,
                ),
                (
                    "ET2",
                    106,
                    120,
                    stoppage(
                        "ET2",
                        120,
                        facts.second_extra_half_started_at,
                        facts.second_extra_half_ended_at,
                    ),
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
        profile.regulation_seconds + 20.0,
        profile.regulation_seconds
        + 0.5 * extra_minutes
        + (profile.penalty_seconds if penalties else 0),
    )


def _landscape_roles(states: Sequence[dict[str, Any]]) -> list[str]:
    roles = {str(state["role"]) for state in states}
    if roles == {"home", "away"}:
        return ["home", "away"]
    if roles == {"home_win", "draw", "away_win"}:
        return ["home_win", "draw", "away_win"]
    raise ValueError(f"invalid portrait role inventory: {sorted(roles)}")


def _is_scoring_annotation(annotation: dict[str, Any]) -> bool:
    event_type = str(annotation["event_type"]).casefold()
    return (
        annotation["period"] != "PENS"
        and not annotation["is_penalty_shootout"]
        and not annotation["is_revoked"]
        and event_type in {"goal", "own goal", "penalty scored"}
    )


def _normalize_scores(
    facts: MatchFacts, annotations: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    checkpoints = [
        {
            "event_order": 0,
            "video_start_seconds": 0.0,
            "home_score": 0,
            "away_score": 0,
        }
    ]
    current = (0, 0)
    for annotation in annotations:
        if _is_scoring_annotation(annotation):
            supplied = (annotation["home_score"], annotation["away_score"])
            if supplied[0] is None or supplied[1] is None:
                raise ValueError(
                    f"scoring event {annotation['event_id']} requires a "
                    "post-event score"
                )
            next_score = (int(supplied[0]), int(supplied[1]))
            delta = (next_score[0] - current[0], next_score[1] - current[1])
            if delta not in {(1, 0), (0, 1)}:
                raise ValueError(
                    f"scoring event {annotation['event_id']} must increment "
                    "exactly one team by one"
                )
            current = next_score
            checkpoints.append(
                {
                    "event_order": annotation["event_order"],
                    "video_start_seconds": annotation["video_end_seconds"],
                    "home_score": current[0],
                    "away_score": current[1],
                }
            )
        annotation["home_score"], annotation["away_score"] = current

    if facts.home_score is not None and facts.away_score is not None:
        final_score = (facts.home_score, facts.away_score)
        if current != final_score:
            raise ValueError(
                "chronological event score does not match the final MatchFacts score"
            )
    return checkpoints


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
    penalty_seconds = (
        profile.penalty_seconds
        if any(event.period == "PENS" for event in events)
        else 0.0
    )
    football_seconds = (
        duration
        - profile.pre_match_seconds
        - profile.halftime_seconds
        - profile.post_match_seconds
        - penalty_seconds
    )
    specs = _minute_specs(facts, events)
    minute_rows: list[dict[str, Any]] = []
    weights = sum(
        end - start + 1 + stoppage_count
        for _, start, end, stoppage_count, _, _ in specs
    )
    source_timestamps = [
        int(row["timestamp_ms"]) for row in [*states, *trades] if "timestamp_ms" in row
    ]
    first_half_start = _utc(facts.first_half_started_at, "first_half")
    final_whistle = _utc(
        facts.match_ended_at
        or facts.second_extra_half_ended_at
        or facts.second_half_ended_at,
        "match_ended_at",
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
    fixed_segments = []
    if profile.pre_match_seconds:
        fixed_segments.append(
            {
                "kind": "pre_match",
                "period": "PRE",
                "minute_label": "PRE",
                "source_start": _iso(min(market_start, first_half_start)),
                "source_end": _iso(first_half_start),
                "video_start_seconds": 0.0,
                "video_end_seconds": profile.pre_match_seconds,
            }
        )
    cursor = profile.pre_match_seconds
    for period, first, last, stoppage_count, source_start, source_end in specs:
        minutes = [(value, None) for value in range(first, last + 1)] + [
            (last, value) for value in range(1, stoppage_count + 1)
        ]
        period_start = _utc(source_start, period)
        period_end = _utc(source_end, period)
        for offset, (minute, added) in enumerate(minutes):
            weight = 1.0
            video_start = cursor
            cursor += football_seconds * weight / weights
            band_start = period_start + timedelta(minutes=offset)
            band_end = (
                period_end
                if offset == len(minutes) - 1
                else band_start + timedelta(minutes=1)
            )
            if band_start >= band_end or band_end > period_end:
                label = f"{minute}+{added}" if added is not None else str(minute)
                raise ValueError(
                    f"football band {period} {label} has no source duration"
                )
            minute_rows.append(
                {
                    "period": period,
                    "kind": "football_minute",
                    "match_minute": minute,
                    "stoppage_minute": added,
                    "minute_label": (
                        f"{minute}+{added}" if added is not None else str(minute)
                    ),
                    "source_start": _iso(band_start),
                    "source_end": _iso(band_end),
                    "video_start_seconds": round(video_start, 9),
                    "video_end_seconds": round(cursor, 9),
                    "weight": weight,
                }
            )
        if period == "1H" and profile.halftime_seconds:
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
            _utc(facts.match_ended_at, "match_ended_at")
            if facts.match_ended_at
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
    if profile.post_match_seconds:
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
    minute_band_index = {
        (str(row["period"]), row["match_minute"], row["stoppage_minute"]): index
        for index, row in enumerate(minute_rows)
        if row["period"] != "PENS"
    }
    for event in events:
        if event.period == "PENS":
            band = next(row for row in minute_rows if row["period"] == "PENS")
        else:
            try:
                band_index = minute_band_index[
                    (event.period, event.match_minute, event.stoppage_minute)
                ]
            except KeyError as exc:
                raise ValueError(
                    f"event {event.event_id} falls outside its period"
                ) from exc
            band = minute_rows[band_index]
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
    score_checkpoints = _normalize_scores(facts, annotations)
    metrics = _market_metrics(states, trades)
    reactions = _reactions(minute_rows, annotations, metrics)
    story = {
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
    _validate_story(facts, story)
    return story


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

    def before(
        rows: Sequence[dict[str, Any]],
        timestamps: Sequence[int],
        boundary: int,
        period_start: int | None,
    ) -> dict[str, Any] | None:
        index = bisect.bisect_left(timestamps, boundary) - 1
        if index < 0 or (period_start is not None and timestamps[index] < period_start):
            return None
        return {field: rows[index][field] for field in projected_fields}

    def after(
        rows: Sequence[dict[str, Any]],
        timestamps: Sequence[int],
        boundary: int,
        period_end: int,
    ) -> dict[str, Any] | None:
        index = bisect.bisect_left(timestamps, boundary)
        if index >= len(rows) or timestamps[index] > period_end:
            return None
        return {field: rows[index][field] for field in projected_fields}

    by_role: dict[str, list[dict[str, Any]]] = {}
    for metric in metrics:
        by_role.setdefault(str(metric["role"]), []).append(metric)
    timestamps_by_role: dict[str, list[int]] = {}
    for role, rows in by_role.items():
        rows.sort(key=lambda row: (int(row["timestamp_ms"]), row["event_sequence"]))
        timestamps_by_role[role] = [int(row["timestamp_ms"]) for row in rows]
    minute_band_index = {
        (str(row["period"]), row["match_minute"], row["stoppage_minute"]): index
        for index, row in enumerate(minute_rows)
        if row["period"] != "PENS"
    }
    period_end_by_name = {
        str(row["period"]): max(
            _story_timestamp_floor_ms(candidate["source_end"])
            for candidate in minute_rows
            if candidate["period"] == row["period"]
        )
        for row in minute_rows
        if row["period"] != "PENS"
    }
    period_start_by_name = {
        str(row["period"]): min(
            _story_timestamp_ceil_ms(candidate["source_start"])
            for candidate in minute_rows
            if candidate["period"] == row["period"]
        )
        for row in minute_rows
        if row["period"] != "PENS"
    }
    reactions = []
    for annotation in annotations:
        if annotation["is_penalty_shootout"]:
            continue
        band_index = minute_band_index[
            (
                str(annotation["period"]),
                annotation["match_minute"],
                annotation["stoppage_minute"],
            )
        ]
        band = minute_rows[band_index]
        start = _story_timestamp_ceil_ms(band["source_start"])
        end = _story_timestamp_ceil_ms(band["source_end"])
        following_end = (
            _story_timestamp_ceil_ms(minute_rows[band_index + 1]["source_end"])
            if band_index + 1 < len(minute_rows)
            and minute_rows[band_index + 1]["period"] == band["period"]
            else None
        )
        period_end = period_end_by_name[str(band["period"])]
        period_start = (
            None
            if band["period"] == "1H"
            else period_start_by_name[str(band["period"])]
        )
        for role, rows in sorted(by_role.items()):
            role_timestamps = timestamps_by_role[role]
            reactions.append(
                {
                    "event_id": annotation["event_id"],
                    "role": role,
                    "alignment": "minute-aligned",
                    "label": "minute-aligned market move",
                    "primary": {
                        "source_start_ms": start,
                        "source_end_ms": end,
                        "before": before(rows, role_timestamps, start, period_start),
                        "after": after(rows, role_timestamps, end, period_end),
                    },
                    "extended": {
                        "source_start_ms": start,
                        "source_end_ms": following_end,
                        "before": before(rows, role_timestamps, start, period_start),
                        "after": (
                            after(rows, role_timestamps, following_end, period_end)
                            if following_end is not None
                            else None
                        ),
                    },
                }
            )
    return reactions


def _story_datetime(value: Any) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _story_timestamp_microseconds(value: Any) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (_story_datetime(value).astimezone(timezone.utc) - epoch) // timedelta(
        microseconds=1
    )


def _story_timestamp_floor_ms(value: Any) -> int:
    return _story_timestamp_microseconds(value) // 1_000


def _story_timestamp_ceil_ms(value: Any) -> int:
    microseconds = _story_timestamp_microseconds(value)
    return -(-microseconds // 1_000)


def _validate_story(facts: MatchFacts, story: dict[str, Any]) -> None:
    """Fail closed if derived story fields disagree with their source invariants."""

    def require(condition: bool, message: str) -> None:
        if not condition:
            raise ValueError(f"invalid market portrait story: {message}")

    bands = story["football_minute_bands"]
    regular_bands = [row for row in bands if row["period"] != "PENS"]
    period_boundaries = {
        "1H": (facts.first_half_started_at, facts.first_half_ended_at),
        "2H": (facts.second_half_started_at, facts.second_half_ended_at),
    }
    if facts.first_extra_half_started_at and facts.first_extra_half_ended_at:
        period_boundaries.update(
            {
                "ET1": (
                    facts.first_extra_half_started_at,
                    facts.first_extra_half_ended_at,
                ),
                "ET2": (
                    facts.second_extra_half_started_at,
                    facts.second_extra_half_ended_at,
                ),
            }
        )
    require(
        {str(row["period"]) for row in regular_bands} == set(period_boundaries),
        "football period inventory does not match MatchFacts",
    )
    video_durations = []
    for period, (expected_start, expected_end) in period_boundaries.items():
        rows = [row for row in regular_bands if row["period"] == period]
        require(bool(rows), f"{period} has no football bands")
        require(
            _story_datetime(rows[0]["source_start"]) == _utc(expected_start, period),
            f"{period} does not begin at its actual boundary",
        )
        require(
            _story_datetime(rows[-1]["source_end"]) == _utc(expected_end, period),
            f"{period} does not end at its actual boundary",
        )
        for index, row in enumerate(rows):
            source_start = _story_datetime(row["source_start"])
            source_end = _story_datetime(row["source_end"])
            source_duration = source_end - source_start
            require(source_duration > timedelta(0), "football band is empty")
            if index < len(rows) - 1:
                require(
                    source_duration == timedelta(minutes=1),
                    "only a period's final football band may be clamped",
                )
                require(
                    source_end == _story_datetime(rows[index + 1]["source_start"]),
                    "football source bands do not tile their period",
                )
            else:
                require(
                    source_duration <= timedelta(minutes=1, milliseconds=1),
                    "final football band exceeds the one-millisecond tolerance",
                )
            require(row["weight"] == 1.0, "football bands must have equal weight")
            video_duration = float(row["video_end_seconds"]) - float(
                row["video_start_seconds"]
            )
            require(video_duration > 0, "football video band is empty")
            video_durations.append(video_duration)
    require(
        max(video_durations) - min(video_durations) <= 2e-9,
        "football video bands are not uniformly weighted",
    )

    band_by_key = {
        (
            row["period"],
            row["match_minute"],
            row["stoppage_minute"],
        ): row
        for row in bands
    }
    annotation_by_id = {}
    for annotation in story["annotations"]:
        require(
            annotation["event_id"] not in annotation_by_id,
            "annotation event IDs are not unique",
        )
        annotation_by_id[annotation["event_id"]] = annotation
        key = (
            annotation["period"],
            annotation["match_minute"],
            annotation["stoppage_minute"],
        )
        band = band_by_key.get(key)
        require(band is not None, "annotation does not map to a football band")
        require(
            annotation["video_start_seconds"] == band["video_start_seconds"]
            and annotation["video_end_seconds"] == band["video_end_seconds"],
            "annotation video bounds disagree with its football band",
        )
        require(
            isinstance(annotation["home_score"], int)
            and isinstance(annotation["away_score"], int)
            and annotation["home_score"] >= 0
            and annotation["away_score"] >= 0,
            "annotation score is not a non-negative pair",
        )

    checkpoints = story["score_checkpoints"]
    require(
        checkpoints
        and checkpoints[0]
        == {
            "event_order": 0,
            "video_start_seconds": 0.0,
            "home_score": 0,
            "away_score": 0,
        },
        "score checkpoints do not begin at 0-0",
    )
    scoring_annotations = [
        annotation
        for annotation in story["annotations"]
        if _is_scoring_annotation(annotation)
    ]
    require(
        len(checkpoints) == len(scoring_annotations) + 1,
        "score checkpoint count does not match scoring events",
    )
    for checkpoint, annotation in zip(checkpoints[1:], scoring_annotations):
        require(
            checkpoint["event_order"] == annotation["event_order"]
            and checkpoint["video_start_seconds"] == annotation["video_end_seconds"]
            and checkpoint["home_score"] == annotation["home_score"]
            and checkpoint["away_score"] == annotation["away_score"],
            "score checkpoint is not effective at its event band end",
        )

    roles = sorted({str(row["role"]) for row in story["market_metrics"]})
    expected_reactions = [
        (annotation["event_id"], role)
        for annotation in story["annotations"]
        if not annotation["is_penalty_shootout"]
        for role in roles
    ]
    actual_reactions = [
        (reaction["event_id"], reaction["role"]) for reaction in story["reactions"]
    ]
    require(
        actual_reactions == expected_reactions
        and len(actual_reactions) == len(set(actual_reactions)),
        "reaction event-role inventory is incomplete, duplicated, or unsorted",
    )
    period_end_ms = {
        period: _story_timestamp_floor_ms(rows[-1]["source_end"])
        for period in period_boundaries
        if (rows := [row for row in regular_bands if row["period"] == period])
    }
    period_start_ms = {
        period: _story_timestamp_ceil_ms(rows[0]["source_start"])
        for period in period_boundaries
        if (rows := [row for row in regular_bands if row["period"] == period])
    }
    band_indexes = {id(row): index for index, row in enumerate(bands)}
    for reaction in story["reactions"]:
        annotation = annotation_by_id[reaction["event_id"]]
        band = band_by_key[
            (
                annotation["period"],
                annotation["match_minute"],
                annotation["stoppage_minute"],
            )
        ]
        start = _story_timestamp_ceil_ms(band["source_start"])
        end = _story_timestamp_ceil_ms(band["source_end"])
        index = band_indexes[id(band)]
        following_end = (
            _story_timestamp_ceil_ms(bands[index + 1]["source_end"])
            if index + 1 < len(bands) and bands[index + 1]["period"] == band["period"]
            else None
        )
        primary = reaction["primary"]
        extended = reaction["extended"]
        require(
            primary["source_start_ms"] == start
            and primary["source_end_ms"] == end
            and extended["source_start_ms"] == start
            and extended["source_end_ms"] == following_end,
            "reaction bounds disagree with its football bands",
        )
        require(
            primary["before"] == extended["before"],
            "reaction windows disagree on their pre-event observation",
        )
        if primary["before"] is not None:
            require(
                int(primary["before"]["timestamp_ms"]) < start
                and (
                    band["period"] == "1H"
                    or int(primary["before"]["timestamp_ms"])
                    >= period_start_ms[str(band["period"])]
                ),
                "reaction before observation is not strictly pre-window",
            )
        if primary["after"] is not None:
            require(
                end
                <= int(primary["after"]["timestamp_ms"])
                <= period_end_ms[str(band["period"])],
                "primary reaction after observation crosses its period",
            )
        if extended["after"] is not None:
            require(
                following_end is not None
                and following_end
                <= int(extended["after"]["timestamp_ms"])
                <= period_end_ms[str(band["period"])],
                "extended reaction after observation crosses its period",
            )

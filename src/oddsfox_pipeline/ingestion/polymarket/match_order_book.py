"""Resumable PMXT historical L2 order-book ingestion for approved WC2026 games."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable

import requests
import yaml
from requests.adapters import HTTPAdapter

from oddsfox_pipeline.config.settings import (
    PMXT_API_KEY,
    PMXT_API_URL,
)
from oddsfox_pipeline.ingestion.polymarket.markets.fetch import (
    build_client as build_gamma_client,
)
from oddsfox_pipeline.resources.http import APIClient
from oddsfox_pipeline.resources.http_retry import (
    exponential_backoff_seconds,
    is_transient_status,
    retry_after_seconds,
)
from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    merge_match_order_book_snapshots,
)
from oddsfox_pipeline.storage.duckdb.match_order_book import (
    acquire_scan,
    complete_window,
    next_pending_window,
    publish_scan,
    published_scan_summary,
    reserve_api_attempt,
    scan_progress_summary,
    set_scan_status,
    split_window,
)

logger = logging.getLogger(__name__)

PMXT_ORDER_BOOK_ENDPOINT = "/api/polymarket/fetchOrderBook"
PMXT_ORDER_BOOK_SOURCE = "api.pmxt.dev/api/polymarket/fetchOrderBook"
PMXT_MAX_RANGE_SNAPSHOTS = 1_000
_HEX_32_RE = re.compile(r"0x[0-9a-f]{64}")
_NUMERIC_ID_RE = re.compile(r"[1-9][0-9]*")
_SLUG_RE = re.compile(r"[a-z0-9][a-z0-9-]*")


@dataclass(frozen=True)
class MatchOrderBookOutcome:
    label: str
    clob_token_id: str
    role: str


@dataclass(frozen=True)
class MatchOrderBookTarget:
    fifa_match_id: int
    stage: str
    home_team: str
    away_team: str
    event_id: str
    event_slug: str
    market_id: str
    market_slug: str
    market_type: str
    condition_id: str
    accepting_orders_at: datetime
    closed_at: datetime
    outcomes: tuple[MatchOrderBookOutcome, ...]

    @property
    def window_start_ms(self) -> int:
        return int(self.accepting_orders_at.timestamp() * 1_000)

    @property
    def window_end_ms(self) -> int:
        return int(self.closed_at.timestamp() * 1_000)


@dataclass(frozen=True)
class MatchOrderBookManifest:
    version: int
    targets: tuple[MatchOrderBookTarget, ...]
    sha256: str


class MatchOrderBookSyncError(RuntimeError):
    """PMXT failure carrying safe Dagster metadata."""

    def __init__(self, message: str, summary: dict[str, Any]):
        super().__init__(message)
        self.summary = summary


class MatchOrderBookPaused(MatchOrderBookSyncError):
    """Resumable PMXT scan paused by a local or upstream credit limit."""


class _PmxtEnvelopeError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool):
        super().__init__(message)
        self.retryable = retryable


def default_order_book_targets_path() -> Path:
    return Path(__file__).resolve().parent / "seeds" / "order_book_targets.yml"


def _utc_datetime(value: Any, *, field: str) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError(f"{field} must be a UTC timestamp")
    normalized = raw.replace("Z", "+00:00")
    if re.search(r"[+-]\d{2}$", normalized):
        normalized += ":00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _required_text(payload: dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} must not be blank")
    return value


def _manifest_payload(manifest: MatchOrderBookManifest) -> dict[str, Any]:
    return {
        "version": manifest.version,
        "targets": [
            {
                "fifa_match_id": target.fifa_match_id,
                "stage": target.stage,
                "home_team": target.home_team,
                "away_team": target.away_team,
                "event_id": target.event_id,
                "event_slug": target.event_slug,
                "market_id": target.market_id,
                "market_slug": target.market_slug,
                "market_type": target.market_type,
                "condition_id": target.condition_id,
                "accepting_orders_at": target.accepting_orders_at.isoformat(),
                "closed_at": target.closed_at.isoformat(),
                "outcomes": [
                    {
                        "label": outcome.label,
                        "clob_token_id": outcome.clob_token_id,
                        "role": outcome.role,
                    }
                    for outcome in target.outcomes
                ],
            }
            for target in manifest.targets
        ],
    }


def load_order_book_manifest(
    path: Path | None = None,
) -> MatchOrderBookManifest:
    manifest_path = path or default_order_book_targets_path()
    payload = yaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid order-book manifest root in {manifest_path}")
    declared_hash = payload.get("content_sha256")
    if declared_hash is not None:
        unhashed = {
            key: value for key, value in payload.items() if key != "content_sha256"
        }
        actual_hash = hashlib.sha256(
            json.dumps(unhashed, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        if declared_hash != actual_hash:
            raise ValueError("order-book manifest content_sha256 does not match")
    version = payload.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise ValueError("order-book manifest version must be a positive integer")
    raw_targets = payload.get("targets")
    if not isinstance(raw_targets, list) or not raw_targets:
        raise ValueError("order-book manifest targets must be a non-empty list")

    targets: list[MatchOrderBookTarget] = []
    seen_markets: set[str] = set()
    seen_conditions: set[str] = set()
    seen_tokens: set[str] = set()
    for raw_target in raw_targets:
        if not isinstance(raw_target, dict):
            raise ValueError("each order-book target must be a mapping")
        fifa_match_id = raw_target.get("fifa_match_id")
        if (
            isinstance(fifa_match_id, bool)
            or not isinstance(fifa_match_id, int)
            or not 1 <= fifa_match_id <= 104
        ):
            raise ValueError("fifa_match_id must identify a WC2026 match")
        event_id = _required_text(raw_target, "event_id")
        market_id = _required_text(raw_target, "market_id")
        condition_id = _required_text(raw_target, "condition_id").lower()
        event_slug = _required_text(raw_target, "event_slug").lower()
        market_slug = _required_text(raw_target, "market_slug").lower()
        market_type = _required_text(raw_target, "market_type")
        if fifa_match_id <= 72 and market_type != "moneyline":
            raise ValueError("group order-book targets must be moneyline markets")
        if fifa_match_id >= 73 and market_type != "soccer_team_to_advance":
            raise ValueError(
                "knockout order-book targets must be soccer_team_to_advance"
            )
        if not _NUMERIC_ID_RE.fullmatch(event_id):
            raise ValueError(f"Invalid event_id {event_id!r}")
        if not _NUMERIC_ID_RE.fullmatch(market_id):
            raise ValueError(f"Invalid market_id {market_id!r}")
        if not _HEX_32_RE.fullmatch(condition_id):
            raise ValueError(f"Invalid condition_id {condition_id!r}")
        if not _SLUG_RE.fullmatch(event_slug) or not _SLUG_RE.fullmatch(market_slug):
            raise ValueError("event_slug and market_slug must be lowercase slugs")
        start = _utc_datetime(
            raw_target.get("accepting_orders_at"), field="accepting_orders_at"
        )
        end = _utc_datetime(raw_target.get("closed_at"), field="closed_at")
        if start >= end:
            raise ValueError("accepting_orders_at must precede closed_at")

        raw_outcomes = raw_target.get("outcomes")
        if not isinstance(raw_outcomes, list) or not raw_outcomes:
            expected = (
                "exactly two outcomes" if fifa_match_id >= 73 else "a Yes outcome"
            )
            raise ValueError(f"each order-book target must have {expected}")
        outcomes: list[MatchOrderBookOutcome] = []
        for raw_outcome in raw_outcomes:
            if not isinstance(raw_outcome, dict):
                raise ValueError("each outcome must be a mapping")
            label = _required_text(raw_outcome, "label")
            token_id = _required_text(raw_outcome, "clob_token_id")
            if not _NUMERIC_ID_RE.fullmatch(token_id):
                raise ValueError(f"Invalid clob_token_id for {label!r}")
            if token_id in seen_tokens:
                raise ValueError(f"Duplicate clob_token_id {token_id}")
            seen_tokens.add(token_id)
            role = str(raw_outcome.get("role") or "").strip()
            if not role:
                if (
                    label.casefold()
                    == _required_text(raw_target, "home_team").casefold()
                ):
                    role = "home"
                elif (
                    label.casefold()
                    == _required_text(raw_target, "away_team").casefold()
                ):
                    role = "away"
            if role not in {"home", "away", "home_win", "draw", "away_win"}:
                raise ValueError(f"Invalid landscape role {role!r}")
            outcomes.append(
                MatchOrderBookOutcome(
                    label=label,
                    clob_token_id=token_id,
                    role=role,
                )
            )
        if len({outcome.label.casefold() for outcome in outcomes}) != len(outcomes):
            raise ValueError("outcome labels must be distinct")

        for value, seen, label in (
            (market_id, seen_markets, "market_id"),
            (condition_id, seen_conditions, "condition_id"),
        ):
            if value in seen:
                raise ValueError(f"Duplicate {label} {value}")
            seen.add(value)
        targets.append(
            MatchOrderBookTarget(
                fifa_match_id=fifa_match_id,
                stage=_required_text(raw_target, "stage"),
                home_team=_required_text(raw_target, "home_team"),
                away_team=_required_text(raw_target, "away_team"),
                event_id=event_id,
                event_slug=event_slug,
                market_id=market_id,
                market_slug=market_slug,
                market_type=market_type,
                condition_id=condition_id,
                accepting_orders_at=start,
                closed_at=end,
                outcomes=tuple(outcomes),
            )
        )
        if (
            fifa_match_id >= 73
            and sum(target.fifa_match_id == fifa_match_id for target in targets) > 1
        ):
            raise ValueError(f"Duplicate fifa_match_id {fifa_match_id}")

    match_ids = {target.fifa_match_id for target in targets}
    identities = {
        (target.stage, target.home_team, target.away_team) for target in targets
    }
    roles = [outcome.role for target in targets for outcome in target.outcomes]
    if len(match_ids) != 1 or len(identities) != 1:
        raise ValueError("a target manifest must describe exactly one match")
    fifa_match_id = next(iter(match_ids))
    if fifa_match_id <= 72:
        if len(targets) != 3 or sorted(roles) != [
            "away_win",
            "draw",
            "home_win",
        ]:
            raise ValueError(
                "group target must select exactly home_win, draw, and away_win"
            )
        if any(
            len(target.outcomes) != 1 or target.outcomes[0].label.casefold() != "yes"
            for target in targets
        ):
            raise ValueError("group target must select only each literal Yes token")
    elif len(targets) != 1 or sorted(roles) != ["away", "home"]:
        raise ValueError(
            "knockout target must select the named home and away outcome tokens"
        )

    provisional = MatchOrderBookManifest(
        version=version, targets=tuple(targets), sha256=""
    )
    canonical = json.dumps(
        _manifest_payload(provisional),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return MatchOrderBookManifest(
        version=version,
        targets=tuple(targets),
        sha256=hashlib.sha256(canonical).hexdigest(),
    )


def _json_string_list(value: Any, *, field: str) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Gamma {field} is not valid JSON") from exc
    if not isinstance(value, list):
        raise ValueError(f"Gamma {field} must be a list")
    return [str(item) for item in value]


def validate_gamma_targets(
    manifest: MatchOrderBookManifest,
    client: Any | None = None,
) -> None:
    gamma = client or build_gamma_client()
    for target in manifest.targets:
        market = gamma.get(f"/markets/slug/{target.market_slug}")
        if not isinstance(market, dict):
            raise ValueError(f"Gamma returned no market for {target.market_slug}")
        events = market.get("events") or []
        event = events[0] if isinstance(events, list) and events else {}
        actual = {
            "market_id": str(market.get("id") or ""),
            "market_slug": str(market.get("slug") or "").lower(),
            "market_type": str(market.get("sportsMarketType") or ""),
            "condition_id": str(market.get("conditionId") or "").lower(),
            "event_id": str(event.get("id") or ""),
            "event_slug": str(event.get("slug") or "").lower(),
        }
        expected = {
            "market_id": target.market_id,
            "market_slug": target.market_slug,
            "market_type": target.market_type,
            "condition_id": target.condition_id,
            "event_id": target.event_id,
            "event_slug": target.event_slug,
        }
        mismatches = [
            key
            for key, expected_value in expected.items()
            if actual[key] != expected_value
        ]
        outcomes = _json_string_list(market.get("outcomes"), field="outcomes")
        tokens = _json_string_list(market.get("clobTokenIds"), field="clobTokenIds")
        gamma_tokens = dict(zip(outcomes, tokens, strict=True))
        if target.fifa_match_id >= 73:
            if outcomes != [outcome.label for outcome in target.outcomes]:
                mismatches.append("outcomes")
            if tokens != [outcome.clob_token_id for outcome in target.outcomes]:
                mismatches.append("clob_token_ids")
        else:
            outcome = target.outcomes[0]
            if gamma_tokens.get(outcome.label) != outcome.clob_token_id:
                mismatches.append("clob_token_ids")
        if market.get("closed") is not True:
            mismatches.append("closed")
        accepting = _utc_datetime(
            market.get("acceptingOrdersTimestamp"),
            field="Gamma acceptingOrdersTimestamp",
        )
        closed = _utc_datetime(market.get("closedTime"), field="Gamma closedTime")
        if accepting != target.accepting_orders_at:
            mismatches.append("accepting_orders_at")
        if closed != target.closed_at:
            mismatches.append("closed_at")
        if mismatches:
            raise ValueError(
                f"Gamma target contract mismatch for {target.market_slug}: "
                + ", ".join(sorted(set(mismatches)))
            )


def build_pmxt_client(
    *,
    requests_per_minute: int,
    request_timeout: float | tuple[float, float] | None = None,
) -> APIClient:
    client = APIClient(
        base_url=PMXT_API_URL,
        retries=0,
        requests_per_second=requests_per_minute / 60.0,
        request_timeout=request_timeout,
    )
    no_retry_adapter = HTTPAdapter(max_retries=0)
    client.session.mount("http://", no_retry_adapter)
    client.session.mount("https://", no_retry_adapter)
    return client


def _decimal_string(
    value: Any,
    *,
    field: str,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
    strictly_positive: bool = False,
) -> str:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a decimal") from exc
    if not number.is_finite():
        raise ValueError(f"{field} must be finite")
    if strictly_positive and number <= 0:
        raise ValueError(f"{field} must be positive")
    if minimum is not None and number < minimum:
        raise ValueError(f"{field} must be >= {minimum}")
    if maximum is not None and number > maximum:
        raise ValueError(f"{field} must be <= {maximum}")
    if number == 0:
        return "0"
    _, raw_digits, exponent = number.as_tuple()
    digits = raw_digits
    while digits and digits[-1] == 0:
        digits = digits[:-1]
        exponent += 1
    integer_digits = (
        len(digits) + exponent if exponent >= 0 else max(len(digits) + exponent, 1)
    )
    if integer_digits > 20 or max(-exponent, 0) > 18:
        raise ValueError(f"{field} must fit DECIMAL(38,18) exactly")
    normalized = format(number, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized


def _normalize_levels(raw: Any, *, side: str) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        raise ValueError(f"{side} must be a list")
    levels: list[dict[str, Any]] = []
    prices: list[Decimal] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError(f"{side} levels must be objects")
        price = _decimal_string(
            item.get("price"),
            field=f"{side}.price",
            minimum=Decimal("0"),
            maximum=Decimal("1"),
        )
        size = _decimal_string(
            item.get("size"), field=f"{side}.size", strictly_positive=True
        )
        order_count = item.get("orderCount")
        if order_count is not None:
            if (
                isinstance(order_count, bool)
                or not isinstance(order_count, int)
                or order_count < 0
            ):
                raise ValueError(f"{side}.orderCount must be a nonnegative integer")
        levels.append({"price": price, "size": size, "order_count": order_count})
        prices.append(Decimal(price))
    if len(prices) != len(set(prices)):
        raise ValueError(f"{side} contains duplicate prices")
    expected = sorted(prices, reverse=side == "bids")
    if prices != expected:
        raise ValueError(f"{side} levels are not source-sorted")
    return levels


def normalize_pmxt_snapshot(
    snapshot: Any,
    *,
    manifest: MatchOrderBookManifest,
    target: MatchOrderBookTarget,
    outcome: MatchOrderBookOutcome,
    scan_id: str,
    window_start_ms: int,
    window_end_ms: int,
    provider_sequence: int = 0,
    ingested_at: datetime | None = None,
) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        raise ValueError("PMXT snapshot must be an object")
    raw_timestamp = snapshot.get("timestamp")
    try:
        timestamp_decimal = Decimal(str(raw_timestamp))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("PMXT snapshot timestamp must be an integer") from exc
    timestamp = int(timestamp_decimal)
    if timestamp_decimal != timestamp:
        raise ValueError("PMXT snapshot timestamp must be an integer")
    if not window_start_ms <= timestamp <= window_end_ms:
        raise ValueError(
            f"PMXT snapshot timestamp {timestamp} is outside requested range"
        )
    bids = _normalize_levels(snapshot.get("bids"), side="bids")
    asks = _normalize_levels(snapshot.get("asks"), side="asks")
    is_neg_risk = snapshot.get("isNegRisk")
    if is_neg_risk is not None and not isinstance(is_neg_risk, bool):
        raise ValueError("PMXT isNegRisk must be boolean or null")
    last_trade_price = snapshot.get("lastTradePrice")
    if last_trade_price is not None:
        last_trade_price = _decimal_string(
            last_trade_price,
            field="lastTradePrice",
            minimum=Decimal("0"),
            maximum=Decimal("1"),
        )
    canonical = {
        "clob_token_id": outcome.clob_token_id,
        "timestamp": timestamp,
        "bids": bids,
        "asks": asks,
        "is_neg_risk": is_neg_risk,
        "last_trade_price": last_trade_price,
    }
    snapshot_sha256 = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    loaded_at = ingested_at or datetime.now(timezone.utc)
    if loaded_at.tzinfo is not None:
        loaded_at = loaded_at.astimezone(timezone.utc).replace(tzinfo=None)
    return {
        "scan_id": scan_id,
        "manifest_sha256": manifest.sha256,
        "fifa_match_id": target.fifa_match_id,
        "stage": target.stage,
        "home_team": target.home_team,
        "away_team": target.away_team,
        "event_id": target.event_id,
        "event_slug": target.event_slug,
        "market_id": target.market_id,
        "market_slug": target.market_slug,
        "market_type": target.market_type,
        "condition_id": target.condition_id,
        "outcome_label": outcome.label,
        "landscape_role": outcome.role,
        "clob_token_id": outcome.clob_token_id,
        "window_start_ms": window_start_ms,
        "window_end_ms": window_end_ms,
        "snapshot_timestamp_ms": timestamp,
        "snapshot_at": datetime.fromtimestamp(
            timestamp / 1_000, tz=timezone.utc
        ).replace(tzinfo=None),
        "snapshot_sha256": snapshot_sha256,
        "provider_sequence": provider_sequence,
        "bids_json": json.dumps(bids, sort_keys=True, separators=(",", ":")),
        "asks_json": json.dumps(asks, sort_keys=True, separators=(",", ":")),
        "is_neg_risk": is_neg_risk,
        "last_trade_price": last_trade_price,
        "source_endpoint": PMXT_ORDER_BOOK_SOURCE,
        "ingested_at": loaded_at,
    }


def _pmxt_books(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise _PmxtEnvelopeError("PMXT returned a non-object response", retryable=False)
    if payload.get("success") is not True:
        error = payload.get("error")
        if isinstance(error, dict):
            retryable = error.get("retryable") is True
        else:
            retryable = False
        raise _PmxtEnvelopeError("PMXT request failed", retryable=retryable)
    data = payload.get("data")
    if not isinstance(data, list):
        raise _PmxtEnvelopeError(
            "PMXT historical range did not return a snapshot list",
            retryable=False,
        )
    return data


def _window_target(
    manifest: MatchOrderBookManifest, window: dict[str, Any]
) -> tuple[MatchOrderBookTarget, MatchOrderBookOutcome]:
    matches = [
        (target, outcome)
        for target in manifest.targets
        if target.market_id == str(window["market_id"])
        for outcome in target.outcomes
        if outcome.clob_token_id == str(window["clob_token_id"])
    ]
    if len(matches) != 1:
        raise RuntimeError("PMXT work window no longer matches the target manifest")
    return matches[0]


def _request_window(
    *,
    conn: Any,
    client: Any,
    api_key: str,
    scan_id: str,
    lease_owner: str,
    window: dict[str, Any],
    monthly_credit_budget: int,
    transient_retries: int,
    transient_backoff_seconds: float,
    sleep_fn: Callable[[float], None],
) -> list[dict[str, Any]]:
    for retry_number in range(transient_retries + 1):
        if not reserve_api_attempt(
            conn,
            scan_id=scan_id,
            lease_owner=lease_owner,
            token_id=str(window["clob_token_id"]),
            window_start_ms=int(window["window_start_ms"]),
            window_end_ms=int(window["window_end_ms"]),
            monthly_credit_budget=monthly_credit_budget,
        ):
            raise MatchOrderBookPaused(
                "PMXT local monthly credit budget reached",
                {"status": "paused", "scan_id": scan_id, "reason": "credit_budget"},
            )
        try:
            payload = client.post(
                PMXT_ORDER_BOOK_ENDPOINT,
                headers={"Authorization": f"Bearer {api_key}"},
                json={
                    "args": [
                        str(window["market_id"]),
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
        except _PmxtEnvelopeError as exc:
            transient = exc.retryable
            status = 0
            caught: BaseException = exc
        except requests.RequestException as exc:
            status = int(exc.response.status_code) if exc.response is not None else 0
            transient = is_transient_status(status)
            caught = exc
        if transient and retry_number < transient_retries:
            retry_after = (
                retry_after_seconds(getattr(caught, "response", None))
                if isinstance(caught, requests.RequestException)
                else None
            )
            delay = retry_after or max(
                transient_backoff_seconds,
                exponential_backoff_seconds(retry_number + 1),
            )
            sleep_fn(delay)
            continue
        if status == 429:
            raise MatchOrderBookPaused(
                "PMXT rate or credit limit remained exhausted",
                {"status": "paused", "scan_id": scan_id, "reason": "upstream_429"},
            ) from caught
        raise caught
    raise AssertionError("unreachable")  # pragma: no cover


def sync_match_order_book_history(
    conn: Any,
    *,
    api_key: str | None = None,
    requests_per_minute: int = 50,
    monthly_credit_budget: int = 20_000,
    transient_retries: int = 4,
    transient_backoff_seconds: float = 1.0,
    force: bool = False,
    lease_owner: str = "local",
    manifest_path: Path | None = None,
    gamma_client: Any | None = None,
    pmxt_client: Any | None = None,
    merge_rows_fn: Callable[[Iterable[dict[str, Any]], Any], None] = (
        merge_match_order_book_snapshots
    ),
    sleep_fn: Callable[[float], None] = time.sleep,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Fetch, checkpoint, and atomically publish all approved target histories."""
    manifest = load_order_book_manifest(manifest_path)
    scan_id, published, resumed = acquire_scan(
        conn,
        manifest_version=manifest.version,
        manifest_sha256=manifest.sha256,
        targets=manifest.targets,
        lease_owner=lease_owner,
        force=force,
    )
    if published:
        return published_scan_summary(conn, scan_id)

    key = (api_key if api_key is not None else PMXT_API_KEY).strip()
    if not key:
        exc = ValueError("PMXT_API_KEY is required for an unpublished order-book scan")
        set_scan_status(
            conn,
            scan_id,
            "failed",
            exc,
            lease_owner=lease_owner,
        )
        summary = scan_progress_summary(conn, scan_id)
        summary["error_type"] = exc.__class__.__name__
        raise MatchOrderBookSyncError(str(exc), summary) from exc

    client = pmxt_client or build_pmxt_client(requests_per_minute=requests_per_minute)
    try:
        validate_gamma_targets(manifest, gamma_client or build_gamma_client())
        completed_windows = 0
        while True:
            window = next_pending_window(conn, scan_id)
            if window is None:
                break
            target, outcome = _window_target(manifest, window)
            books = _request_window(
                conn=conn,
                client=client,
                api_key=key,
                scan_id=scan_id,
                lease_owner=lease_owner,
                window=window,
                monthly_credit_budget=monthly_credit_budget,
                transient_retries=transient_retries,
                transient_backoff_seconds=transient_backoff_seconds,
                sleep_fn=sleep_fn,
            )
            if len(books) > PMXT_MAX_RANGE_SNAPSHOTS:
                raise ValueError("PMXT returned more than the requested snapshot limit")
            rows_by_key: dict[tuple[int, str], dict[str, Any]] = {}
            for provider_sequence, book in enumerate(books):
                row = normalize_pmxt_snapshot(
                    book,
                    manifest=manifest,
                    target=target,
                    outcome=outcome,
                    scan_id=scan_id,
                    window_start_ms=int(window["window_start_ms"]),
                    window_end_ms=int(window["window_end_ms"]),
                    provider_sequence=provider_sequence,
                )
                rows_by_key.setdefault(
                    (row["snapshot_timestamp_ms"], row["snapshot_sha256"]),
                    row,
                )
            if len(books) == PMXT_MAX_RANGE_SNAPSHOTS:
                split_window(
                    conn,
                    scan_id=scan_id,
                    lease_owner=lease_owner,
                    window=window,
                )
                if progress_callback:
                    progress_callback(
                        "split",
                        {
                            "scan_id": scan_id,
                            "token_id": outcome.clob_token_id,
                            "window_start_ms": window["window_start_ms"],
                            "window_end_ms": window["window_end_ms"],
                        },
                    )
                continue
            rows = list(rows_by_key.values())
            if rows:
                merge_rows_fn(rows, conn)
            complete_window(
                conn,
                scan_id=scan_id,
                lease_owner=lease_owner,
                window=window,
                snapshot_hashes=[str(row["snapshot_sha256"]) for row in rows],
            )
            completed_windows += 1
            if progress_callback:
                progress_callback(
                    "loaded",
                    {
                        "scan_id": scan_id,
                        "completed_windows": completed_windows,
                        "snapshots": len(rows),
                        "resumed": resumed,
                    },
                )
        summary = publish_scan(conn, scan_id, lease_owner=lease_owner)
        summary.update(scan_progress_summary(conn, scan_id))
        summary.update(
            {
                "manifest_sha256": manifest.sha256,
                "target_count": len(manifest.targets),
                "resumed": resumed,
                "noop": False,
            }
        )
        return summary
    except MatchOrderBookPaused as exc:
        set_scan_status(
            conn,
            scan_id,
            "paused",
            exc,
            lease_owner=lease_owner,
        )
        exc.summary.update(scan_progress_summary(conn, scan_id))
        exc.summary["resumed"] = resumed
        exc.summary["reason"] = exc.summary.get("reason", "paused")
        raise
    except Exception as exc:
        set_scan_status(
            conn,
            scan_id,
            "failed",
            exc,
            lease_owner=lease_owner,
        )
        summary = scan_progress_summary(conn, scan_id)
        summary.update(
            {
                "manifest_sha256": manifest.sha256,
                "resumed": resumed,
                "error_type": exc.__class__.__name__,
            }
        )
        raise MatchOrderBookSyncError(
            "PMXT order-book scan failed",
            summary,
        ) from exc


__all__ = [
    "MatchOrderBookManifest",
    "MatchOrderBookOutcome",
    "MatchOrderBookPaused",
    "MatchOrderBookSyncError",
    "MatchOrderBookTarget",
    "build_pmxt_client",
    "default_order_book_targets_path",
    "load_order_book_manifest",
    "normalize_pmxt_snapshot",
    "sync_match_order_book_history",
    "validate_gamma_targets",
]

"""Bounded one-minute CLOB history for completed WC2026 matches."""

from __future__ import annotations

import json
import logging
import math
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import local
from typing import Any, Callable, Iterable

import duckdb

from oddsfox_pipeline.config.settings import CLOB_API_URL
from oddsfox_pipeline.ingestion.polymarket.odds.execution import (
    fetch_window_with_auto_split,
)
from oddsfox_pipeline.ingestion.polymarket.odds.fetch import build_client
from oddsfox_pipeline.resources.http import RateLimiter
from oddsfox_pipeline.resources.progress_guardrails import ProgressGuardrail
from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    load_match_minute_odds_history_stage,
)

logger = logging.getLogger(__name__)

EXPECTED_GAMES = 104
EXPECTED_GROUP_GAMES = 72
EXPECTED_KNOCKOUT_GAMES = 32
EXPECTED_GROUP_MARKETS = 216
EXPECTED_MARKETS = 248
EXPECTED_TOKENS = 496


@dataclass(frozen=True)
class MatchMinuteTokenPlan:
    market_id: str
    token_id: str
    started_at: datetime
    finished_at: datetime


def _json_list(value: Any) -> list[str]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value]


def _team_key(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore")
    return re.sub(r"[^a-z0-9]", "", ascii_value.decode().casefold())


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc)


def _pair_key(teams: Iterable[str], started_at: datetime) -> tuple[str, str, int]:
    keys = sorted(_team_key(team) for team in teams)
    if len(keys) != 2 or not all(keys) or keys[0] == keys[1]:
        raise ValueError(f"Invalid WC2026 team pair: {list(teams)!r}")
    return keys[0], keys[1], int(_utc(started_at).timestamp())


def _market_rows(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT id, event_id, event_slug, event_title, event_start_time,
               event_finished_time, event_ended, sports_market_type,
               group_item_title, outcomes, clob_token_ids
        FROM polymarket_wc2026_raw.markets
        WHERE closed = TRUE
          AND sports_market_type IN ('moneyline', 'soccer_team_to_advance')
        ORDER BY event_id, id
        """
    )
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def select_match_minute_token_plans(
    conn: duckdb.DuckDBPyConnection,
) -> list[MatchMinuteTokenPlan]:
    """Select and strictly validate the completed 104-game market inventory."""
    rows = _market_rows(conn)
    moneyline_by_event: dict[str, list[dict[str, Any]]] = {}
    advance_rows: list[dict[str, Any]] = []
    for row in rows:
        if row["sports_market_type"] == "moneyline":
            moneyline_by_event.setdefault(str(row["event_id"]), []).append(row)
        else:
            advance_rows.append(row)

    if len(moneyline_by_event) != EXPECTED_GAMES:
        raise ValueError(
            f"Expected {EXPECTED_GAMES} primary moneyline events; "
            f"found {len(moneyline_by_event)}"
        )
    if len(advance_rows) != EXPECTED_KNOCKOUT_GAMES:
        raise ValueError(
            f"Expected {EXPECTED_KNOCKOUT_GAMES} advance markets; "
            f"found {len(advance_rows)}"
        )

    primary_by_pair: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for event_id, event_rows in moneyline_by_event.items():
        if len(event_rows) != 3:
            raise ValueError(
                f"Primary event {event_id} must have three moneyline markets; "
                f"found {len(event_rows)}"
            )
        first = event_rows[0]
        if (
            first["event_start_time"] is None
            or first["event_finished_time"] is None
            or first["event_ended"] is not True
        ):
            raise ValueError(f"Primary event {event_id} has no valid timing window")
        teams = [
            str(row["group_item_title"])
            for row in event_rows
            if not str(row["group_item_title"] or "").casefold().startswith("draw")
        ]
        key = _pair_key(teams, first["event_start_time"])
        primary_by_pair.setdefault(key, []).append(first)

    if any(len(events) != 1 for events in primary_by_pair.values()):
        raise ValueError("Duplicate or ambiguous primary WC2026 match events")

    matched_primary_ids: set[str] = set()
    selected_rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for advance in advance_rows:
        outcomes = _json_list(advance["outcomes"])
        started_at = advance["event_start_time"] or advance.get("game_start_time")
        if started_at is None:
            raise ValueError(f"Advance market {advance['id']} has no start time")
        matches = primary_by_pair.get(_pair_key(outcomes, started_at), [])
        if len(matches) != 1:
            raise ValueError(
                f"Advance market {advance['id']} matched {len(matches)} primary events"
            )
        primary = matches[0]
        primary_id = str(primary["event_id"])
        if primary_id in matched_primary_ids:
            raise ValueError(
                f"Primary event {primary_id} has duplicate advance markets"
            )
        matched_primary_ids.add(primary_id)
        selected_rows.append((advance, primary))

    group_event_ids = set(moneyline_by_event) - matched_primary_ids
    if len(group_event_ids) != EXPECTED_GROUP_GAMES:
        raise ValueError(
            f"Expected {EXPECTED_GROUP_GAMES} group events; found {len(group_event_ids)}"
        )
    for event_id in sorted(group_event_ids):
        selected_rows.extend(
            (market, market) for market in moneyline_by_event[event_id]
        )

    if len(selected_rows) != EXPECTED_MARKETS:
        raise ValueError(
            f"Expected {EXPECTED_MARKETS} selected markets; found {len(selected_rows)}"
        )

    plans: list[MatchMinuteTokenPlan] = []
    seen_tokens: set[str] = set()
    for market, primary in selected_rows:
        outcomes = _json_list(market["outcomes"])
        tokens = _json_list(market["clob_token_ids"])
        if len(outcomes) != 2 or len(tokens) != 2 or len(set(tokens)) != 2:
            raise ValueError(
                f"Market {market['id']} must map exactly two outcome tokens"
            )
        if market["sports_market_type"] == "moneyline" and {
            outcome.casefold() for outcome in outcomes
        } != {"yes", "no"}:
            raise ValueError(
                f"Moneyline market {market['id']} outcomes must be literal Yes and No"
            )
        started_at = _utc(primary["event_start_time"])
        finished_at = _utc(primary["event_finished_time"])
        if finished_at <= started_at:
            raise ValueError(f"Market {market['id']} has an invalid primary window")
        for token_id in tokens:
            if token_id in seen_tokens:
                raise ValueError(f"Token {token_id} maps to more than one market")
            seen_tokens.add(token_id)
            plans.append(
                MatchMinuteTokenPlan(
                    market_id=str(market["id"]),
                    token_id=token_id,
                    started_at=started_at,
                    finished_at=finished_at,
                )
            )

    if len(plans) != EXPECTED_TOKENS:
        raise ValueError(f"Expected {EXPECTED_TOKENS} tokens; found {len(plans)}")
    return plans


def _fetch_plan(
    plan: MatchMinuteTokenPlan,
    client: Any,
    fetch_window_fn: Callable[..., Any],
    *,
    transient_retries: int,
    transient_backoff_seconds: float,
) -> list[tuple[str, int, float]]:
    exact_start = plan.started_at.timestamp()
    exact_end = plan.finished_at.timestamp()
    padded_start = (math.floor(exact_start) // 60) * 60
    padded_end = math.ceil(math.ceil(exact_end) / 60) * 60
    rows = fetch_window_fn(
        client,
        plan.token_id,
        padded_start,
        padded_end,
        1,
        300,
        transient_retries,
        transient_backoff_seconds,
    )
    if rows is None:
        raise RuntimeError(f"Transient CLOB failure for token {plan.token_id}")
    filtered = [
        (str(token), int(timestamp), float(price))
        for token, timestamp, price in rows
        if exact_start <= int(timestamp) <= exact_end
    ]
    if not filtered:
        raise RuntimeError(f"Empty in-game CLOB history for token {plan.token_id}")
    if any(token != plan.token_id for token, _, _ in filtered):
        raise ValueError(f"CLOB returned a mismatched token for {plan.token_id}")
    if any(not 0.0 <= price <= 1.0 for _, _, price in filtered):
        raise ValueError(f"CLOB returned an invalid probability for {plan.token_id}")
    return filtered


def sync_match_minute_odds_history(
    conn: duckdb.DuckDBPyConnection,
    *,
    log: Any = logger,
    workers: int = 20,
    requests_per_second: int = 20,
    transient_retries: int = 2,
    transient_backoff_seconds: float = 0.25,
    progress_log_interval_seconds: int = 60,
    no_progress_soft_timeout_seconds: int | None = 900,
    no_progress_hard_timeout_seconds: int | None = 2700,
    client_factory: Callable[[], Any] | None = None,
    fetch_window_fn: Callable[..., Any] = fetch_window_with_auto_split,
    persist_fn: Callable[..., Any] = load_match_minute_odds_history_stage,
) -> dict[str, int]:
    """Refetch all bounded windows, then publish only after every token succeeds."""
    plans = select_match_minute_token_plans(conn)
    limiter = RateLimiter(requests_per_second)
    worker_state = local()

    def client() -> Any:
        value = getattr(worker_state, "client", None)
        if value is None:
            value = (
                client_factory()
                if client_factory
                else build_client(CLOB_API_URL, rate_limiter=limiter)
            )
            worker_state.client = value
        return value

    def fetch(plan: MatchMinuteTokenPlan):
        return _fetch_plan(
            plan,
            client(),
            fetch_window_fn,
            transient_retries=transient_retries,
            transient_backoff_seconds=transient_backoff_seconds,
        )

    guardrail = ProgressGuardrail(
        asset="polymarket_wc2026_match_minute_odds_backfill",
        logger=log,
        progress_log_interval_seconds=progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=no_progress_hard_timeout_seconds,
        work_log_interval=25,
    )
    fetched: list[tuple[MatchMinuteTokenPlan, list[tuple[str, int, float]]]] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(fetch, plan): plan for plan in plans}
        try:
            for future in as_completed(futures):
                plan = futures[future]
                fetched.append((plan, future.result()))
                guardrail.record_progress(
                    phase="fetch_token",
                    diagnostics={"token_id": plan.token_id},
                )
        except Exception:
            for future in futures:
                future.cancel()
            raise

    ingested_at = datetime.now(timezone.utc)
    rows = [
        {
            "market_id": plan.market_id,
            "clobTokenId": token_id,
            "timestamp": timestamp,
            "price": price,
            "fidelity_minutes": 1,
            "window_start_at": plan.started_at,
            "window_end_at": plan.finished_at,
            "ingested_at": ingested_at,
        }
        for plan, history in fetched
        for token_id, timestamp, price in history
    ]
    persist_fn(rows, conn)
    return {
        "games": EXPECTED_GAMES,
        "markets": EXPECTED_MARKETS,
        "tokens": len(fetched),
        "rows": len(rows),
    }


__all__ = [
    "EXPECTED_GAMES",
    "EXPECTED_GROUP_MARKETS",
    "EXPECTED_MARKETS",
    "EXPECTED_TOKENS",
    "MatchMinuteTokenPlan",
    "select_match_minute_token_plans",
    "sync_match_minute_odds_history",
]

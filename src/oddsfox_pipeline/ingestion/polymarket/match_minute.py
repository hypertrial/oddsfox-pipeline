"""Bounded one-minute CLOB history for completed WC2026 matches."""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from uuid import uuid4

import duckdb

from oddsfox_pipeline.config.settings import CLOB_API_URL
from oddsfox_pipeline.ingestion.polymarket.odds.execution import (
    fetch_group_window_with_auto_split,
    fetch_window_with_auto_split,
)
from oddsfox_pipeline.ingestion.polymarket.odds.minute_batch import (
    DEFAULT_MINUTE_AUTO_TUNE_MAX_RPS,
    DEFAULT_MINUTE_BATCH_GROUP_SIZE,
    DEFAULT_MINUTE_REQUESTS_PER_SECOND,
    DEFAULT_MINUTE_WINDOW_HOURS,
    DEFAULT_MINUTE_WORKERS,
    MinuteFetchResult,
    borrow_duckdb_connection,
    call_minute_persist,
    cleanup_minute_odds_publish_cache,
    fetch_and_write_minute_history_parquet_shards,
    resolve_minute_token_reuse,
    sample_minute_market_plans,
    synthesize_reused_minute_fetch_results,
)
from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    load_match_minute_fetch_audit,
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


@dataclass(frozen=True)
class MatchMinuteFetchResult:
    plan: MatchMinuteTokenPlan
    fetch_status: str
    history: tuple[tuple[str, int, float], ...]
    request_start_epoch: int
    request_end_epoch: int
    source_row_count: int
    in_game_history_sha256: str | None
    fetch_started_at: datetime
    fetch_finished_at: datetime
    error_type: str | None = None
    error_message: str | None = None
    history_row_count: int = 0


class MatchMinuteSyncError(RuntimeError):
    """Match-minute failure carrying metrics suitable for run observability."""

    def __init__(self, message: str, summary: dict[str, Any]):
        super().__init__(message)
        self.summary = summary


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


def _to_match_fetch_result(result: MinuteFetchResult) -> MatchMinuteFetchResult:
    plan = result.plan
    if not isinstance(plan, MatchMinuteTokenPlan):
        plan = MatchMinuteTokenPlan(
            market_id=plan.market_id,
            token_id=plan.token_id,
            started_at=plan.started_at,
            finished_at=plan.finished_at,
        )
    return MatchMinuteFetchResult(
        plan=plan,
        fetch_status=result.fetch_status,
        history=result.history,
        request_start_epoch=result.request_start_epoch,
        request_end_epoch=result.request_end_epoch,
        source_row_count=result.source_row_count,
        in_game_history_sha256=result.history_sha256,
        fetch_started_at=result.fetch_started_at,
        fetch_finished_at=result.fetch_finished_at,
        error_type=result.error_type,
        error_message=result.error_message,
        history_row_count=result.history_row_count,
    )


def sync_match_minute_odds_history(
    conn: duckdb.DuckDBPyConnection | None = None,
    *,
    connection_factory: Callable[..., Any] | None = None,
    log: Any = logger,
    workers: int = DEFAULT_MINUTE_WORKERS,
    requests_per_second: int = DEFAULT_MINUTE_REQUESTS_PER_SECOND,
    batch_group_size: int = DEFAULT_MINUTE_BATCH_GROUP_SIZE,
    window_hours: int = DEFAULT_MINUTE_WINDOW_HOURS,
    auto_tune_rps: bool = True,
    auto_tune_max_rps: int | None = DEFAULT_MINUTE_AUTO_TUNE_MAX_RPS,
    transient_retries: int = 2,
    transient_backoff_seconds: float = 0.25,
    progress_log_interval_seconds: int = 60,
    no_progress_soft_timeout_seconds: int | None = 900,
    no_progress_hard_timeout_seconds: int | None = 2700,
    progress_poll_seconds: int = 5,
    client_factory: Callable[[], Any] | None = None,
    fetch_window_fn: Callable[..., Any] = fetch_window_with_auto_split,
    fetch_group_window_fn: Callable[..., Any] = fetch_group_window_with_auto_split,
    persist_fn: Callable[..., Any] = load_match_minute_odds_history_stage,
    audit_persist_fn: Callable[..., Any] = load_match_minute_fetch_audit,
    market_sample_fraction: float | None = None,
    market_sample_seed: str | None = None,
) -> dict[str, Any]:
    """Refetch all bounded windows, then publish only after every token succeeds.

    Pass ``connection_factory`` (for example ``get_connection``) so DuckDB is
    borrowed only for plan selection and publish, not during CLOB fetch.
    Optional ``market_sample_fraction`` keeps the full 104/248/496 inventory
    validation, then deterministically samples markets before CLOB fetch.
    """
    with borrow_duckdb_connection(
        conn, connection_factory=connection_factory
    ) as active:
        plans = select_match_minute_token_plans(active)
    sample_manifest: dict[str, Any] | None = None
    if market_sample_fraction is not None:
        if market_sample_seed is None or not str(market_sample_seed).strip():
            raise ValueError("market_sample_seed is required when sampling markets")
        plans, sample_manifest = sample_minute_market_plans(
            plans,
            fraction=float(market_sample_fraction),
            seed=str(market_sample_seed),
        )
        log.info(
            "Match-minute sampling %s/%s markets (%s/%s tokens) fraction=%s seed=%s",
            sample_manifest["selected_markets"],
            sample_manifest["population_markets"],
            sample_manifest["selected_tokens"],
            sample_manifest["population_tokens"],
            sample_manifest["sample_fraction"],
            sample_manifest["sample_seed"],
        )
    fetch_run_id = str(uuid4())
    with borrow_duckdb_connection(
        conn, connection_factory=connection_factory
    ) as active:
        _previous, reuse_ids, published_windows = resolve_minute_token_reuse(
            plans,
            leg="match",
            conn=active,
        )
    reuse_plans = [plan for plan in plans if plan.token_id in reuse_ids]
    fetch_plans = [plan for plan in plans if plan.token_id not in reuse_ids]
    if reuse_plans:
        log.info(
            "Match-minute reusing %s/%s token(s) from prior snapshot; fetching %s",
            len(reuse_plans),
            len(plans),
            len(fetch_plans),
        )
    fetched = [
        _to_match_fetch_result(result)
        for result in synthesize_reused_minute_fetch_results(
            reuse_plans,
            published_windows=published_windows,
        )
    ]
    shard_paths: list[Path] = []
    fetch_metrics: dict[str, int] = {}
    if fetch_plans:
        fresh, shard_paths, fetch_metrics = (
            fetch_and_write_minute_history_parquet_shards(
                fetch_plans,
                fetch_run_id=fetch_run_id,
                ingested_at=datetime.now(timezone.utc),
                asset_name="polymarket_wc2026_match_minute_odds_backfill",
                log=log,
                workers=workers,
                requests_per_second=requests_per_second,
                batch_group_size=batch_group_size,
                window_hours=window_hours,
                auto_tune_rps=auto_tune_rps,
                auto_tune_max_rps=auto_tune_max_rps,
                transient_retries=transient_retries,
                transient_backoff_seconds=transient_backoff_seconds,
                progress_log_interval_seconds=progress_log_interval_seconds,
                no_progress_soft_timeout_seconds=no_progress_soft_timeout_seconds,
                no_progress_hard_timeout_seconds=no_progress_hard_timeout_seconds,
                progress_poll_seconds=progress_poll_seconds,
                client_factory=client_factory,
                fetch_window_fn=fetch_window_fn,
                fetch_group_window_fn=fetch_group_window_fn,
                empty_error_message_fn=(
                    lambda p: f"Empty in-game CLOB history for token {p.token_id}"
                ),
            )
        )
        fetched.extend(_to_match_fetch_result(result) for result in fresh)
    audit_rows = [
        {
            "fetch_run_id": fetch_run_id,
            "market_id": result.plan.market_id,
            "clobTokenId": result.plan.token_id,
            "fetch_status": result.fetch_status,
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": result.plan.started_at,
            "exact_window_end_at": result.plan.finished_at,
            "request_start_epoch": result.request_start_epoch,
            "request_end_epoch": result.request_end_epoch,
            "source_row_count": result.source_row_count,
            "in_game_row_count": (
                result.history_row_count
                if result.plan.token_id not in reuse_ids
                else int(result.source_row_count)
            ),
            "in_game_history_sha256": result.in_game_history_sha256,
            "source_endpoint": f"{CLOB_API_URL.rstrip('/')}/prices-history",
            "fetch_started_at": result.fetch_started_at,
            "fetch_finished_at": result.fetch_finished_at,
            "error_type": result.error_type,
            "error_message": result.error_message,
        }
        for result in fetched
    ]

    status_counts = {
        status: sum(result.fetch_status == status for result in fetched)
        for status in ("success", "empty", "error", "cancelled")
    }
    summary: dict[str, Any] = {
        "status": "fetched",
        "fetch_run_id": fetch_run_id,
        "games": EXPECTED_GAMES,
        "markets": len({result.plan.market_id for result in fetched}),
        "tokens": len(fetched),
        **{f"{status}_tokens": count for status, count in status_counts.items()},
        "rows": sum(result.history_row_count for result in fetched),
        **fetch_metrics,
    }
    if sample_manifest is not None:
        summary.update(sample_manifest)
    failures = [result for result in fetched if result.fetch_status != "success"]

    with borrow_duckdb_connection(
        conn, connection_factory=connection_factory
    ) as active:
        try:
            audit_persist_fn(audit_rows, active)
        except Exception as exc:
            summary.update(status="audit_error", error_type=exc.__class__.__name__)
            cleanup_minute_odds_publish_cache(fetch_run_id)
            raise MatchMinuteSyncError(str(exc), summary) from exc

        if failures:
            first = failures[0]
            summary["status"] = "fetch_failed"
            cleanup_minute_odds_publish_cache(fetch_run_id)
            raise MatchMinuteSyncError(
                first.error_message or "CLOB fetch failed", summary
            )

    try:
        published_tokens = len(fetched)
        with borrow_duckdb_connection(
            conn, connection_factory=connection_factory
        ) as active:
            try:
                call_minute_persist(
                    persist_fn,
                    shard_paths,
                    active,
                    fetch_run_id=fetch_run_id,
                    reuse_token_ids=reuse_ids,
                )
            except Exception as exc:
                summary.update(
                    status="publish_error", error_type=exc.__class__.__name__
                )
                raise MatchMinuteSyncError(str(exc), summary) from exc
    finally:
        cleanup_minute_odds_publish_cache(fetch_run_id)
    summary["status"] = "published"
    summary["raw_published_tokens"] = published_tokens
    summary["reused_tokens"] = len(reuse_ids)
    return summary


__all__ = [
    "EXPECTED_GAMES",
    "EXPECTED_GROUP_MARKETS",
    "EXPECTED_MARKETS",
    "EXPECTED_TOKENS",
    "MatchMinuteFetchResult",
    "MatchMinuteSyncError",
    "MatchMinuteTokenPlan",
    "select_match_minute_token_plans",
    "sync_match_minute_odds_history",
]

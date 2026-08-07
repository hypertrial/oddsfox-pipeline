"""Bounded one-minute CLOB history for WC2026 futures (non-match) markets."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import uuid4

import duckdb

from oddsfox_pipeline.config.settings import (
    CLOB_API_URL,
    POLYMARKET_WC2026_TOURNAMENT_END_UTC,
    POLYMARKET_WC2026_TOURNAMENT_START_UTC,
)
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
    execute_minute_fetches,
)
from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    load_futures_minute_fetch_audit,
    load_futures_minute_odds_history_stage,
)

logger = logging.getLogger(__name__)

# Match-level sports types are owned by match_minute; everything else is futures.
_MATCH_SPORTS_MARKET_TYPES = frozenset({"moneyline", "soccer_team_to_advance"})


@dataclass(frozen=True)
class FuturesMinuteTokenPlan:
    market_id: str
    token_id: str
    started_at: datetime
    finished_at: datetime


@dataclass(frozen=True)
class FuturesMinuteFetchResult:
    plan: FuturesMinuteTokenPlan
    fetch_status: str
    history: tuple[tuple[str, int, float], ...]
    request_start_epoch: int
    request_end_epoch: int
    source_row_count: int
    window_history_sha256: str | None
    fetch_started_at: datetime
    fetch_finished_at: datetime
    error_type: str | None = None
    error_message: str | None = None


class FuturesMinuteSyncError(RuntimeError):
    """Futures-minute failure carrying metrics suitable for run observability."""

    def __init__(self, message: str, summary: dict[str, Any]):
        super().__init__(message)
        self.summary = summary


def _json_list(value: Any) -> list[str]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc)


def _parse_contract_bound(raw: str) -> datetime:
    return datetime.fromisoformat(str(raw).strip().replace("Z", "+00:00")).astimezone(
        timezone.utc
    )


def tournament_window_bounds(
    *,
    tournament_start_utc: str = POLYMARKET_WC2026_TOURNAMENT_START_UTC,
    tournament_end_utc: str = POLYMARKET_WC2026_TOURNAMENT_END_UTC,
) -> tuple[datetime, datetime]:
    start = _parse_contract_bound(tournament_start_utc)
    end = _parse_contract_bound(tournament_end_utc)
    if end <= start:
        raise ValueError(
            f"Invalid tournament window: start={start.isoformat()} end={end.isoformat()}"
        )
    return start, end


def resolve_futures_token_window(
    *,
    created_at: datetime | None,
    end_date: datetime | None,
    tournament_start: datetime,
    tournament_end: datetime,
) -> tuple[datetime, datetime] | None:
    """Return the inclusive minute window for one futures market, or None if empty."""
    start = tournament_start
    if created_at is not None:
        start = max(start, _utc(created_at))
    end = tournament_end
    if end_date is not None:
        end = min(end, _utc(end_date))
    if end <= start:
        return None
    return start, end


def select_futures_minute_token_plans(
    conn: duckdb.DuckDBPyConnection,
    *,
    tournament_start_utc: str = POLYMARKET_WC2026_TOURNAMENT_START_UTC,
    tournament_end_utc: str = POLYMARKET_WC2026_TOURNAMENT_END_UTC,
) -> list[FuturesMinuteTokenPlan]:
    """Select registry-eligible WC2026 markets that are not match working-set types."""
    tournament_start, tournament_end = tournament_window_bounds(
        tournament_start_utc=tournament_start_utc,
        tournament_end_utc=tournament_end_utc,
    )
    cursor = conn.execute(
        """
        SELECT
            m.id AS market_id,
            m.clob_token_ids,
            m.created_at,
            m.end_date,
            m.sports_market_type
        FROM polymarket_wc2026_raw.markets AS m
        INNER JOIN polymarket_wc2026_ops.market_scope_registry AS r
            ON m.id = r.market_id
        WHERE lower(r.scope_name) = 'wc2026'
          AND coalesce(r.is_event_volume_eligible, FALSE)
          AND (
              m.sports_market_type IS NULL
              OR lower(m.sports_market_type) NOT IN ('moneyline', 'soccer_team_to_advance')
          )
        ORDER BY m.id
        """
    )
    columns = [item[0] for item in cursor.description]
    rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
    if not rows:
        raise ValueError(
            "No registry-eligible WC2026 futures markets found for minute backfill"
        )

    plans: list[FuturesMinuteTokenPlan] = []
    seen_tokens: set[str] = set()
    skipped_empty_window = 0
    for row in rows:
        sports_type = (row.get("sports_market_type") or "").strip().casefold()
        if sports_type in _MATCH_SPORTS_MARKET_TYPES:
            continue
        window = resolve_futures_token_window(
            created_at=row.get("created_at"),
            end_date=row.get("end_date"),
            tournament_start=tournament_start,
            tournament_end=tournament_end,
        )
        if window is None:
            skipped_empty_window += 1
            continue
        started_at, finished_at = window
        tokens = _json_list(row.get("clob_token_ids"))
        if len(tokens) < 1 or len(set(tokens)) != len(tokens):
            raise ValueError(
                f"Futures market {row['market_id']} must map distinct CLOB tokens"
            )
        for token_id in tokens:
            if token_id in seen_tokens:
                raise ValueError(f"Token {token_id} maps to more than one market")
            seen_tokens.add(token_id)
            plans.append(
                FuturesMinuteTokenPlan(
                    market_id=str(row["market_id"]),
                    token_id=token_id,
                    started_at=started_at,
                    finished_at=finished_at,
                )
            )

    if not plans:
        raise ValueError(
            "No futures-minute token plans after window filtering "
            f"(markets={len(rows)}, empty_windows={skipped_empty_window})"
        )
    return plans


def _to_futures_fetch_result(result: MinuteFetchResult) -> FuturesMinuteFetchResult:
    plan = result.plan
    if not isinstance(plan, FuturesMinuteTokenPlan):
        plan = FuturesMinuteTokenPlan(
            market_id=plan.market_id,
            token_id=plan.token_id,
            started_at=plan.started_at,
            finished_at=plan.finished_at,
        )
    return FuturesMinuteFetchResult(
        plan=plan,
        fetch_status=result.fetch_status,
        history=result.history,
        request_start_epoch=result.request_start_epoch,
        request_end_epoch=result.request_end_epoch,
        source_row_count=result.source_row_count,
        window_history_sha256=result.history_sha256,
        fetch_started_at=result.fetch_started_at,
        fetch_finished_at=result.fetch_finished_at,
        error_type=result.error_type,
        error_message=result.error_message,
    )


def sync_futures_minute_odds_history(
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
    persist_fn: Callable[..., Any] = load_futures_minute_odds_history_stage,
    audit_persist_fn: Callable[..., Any] = load_futures_minute_fetch_audit,
    tournament_start_utc: str = POLYMARKET_WC2026_TOURNAMENT_START_UTC,
    tournament_end_utc: str = POLYMARKET_WC2026_TOURNAMENT_END_UTC,
) -> dict[str, Any]:
    """Refetch all futures windows; empty history is audited and skipped on publish.

    Illiquid futures tokens often return no in-window CLOB points. Treat ``empty``
    as a non-blocking audit outcome. Fail closed only on ``error`` / ``cancelled``,
    or when zero tokens succeed.

    Pass ``connection_factory`` (for example ``get_connection``) so DuckDB is
    borrowed only for plan selection and publish, not during CLOB fetch.
    """
    with borrow_duckdb_connection(
        conn, connection_factory=connection_factory
    ) as active:
        plans = select_futures_minute_token_plans(
            active,
            tournament_start_utc=tournament_start_utc,
            tournament_end_utc=tournament_end_utc,
        )
    fetch_run_id = str(uuid4())
    fetched = [
        _to_futures_fetch_result(result)
        for result in execute_minute_fetches(
            plans,
            asset_name="polymarket_wc2026_minute_odds_backfill",
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
                lambda p: f"Empty in-window CLOB history for token {p.token_id}"
            ),
        )
    ]
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
            "window_row_count": len(result.history),
            "window_history_sha256": result.window_history_sha256,
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
        "markets": len({result.plan.market_id for result in fetched}),
        "tokens": len(fetched),
        **{f"{status}_tokens": count for status, count in status_counts.items()},
        "rows": sum(len(result.history) for result in fetched),
    }

    hard_failures = [
        result
        for result in fetched
        if result.fetch_status in {"error", "cancelled"}
    ]
    success = [result for result in fetched if result.fetch_status == "success"]

    log.info(
        "Futures CLOB fetch done; entering DuckDB audit/publish "
        "(success=%s empty=%s error=%s cancelled=%s rows=%s fetch_run_id=%s)",
        status_counts["success"],
        status_counts["empty"],
        status_counts["error"],
        status_counts["cancelled"],
        summary["rows"],
        fetch_run_id,
    )

    with borrow_duckdb_connection(
        conn, connection_factory=connection_factory
    ) as active:
        log.info(
            "Futures-minute writing fetch audit (%s token(s)) to DuckDB",
            len(audit_rows),
        )
        try:
            audit_persist_fn(audit_rows, active)
        except Exception as exc:
            summary.update(status="audit_error", error_type=exc.__class__.__name__)
            raise FuturesMinuteSyncError(str(exc), summary) from exc

        if hard_failures:
            first = hard_failures[0]
            summary["status"] = "fetch_failed"
            raise FuturesMinuteSyncError(
                first.error_message or "CLOB fetch failed", summary
            )

        if not success:
            summary["status"] = "fetch_failed"
            raise FuturesMinuteSyncError(
                "No successful futures-minute CLOB history to publish", summary
            )
        if status_counts["empty"]:
            log.info(
                "Futures-minute empty in-window history for %s token(s); publishing %s",
                status_counts["empty"],
                len(success),
            )

        ingested_at = datetime.now(timezone.utc)
        rows = [
            {
                "market_id": result.plan.market_id,
                "clobTokenId": token_id,
                "timestamp": timestamp,
                "price": price,
                "fidelity_minutes": 1,
                "window_start_at": result.plan.started_at,
                "window_end_at": result.plan.finished_at,
                "ingested_at": ingested_at,
            }
            for result in success
            for token_id, timestamp, price in result.history
        ]
        log.info(
            "Futures-minute staging/publishing %s token(s) (%s rows) to DuckDB",
            len(success),
            len(rows),
        )
        try:
            persist_fn(rows, active, fetch_run_id=fetch_run_id)
        except Exception as exc:
            summary.update(status="publish_error", error_type=exc.__class__.__name__)
            raise FuturesMinuteSyncError(str(exc), summary) from exc
    summary["status"] = "published"
    summary["raw_published_tokens"] = len(success)
    log.info(
        "Futures-minute published %s token(s) (%s rows) to DuckDB",
        len(success),
        len(rows),
    )
    return summary


__all__ = [
    "FuturesMinuteFetchResult",
    "FuturesMinuteSyncError",
    "FuturesMinuteTokenPlan",
    "resolve_futures_token_window",
    "select_futures_minute_token_plans",
    "sync_futures_minute_odds_history",
    "tournament_window_bounds",
]

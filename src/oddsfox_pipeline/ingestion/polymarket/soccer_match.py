"""Strict match-result registry and minute plans for Polymarket soccer."""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable
from uuid import uuid4

import duckdb

from oddsfox_pipeline.config.settings import CLOB_API_URL
from oddsfox_pipeline.ingestion.polymarket.event_catalog import (
    SOCCER_TAG_CREATED_AT,
)
from oddsfox_pipeline.ingestion.polymarket.match_minute import MatchMinuteTokenPlan
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
    borrow_duckdb_connection,
    call_minute_persist,
    cleanup_minute_odds_publish_cache,
    fetch_and_write_minute_history_parquet_shards,
    resolve_minute_token_reuse,
)
from oddsfox_pipeline.naming import SCOPE_SOCCER
from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    load_match_minute_fetch_audit,
    load_match_minute_odds_history_stage,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_polymarket_tables,
)

_EVENT_TEAMS = re.compile(
    r"^(?:[^:]+:\s*)?(?P<home>[^:]+?)\s+vs\.?\s+(?P<away>.+?)\s*$",
    re.I,
)
_BEAT = re.compile(r"^will\s+(?P<winner>.+?)\s+beat\s+(?P<loser>.+?)\??$", re.I)
_DRAW = re.compile(
    r"^will\s+(?P<home>.+?)\s+vs\.?\s+(?P<away>.+?)\s+end\s+in\s+a\s+draw\??$",
    re.I,
)
_ROLES = ("home_win", "draw", "away_win")


@dataclass(frozen=True)
class SoccerRegistryResult:
    rows: tuple[dict[str, Any], ...]
    exclusions: tuple[dict[str, Any], ...]


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc)


def _db_timestamp(value: datetime) -> datetime:
    """Return a naive UTC value for DuckDB's timezone-less TIMESTAMP columns."""
    return _utc(value).replace(tzinfo=None)


def _key(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore")
    return re.sub(r"[^a-z0-9]", "", ascii_value.decode().casefold())


def _json_list(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value]


def _event_teams(title: str) -> tuple[str, str] | None:
    match = _EVENT_TEAMS.search(title.strip())
    if match is None:
        return None
    home = match.group("home").strip()
    away = match.group("away").strip()
    if not _key(home) or not _key(away) or _key(home) == _key(away):
        return None
    return home, away


def _market_role(
    market: dict[str, Any], *, home_team: str, away_team: str
) -> str | None:
    home_key, away_key = _key(home_team), _key(away_team)
    structured = (
        str(market.get("sports_market_type") or "").strip().casefold() == "moneyline"
    )
    label = str(market.get("group_item_title") or "").strip()
    if structured and label:
        label_key = _key(label)
        if label.casefold() == "draw":
            return "draw"
        if label_key == home_key:
            return "home_win"
        if label_key == away_key:
            return "away_win"
    question = str(market.get("question") or "").strip()
    draw = _DRAW.fullmatch(question)
    if draw and {_key(draw.group("home")), _key(draw.group("away"))} == {
        home_key,
        away_key,
    }:
        return "draw"
    beat = _BEAT.fullmatch(question)
    if beat and {_key(beat.group("winner")), _key(beat.group("loser"))} == {
        home_key,
        away_key,
    }:
        return "home_win" if _key(beat.group("winner")) == home_key else "away_win"
    return None


def _tokens(market: dict[str, Any]) -> tuple[str, str] | None:
    outcomes = [item.casefold() for item in _json_list(market.get("outcomes"))]
    tokens = _json_list(market.get("clob_token_ids"))
    if len(outcomes) != 2 or len(tokens) != 2 or set(outcomes) != {"yes", "no"}:
        return None
    if not all(tokens) or tokens[0] == tokens[1]:
        return None
    return tokens[outcomes.index("yes")], tokens[outcomes.index("no")]


def _registry_for_event(
    event: dict[str, Any],
    markets: Iterable[dict[str, Any]],
    *,
    refreshed_at: datetime,
) -> tuple[list[dict[str, Any]], str | None]:
    teams = _event_teams(str(event.get("event_title") or ""))
    if teams is None:
        return [], "unparseable_event_teams"
    home_team, away_team = teams
    by_role: dict[str, tuple[dict[str, Any], tuple[str, str]]] = {}
    kickoff_values: list[datetime] = []
    for market in markets:
        role = _market_role(market, home_team=home_team, away_team=away_team)
        token_pair = _tokens(market)
        if role is None or token_pair is None:
            continue
        sports_type = str(market.get("sports_market_type") or "").casefold()
        if sports_type and sports_type != "moneyline":
            continue
        if role in by_role:
            return [], f"duplicate_{role}_market"
        by_role[role] = (market, token_pair)
        kickoff = market.get("game_start_time")
        if isinstance(kickoff, datetime):
            kickoff_values.append(_utc(kickoff))
    if set(by_role) != set(_ROLES):
        return [], "incomplete_match_result_markets"
    market_ids = [str(market.get("market_id") or "") for market, _ in by_role.values()]
    if not all(market_ids) or len(set(market_ids)) != 3:
        return [], "duplicate_or_missing_market_id"
    all_tokens = [token for _, pair in by_role.values() for token in pair]
    if len(all_tokens) != len(set(all_tokens)):
        return [], "duplicate_clob_token"
    event_start = event.get("event_start_at")
    if len(kickoff_values) == 3 and len(set(kickoff_values)) == 1:
        started_at = kickoff_values[0]
        kickoff_source = "market_game_start_time"
    elif not kickoff_values and isinstance(event_start, datetime):
        started_at = _utc(event_start)
        kickoff_source = "event_start_time"
    else:
        return [], "missing_or_inconsistent_kickoff"

    explicit_finish = event.get("finished_at")
    cap = started_at + timedelta(hours=5)
    explicit_finish_valid = (
        isinstance(explicit_finish, datetime) and _utc(explicit_finish) >= started_at
    )
    if explicit_finish_valid:
        finished_at = _utc(explicit_finish)
        timing_status = "explicit_finish"
        timing_confidence = "high"
    else:
        closure_candidates = [event.get("closed_at")]
        closure_candidates.extend(
            market.get(field)
            for market, _ in by_role.values()
            for field in ("event_finished_time", "end_date")
        )
        valid_closures = sorted(
            _utc(value)
            for value in closure_candidates
            if isinstance(value, datetime) and _utc(value) > started_at
        )
    if not explicit_finish_valid and valid_closures:
        finished_at = min(valid_closures[0], cap)
        timing_status = "inferred_closure"
        timing_confidence = "medium"
    elif not explicit_finish_valid:
        finished_at = cap
        timing_status = "inferred_five_hour_cap"
        timing_confidence = "low"

    coverage_tier = event.get("coverage_tier")
    if coverage_tier not in {"guaranteed_tag_era", "pre_tag_best_effort"}:
        created_at = event.get("created_at")
        boundary = datetime.fromisoformat(SOCCER_TAG_CREATED_AT.replace("Z", "+00:00"))
        coverage_tier = (
            "guaranteed_tag_era"
            if isinstance(created_at, datetime) and _utc(created_at) >= boundary
            else "pre_tag_best_effort"
        )
    rows: list[dict[str, Any]] = []
    for role in _ROLES:
        market, (yes_token, no_token) = by_role[role]
        rows.append(
            {
                "event_id": str(event["event_id"]),
                "market_id": str(market["market_id"]),
                "result_role": role,
                "home_team": home_team,
                "away_team": away_team,
                "yes_token_id": yes_token,
                "no_token_id": no_token,
                "window_start_at": started_at,
                "window_end_at": finished_at,
                "kickoff_source": kickoff_source,
                "timing_status": timing_status,
                "timing_confidence": timing_confidence,
                "coverage_tier": coverage_tier,
                "refreshed_at": refreshed_at,
            }
        )
    return rows, None


def build_soccer_match_result_registry(
    events: Iterable[dict[str, Any]],
    markets_by_event: dict[str, list[dict[str, Any]]],
    *,
    refreshed_at: datetime | None = None,
) -> SoccerRegistryResult:
    captured_at = refreshed_at or datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    for event in events:
        event_id = str(event.get("event_id") or "").strip()
        event_rows, reason = _registry_for_event(
            event,
            markets_by_event.get(event_id, []),
            refreshed_at=captured_at,
        )
        if reason is None:
            rows.extend(event_rows)
        else:
            exclusions.append(
                {
                    "event_id": event_id,
                    "event_title": event.get("event_title"),
                    "exclusion_reason": reason,
                    "refreshed_at": captured_at,
                }
            )
    return SoccerRegistryResult(tuple(rows), tuple(exclusions))


def refresh_soccer_match_result_registry(
    conn: duckdb.DuckDBPyConnection,
) -> dict[str, int]:
    """Rebuild the current strict registry from the latest catalog observation."""
    bootstrap_polymarket_tables(conn, scope_name=SCOPE_SOCCER)
    events_table = polymarket_raw_tbl(SCOPE_SOCCER, "events")
    markets_table = polymarket_raw_tbl(SCOPE_SOCCER, "markets")
    events_cursor = conn.execute(
        f"""
        SELECT * FROM {events_table} ORDER BY event_id
        """
    )
    event_columns = [item[0] for item in events_cursor.description]
    events = [
        dict(zip(event_columns, row, strict=True)) for row in events_cursor.fetchall()
    ]
    markets_cursor = conn.execute(
        f"""
        SELECT id AS market_id, * EXCLUDE (id)
        FROM {markets_table}
        ORDER BY event_id, id
        """
    )
    market_columns = [item[0] for item in markets_cursor.description]
    markets_by_event: dict[str, list[dict[str, Any]]] = {}
    for row in markets_cursor.fetchall():
        market = dict(zip(market_columns, row, strict=True))
        event_id = str(market.get("event_id") or "")
        markets_by_event.setdefault(event_id, []).append(market)
    result = build_soccer_match_result_registry(events, markets_by_event)
    registry = polymarket_ops_tbl(SCOPE_SOCCER, "match_result_registry")
    exclusions = polymarket_ops_tbl(SCOPE_SOCCER, "match_result_registry_exclusions")
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(f"DELETE FROM {registry}")
        conn.execute(f"DELETE FROM {exclusions}")
        if result.rows:
            conn.executemany(
                f"INSERT INTO {registry} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    tuple(
                        _db_timestamp(value) if isinstance(value, datetime) else value
                        for value in row.values()
                    )
                    for row in result.rows
                ],
            )
        if result.exclusions:
            conn.executemany(
                f"INSERT INTO {exclusions} VALUES (?, ?, ?, ?)",
                [
                    tuple(
                        _db_timestamp(value) if isinstance(value, datetime) else value
                        for value in row.values()
                    )
                    for row in result.exclusions
                ],
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {
        "events": len(events),
        "matches": len(result.rows) // 3,
        "markets": len(result.rows),
        "excluded_events": len(result.exclusions),
    }


def select_soccer_match_minute_token_plans(
    conn: duckdb.DuckDBPyConnection,
    *,
    completion_grace_minutes: int = 60,
    now: datetime | None = None,
    game_sample_size: int | None = None,
) -> list[MatchMinuteTokenPlan]:
    cutoff = _utc(now or datetime.now(timezone.utc)) - timedelta(
        minutes=max(0, int(completion_grace_minutes))
    )
    registry = polymarket_ops_tbl(SCOPE_SOCCER, "match_result_registry")
    rows = conn.execute(
        f"""
        SELECT event_id, market_id, yes_token_id, no_token_id,
               window_start_at, window_end_at
        FROM {registry}
        WHERE window_end_at <= ?
        ORDER BY event_id, result_role
        """,
        [cutoff],
    ).fetchall()
    if game_sample_size is not None:
        event_ids = list(dict.fromkeys(str(row[0]) for row in rows))
        if len(event_ids) > game_sample_size:
            indexes = {
                round(index * (len(event_ids) - 1) / (game_sample_size - 1))
                for index in range(game_sample_size)
            }
            sampled_event_ids = {event_ids[index] for index in indexes}
            rows = [row for row in rows if str(row[0]) in sampled_event_ids]
    plans: list[MatchMinuteTokenPlan] = []
    for _, market_id, yes_token, no_token, started_at, finished_at in rows:
        for token_id in (yes_token, no_token):
            plans.append(
                MatchMinuteTokenPlan(
                    market_id=str(market_id),
                    token_id=str(token_id),
                    started_at=_utc(started_at),
                    finished_at=_utc(finished_at),
                )
            )
    return plans


def _terminal_empty_token_ids(
    conn: duckdb.DuckDBPyConnection,
    plans: Iterable[MatchMinuteTokenPlan],
    *,
    empty_retry_hours: int,
    now: datetime,
) -> set[str]:
    audit = polymarket_ops_tbl(SCOPE_SOCCER, "match_minute_odds_fetch_audit")
    terminal = polymarket_ops_tbl(
        SCOPE_SOCCER, "match_minute_odds_terminal_unavailable"
    )
    latest = {
        (str(row[0]), _utc(row[1]), _utc(row[2])): (str(row[3]), str(row[4]))
        for row in conn.execute(
            f"""
            SELECT "clobTokenId", exact_window_start_at,
                   exact_window_end_at, fetch_status, market_id
            FROM {audit}
            QUALIFY row_number() OVER (
                PARTITION BY "clobTokenId", exact_window_start_at, exact_window_end_at
                ORDER BY fetch_finished_at DESC, fetch_run_id DESC
            ) = 1
            """
        ).fetchall()
    }
    retry_delta = timedelta(hours=max(0, int(empty_retry_hours)))
    terminal_plans = [
        plan
        for plan in plans
        if latest.get((plan.token_id, plan.started_at, plan.finished_at), (None, None))[
            0
        ]
        == "empty"
        and now >= plan.finished_at + retry_delta
    ]
    if terminal_plans:
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {terminal} VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    latest[(plan.token_id, plan.started_at, plan.finished_at)][1],
                    plan.token_id,
                    _db_timestamp(plan.started_at),
                    _db_timestamp(plan.finished_at),
                    max(0, int(empty_retry_hours)),
                    _db_timestamp(now),
                )
                for plan in terminal_plans
            ],
        )
    return {plan.token_id for plan in terminal_plans}


def sync_soccer_match_minute_odds_history(
    conn: duckdb.DuckDBPyConnection | None = None,
    *,
    connection_factory: Callable[..., Any] | None = None,
    log: Any,
    workers: int = DEFAULT_MINUTE_WORKERS,
    requests_per_second: int = DEFAULT_MINUTE_REQUESTS_PER_SECOND,
    batch_group_size: int = DEFAULT_MINUTE_BATCH_GROUP_SIZE,
    window_hours: int = DEFAULT_MINUTE_WINDOW_HOURS,
    auto_tune_rps: bool = True,
    auto_tune_max_rps: int | None = DEFAULT_MINUTE_AUTO_TUNE_MAX_RPS,
    transient_retries: int = 2,
    transient_backoff_seconds: float = 0.25,
    completion_grace_minutes: int = 60,
    empty_retry_hours: int = 72,
    force: bool = False,
    game_sample_size: int | None = None,
    client_factory: Callable[[], Any] | None = None,
    fetch_window_fn: Callable[..., Any] = fetch_window_with_auto_split,
    fetch_group_window_fn: Callable[..., Any] = fetch_group_window_with_auto_split,
) -> dict[str, Any]:
    """Incrementally publish successful soccer match-result token windows."""
    now = datetime.now(timezone.utc)
    with borrow_duckdb_connection(
        conn, connection_factory=connection_factory
    ) as active:
        plans = select_soccer_match_minute_token_plans(
            active,
            completion_grace_minutes=completion_grace_minutes,
            now=now,
            game_sample_size=game_sample_size,
        )
        terminal_empty = (
            set()
            if force
            else _terminal_empty_token_ids(
                active, plans, empty_retry_hours=empty_retry_hours, now=now
            )
        )
        _previous, reuse_ids, published_windows = resolve_minute_token_reuse(
            plans,
            leg="match",
            conn=active,
            scope_name=SCOPE_SOCCER,
        )
    if force:
        reuse_ids = set()
    plans = [plan for plan in plans if plan.token_id not in terminal_empty]
    if not plans:
        return {
            "status": "no_op",
            "tokens": 0,
            "terminal_empty_tokens": len(terminal_empty),
            "attempted_tokens": 0,
            "raw_published_tokens": 0,
            "reused_tokens": 0,
            "audit_amplification": 0.0,
            "max_inflight_futures": 0,
            "peak_buffered_rows": 0,
            "spilled_rows": 0,
            "shard_count": 0,
        }
    reuse_plans = [plan for plan in plans if plan.token_id in reuse_ids]
    fetch_plans = [plan for plan in plans if plan.token_id not in reuse_ids]
    if not fetch_plans:
        return {
            "status": "no_op",
            "tokens": len(plans),
            "reused_tokens": len(reuse_plans),
            "terminal_empty_tokens": len(terminal_empty),
            "attempted_tokens": 0,
            "raw_published_tokens": len(reuse_plans),
            "audit_amplification": 0.0,
            "max_inflight_futures": 0,
            "peak_buffered_rows": 0,
            "spilled_rows": 0,
            "shard_count": 0,
        }

    fetch_run_id = str(uuid4())
    del published_windows
    fetched, shard_paths, fetch_metrics = fetch_and_write_minute_history_parquet_shards(
        fetch_plans,
        fetch_run_id=fetch_run_id,
        ingested_at=datetime.now(timezone.utc),
        asset_name="polymarket_soccer_match_result_minute_odds_ingest",
        log=log,
        workers=workers,
        requests_per_second=requests_per_second,
        batch_group_size=batch_group_size,
        window_hours=window_hours,
        auto_tune_rps=auto_tune_rps,
        auto_tune_max_rps=auto_tune_max_rps,
        transient_retries=transient_retries,
        transient_backoff_seconds=transient_backoff_seconds,
        client_factory=client_factory,
        fetch_window_fn=fetch_window_fn,
        fetch_group_window_fn=fetch_group_window_fn,
        empty_error_message_fn=(
            lambda p: f"Empty in-game CLOB history for token {p.token_id}"
        ),
    )
    audit_rows = [
        {
            "fetch_run_id": fetch_run_id,
            "market_id": result.plan.market_id,
            "clobTokenId": result.plan.token_id,
            "fetch_status": result.fetch_status,
            "raw_published": False,
            "fidelity_minutes": 1,
            "exact_window_start_at": _db_timestamp(result.plan.started_at),
            "exact_window_end_at": _db_timestamp(result.plan.finished_at),
            "request_start_epoch": result.request_start_epoch,
            "request_end_epoch": result.request_end_epoch,
            "source_row_count": result.source_row_count,
            "in_game_row_count": result.history_row_count,
            "in_game_history_sha256": result.history_sha256,
            "source_endpoint": f"{CLOB_API_URL.rstrip('/')}/prices-history",
            "fetch_started_at": _db_timestamp(result.fetch_started_at),
            "fetch_finished_at": _db_timestamp(result.fetch_finished_at),
            "error_type": result.error_type,
            "error_message": result.error_message,
        }
        for result in fetched
    ]
    try:
        with borrow_duckdb_connection(
            conn, connection_factory=connection_factory
        ) as active:
            load_match_minute_fetch_audit(audit_rows, active, scope_name=SCOPE_SOCCER)
    except Exception:
        cleanup_minute_odds_publish_cache(fetch_run_id)
        raise

    successes = [result for result in fetched if result.fetch_status == "success"]
    hard_failures = [
        result for result in fetched if result.fetch_status in {"error", "cancelled"}
    ]
    if not successes and hard_failures:
        cleanup_minute_odds_publish_cache(fetch_run_id)
        raise RuntimeError("All due soccer minute-history token fetches failed")

    try:
        if successes or reuse_ids:

            def persist(paths, active, **kwargs):
                return load_match_minute_odds_history_stage(
                    paths,
                    active,
                    scope_name=SCOPE_SOCCER,
                    audit_mode="success_only",
                    **kwargs,
                )

            with borrow_duckdb_connection(
                conn, connection_factory=connection_factory
            ) as active:
                call_minute_persist(
                    persist,
                    shard_paths,
                    active,
                    fetch_run_id=fetch_run_id,
                    reuse_token_ids=set(reuse_ids),
                )
    finally:
        cleanup_minute_odds_publish_cache(fetch_run_id)

    counts = {
        status: sum(result.fetch_status == status for result in fetched)
        for status in ("success", "empty", "error", "cancelled")
    }
    partial = any(counts[status] for status in ("empty", "error", "cancelled"))
    return {
        "status": "partial" if partial else "published",
        "fetch_run_id": fetch_run_id,
        "matches": len({plan.market_id for plan in plans}) // 3,
        "markets": len({plan.market_id for plan in plans}),
        "tokens": len(plans),
        "attempted_tokens": len(fetched),
        **{f"{status}_tokens": value for status, value in counts.items()},
        "raw_published_tokens": len(successes) + len(reuse_plans),
        "reused_tokens": len(reuse_plans),
        "terminal_empty_tokens": len(terminal_empty),
        "audit_amplification": len(fetched) / max(len(fetch_plans), 1),
        **fetch_metrics,
    }


__all__ = [
    "SoccerRegistryResult",
    "build_soccer_match_result_registry",
    "refresh_soccer_match_result_registry",
    "select_soccer_match_minute_token_plans",
    "sync_soccer_match_minute_odds_history",
]

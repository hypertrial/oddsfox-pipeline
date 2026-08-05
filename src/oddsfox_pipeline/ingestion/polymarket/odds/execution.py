from __future__ import annotations

from datetime import datetime, timedelta, timezone
from queue import Queue
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from oddsfox_pipeline.ingestion.polymarket.odds.fetch import (
    BadRequestError,
    PermanentAPIError,
    fetch_batch_token_history_with_retry,
    fetch_token_history_with_retry,
)
from oddsfox_pipeline.ingestion.polymarket.odds.support import (
    DEFAULT_EMPTY_RETRY_BASE_HOURS,
    DEFAULT_EMPTY_RETRY_MAX_HOURS,
    DEFAULT_ERROR_RETRY_MINUTES,
    DEFAULT_ROUTINE_INTERVAL_HOURS,
    DEFAULT_TRANSIENT_BACKOFF_SECONDS,
    DEFAULT_TRANSIENT_RETRIES,
    GroupPlan,
    InflightGroupFuture,
    InflightTokenFuture,
    TokenPlan,
)
from oddsfox_pipeline.resources.http import RateLimiter


def iter_windows(start_ts: int, end_ts: int, window_seconds: int):
    cursor = start_ts
    while cursor < end_ts:
        next_ts = min(end_ts, cursor + window_seconds)
        yield cursor, next_ts
        cursor = next_ts


def default_rate_limiter_factory(rps: int | None):
    if not rps:
        return None
    return RateLimiter(rps)


def checked_at_from_plan(plan: TokenPlan) -> datetime:
    return datetime.fromtimestamp(int(plan.end_ts), tz=timezone.utc)


def empty_retry_next_check(
    checked_at: datetime,
    *,
    empty_run_streak: int,
    base_seconds: int,
    max_seconds: int,
) -> datetime:
    multiplier = max(0, int(empty_run_streak) - 1)
    delay_seconds = max(0, int(base_seconds)) * (2**multiplier)
    if max_seconds > 0:
        delay_seconds = min(delay_seconds, max_seconds)
    return checked_at + timedelta(seconds=delay_seconds)


def is_interval_too_long_error(exc: BadRequestError) -> bool:
    body = getattr(exc, "body", "") or ""
    message = str(exc)
    return "interval is too long" in f"{body} {message}".lower()


def fetch_window_with_auto_split(
    client,
    token_id: str,
    start_ts: int,
    end_ts: int,
    fidelity: int,
    min_window_seconds: int,
    transient_retries: int = DEFAULT_TRANSIENT_RETRIES,
    transient_backoff_seconds: float = DEFAULT_TRANSIENT_BACKOFF_SECONDS,
    status_hook: Callable[[int], None] | None = None,
    fetch_token_history_fn: Callable[..., object] = fetch_token_history_with_retry,
) -> Optional[List[Tuple[str, int, float]]]:
    stack: List[Tuple[int, int]] = [(start_ts, end_ts)]
    out: List[Tuple[str, int, float]] = []
    while stack:
        s_ts, e_ts = stack.pop()
        if s_ts >= e_ts:
            continue
        try:
            chunk = fetch_token_history_fn(
                client,
                token_id,
                start_ts=s_ts,
                end_ts=e_ts,
                fidelity=fidelity,
                now_ts=e_ts,
                transient_retries=transient_retries,
                transient_backoff_base_seconds=transient_backoff_seconds,
                status_hook=status_hook,
            )
        except BadRequestError as exc:
            span = e_ts - s_ts
            if is_interval_too_long_error(exc) and span > min_window_seconds:
                mid = s_ts + (span // 2)
                stack.append((mid, e_ts))
                stack.append((s_ts, mid))
                continue
            raise
        if chunk is None:
            return None
        out.extend(chunk)
    return out


def _fallback_group_window_per_token(
    client,
    token_ids: Sequence[str],
    start_ts: int,
    end_ts: int,
    fidelity: int,
    min_window_seconds: int,
    transient_retries: int,
    transient_backoff_seconds: float,
    status_hook: Callable[[int], None] | None,
    fetch_token_history_fn: Callable[..., object],
) -> Dict[str, List[Tuple[str, int, float]] | None | Exception]:
    """Isolate PermanentAPIError by falling back to per-token fetches."""
    out: Dict[str, List[Tuple[str, int, float]] | None | Exception] = {}
    for token_id in token_ids:
        try:
            chunk = fetch_window_with_auto_split(
                client,
                token_id,
                start_ts,
                end_ts,
                fidelity,
                min_window_seconds,
                transient_retries,
                transient_backoff_seconds,
                status_hook,
                fetch_token_history_fn=fetch_token_history_fn,
            )
            out[token_id] = chunk
        except (BadRequestError, PermanentAPIError) as exc:
            out[token_id] = exc
    return out


def fetch_group_window_with_auto_split(
    client,
    token_ids: Sequence[str],
    start_ts: int,
    end_ts: int,
    fidelity: int,
    min_window_seconds: int,
    transient_retries: int = DEFAULT_TRANSIENT_RETRIES,
    transient_backoff_seconds: float = DEFAULT_TRANSIENT_BACKOFF_SECONDS,
    status_hook: Callable[[int], None] | None = None,
    fetch_batch_token_history_fn: Callable[
        ..., object
    ] = fetch_batch_token_history_with_retry,
    fetch_token_history_fn: Callable[..., object] = fetch_token_history_with_retry,
) -> Dict[str, List[Tuple[str, int, float]] | None | Exception]:
    """
    Fetch one window for many tokens via batch API with auto-split.

    Returns a map token_id -> records | None (transient) | Exception (permanent).
    """
    markets = [str(token_id) for token_id in token_ids if token_id]
    if not markets:
        return {}
    stack: List[Tuple[int, int]] = [(start_ts, end_ts)]
    accumulated: Dict[str, List[Tuple[str, int, float]]] = {
        token_id: [] for token_id in markets
    }
    permanent: Dict[str, Exception] = {}
    transient: set[str] = set()
    active = set(markets)

    def _apply_per_token_fallback(
        window_ids: Sequence[str], s_ts: int, e_ts: int
    ) -> None:
        fallback = _fallback_group_window_per_token(
            client,
            window_ids,
            s_ts,
            e_ts,
            fidelity,
            min_window_seconds,
            transient_retries,
            transient_backoff_seconds,
            status_hook,
            fetch_token_history_fn,
        )
        for token_id, value in fallback.items():
            if isinstance(value, Exception):
                permanent[token_id] = value
                active.discard(token_id)
            elif value is None:
                transient.add(token_id)
                active.discard(token_id)
            else:
                accumulated[token_id].extend(value)

    while stack and active:
        s_ts, e_ts = stack.pop()
        if s_ts >= e_ts:
            continue
        active_markets = [token_id for token_id in markets if token_id in active]
        if not active_markets:
            break
        try:
            chunk_map = fetch_batch_token_history_fn(
                client,
                active_markets,
                start_ts=s_ts,
                end_ts=e_ts,
                fidelity=fidelity,
                now_ts=e_ts,
                transient_retries=transient_retries,
                transient_backoff_base_seconds=transient_backoff_seconds,
                status_hook=status_hook,
            )
        except BadRequestError as exc:
            span = e_ts - s_ts
            if is_interval_too_long_error(exc) and span > min_window_seconds:
                mid = s_ts + (span // 2)
                stack.append((mid, e_ts))
                stack.append((s_ts, mid))
                continue
            # Non-splitable 400 can be one bad market in the batch; isolate.
            _apply_per_token_fallback(active_markets, s_ts, e_ts)
            continue
        except PermanentAPIError:
            _apply_per_token_fallback(active_markets, s_ts, e_ts)
            continue
        if chunk_map is None:
            for token_id in active_markets:
                transient.add(token_id)
                active.discard(token_id)
            continue
        if not isinstance(chunk_map, dict):
            for token_id in active_markets:
                transient.add(token_id)
                active.discard(token_id)
            continue
        for token_id in active_markets:
            records = chunk_map.get(token_id) or []
            accumulated[token_id].extend(records)

    out: Dict[str, List[Tuple[str, int, float]] | None | Exception] = {}
    for token_id in markets:
        if token_id in permanent:
            out[token_id] = permanent[token_id]
        elif token_id in transient:
            out[token_id] = None
        else:
            out[token_id] = accumulated[token_id]
    return out


def _finalize_token_result(
    *,
    plan: TokenPlan,
    checked_at: datetime,
    rows_fetched: int,
    windows_processed: int,
    had_transient_error: bool,
    max_seen_ts: int,
    max_contiguous_seen_ts: int,
    contiguous_checked_until_ts: int,
    routine_interval_seconds: int,
    empty_retry_base_seconds: int,
    empty_retry_max_seconds: int,
    error_retry_seconds: int,
    permanent_error: bool = False,
) -> tuple[Dict[str, int | bool], tuple]:
    if permanent_error:
        state_row = (plan.token_id, plan.end_ts, checked_at, None, 0, False)
        return (
            {
                "rows": rows_fetched,
                "windows": windows_processed,
                "empty": rows_fetched == 0,
                "error": 1,
                "permanent_error": 1,
                "fully_checked": False,
            },
            state_row,
        )
    if had_transient_error:
        if max_contiguous_seen_ts > plan.start_ts:
            cursor_ts: int | None = max_contiguous_seen_ts
        elif rows_fetched == 0 and contiguous_checked_until_ts > plan.start_ts:
            cursor_ts = contiguous_checked_until_ts
        else:
            cursor_ts = None
    elif rows_fetched > 0:
        cursor_ts = max_seen_ts
    else:
        cursor_ts = plan.end_ts
    fully_checked = bool(plan.is_closed and not had_transient_error)
    if had_transient_error:
        next_check_at = checked_at + timedelta(seconds=max(0, error_retry_seconds))
        empty_run_streak = 0
    elif rows_fetched > 0:
        next_check_at = (
            None
            if fully_checked
            else checked_at + timedelta(seconds=max(0, routine_interval_seconds))
        )
        empty_run_streak = 0
    else:
        empty_run_streak = int(plan.empty_run_streak) + 1
        next_check_at = (
            None
            if fully_checked
            else empty_retry_next_check(
                checked_at,
                empty_run_streak=empty_run_streak,
                base_seconds=empty_retry_base_seconds,
                max_seconds=empty_retry_max_seconds,
            )
        )
    state_row = (
        plan.token_id,
        cursor_ts,
        checked_at,
        next_check_at,
        empty_run_streak,
        fully_checked,
    )
    return (
        {
            "rows": rows_fetched,
            "windows": windows_processed,
            "empty": rows_fetched == 0,
            "error": 1 if had_transient_error else 0,
            "permanent_error": 0,
            "fully_checked": fully_checked,
        },
        state_row,
    )


def sync_token_plan(
    plan: TokenPlan,
    client,
    write_queue: Queue,
    window_seconds: int,
    writer_chunk_rows: int,
    min_split_window_seconds: int,
    routine_interval_seconds: int = DEFAULT_ROUTINE_INTERVAL_HOURS * 3600,
    empty_retry_base_seconds: int = DEFAULT_EMPTY_RETRY_BASE_HOURS * 3600,
    empty_retry_max_seconds: int = DEFAULT_EMPTY_RETRY_MAX_HOURS * 3600,
    error_retry_seconds: int = DEFAULT_ERROR_RETRY_MINUTES * 60,
    transient_retries: int = DEFAULT_TRANSIENT_RETRIES,
    transient_backoff_seconds: float = DEFAULT_TRANSIENT_BACKOFF_SECONDS,
    status_hook: Callable[[int], None] | None = None,
    fetch_window_fn: Callable[..., object] = fetch_window_with_auto_split,
) -> Dict[str, int | bool]:
    client = client() if callable(client) else client
    token_id = plan.token_id
    checked_at = checked_at_from_plan(plan)
    rows_fetched = 0
    windows_processed = 0
    had_transient_error = False
    max_seen_ts = plan.start_ts
    max_contiguous_seen_ts = plan.start_ts
    contiguous_windows_ok = True
    contiguous_checked_until_ts = plan.start_ts
    odds_buffer: List[Tuple[str, int, float]] = []
    for window_start, window_end in iter_windows(
        plan.start_ts, plan.end_ts, window_seconds
    ):
        windows_processed += 1
        try:
            chunk = fetch_window_fn(
                client,
                token_id,
                window_start,
                window_end,
                plan.fidelity,
                min_split_window_seconds,
                transient_retries,
                transient_backoff_seconds,
                status_hook,
            )
        except (BadRequestError, PermanentAPIError) as exc:
            reason = str(exc)
            write_queue.put(("skipped_tokens", [(token_id, reason)]))
            write_queue.put(
                ("token_state", [(token_id, plan.end_ts, checked_at, None, 0, False)])
            )
            return {
                "rows": rows_fetched,
                "windows": windows_processed,
                "empty": rows_fetched == 0,
                "error": 1,
                "permanent_error": 1,
                "fully_checked": False,
            }
        if chunk is None:
            had_transient_error = True
            contiguous_windows_ok = False
            continue
        if contiguous_windows_ok:
            contiguous_checked_until_ts = window_end
        if not chunk:
            continue
        rows_fetched += len(chunk)
        window_max_ts = max(ts for _, ts, _ in chunk)
        max_seen_ts = max(max_seen_ts, window_max_ts)
        if contiguous_windows_ok:
            max_contiguous_seen_ts = max(max_contiguous_seen_ts, window_max_ts)
        odds_buffer.extend(chunk)
        if len(odds_buffer) >= writer_chunk_rows:
            write_queue.put(("odds", odds_buffer))
            odds_buffer = []
    if odds_buffer:
        write_queue.put(("odds", odds_buffer))
    result, state_row = _finalize_token_result(
        plan=plan,
        checked_at=checked_at,
        rows_fetched=rows_fetched,
        windows_processed=windows_processed,
        had_transient_error=had_transient_error,
        max_seen_ts=max_seen_ts,
        max_contiguous_seen_ts=max_contiguous_seen_ts,
        contiguous_checked_until_ts=contiguous_checked_until_ts,
        routine_interval_seconds=routine_interval_seconds,
        empty_retry_base_seconds=empty_retry_base_seconds,
        empty_retry_max_seconds=empty_retry_max_seconds,
        error_retry_seconds=error_retry_seconds,
    )
    write_queue.put(("token_state", [state_row]))
    return result


def sync_token_group_plan(
    group: GroupPlan,
    client,
    write_queue: Queue,
    window_seconds: int,
    writer_chunk_rows: int,
    min_split_window_seconds: int,
    routine_interval_seconds: int = DEFAULT_ROUTINE_INTERVAL_HOURS * 3600,
    empty_retry_base_seconds: int = DEFAULT_EMPTY_RETRY_BASE_HOURS * 3600,
    empty_retry_max_seconds: int = DEFAULT_EMPTY_RETRY_MAX_HOURS * 3600,
    error_retry_seconds: int = DEFAULT_ERROR_RETRY_MINUTES * 60,
    transient_retries: int = DEFAULT_TRANSIENT_RETRIES,
    transient_backoff_seconds: float = DEFAULT_TRANSIENT_BACKOFF_SECONDS,
    status_hook: Callable[[int], None] | None = None,
    fetch_group_window_fn: Callable[..., object] = fetch_group_window_with_auto_split,
) -> Dict[str, Dict[str, int | bool]]:
    """Sync a GroupPlan via batch CLOB fetch; returns per-token result dicts."""
    client = client() if callable(client) else client
    plans_by_id = {plan.token_id: plan for plan in group.token_plans}
    token_ids = list(plans_by_id)
    checked_at_by_id = {
        token_id: checked_at_from_plan(plan) for token_id, plan in plans_by_id.items()
    }
    rows_by_id = {token_id: 0 for token_id in token_ids}
    windows_by_id = {token_id: 0 for token_id in token_ids}
    transient_by_id = {token_id: False for token_id in token_ids}
    permanent_by_id: Dict[str, Exception | None] = {
        token_id: None for token_id in token_ids
    }
    max_seen_by_id = {token_id: plan.start_ts for token_id, plan in plans_by_id.items()}
    max_contig_by_id = {
        token_id: plan.start_ts for token_id, plan in plans_by_id.items()
    }
    contig_ok_by_id = {token_id: True for token_id in token_ids}
    contig_until_by_id = {
        token_id: plan.start_ts for token_id, plan in plans_by_id.items()
    }
    odds_buffer: List[Tuple[str, int, float]] = []
    active_ids = set(token_ids)

    for window_start, window_end in iter_windows(
        group.group_start_ts, group.group_end_ts, window_seconds
    ):
        window_ids = [
            token_id
            for token_id in token_ids
            if token_id in active_ids
            and plans_by_id[token_id].start_ts < window_end
            and plans_by_id[token_id].end_ts > window_start
        ]
        if not window_ids:
            continue
        for token_id in window_ids:
            windows_by_id[token_id] += 1
        try:
            chunk_map = fetch_group_window_fn(
                client,
                window_ids,
                window_start,
                window_end,
                group.fidelity,
                min_split_window_seconds,
                transient_retries,
                transient_backoff_seconds,
                status_hook,
            )
        except BadRequestError as exc:
            # Non-splitable batch BadRequest: mark remaining active tokens permanent.
            for token_id in list(active_ids):
                permanent_by_id[token_id] = exc
                active_ids.discard(token_id)
            break

        if not isinstance(chunk_map, dict):
            for token_id in window_ids:
                transient_by_id[token_id] = True
                contig_ok_by_id[token_id] = False
            continue

        for token_id in window_ids:
            value = chunk_map.get(token_id)
            if isinstance(value, Exception):
                permanent_by_id[token_id] = value
                active_ids.discard(token_id)
                continue
            if value is None:
                transient_by_id[token_id] = True
                contig_ok_by_id[token_id] = False
                continue
            plan = plans_by_id[token_id]
            filtered = [
                (tid, ts, price)
                for tid, ts, price in value
                if plan.start_ts <= int(ts) <= plan.end_ts
            ]
            if contig_ok_by_id[token_id]:
                contig_until_by_id[token_id] = min(window_end, plan.end_ts)
            if not filtered:
                continue
            rows_by_id[token_id] += len(filtered)
            window_max_ts = max(int(ts) for _, ts, _ in filtered)
            max_seen_by_id[token_id] = max(max_seen_by_id[token_id], window_max_ts)
            if contig_ok_by_id[token_id]:
                max_contig_by_id[token_id] = max(
                    max_contig_by_id[token_id], window_max_ts
                )
            odds_buffer.extend(filtered)
            if len(odds_buffer) >= writer_chunk_rows:
                write_queue.put(("odds", odds_buffer))
                odds_buffer = []

    if odds_buffer:
        write_queue.put(("odds", odds_buffer))

    results: Dict[str, Dict[str, int | bool]] = {}
    skip_rows: List[Tuple[str, str]] = []
    state_rows: List[tuple] = []
    for token_id, plan in plans_by_id.items():
        permanent = permanent_by_id.get(token_id)
        if permanent is not None:
            skip_rows.append((token_id, str(permanent)))
            result, state_row = _finalize_token_result(
                plan=plan,
                checked_at=checked_at_by_id[token_id],
                rows_fetched=rows_by_id[token_id],
                windows_processed=windows_by_id[token_id],
                had_transient_error=False,
                max_seen_ts=max_seen_by_id[token_id],
                max_contiguous_seen_ts=max_contig_by_id[token_id],
                contiguous_checked_until_ts=contig_until_by_id[token_id],
                routine_interval_seconds=routine_interval_seconds,
                empty_retry_base_seconds=empty_retry_base_seconds,
                empty_retry_max_seconds=empty_retry_max_seconds,
                error_retry_seconds=error_retry_seconds,
                permanent_error=True,
            )
        else:
            result, state_row = _finalize_token_result(
                plan=plan,
                checked_at=checked_at_by_id[token_id],
                rows_fetched=rows_by_id[token_id],
                windows_processed=windows_by_id[token_id],
                had_transient_error=transient_by_id[token_id],
                max_seen_ts=max_seen_by_id[token_id],
                max_contiguous_seen_ts=max_contig_by_id[token_id],
                contiguous_checked_until_ts=contig_until_by_id[token_id],
                routine_interval_seconds=routine_interval_seconds,
                empty_retry_base_seconds=empty_retry_base_seconds,
                empty_retry_max_seconds=empty_retry_max_seconds,
                error_retry_seconds=error_retry_seconds,
            )
        results[token_id] = result
        state_rows.append(state_row)
    if skip_rows:
        write_queue.put(("skipped_tokens", skip_rows))
    if state_rows:
        write_queue.put(("token_state", state_rows))
    return results


__all__ = [
    "InflightGroupFuture",
    "InflightTokenFuture",
    "checked_at_from_plan",
    "default_rate_limiter_factory",
    "empty_retry_next_check",
    "fetch_group_window_with_auto_split",
    "fetch_window_with_auto_split",
    "iter_windows",
    "sync_token_group_plan",
    "sync_token_plan",
]

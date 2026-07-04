from __future__ import annotations

from typing import List, Optional, Tuple

from oddsfox_pipeline.ingestion.polymarket import scope_sql

# ponytail: module-reference import (not `from ... import name`) so this file can
# be imported mid-load of `scope_sql` (which imports storage.duckdb.schemas.constants,
# which triggers this package's __init__ before scope_sql finishes defining its
# names). Attribute access below happens at call time, after both modules are fully
# loaded, so it sidesteps the circular-import ordering.
from oddsfox_pipeline.storage.duckdb.connection import (
    ensure_duck_db,
    get_connection,
    polymarket_wc2026_ops_tbl,
    polymarket_wc2026_raw_tbl,
)

_TAB_MARKETS = polymarket_wc2026_raw_tbl("markets")
_TAB_MARKET_TOKENS = polymarket_wc2026_raw_tbl("market_tokens")
_TAB_TOKEN_SYNC_LEDGER = polymarket_wc2026_ops_tbl("token_sync_ledger")
_TAB_TOKEN_SYNC_SKIPS = polymarket_wc2026_ops_tbl("token_sync_skips")
_TAB_MARKET_METADATA_UNRESOLVED = polymarket_wc2026_ops_tbl(
    "market_metadata_unresolved"
)


def _fetch_market_ids(base_query: str, limit: Optional[int] = None) -> List[str]:
    query = base_query
    if limit:
        query += f" LIMIT {int(limit)}"
    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
        return [row[0] for row in rows]


def _market_scope_where_clause(market_scope: str | None, alias: str = "m") -> str:
    return scope_sql.market_scope_sql(market_scope, alias)


def _ended_market_where_clause(
    ended_market_grace_days: int | None,
    alias: str = "m",
) -> str:
    if ended_market_grace_days is None:
        return ""
    days = max(0, int(ended_market_grace_days))
    return (
        f"AND NOT ({alias}.end_date IS NOT NULL "
        f"AND {alias}.end_date < CURRENT_TIMESTAMP - INTERVAL {days} DAY)"
    )


_DUE_TOKEN_JOIN_SQL = f"""
    FROM {_TAB_MARKET_TOKENS} mt
    JOIN {_TAB_MARKETS} m ON mt.market_id = m.id
    CROSS JOIN LATERAL json_each(mt.clobTokenIds) AS je
    LEFT JOIN {_TAB_TOKEN_SYNC_LEDGER} l
      ON l.clobTokenId = json_extract_string(je.value, '$')
    LEFT JOIN {_TAB_TOKEN_SYNC_SKIPS} s
      ON s.clobTokenId = json_extract_string(je.value, '$')
"""


def _due_token_base_where(
    cutoff_created_at: Optional[str],
    params: List,
) -> str:
    """Shared base WHERE predicate for due-token queries; appends to params in place."""
    predicates = [
        "mt.clobTokenIds IS NOT NULL",
        "mt.clobTokenIds != '[]'",
        "LEFT(LTRIM(mt.clobTokenIds), 1) = '['",
        "json_extract_string(je.value, '$') IS NOT NULL",
        "s.clobTokenId IS NULL",
        "NOT (COALESCE(m.closed, FALSE) = TRUE AND COALESCE(l.fully_checked, FALSE) = TRUE)",
        "(l.clobTokenId IS NULL OR l.next_check_at IS NULL OR l.next_check_at <= CURRENT_TIMESTAMP)",
    ]
    if cutoff_created_at:
        predicates.append("m.created_at >= ?")
        params.append(cutoff_created_at)
    return " AND ".join(predicates)


def _due_token_routine_filters(
    cutoff_created_at: Optional[str],
    params: List,
    *,
    market_scope: str = "wc2026",
    ended_market_grace_days: int | None = None,
    min_volume: float | None = None,
    max_volume: float | None = None,
    alias: str = "m",
) -> str:
    """Shared scope/volume/ended filters for due-token iterators and counts."""
    return (
        f"{_due_token_base_where(cutoff_created_at, params)}"
        f"{_market_scope_where_clause(market_scope, alias)}"
        f"{_volume_where_clause(min_volume, max_volume, alias)}"
        f"{_ended_market_where_clause(ended_market_grace_days, alias)}"
    )


def _validate_volume_bound(value: float | None, *, name: str) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number, got {value!r}") from exc
    if parsed < 0:
        raise ValueError(f"{name} must be >= 0, got {parsed}")
    return parsed


def _volume_where_clause(
    min_volume: float | None,
    max_volume: float | None,
    alias: str = "m",
) -> str:
    """Return SQL AND-clause fragments for market volume bounds (inlined literals)."""
    min_volume = _validate_volume_bound(min_volume, name="min_volume")
    max_volume = _validate_volume_bound(max_volume, name="max_volume")
    if min_volume is None and max_volume is None:
        return ""
    clauses: list[str] = []
    if min_volume is not None:
        clauses.append(f"COALESCE({alias}.volume, 0) >= {min_volume}")
    if max_volume is not None:
        clauses.append(f"COALESCE({alias}.volume, 0) < {max_volume}")
    return "AND " + " AND ".join(clauses)


def _missing_tokens_predicate(alias: str) -> str:
    return f"{alias}.id NOT IN (SELECT market_id FROM {_TAB_MARKET_TOKENS})"


def _missing_slug_predicate(alias: str) -> str:
    return f"({alias}.slug IS NULL OR {alias}.slug = '')"


def _missing_event_slug_predicate(alias: str) -> str:
    return f"""(
        ({alias}.event_slug IS NULL OR {alias}.event_slug = '')
        AND NOT EXISTS (
            SELECT 1
            FROM {_TAB_MARKET_METADATA_UNRESOLVED} u
            WHERE u.market_id = {alias}.id
              AND u.field_name = 'event_slug'
              AND u.next_retry_at > CURRENT_TIMESTAMP
        )
    )"""


def _missing_end_date_predicate(alias: str) -> str:
    return f"({alias}.end_date IS NULL OR CAST({alias}.end_date AS VARCHAR) = '')"


def get_market_count() -> int:
    ensure_duck_db()
    with get_connection() as conn:
        result = conn.execute(f"SELECT COUNT(*) FROM {_TAB_MARKETS}").fetchone()
        return int(result[0]) if result else 0


def get_all_market_ids() -> set:
    """
    Get all existing market IDs as a set for efficient lookup.
    Used for early stopping in incremental syncs.
    """
    ensure_duck_db()
    with get_connection() as conn:
        rows = conn.execute(f"SELECT id FROM {_TAB_MARKETS}").fetchall()
        return {row[0] for row in rows}


def get_markets_without_tokens(limit: Optional[int] = None) -> List[str]:
    ensure_duck_db()
    return _fetch_market_ids(
        f"""
        SELECT id
        FROM {_TAB_MARKETS}
        WHERE id NOT IN (SELECT market_id FROM {_TAB_MARKET_TOKENS})
    """,
        limit=limit,
    )


def get_markets_missing_any_metadata(
    *,
    include_tokens: bool = True,
    include_slugs: bool = True,
    include_event_slugs: bool = True,
    include_end_dates: bool = True,
    limit: Optional[int] = None,
    market_scope: str = "wc2026",
) -> List[str]:
    """Return market ids missing any requested metadata field."""
    ensure_duck_db()
    alias = "m"
    predicates: list[str] = []
    if include_tokens:
        predicates.append(_missing_tokens_predicate(alias))
    if include_slugs:
        predicates.append(_missing_slug_predicate(alias))
    if include_event_slugs:
        predicates.append(_missing_event_slug_predicate(alias))
    if include_end_dates:
        predicates.append(_missing_end_date_predicate(alias))
    if not predicates:
        return []
    scope_clause = _market_scope_where_clause(market_scope, alias)
    query = f"""
        SELECT {alias}.id
        FROM {_TAB_MARKETS} {alias}
        WHERE ({" OR ".join(predicates)})
        {scope_clause}
    """
    if limit:
        query += f" LIMIT {int(limit)}"
    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
        return [str(row[0]) for row in rows]


def get_markets_with_tokens() -> List[Tuple[str, str, Optional[str], Optional[bool]]]:
    """
    Get all markets that have associated tokens, including creation date and closed status.
    """
    ensure_duck_db()
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT mt.market_id, mt.clobTokenIds, m.created_at, m.closed
            FROM {_TAB_MARKET_TOKENS} mt
            JOIN {_TAB_MARKETS} m ON mt.market_id = m.id
            WHERE mt.clobTokenIds IS NOT NULL AND mt.clobTokenIds != '[]'
            """
        ).fetchall()
        return rows


def iter_markets_with_tokens(
    page_size: int = 5_000,
    *,
    cutoff_created_at: Optional[str] = None,
    json_array_only: bool = False,
    market_scope: str = "wc2026",
    ended_market_grace_days: int | None = None,
    min_volume: float | None = None,
    max_volume: float | None = None,
):
    """
    Stream markets that have token mappings in bounded pages.

    This avoids loading the full market-token corpus into memory for large syncs.
    """
    ensure_duck_db()
    query_parts = [
        f"""
        SELECT mt.market_id, mt.clobTokenIds, m.created_at, m.closed
        FROM {_TAB_MARKET_TOKENS} mt
        JOIN {_TAB_MARKETS} m ON mt.market_id = m.id
        WHERE mt.clobTokenIds IS NOT NULL AND mt.clobTokenIds != '[]'
        """
    ]
    params: List = []
    if cutoff_created_at:
        query_parts.append("AND m.created_at >= ?")
        params.append(cutoff_created_at)
    if json_array_only:
        query_parts.append("AND LEFT(LTRIM(mt.clobTokenIds), 1) = '['")
    query_parts.append(_market_scope_where_clause(market_scope, "m"))
    query_parts.append(_volume_where_clause(min_volume, max_volume, "m"))
    query_parts.append(_ended_market_where_clause(ended_market_grace_days, "m"))
    query = "\n".join(query_parts)
    with get_connection() as conn:
        cursor = conn.execute(query, params) if params else conn.execute(query)
        while True:
            rows = cursor.fetchmany(page_size)
            if not rows:
                break
            yield rows


def iter_due_market_tokens(
    page_size: int = 5_000,
    *,
    cutoff_created_at: Optional[str] = None,
    market_scope: str = "wc2026",
    ended_market_grace_days: int | None = None,
    min_volume: float | None = None,
    max_volume: float | None = None,
):
    """
    Stream only market-token pairs that are due for routine odds syncing.

    A token is due when it has no ledger row yet or its next_check_at is NULL / in
    the past. Persisted skip reasons and fully checked closed tokens are excluded
    here so routine runs do not revisit them.
    """
    ensure_duck_db()
    params: List = []
    query = f"""
        SELECT
            mt.market_id,
            json_extract_string(je.value, '$') AS clobTokenId,
            m.created_at,
            m.closed
        {_DUE_TOKEN_JOIN_SQL}
        WHERE {
        _due_token_routine_filters(
            cutoff_created_at,
            params,
            market_scope=market_scope,
            ended_market_grace_days=ended_market_grace_days,
            min_volume=min_volume,
            max_volume=max_volume,
        )
    }
    """
    with get_connection() as conn:
        cursor = conn.execute(query, params) if params else conn.execute(query)
        while True:
            rows = cursor.fetchmany(page_size)
            if not rows:
                break
            yield rows


def count_due_market_token_exclusions(
    *,
    cutoff_created_at: Optional[str] = None,
    market_scope: str = "wc2026",
    ended_market_grace_days: int | None = None,
    min_volume: float | None = None,
    max_volume: float | None = None,
) -> dict[str, int]:
    """Count due-token candidates skipped by routine scope/freshness filters."""
    scope_sql.validate_market_scope(market_scope)
    ensure_duck_db()
    params: List = []
    base_sql = _due_token_base_where(cutoff_created_at, params)
    volume_sql = _volume_where_clause(min_volume, max_volume, "m")
    scope_skip = 0
    ended_skip = 0
    with get_connection() as conn:
        predicate = scope_sql.market_scope_predicate_sql(market_scope, "m")
        row = conn.execute(
            f"""
            SELECT COUNT(*)
            {_DUE_TOKEN_JOIN_SQL}
            WHERE {base_sql}
              {volume_sql}
              AND NOT ({predicate})
            """,
            params,
        ).fetchone()
        scope_skip = int(row[0]) if row and row[0] is not None else 0
        if ended_market_grace_days is not None:
            days = max(0, int(ended_market_grace_days))
            row = conn.execute(
                f"""
                SELECT COUNT(*)
                {_DUE_TOKEN_JOIN_SQL}
                WHERE {base_sql}
                  {volume_sql}
                  AND ({predicate})
                  AND m.end_date IS NOT NULL
                  AND m.end_date < CURRENT_TIMESTAMP - INTERVAL {days} DAY
                """,
                params,
            ).fetchone()
            ended_skip = int(row[0]) if row and row[0] is not None else 0
    return {"scope_skip": scope_skip, "ended_market_skip": ended_skip}


def count_candidate_market_tokens(
    *,
    cutoff_created_at: Optional[str] = None,
    market_scope: str = "wc2026",
    ended_market_grace_days: int | None = None,
    due_only: bool = True,
    min_volume: float | None = None,
    max_volume: float | None = None,
) -> dict[str, int]:
    """Count market-token candidates for odds sync planning (approximate upper bound).

    When ``due_only`` is True, mirrors ``iter_due_market_tokens`` (ledger/skip filters).
    When False, mirrors ``iter_markets_with_tokens`` with ``json_array_only=True``.
    Per-token planner drops (recent_skip, invalid id, dup) are not reflected here.
    """
    scope_sql.validate_market_scope(market_scope)
    ensure_duck_db()
    params: List = []
    if due_only:
        query = f"""
            SELECT COUNT(*), COUNT(DISTINCT mt.market_id)
            {_DUE_TOKEN_JOIN_SQL}
            WHERE {
            _due_token_routine_filters(
                cutoff_created_at,
                params,
                market_scope=market_scope,
                ended_market_grace_days=ended_market_grace_days,
                min_volume=min_volume,
                max_volume=max_volume,
            )
        }
        """
    else:
        base_where = [
            "mt.clobTokenIds IS NOT NULL",
            "mt.clobTokenIds != '[]'",
            "LEFT(LTRIM(mt.clobTokenIds), 1) = '['",
        ]
        if cutoff_created_at:
            base_where.append("m.created_at >= ?")
            params.append(cutoff_created_at)
        query = f"""
            SELECT
                COALESCE(SUM(json_array_length(mt.clobTokenIds)), 0),
                COUNT(*)
            FROM {_TAB_MARKET_TOKENS} mt
            JOIN {_TAB_MARKETS} m ON mt.market_id = m.id
            WHERE {" AND ".join(base_where)}
            {_market_scope_where_clause(market_scope, "m")}
            {_volume_where_clause(min_volume, max_volume, "m")}
            {_ended_market_where_clause(ended_market_grace_days, "m")}
        """
    with get_connection() as conn:
        row = conn.execute(query, params).fetchone()
    candidate_tokens = int(row[0]) if row and row[0] is not None else 0
    candidate_markets = int(row[1]) if row and row[1] is not None else 0
    return {
        "candidate_tokens": candidate_tokens,
        "candidate_markets": candidate_markets,
    }


def get_markets_without_slugs(limit: Optional[int] = None) -> List[str]:
    ensure_duck_db()
    return _fetch_market_ids(
        f"""
        SELECT id
        FROM {_TAB_MARKETS}
        WHERE (slug IS NULL OR slug = '')
          AND TRY_CAST(id AS BIGINT) IS NOT NULL
          AND TRY_CAST(id AS BIGINT) > 0
    """,
        limit=limit,
    )


def get_markets_without_event_slugs(limit: Optional[int] = None) -> List[str]:
    ensure_duck_db()
    return _fetch_market_ids(
        f"""
        SELECT id
        FROM {_TAB_MARKETS}
        WHERE (event_slug IS NULL OR event_slug = '')
          AND TRY_CAST(id AS BIGINT) IS NOT NULL
          AND TRY_CAST(id AS BIGINT) > 0
        ORDER BY TRY_CAST(id AS BIGINT)
    """,
        limit=limit,
    )


def get_markets_without_end_date(limit: Optional[int] = None) -> List[str]:
    ensure_duck_db()
    return _fetch_market_ids(
        f"""
        SELECT id
        FROM {_TAB_MARKETS}
        WHERE (end_date IS NULL OR CAST(end_date AS VARCHAR) = '')
    """,
        limit=limit,
    )


__all__ = [
    "_fetch_market_ids",
    "_validate_volume_bound",
    "_volume_where_clause",
    "count_candidate_market_tokens",
    "count_due_market_token_exclusions",
    "get_all_market_ids",
    "get_market_count",
    "get_markets_missing_any_metadata",
    "get_markets_with_tokens",
    "get_markets_without_end_date",
    "get_markets_without_event_slugs",
    "get_markets_without_slugs",
    "get_markets_without_tokens",
    "iter_due_market_tokens",
    "iter_markets_with_tokens",
]

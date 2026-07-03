#!/usr/bin/env python3
"""Audit selected Polymarket scope: registry vs allowlist vs strict filter."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
for _rp in (REPO_ROOT, REPO_ROOT / "src"):
    if str(_rp) not in sys.path:
        sys.path.insert(0, str(_rp))

from oddsfox_pipeline.ingestion.polymarket.market_scope import (  # noqa: E402
    DEFAULT_MARKET_SCOPE,
    load_market_scope_config,
    market_scope_predicate_sql,
)
from oddsfox_pipeline.storage.duckdb.connection import (  # noqa: E402
    ensure_duck_db,
    get_connection,
)
from oddsfox_pipeline.storage.duckdb.market_scope_registry import (  # noqa: E402
    registry_market_count,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import (  # noqa: E402
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)


def _registry_by_source(conn, scope_name: str) -> dict[str, int]:
    tab = polymarket_ops_tbl("market_scope_registry")
    rows = conn.execute(
        f"""
        SELECT source, COUNT(*)::BIGINT
        FROM {tab}
        WHERE scope_name = ?
        GROUP BY source
        ORDER BY source
        """,
        [scope_name],
    ).fetchall()
    return {str(r[0]): int(r[1]) for r in rows}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scope-name",
        default=DEFAULT_MARKET_SCOPE,
        help=f"Configured market scope preset to audit (default: {DEFAULT_MARKET_SCOPE}).",
    )
    parser.add_argument(
        "--fail-on-allowlist-gaps",
        action="store_true",
        help="Exit 1 when markets have allowlisted event_slug but are not strict-scoped.",
    )
    parser.add_argument(
        "--fail-on-discovery-rows",
        action="store_true",
        help="Exit 1 when registry still has source=discovery rows (stale warehouse).",
    )
    args = parser.parse_args()

    ensure_duck_db()
    cfg = load_market_scope_config(scope_name=args.scope_name)
    m = polymarket_raw_tbl("markets")
    strict_sql = market_scope_predicate_sql(cfg.scope_name, "m")

    with get_connection() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM {m}").fetchone()[0]
        strict_n = conn.execute(
            f"SELECT COUNT(*) FROM {m} m WHERE {strict_sql}"
        ).fetchone()[0]
        slug_list = ", ".join(f"'{s}'" for s in cfg.event_slugs)
        gap_n = 0
        if slug_list:
            gap_n = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {m} m
                WHERE lower(coalesce(m.event_slug, '')) IN ({slug_list})
                  AND NOT ({strict_sql})
                """
            ).fetchone()[0]
        by_source = _registry_by_source(conn, cfg.scope_name)

    discovery_n = by_source.get("discovery", 0)

    print(f"Markets total: {total}")
    print(f"Scope name: {cfg.scope_name}")
    print(f"Registry rows: {registry_market_count(cfg.scope_name)}")
    print(f"Strict selected-scope markets: {strict_n}")
    print(f"Allowlisted event_slug not strict-scoped: {gap_n}")
    print(f"Registry by source: {by_source}")
    print(f"Configured event_slugs: {cfg.event_slugs}")
    print(f"Configured prefixes: {cfg.event_slug_prefixes}")

    exit_code = 0
    if args.fail_on_allowlist_gaps and gap_n:
        exit_code = 1
    if args.fail_on_discovery_rows and discovery_n:
        exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

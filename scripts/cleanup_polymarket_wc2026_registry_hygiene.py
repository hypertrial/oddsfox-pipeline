#!/usr/bin/env python3
"""Remove synthetic and ineligible API orphan rows from the WC2026 registry.

Dry-run by default. Pass ``--apply`` to delete. Stop Dagster/dbt writers first.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

ensure_src_on_path()

import duckdb  # noqa: E402

from oddsfox_pipeline.config import settings  # noqa: E402
from oddsfox_pipeline.storage.duckdb.schemas.constants import (  # noqa: E402
    polymarket_ops_tbl,
    polymarket_raw_tbl,
)

_SCOPE = "wc2026"
_REGISTRY = polymarket_ops_tbl(_SCOPE, "market_scope_registry")
_EVENT_SNAPSHOTS = polymarket_raw_tbl(_SCOPE, "event_snapshots")
_EVENT_MARKET_SNAPSHOTS = polymarket_raw_tbl(_SCOPE, "event_market_snapshots")
_SYNTHETIC_EVENT_IDS = ("evt-A", "evt-B")
_SYNTHETIC_MARKET_IDS = ("m-shared",)


def _scalar(
    conn: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None
) -> int:
    row = conn.execute(sql, params or []).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _counts(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    synthetic_events = ", ".join("?" for _ in _SYNTHETIC_EVENT_IDS)
    synthetic_markets = ", ".join("?" for _ in _SYNTHETIC_MARKET_IDS)
    return {
        "registry_synthetic": _scalar(
            conn,
            f"""
            SELECT COUNT(*) FROM {_REGISTRY}
            WHERE scope_name = ?
              AND (
                market_id IN ({synthetic_markets})
                OR event_id IN ({synthetic_events})
              )
            """,
            [_SCOPE, *_SYNTHETIC_MARKET_IDS, *_SYNTHETIC_EVENT_IDS],
        ),
        "event_market_synthetic": _scalar(
            conn,
            f"""
            SELECT COUNT(*) FROM {_EVENT_MARKET_SNAPSHOTS}
            WHERE event_id IN ({synthetic_events})
               OR market_id IN ({synthetic_markets})
            """,
            [*_SYNTHETIC_EVENT_IDS, *_SYNTHETIC_MARKET_IDS],
        ),
        "event_snapshots_synthetic": _scalar(
            conn,
            f"""
            SELECT COUNT(*) FROM {_EVENT_SNAPSHOTS}
            WHERE event_id IN ({synthetic_events})
            """,
            list(_SYNTHETIC_EVENT_IDS),
        ),
        "registry_ineligible_api": _scalar(
            conn,
            f"""
            SELECT COUNT(*) FROM {_REGISTRY}
            WHERE scope_name = ?
              AND source IN ('events_api', 'markets_api')
              AND NOT coalesce(is_event_volume_eligible, false)
            """,
            [_SCOPE],
        ),
    }


def cleanup_registry_hygiene(
    conn: duckdb.DuckDBPyConnection, *, apply: bool = False
) -> dict[str, int]:
    """Delete synthetic catalog contamination and ineligible API orphans."""
    before = _counts(conn)
    if not apply:
        return {f"would_delete_{key}": value for key, value in before.items()}

    synthetic_events = ", ".join("?" for _ in _SYNTHETIC_EVENT_IDS)
    synthetic_markets = ", ".join("?" for _ in _SYNTHETIC_MARKET_IDS)

    conn.execute(
        f"""
        DELETE FROM {_REGISTRY}
        WHERE scope_name = ?
          AND (
            market_id IN ({synthetic_markets})
            OR event_id IN ({synthetic_events})
          )
        """,
        [_SCOPE, *_SYNTHETIC_MARKET_IDS, *_SYNTHETIC_EVENT_IDS],
    )
    conn.execute(
        f"""
        DELETE FROM {_EVENT_MARKET_SNAPSHOTS}
        WHERE event_id IN ({synthetic_events})
           OR market_id IN ({synthetic_markets})
        """,
        [*_SYNTHETIC_EVENT_IDS, *_SYNTHETIC_MARKET_IDS],
    )
    conn.execute(
        f"""
        DELETE FROM {_EVENT_SNAPSHOTS}
        WHERE event_id IN ({synthetic_events})
        """,
        list(_SYNTHETIC_EVENT_IDS),
    )
    conn.execute(
        f"""
        DELETE FROM {_REGISTRY}
        WHERE scope_name = ?
          AND source IN ('events_api', 'markets_api')
          AND NOT coalesce(is_event_volume_eligible, false)
        """,
        [_SCOPE],
    )

    after = _counts(conn)
    return {
        **{f"deleted_{key}": before[key] for key in before},
        **{f"remaining_{key}": after[key] for key in after},
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb-path", type=Path, default=None)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Perform deletes (default is dry-run).",
    )
    args = parser.parse_args(argv)

    duck = Path(args.duckdb_path or settings.DUCKDB_PATH).resolve()
    if not duck.is_file():
        sys.stderr.write(f"Warehouse not found: {duck}\n")
        return 1

    # Writers must be stopped; open read-write only when applying.
    conn = duckdb.connect(str(duck), read_only=not args.apply)
    try:
        result = cleanup_registry_hygiene(conn, apply=args.apply)
    finally:
        conn.close()

    mode = "applied" if args.apply else "dry-run"
    print(f"cleanup_polymarket_wc2026_registry_hygiene ({mode}): {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

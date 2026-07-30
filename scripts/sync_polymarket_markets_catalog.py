#!/usr/bin/env python3
"""Sync every Gamma market with volume >= $100k into polymarket_catalog_raw.markets.

Uses ``GET /markets/keyset`` with ``volume_num_min`` and cursor pagination
(``next_cursor`` -> ``after_cursor``). That is the market-grain admission path:

- Filters on **market** volume (not event volume).
- Completes platform-wide without the /events offset fallback (offset >~2000
  returns 422).
- Omitting ``closed`` only returns open markets; ``closed=any`` runs both
  ``closed=false`` and ``closed=true`` passes and dedupes by market id.

Replaces the catalog raw table each run.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Final

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

REPO_ROOT: Final[Path] = ensure_src_on_path()

from oddsfox_pipeline.ingestion.polymarket.dlt_source import (  # noqa: E402
    normalize_market_payloads_for_dlt,
)
from oddsfox_pipeline.ingestion.polymarket.errors import gamma_get  # noqa: E402
from oddsfox_pipeline.ingestion.polymarket.markets.fetch import (  # noqa: E402
    build_client,
)
from oddsfox_pipeline.storage.duckdb.connection import (  # noqa: E402
    open_duckdb_connection,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import (  # noqa: E402
    POLYMARKET_CATALOG_RAW_SCHEMA,
)

logger = logging.getLogger(__name__)

CATALOG_SCHEMA: Final = POLYMARKET_CATALOG_RAW_SCHEMA
CATALOG_TABLE: Final = "markets"
DEFAULT_VOLUME_MIN: Final = 100_000.0
MARKETS_KEYSET_LIMIT: Final = 100


def _closed_passes(keyset_closed: bool | None) -> tuple[bool | None, ...]:
    # Gamma treats omitted closed as open-ish; both passes needed for "any".
    if keyset_closed is None:
        return (False, True)
    return (keyset_closed,)


def collect_high_volume_markets(
    *,
    volume_min: float = DEFAULT_VOLUME_MIN,
    keyset_closed: bool | None = None,
    max_pages: int | None = None,
    client: Any | None = None,
) -> list[dict[str, Any]]:
    """Page ``/markets/keyset`` for markets at or above ``volume_min``."""
    http = client or build_client()
    markets: list[dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    pages_total = 0

    for closed in _closed_passes(keyset_closed):
        cursor: str | None = None
        pages = 0
        while True:
            if max_pages is not None and pages_total >= max_pages:
                logger.warning(
                    "catalog sync stopped at max_pages=%s (partial catalog)",
                    max_pages,
                )
                return markets
            params: dict[str, Any] = {
                "limit": MARKETS_KEYSET_LIMIT,
                "volume_num_min": volume_min,
            }
            if closed is not None:
                params["closed"] = closed
            if cursor:
                params["after_cursor"] = cursor
            payload = gamma_get(http, "/markets/keyset", params=params)
            page = payload.get("markets") if isinstance(payload, dict) else payload
            page = page or []
            pages += 1
            pages_total += 1
            for market in page:
                if not isinstance(market, dict):
                    continue
                market_id = str(market.get("id") or "").strip()
                if not market_id or market_id in seen_market_ids:
                    continue
                volume = market.get("volumeNum", market.get("volume"))
                try:
                    volume_value = float(volume) if volume is not None else 0.0
                except (TypeError, ValueError):
                    volume_value = 0.0
                if volume_value < volume_min:
                    continue
                seen_market_ids.add(market_id)
                markets.append(market)
            next_cursor = (
                payload.get("next_cursor") if isinstance(payload, dict) else None
            )
            if pages % 20 == 0 or not page or not next_cursor:
                logger.info(
                    "catalog sync progress closed=%s pages=%s markets=%s",
                    closed,
                    pages,
                    len(markets),
                )
            if not page or not next_cursor:
                break
            if cursor is not None and next_cursor == cursor:
                logger.warning(
                    "catalog sync non-advancing cursor closed=%s after %s pages",
                    closed,
                    pages,
                )
                break
            cursor = next_cursor

    logger.info(
        "catalog sync collected pages=%s markets=%s volume_min=%s",
        pages_total,
        len(markets),
        volume_min,
    )
    return markets


def land_catalog_markets(
    rows: list[dict[str, Any]],
    *,
    duckdb_path: Path,
) -> int:
    if not rows:
        raise RuntimeError("No catalog markets to land")
    import polars as pl

    # Catalog payloads mix null/string/datetime cells; all-null columns must not
    # infer as Int (DuckDB then breaks coalesce(game_start_time, event_start_time)).
    frame = pl.DataFrame(rows, infer_schema_length=None)
    timestamp_cols = (
        "created_at",
        "scraped_at",
        "end_date",
        "event_start_time",
        "event_finished_time",
        "game_start_time",
    )
    string_cols = (
        "event_title",
        "event_game_id",
        "tags",
        "winning_outcome",
        "winning_clob_token_id",
    )
    bool_cols = ("event_ended", "is_resolved")
    casts: list[pl.Expr] = []
    for name in timestamp_cols:
        if name in frame.columns:
            casts.append(pl.col(name).cast(pl.Datetime, strict=False))
    for name in string_cols:
        if name in frame.columns:
            casts.append(pl.col(name).cast(pl.Utf8, strict=False))
    for name in bool_cols:
        if name in frame.columns:
            casts.append(pl.col(name).cast(pl.Boolean, strict=False))
    if casts:
        frame = frame.with_columns(casts)
    conn = open_duckdb_connection(duckdb_path, read_only=False)
    try:
        conn.execute(f"create schema if not exists {CATALOG_SCHEMA}")
        conn.execute(f"drop table if exists {CATALOG_SCHEMA}.{CATALOG_TABLE}")
        conn.register("_catalog_markets", frame)
        conn.execute(
            f"create table {CATALOG_SCHEMA}.{CATALOG_TABLE} as "
            "select * from _catalog_markets"
        )
        count = conn.execute(
            f"select count(*) from {CATALOG_SCHEMA}.{CATALOG_TABLE}"
        ).fetchone()
        return int(count[0]) if count else 0
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--duckdb-path", type=Path, default=None)
    p.add_argument("--volume-min", type=float, default=DEFAULT_VOLUME_MIN)
    p.add_argument(
        "--keyset-closed",
        choices=("false", "true", "any"),
        default="any",
        help="Gamma closed filter (default: any = open + closed passes).",
    )
    p.add_argument("--max-pages", type=int, default=None)
    args = p.parse_args(argv)

    from oddsfox_pipeline.config import settings

    closed: bool | None
    if args.keyset_closed == "false":
        closed = False
    elif args.keyset_closed == "true":
        closed = True
    else:
        closed = None

    duck = Path(args.duckdb_path or settings.DUCKDB_PATH).resolve()
    raw_markets = collect_high_volume_markets(
        volume_min=args.volume_min,
        keyset_closed=closed,
        max_pages=args.max_pages,
    )
    rows = normalize_market_payloads_for_dlt(raw_markets)
    rows = [
        row for row in rows if float(row.get("volume") or 0.0) >= float(args.volume_min)
    ]
    landed = land_catalog_markets(rows, duckdb_path=duck)
    print(
        f"Landed {landed} markets into {CATALOG_SCHEMA}.{CATALOG_TABLE} "
        f"(volume_min={args.volume_min:g}) at {duck}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

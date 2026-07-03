#!/usr/bin/env python3
"""
Count WC2026 Polymarket Gamma events via GET /events/keyset.

Defaults match routine ingestion (``MarketsSyncConfig``):
  - all scope tags from ``market_scopes.yml`` / ``WC2026_POLYMARKET_SCOPE_EVENT_TAGS``
  - default ``closed=false`` and ``volume_min=10000`` (omit via env ``any`` / empty volume)
  - ``related_tags=true`` when ``WC2026_POLYMARKET_SCOPE_KEYSET_RELATED_TAGS`` is on

Logs progress every N events (default 1000) per tag and prints per-tag totals.

Usage:
  python3 scripts/count_wc2026_gamma_tag_events.py
  python3 scripts/count_wc2026_gamma_tag_events.py --tag 2026-fifa-world-cup
  python3 scripts/count_wc2026_gamma_tag_events.py --tag fifa-world-cup --tag 2026-fifa-world-cup
  python3 scripts/count_wc2026_gamma_tag_events.py --keyset-closed any
  python3 scripts/count_wc2026_gamma_tag_events.py --keyset-volume-min 0
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
for _rp in (REPO_ROOT, REPO_ROOT / "src"):
    if str(_rp) not in sys.path:
        sys.path.insert(0, str(_rp))

from oddsfox_pipeline.config.settings import (  # noqa: E402
    WC2026_POLYMARKET_SCOPE_KEYSET_CLOSED,
    WC2026_POLYMARKET_SCOPE_KEYSET_VOLUME_MIN,
)
from oddsfox_pipeline.ingestion.polymarket.gamma_events import (  # noqa: E402
    iter_gamma_events_keyset,
)
from oddsfox_pipeline.ingestion.polymarket.market_scope import (  # noqa: E402
    DEFAULT_MARKET_SCOPE,
    load_market_scope_config,
    resolve_keyset_tag_slugs,
)
from oddsfox_pipeline.ingestion.polymarket.markets.fetch import (  # noqa: E402
    build_client,
)

logger = logging.getLogger(__name__)


def _parse_keyset_closed(value: str) -> bool | None:
    normalized = value.strip().lower()
    if normalized in {"false", "0", "open"}:
        return False
    if normalized in {"true", "1", "closed"}:
        return True
    if normalized in {"any", "all", "none", "null"}:
        return None
    raise argparse.ArgumentTypeError(
        "keyset-closed must be one of: false (open only), true (closed only), any"
    )


def count_tag_events(
    tag_slug: str,
    *,
    keyset_closed: bool | None = WC2026_POLYMARKET_SCOPE_KEYSET_CLOSED,
    keyset_volume_min: float | None = WC2026_POLYMARKET_SCOPE_KEYSET_VOLUME_MIN,
    log_every: int = 1000,
    max_pages: int | None = None,
) -> int:
    client = build_client()
    total = 0
    pages = 0
    truncated = False
    next_log_at = log_every

    logger.info(
        "start tag=%s keyset_closed=%s keyset_volume_min=%s",
        tag_slug,
        keyset_closed,
        keyset_volume_min,
    )

    for events, meta in iter_gamma_events_keyset(
        client,
        max_pages=max_pages,
        keyset_tag_slug=tag_slug,
        keyset_closed=keyset_closed,
        keyset_volume_min=keyset_volume_min,
    ):
        pages = meta.pages_done
        truncated = meta.truncated
        if not events:
            break
        total += len(events)
        while total >= next_log_at:
            logger.info(
                "tag=%s events=%s pages=%s truncated=%s keyset_closed=%s keyset_volume_min=%s",
                tag_slug,
                total,
                pages,
                truncated,
                keyset_closed,
                keyset_volume_min,
            )
            next_log_at += log_every

    logger.info(
        "tag=%s total_events=%s total_pages=%s truncated=%s keyset_closed=%s keyset_volume_min=%s",
        tag_slug,
        total,
        pages,
        truncated,
        keyset_closed,
        keyset_volume_min,
    )
    return total


def count_scope_tags(
    tag_slugs: list[str],
    *,
    keyset_closed: bool | None = WC2026_POLYMARKET_SCOPE_KEYSET_CLOSED,
    keyset_volume_min: float | None = WC2026_POLYMARKET_SCOPE_KEYSET_VOLUME_MIN,
    log_every: int = 1000,
    max_pages: int | None = None,
) -> dict[str, int]:
    totals: dict[str, int] = {}
    for tag_slug in tag_slugs:
        totals[tag_slug] = count_tag_events(
            tag_slug,
            keyset_closed=keyset_closed,
            keyset_volume_min=keyset_volume_min,
            log_every=log_every,
            max_pages=max_pages,
        )
    logger.info(
        "summary tags=%s grand_total=%s keyset_closed=%s keyset_volume_min=%s",
        tag_slugs,
        sum(totals.values()),
        keyset_closed,
        keyset_volume_min,
    )
    return totals


def main() -> int:
    default_volume_min = WC2026_POLYMARKET_SCOPE_KEYSET_VOLUME_MIN
    default_volume_help = (
        "none" if default_volume_min is None else f"{default_volume_min:g}"
    )
    parser = argparse.ArgumentParser(
        description="Count Gamma /events/keyset rows for WC2026 tag_slug values."
    )
    parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        help="Gamma tag_slug (repeatable). Default: all scope event_tags.",
    )
    parser.add_argument(
        "--scope-name",
        default=DEFAULT_MARKET_SCOPE,
        help=f"Configured market scope preset to count (default: {DEFAULT_MARKET_SCOPE}).",
    )
    parser.add_argument(
        "--keyset-closed",
        type=_parse_keyset_closed,
        default=WC2026_POLYMARKET_SCOPE_KEYSET_CLOSED,
        help=("closed filter: false=open only (default), true=closed only, any=both"),
    )
    parser.add_argument(
        "--keyset-volume-min",
        type=float,
        default=default_volume_min,
        help=(f"Gamma volume_min query filter (default: {default_volume_help})"),
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=1000,
        help="Log after this many events per tag (default: 1000)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional cap on API pages per tag (for smoke tests)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.log_every <= 0:
        parser.error("--log-every must be positive")
    if args.keyset_volume_min is not None and args.keyset_volume_min < 0:
        parser.error("--keyset-volume-min must be >= 0")

    cfg = load_market_scope_config(scope_name=args.scope_name)
    tag_slugs = resolve_keyset_tag_slugs(args.tags, config=cfg)
    if not tag_slugs:
        parser.error("no tag_slug values resolved; pass --tag or configure event_tags")

    count_scope_tags(
        tag_slugs,
        keyset_closed=args.keyset_closed,
        keyset_volume_min=args.keyset_volume_min,
        log_every=args.log_every,
        max_pages=args.max_pages,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

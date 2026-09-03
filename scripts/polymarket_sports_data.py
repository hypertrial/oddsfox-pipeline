#!/usr/bin/env python3
"""Operator-local public sports metadata and historical displayed books (read-only)."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter
from contextlib import nullcontext
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

ensure_src_on_path()

from oddsfox_pipeline.ingestion.polymarket.sports_data.books import (  # noqa: E402
    build,
    historical_quote,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.catalog import (  # noqa: E402
    refresh_catalog,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.recorder import (  # noqa: E402
    record,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.storage import (  # noqa: E402
    GIB,
    BudgetStop,
    Limits,
    Store,
)
from oddsfox_pipeline.ingestion.polymarket.user_activity import (  # noqa: E402
    parse_utc,
    validate_bounds,
)


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--data-root", type=Path, required=True)
    result.add_argument("--retained-gib", type=int, default=100)
    result.add_argument("--download-gib", type=int, default=10)
    result.add_argument("--reserve-gib", type=int, default=50)
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser("catalog-refresh")
    recorder = commands.add_parser("record")
    recorder.add_argument("--duration-seconds", type=int, required=True)
    recorder.add_argument("--tokens-per-connection", type=int, default=500)
    for command in ("backfill", "build"):
        sub = commands.add_parser(command)
        sub.add_argument("--start-utc", type=parse_utc, required=True)
        sub.add_argument("--end-utc", type=parse_utc, required=True)
        if command == "backfill":
            sub.add_argument("--dry-run", action="store_true")
            sub.add_argument(
                "--max-files",
                type=int,
                help="stop after this many ingested or empty archive files",
            )
    commands.add_parser("verify")
    quote = commands.add_parser("quote")
    quote.add_argument("--source", required=True)
    quote.add_argument("--token-id", required=True)
    quote.add_argument("--side", required=True, choices=("BUY", "SELL"))
    quote.add_argument("--quantity", required=True)
    quote.add_argument("--as-of", type=parse_utc, required=True)
    return result


def main():
    args = parser().parse_args()
    store = Store(
        args.data_root,
        Limits(
            args.retained_gib * GIB, args.download_gib * GIB, args.reserve_gib * GIB
        ),
    )
    if hasattr(args, "start_utc"):
        validate_bounds(args.start_utc, args.end_utc)
    try:
        with (
            store.writer() if args.command not in ("verify", "quote") else nullcontext()
        ):
            if args.command == "catalog-refresh":
                result = refresh_catalog(store)
            elif args.command == "record":
                result = asyncio.run(
                    record(
                        store,
                        args.duration_seconds,
                        shard_size=args.tokens_per_connection,
                    )
                )
            elif args.command == "build":
                result = build(store, args.start_utc, args.end_utc)
            elif args.command == "quote":
                result = historical_quote(
                    store,
                    args.source,
                    args.token_id,
                    args.side,
                    args.quantity,
                    args.as_of,
                )
            elif args.command == "backfill":
                from oddsfox_pipeline.ingestion.polymarket.sports_data.archive import (
                    backfill,
                )

                if args.max_files is not None and args.max_files <= 0:
                    raise ValueError("max-files must be positive")
                result = backfill(
                    store,
                    args.start_utc,
                    args.end_utc,
                    dry_run=args.dry_run,
                    max_files=args.max_files,
                )
            else:
                from oddsfox_pipeline.ingestion.polymarket.sports_data.verify import (
                    verify,
                )

                result = verify(store)
        summary = result
        if isinstance(result.get("files"), list):
            summary = {
                key: value
                for key, value in result.items()
                if key
                in (
                    "path",
                    "sha256",
                    "contract",
                    "counts",
                    "status",
                    "complete",
                    "activated_at",
                )
            }
        elif "tokens" in result:
            summary = {
                key: value
                for key, value in result.items()
                if key
                not in (
                    "tokens",
                    "final_states",
                    "token_failure_reasons",
                    "exception_events",
                )
            }
            summary["token_counts"] = dict(Counter(result["tokens"].values()))
            summary["failure_reason_counts"] = dict(
                Counter(
                    reason
                    for reasons in result.get("token_failure_reasons", {}).values()
                    for reason in reasons
                )
            )
            summary["exception_category_counts"] = dict(
                Counter(
                    event["category"] for event in result.get("exception_events", [])
                )
            )
        print(json.dumps(summary, default=str, sort_keys=True))
        if result.get("status") == "budget_stop":
            return 2
        if result.get("status") in (
            "partial_failure",
            "incomplete_no_active_catalog",
            "completed_with_internal_errors",
        ):
            return 3
        if any(
            status in ("pending", "failed")
            for status in result.get("tokens", {}).values()
        ):
            return 3
        if result.get("metrics", {}).get("overflow_dropped_messages", 0):
            return 3
        return 0
    except BudgetStop as exc:
        print(json.dumps({"status": "budget_stop", "reason": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

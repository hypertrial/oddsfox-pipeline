#!/usr/bin/env python3
"""Build an immutable local WC2026 Polygon settlement Kaggle bundle."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

REPO_ROOT = ensure_src_on_path()

from oddsfox_pipeline.config import settings  # noqa: E402
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (  # noqa: E402
    DEFAULT_POLYGON_MARKET_SEED_PATH,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_settlement import (  # noqa: E402
    verify_polygon_settlement_scan,
)
from oddsfox_pipeline.publishing.polygon_settlement import (  # noqa: E402
    DEFAULT_POLYGON_SETTLEMENT_RELEASE_ROOT,
    PolygonSettlementBundleSpec,
    build_polygon_settlement_release,
    current_generator_commit,
)
from oddsfox_pipeline.storage.duckdb.connection import (  # noqa: E402
    open_duckdb_connection,
)
from oddsfox_pipeline.storage.duckdb.polygon_settlement import (  # noqa: E402
    load_polygon_settlement_release_provenance,
    set_polygon_verification_status,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--publisher-name", required=True)
    parser.add_argument("--attribution-url")
    parser.add_argument(
        "--rights-review-status",
        choices=("not_reviewed", "reviewed", "cleared"),
        default="not_reviewed",
    )
    parser.add_argument("--rpc-provider-terms-url")
    parser.add_argument("--rpc-provider-terms-snapshot-sha256")
    parser.add_argument("--rpc-provider-terms-snapshot-at-utc")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_POLYGON_SETTLEMENT_RELEASE_ROOT,
    )
    parser.add_argument("--duckdb-path", type=Path, default=settings.DUCKDB_PATH)
    args = parser.parse_args(argv)

    verification_requested = bool(
        settings.POLYGON_VERIFY_RPC_URL or settings.POLYGON_VERIFY_RPC_PROVIDER_LABEL
    )
    conn = open_duckdb_connection(
        args.duckdb_path.resolve(), read_only=not verification_requested
    )
    try:
        if verification_requested:
            try:
                verify_polygon_settlement_scan(
                    conn,
                    seed_path=DEFAULT_POLYGON_MARKET_SEED_PATH,
                    rpc_url=settings.POLYGON_VERIFY_RPC_URL,
                    provider_label=settings.POLYGON_VERIFY_RPC_PROVIDER_LABEL,
                )
            except Exception as exc:  # verification is explicitly advisory
                stale_provenance = load_polygon_settlement_release_provenance(conn)
                set_polygon_verification_status(
                    conn,
                    str(stale_provenance["scan_id"]),
                    "error",
                )
                sys.stderr.write(
                    "Secondary Polygon verification failed "
                    f"({exc.__class__.__name__}); continuing advisory release.\n"
                )
        provenance = load_polygon_settlement_release_provenance(conn)
        summary = build_polygon_settlement_release(
            conn,
            args.output_root,
            PolygonSettlementBundleSpec(
                dataset_version=args.dataset_version,
                publisher_name=args.publisher_name,
                attribution_url=args.attribution_url,
                rights_review_status=args.rights_review_status,
                rpc_provider_terms_url=args.rpc_provider_terms_url,
                rpc_provider_terms_snapshot_sha256=(
                    args.rpc_provider_terms_snapshot_sha256
                ),
                rpc_provider_terms_snapshot_at_utc=(
                    args.rpc_provider_terms_snapshot_at_utc
                ),
            ),
            provenance=provenance,
            generator_commit=current_generator_commit(REPO_ROOT),
        )
    except (
        duckdb.Error,
        FileExistsError,
        LookupError,
        RuntimeError,
        ValueError,
    ) as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    finally:
        conn.close()

    print(f"Built {summary['rows']:,} rows under {summary['release_dir']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

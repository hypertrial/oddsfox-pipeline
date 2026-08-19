#!/usr/bin/env python3
"""Bootstrap a disposable DuckDB database for CI dbt targets.

Creates Polymarket and Kalshi test raw/ops tables, seeds one ingestion-run event
per platform, and activates a validated synthetic ``oddsfox.reference.v1``
bundle through the production loader.
"""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

ensure_src_on_path()

import oddsfox_pipeline.storage.duckdb.connection as connection  # noqa: E402
from oddsfox_pipeline.contracts.reference_bundle import (  # noqa: E402
    REFERENCE_CONTRACT_VERSION,
    REFERENCE_SCHEMA_VERSION,
    REFERENCE_TABLE_PRIMARY_KEYS,
    load_reference_bundle,
)
from oddsfox_pipeline.contracts.schema import schema_fingerprint  # noqa: E402
from oddsfox_pipeline.storage.duckdb.schemas.kalshi import (  # noqa: E402
    create_all_kalshi_test_raw_tables,
    seed_test_kalshi_ingestion_run_event,
)

_REFERENCE_COLUMNS: dict[str, dict[str, pa.DataType]] = {
    "international_results_wc2026_matches": {
        "match_id": pa.string(),
        "match_date": pa.date32(),
        "home_team": pa.string(),
        "away_team": pa.string(),
        "city": pa.string(),
        "home_score": pa.int64(),
        "away_score": pa.int64(),
        "advancing_team": pa.string(),
        "match_status": pa.string(),
        "source_revision": pa.string(),
        "source_payload_sha256": pa.string(),
        "source_loaded_at": pa.timestamp("us", tz="UTC"),
    },
    "international_results_wc2026_team_aliases": {
        "market_team_name": pa.string(),
        "canonical_team_name": pa.string(),
    },
    "international_results_wc2026_team_status": {
        "team_name": pa.string(),
        "tournament_status": pa.string(),
        "is_still_alive": pa.bool_(),
        "eliminated_stage_key": pa.string(),
        "eliminated_match_date": pa.date32(),
        "next_match_date": pa.date32(),
        "next_stage_key": pa.string(),
        "matches_played": pa.int64(),
        "wins": pa.int64(),
        "draws": pa.int64(),
        "losses": pa.int64(),
        "goals_for": pa.int64(),
        "goals_against": pa.int64(),
        "latest_completed_match_date": pa.date32(),
        "latest_completed_stage_key": pa.string(),
    },
    "openfootball_wc2026_schedule_fixtures": {
        "fifa_match_id": pa.int64(),
        "stage_key": pa.string(),
        "home_team": pa.string(),
        "away_team": pa.string(),
        "kickoff_at_utc": pa.timestamp("us", tz="UTC"),
    },
    "wc2026_fixtures": {
        "match_id": pa.int64(),
        "stage": pa.string(),
        "group_label": pa.string(),
        "home_team": pa.string(),
        "away_team": pa.string(),
        "kickoff_at_et": pa.timestamp("us"),
    },
    "wc2026_team_canonical_aliases": {
        "variant_match_key": pa.string(),
        "canonical_match_key": pa.string(),
    },
    "wc2026_club_strength_history": {
        "club_key": pa.string(),
        "valid_from": pa.date32(),
        "valid_to": pa.date32(),
    },
    "wc2026_source_availability": {
        "source": pa.string(),
        "required_for_v4": pa.bool_(),
        "available": pa.bool_(),
        "latest_snapshot_id": pa.string(),
        "latest_collected_at": pa.timestamp("us", tz="UTC"),
        "age_hours": pa.int64(),
        "row_count": pa.int64(),
        "availability_mode": pa.string(),
    },
    "wc2026_source_provenance": {
        "source": pa.string(),
        "snapshot_id": pa.string(),
        "collected_at": pa.timestamp("us", tz="UTC"),
        "collector_git_sha": pa.string(),
        "collector_container_digest": pa.string(),
        "manifest_sha256": pa.string(),
        "loaded_at": pa.timestamp("us", tz="UTC"),
        "provenance_kind": pa.string(),
    },
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_synthetic_reference_bundle(directory: Path) -> None:
    tables: list[dict[str, object]] = []
    for table, primary_key in REFERENCE_TABLE_PRIMARY_KEYS.items():
        columns = dict(_REFERENCE_COLUMNS.get(table, {}))
        for key in primary_key:
            columns.setdefault(key, pa.string())
        payload = pa.table(
            {name: pa.array([], type=data_type) for name, data_type in columns.items()}
        )
        path = directory / f"{table}.parquet"
        pq.write_table(payload, path)
        tables.append(
            {
                "table": table,
                "path": path.name,
                "schema_fingerprint": schema_fingerprint(payload.schema),
                "primary_key": list(primary_key),
                "row_count": 0,
                "sha256": _sha256(path),
                "date_range": None,
            }
        )
    manifest = {
        "contract_version": REFERENCE_CONTRACT_VERSION,
        "schema_version": REFERENCE_SCHEMA_VERSION,
        "bundle_id": "dbt-ci-reference-v1",
        "status": "complete",
        "scraper_revision": "0" * 40,
        "scraper_image_digest": "sha256:" + "0" * 64,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "predecessor_bundle_id": None,
        "sources": [
            {
                "source": "synthetic-ci",
                "snapshot_id": "synthetic-ci-v1",
                "revision": "synthetic",
                "checksum": "0" * 64,
                "acquired_at": datetime.now(timezone.utc).isoformat(),
                "license": "synthetic-test-data",
            }
        ],
        "tables": tables,
    }
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    checksum_rows = [
        f"{_sha256(directory / str(entry['path']))}  {entry['path']}"
        for entry in tables
    ]
    checksum_rows.append(f"{_sha256(manifest_path)}  manifest.json")
    (directory / "checksums.sha256").write_text("\n".join(sorted(checksum_rows)) + "\n")


from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (  # noqa: E402
    create_all_scope_test_markets_tables,
    seed_test_ingestion_run_event,
)


def bootstrap_dbt_ci_duckdb() -> Path:
    """Reset connection state, init schemas, and seed CI smoke rows."""
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    conn = connection.get_persistent_connection()
    try:
        create_all_scope_test_markets_tables(conn)
        seed_test_ingestion_run_event(conn)
        create_all_kalshi_test_raw_tables(conn)
        seed_test_kalshi_ingestion_run_event(conn)
    finally:
        conn.close()
    path = connection.active_duckdb_path()
    with tempfile.TemporaryDirectory(prefix="oddsfox-reference-ci-") as temp:
        bundle = Path(temp) / "dbt-ci-reference-v1"
        bundle.mkdir()
        _write_synthetic_reference_bundle(bundle)
        load_reference_bundle(bundle, path)
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    path = bootstrap_dbt_ci_duckdb()
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

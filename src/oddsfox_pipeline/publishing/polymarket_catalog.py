"""Immutable Parquet publication for the Polymarket graph catalog."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Final

import duckdb
import pyarrow.parquet as pq

from oddsfox_pipeline.contracts.schema import schema_fingerprint
from oddsfox_pipeline.ingestion.polymarket.catalog import (
    CATALOG_CONTRACT_VERSION,
    CATALOG_PASSES,
    TRADABILITY_PREDICATE_VERSION,
)
from oddsfox_pipeline.publishing._bundle_io import (
    COMMIT_RE,
    SEMVER_RE,
    sha256_file,
    validate_dataset_version,
    write_json,
)
from oddsfox_pipeline.publishing.polygon_settlement import current_generator_commit

DEFAULT_POLYMARKET_CATALOG_RELEASE_ROOT: Final = Path(
    "artifacts/polymarket_catalog/releases"
)
_MART = '"polymarket_catalog_marts"."polymarket_graph_catalog"'
_REQUIRED_COLUMNS: Final = frozenset(
    {
        "contract_version",
        "record_type",
        "record_id",
        "entity_id",
        "from_record_id",
        "to_record_id",
        "relationship_type",
        "title",
        "subtitle",
        "description",
        "resolution_source",
        "slug",
        "canonical_url",
        "content_text",
        "tags_json",
        "series_json",
        "outcomes_json",
        "tradability_evidence_json",
        "attributes_json",
        "is_active",
        "is_closed",
        "is_archived",
        "is_resolved",
        "is_tradable",
        "source_created_at",
        "source_updated_at",
        "start_at",
        "end_at",
        "closed_at",
        "first_observed_at",
        "last_observed_at",
        "last_observed_crawl_id",
        "latest_catalog_crawl_id",
        "present_in_latest_crawl",
        "content_text_sha256",
    }
)
_RELEASE_FILES: Final = frozenset(
    {
        "polymarket_graph_catalog.parquet",
        "manifest.json",
        "schema.json",
        "quality_report.json",
        "checksums.sha256",
    }
)


def validate_polymarket_catalog_release(release_dir: Path) -> dict[str, Any]:
    """Reload and verify a complete immutable catalog release directory."""
    entries = list(release_dir.iterdir())
    files = {path.name for path in entries}
    if (
        files != _RELEASE_FILES
        or any(path.is_symlink() or not path.is_file() for path in entries)
        or len(entries) != len(files)
    ):
        raise RuntimeError(f"catalog release file inventory mismatch: {sorted(files)}")
    checksum_rows: dict[str, str] = {}
    for line in (
        (release_dir / "checksums.sha256").read_text(encoding="utf-8").splitlines()
    ):
        digest, separator, name = line.partition("  ")
        if not separator or name in checksum_rows:
            raise RuntimeError("invalid catalog release checksum inventory")
        checksum_rows[name] = digest
    expected_checksum_names = _RELEASE_FILES - {"checksums.sha256"}
    if set(checksum_rows) != expected_checksum_names:
        raise RuntimeError("catalog release checksum inventory mismatch")
    for name, digest in checksum_rows.items():
        if (
            not re.fullmatch(r"[0-9a-f]{64}", digest)
            or sha256_file(release_dir / name) != digest
        ):
            raise RuntimeError(f"catalog release checksum mismatch: {name}")

    manifest = json.loads((release_dir / "manifest.json").read_text(encoding="utf-8"))
    schema = json.loads((release_dir / "schema.json").read_text(encoding="utf-8"))
    quality = json.loads(
        (release_dir / "quality_report.json").read_text(encoding="utf-8")
    )
    parquet_path = release_dir / "polymarket_graph_catalog.parquet"
    parquet_file = pq.ParquetFile(parquet_path)
    if manifest.get("contract_version") != CATALOG_CONTRACT_VERSION:
        raise RuntimeError("catalog release contract version mismatch")
    if not SEMVER_RE.fullmatch(str(manifest.get("dataset_version", ""))):
        raise RuntimeError("catalog release dataset version mismatch")
    if manifest.get("producer") != "oddsfox-pipeline" or not COMMIT_RE.fullmatch(
        str(manifest.get("pipeline_revision", ""))
    ):
        raise RuntimeError("catalog release producer provenance mismatch")
    passes = manifest.get("source", {}).get("passes", {})
    if set(passes) != {item[0] for item in CATALOG_PASSES}:
        raise RuntimeError("catalog release pass inventory mismatch")
    for pass_name, endpoint, result_key, closed in CATALOG_PASSES:
        item = passes[pass_name]
        if (
            item.get("endpoint") != endpoint
            or item.get("record_type") != result_key
            or item.get("closed") is not closed
            or item.get("complete") is not True
            or not isinstance(item.get("pages"), int)
            or item["pages"] < 1
            or not isinstance(item.get("source_rows"), int)
            or item["source_rows"] < 0
        ):
            raise RuntimeError("catalog release pass inventory mismatch")
    if manifest.get("files") != {
        name: checksum_rows[name]
        for name in (
            "polymarket_graph_catalog.parquet",
            "schema.json",
            "quality_report.json",
        )
    }:
        raise RuntimeError("catalog release manifest checksum mismatch")
    if parquet_file.metadata.num_rows != quality.get("rows"):
        raise RuntimeError("catalog release row count mismatch")
    if schema.get("schema_fingerprint") != schema_fingerprint(
        parquet_file.schema_arrow
    ) or manifest.get("schema_fingerprint") != schema.get("schema_fingerprint"):
        raise RuntimeError("catalog release schema fingerprint mismatch")
    expected_fields = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in parquet_file.schema_arrow
    ]
    if schema.get("fields") != expected_fields:
        raise RuntimeError("catalog release schema inventory mismatch")
    if manifest.get("counts") != quality:
        raise RuntimeError("catalog release quality manifest mismatch")
    record_types = pq.read_table(parquet_path, columns=["record_type"])[
        "record_type"
    ].to_pylist()
    actual_counts = {
        "rows": len(record_types),
        "events": record_types.count("event"),
        "markets": record_types.count("market"),
        "event_market_edges": record_types.count("event_market"),
    }
    if any(quality.get(key) != value for key, value in actual_counts.items()):
        raise RuntimeError("catalog release quality count mismatch")
    return manifest


def _validate_graph(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    columns = {
        str(row[0])
        for row in conn.execute(f"DESCRIBE SELECT * FROM {_MART}").fetchall()
    }
    missing = sorted(_REQUIRED_COLUMNS - columns)
    unexpected = sorted(columns - _REQUIRED_COLUMNS)
    if missing or unexpected:
        raise RuntimeError(
            f"polymarket graph catalog schema drift: missing={missing}, unexpected={unexpected}"
        )
    row = conn.execute(
        f"""
        SELECT count(*) AS rows,
            count(*) FILTER (WHERE record_type = 'event') AS events,
            count(*) FILTER (WHERE record_type = 'market') AS markets,
            count(*) FILTER (WHERE record_type = 'event_market') AS edges,
            count(*) - count(DISTINCT record_id) AS duplicate_ids,
            count(*) FILTER (WHERE content_text IS NULL OR content_text = '') AS empty_text,
            count(*) FILTER (
                WHERE record_type = 'market' AND NOT coalesce(is_tradable, false)
            ) AS nontradable_markets,
            count(*) FILTER (
                WHERE contract_version != '{CATALOG_CONTRACT_VERSION}'
                   OR record_type NOT IN ('event', 'market', 'event_market')
            ) AS invalid_contract_rows,
            count(*) FILTER (
                WHERE (record_type = 'event_market' AND relationship_type != 'contains_market')
                   OR (record_type != 'event_market' AND relationship_type IS NOT NULL)
            ) AS invalid_relationship_rows,
            count(*) FILTER (
                WHERE content_text_sha256 != sha256(content_text)
            ) AS text_hash_mismatches,
            count(*) FILTER (
                WHERE try_cast(tags_json AS JSON) IS NULL
                   OR try_cast(series_json AS JSON) IS NULL
                   OR try_cast(outcomes_json AS JSON) IS NULL
                   OR try_cast(tradability_evidence_json AS JSON) IS NULL
                   OR try_cast(attributes_json AS JSON) IS NULL
            ) AS invalid_json_rows,
            count(*) FILTER (
                WHERE (record_type IN ('event', 'market') AND (
                    entity_id IS NULL OR from_record_id IS NOT NULL OR to_record_id IS NOT NULL
                )) OR (record_type = 'event_market' AND (
                    entity_id IS NOT NULL OR from_record_id IS NULL OR to_record_id IS NULL
                ))
            ) AS invalid_graph_identity_rows
        FROM {_MART}
        """
    ).fetchone()
    if row is None or row[0] == 0:
        raise RuntimeError("polymarket graph catalog is empty")
    quality = dict(
        zip(
            (
                "rows",
                "events",
                "markets",
                "event_market_edges",
                "duplicate_record_ids",
                "empty_content_text",
                "nontradable_markets",
                "invalid_contract_rows",
                "invalid_relationship_rows",
                "text_hash_mismatches",
                "invalid_json_rows",
                "invalid_graph_identity_rows",
            ),
            map(int, row),
            strict=True,
        )
    )
    dangling = int(
        conn.execute(
            f"""
            SELECT count(*) FROM {_MART} AS edge
            WHERE edge.record_type = 'event_market' AND (
                NOT EXISTS (
                    SELECT 1 FROM {_MART} AS node
                    WHERE node.record_id = edge.from_record_id
                ) OR NOT EXISTS (
                    SELECT 1 FROM {_MART} AS node
                    WHERE node.record_id = edge.to_record_id
                )
            )
            """
        ).fetchone()[0]
    )
    quality["dangling_edges"] = dangling
    event_without_edge = int(
        conn.execute(
            f"""
            SELECT count(*) FROM {_MART} AS event
            WHERE event.record_type = 'event' AND NOT EXISTS (
                SELECT 1 FROM {_MART} AS edge
                WHERE edge.record_type = 'event_market'
                  AND edge.from_record_id = event.record_id
            )
            """
        ).fetchone()[0]
    )
    quality["events_without_edges"] = event_without_edge
    raw_counts = conn.execute(
        """
        WITH qualifying_markets AS (
            SELECT DISTINCT market_id
            FROM polymarket_catalog_raw.market_snapshots
            WHERE is_tradable
        ), qualifying_edges AS (
            SELECT DISTINCT edge.event_id, edge.market_id
            FROM polymarket_catalog_raw.event_market_snapshots AS edge
            INNER JOIN qualifying_markets USING (market_id)
        )
        SELECT
            (SELECT count(*) FROM qualifying_markets),
            (SELECT count(*) FROM qualifying_edges),
            (SELECT count(DISTINCT event_id) FROM qualifying_edges)
        """
    ).fetchone()
    quality["raw_qualifying_markets"] = int(raw_counts[0])
    quality["raw_qualifying_edges"] = int(raw_counts[1])
    quality["raw_connected_events"] = int(raw_counts[2])
    quality["raw_reconciliation_issues"] = int(
        quality["markets"] != quality["raw_qualifying_markets"]
        or quality["event_market_edges"] != quality["raw_qualifying_edges"]
        or quality["events"] != quality["raw_connected_events"]
    )
    if any(
        quality[key]
        for key in (
            "duplicate_record_ids",
            "empty_content_text",
            "nontradable_markets",
            "dangling_edges",
            "events_without_edges",
            "invalid_contract_rows",
            "invalid_relationship_rows",
            "text_hash_mismatches",
            "invalid_json_rows",
            "invalid_graph_identity_rows",
            "raw_reconciliation_issues",
        )
    ):
        raise RuntimeError(f"polymarket graph catalog failed validation: {quality}")
    return quality


def build_polymarket_catalog_release(
    conn: duckdb.DuckDBPyConnection,
    output_root: Path,
    *,
    dataset_version: str,
    repository_root: Path | None = None,
) -> dict[str, Any]:
    """Validate and atomically publish one immutable graph-catalog release."""
    validate_dataset_version(dataset_version)
    final = output_root.resolve() / dataset_version
    if final.exists() or final.is_symlink():
        raise FileExistsError(f"catalog release already exists: {final}")
    quality = _validate_graph(conn)
    run = conn.execute(
        """
        SELECT crawl_id, observed_at, completed_at, summary_json
        FROM polymarket_catalog_ops.crawl_runs
        WHERE status = 'complete'
        QUALIFY row_number() OVER (ORDER BY completed_at DESC, crawl_id DESC) = 1
        """
    ).fetchone()
    if run is None:
        raise RuntimeError("no complete catalog crawl exists")
    crawl_id, observed_at, completed_at, summary_json = run
    table = conn.execute(
        f"""
        SELECT * FROM {_MART}
        ORDER BY CASE record_type
            WHEN 'event' THEN 1 WHEN 'market' THEN 2 ELSE 3 END, record_id
        """
    ).to_arrow_table()
    if table.num_rows != quality["rows"]:
        raise RuntimeError("mart changed during release publication")

    root = repository_root or Path(__file__).resolve().parents[3]
    model_path = (
        root / "dbt/models/polymarket_catalog/marts/polymarket_graph_catalog.sql"
    )
    revision = current_generator_commit(root)
    output_root.resolve().mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=f".{dataset_version}.", dir=output_root.resolve())
    )
    try:
        parquet_path = temporary / "polymarket_graph_catalog.parquet"
        pq.write_table(
            table,
            parquet_path,
            compression="zstd",
            version="2.6",
            data_page_version="1.0",
            write_statistics=True,
        )
        schema = {
            "contract_version": CATALOG_CONTRACT_VERSION,
            "schema_fingerprint": schema_fingerprint(table.schema),
            "fields": [
                {
                    "name": field.name,
                    "type": str(field.type),
                    "nullable": field.nullable,
                }
                for field in table.schema
            ],
        }
        quality_report = {
            **quality,
            "status": "complete",
            "crawl_id": str(crawl_id),
            "present_in_latest_crawl": int(
                conn.execute(
                    f"SELECT count(*) FROM {_MART} WHERE present_in_latest_crawl"
                ).fetchone()[0]
            ),
            "retained_from_prior_crawls": int(
                conn.execute(
                    f"SELECT count(*) FROM {_MART} WHERE NOT present_in_latest_crawl"
                ).fetchone()[0]
            ),
        }
        write_json(temporary / "schema.json", schema)
        write_json(temporary / "quality_report.json", quality_report)
        payload_checksums = {
            name: sha256_file(temporary / name)
            for name in (
                "polymarket_graph_catalog.parquet",
                "schema.json",
                "quality_report.json",
            )
        }
        pass_summary = json.loads(summary_json)["passes"]
        pass_inventory = {
            pass_name: {
                "endpoint": endpoint,
                "record_type": result_key,
                "closed": closed,
                **pass_summary[pass_name],
            }
            for pass_name, endpoint, result_key, closed in CATALOG_PASSES
        }
        manifest = {
            "contract_version": CATALOG_CONTRACT_VERSION,
            "dataset_version": dataset_version,
            "producer": "oddsfox-pipeline",
            "pipeline_revision": revision,
            "crawl_id": str(crawl_id),
            "crawl_observed_at": observed_at.isoformat() + "Z",
            "created_at": completed_at.isoformat() + "Z",
            "source": {
                "system": "Polymarket Gamma",
                "base_url": "https://gamma-api.polymarket.com",
                "passes": pass_inventory,
            },
            "tradability_predicate_version": TRADABILITY_PREDICATE_VERSION,
            "counts": quality_report,
            "schema_fingerprint": schema["schema_fingerprint"],
            "mart_query_sha256": hashlib.sha256(model_path.read_bytes()).hexdigest(),
            "files": payload_checksums,
        }
        write_json(temporary / "manifest.json", manifest)
        checksum_names = (*payload_checksums, "manifest.json")
        (temporary / "checksums.sha256").write_text(
            "".join(
                f"{sha256_file(temporary / name)}  {name}\n"
                for name in sorted(checksum_names)
            ),
            encoding="utf-8",
        )
        validate_polymarket_catalog_release(temporary)
        os.replace(temporary, final)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return {
        **quality_report,
        "release_dir": str(final),
        "dataset_version": dataset_version,
    }


__all__ = [
    "DEFAULT_POLYMARKET_CATALOG_RELEASE_ROOT",
    "build_polymarket_catalog_release",
    "validate_polymarket_catalog_release",
]

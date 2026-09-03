"""Offline validation of committed local evidence, coverage and incomplete work."""

from __future__ import annotations

import json
from collections import Counter

import pyarrow.parquet as pq

from oddsfox_pipeline.ingestion.polymarket.sports_data.archive import (
    EXCLUSION_SCHEMA,
    V1_SCHEMA,
    V2_SCHEMA,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.catalog import (
    IDENTITY_SCOPE_SCHEMA,
    MEMBERSHIP_SCHEMA,
    OBSERVATION_SCHEMA,
    OUTCOME_SCHEMA,
    RELATION_SCHEMA,
    active_catalog,
    catalog_targets,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.storage import (
    RAW_SCHEMA,
    STATE_SCHEMA,
    UPDATE_SCHEMA,
    Store,
    sha256,
)


def verify(store: Store):
    checked, counts, coverage = set(), Counter(), []
    recorded_sessions = set()
    committed_sessions = {}
    committed_archives = set()
    for kind in ("catalog", "record", "archive", "build", "benchmark", "session"):
        for path, manifest in store.manifests(kind):
            counts[f"{kind}_manifests"] += 1
            if kind == "record" and manifest.get("session"):
                recorded_sessions.add(manifest["session"])
            seen = set()
            for item in manifest["files"]:
                if item["path"] in seen:
                    raise ValueError("duplicate file in committed manifest")
                seen.add(item["path"])
                if item["path"] not in checked:
                    file = store.validate_file(item)
                    schema = pq.ParquetFile(file).schema_arrow
                    expected = (
                        RAW_SCHEMA
                        if "/raw/" in item["path"]
                        else UPDATE_SCHEMA
                        if "/updates/" in item["path"]
                        else STATE_SCHEMA
                        if "/states/" in item["path"]
                        else OBSERVATION_SCHEMA
                        if "/observations/" in item["path"]
                        else RELATION_SCHEMA
                        if "/relations/" in item["path"]
                        else OUTCOME_SCHEMA
                        if "/outcomes/" in item["path"]
                        else IDENTITY_SCOPE_SCHEMA
                        if "/identity-scope/" in item["path"]
                        else MEMBERSHIP_SCHEMA
                        if "/memberships/" in item["path"]
                        else EXCLUSION_SCHEMA
                        if "/exclusions/" in item["path"]
                        else None
                    )
                    if expected is not None and not schema.equals(expected):
                        raise ValueError("committed Parquet schema mismatch")
                    checked.add(item["path"])
                    counts["rows"] += item["rows"]
            descriptors = {item["path"]: item for item in manifest["files"]}
            for relation in (
                "raw",
                "updates",
                "relations",
                "outcomes",
                "memberships",
                "identity_scope",
                "exclusions",
                "states",
            ):
                for item in manifest.get(relation, []):
                    if descriptors.get(item["path"]) != item:
                        raise ValueError(
                            "output relation is not bound by the manifest file inventory"
                        )
            for item in manifest.get("inputs", []):
                if sha256(store.path(item["path"])) != item["sha256"]:
                    raise ValueError("input manifest checksum mismatch")
            if "taxonomy" in manifest:
                item = manifest["taxonomy"]
                if sha256(store.path(item["path"])) != item["sha256"]:
                    raise ValueError("taxonomy checksum mismatch")
            if "summary" in manifest:
                item = manifest["summary"]
                if sha256(store.path(item["path"])) != item["sha256"]:
                    raise ValueError("session summary checksum mismatch")
                if kind == "session":
                    committed_sessions[item["path"]] = json.loads(
                        store.path(item["path"]).read_bytes()
                    )
            if kind == "catalog" and "acquisition" in manifest:
                item = manifest["acquisition"]
                if sha256(store.path(item["path"])) != item["sha256"]:
                    raise ValueError("catalog acquisition checkpoint checksum mismatch")
            if "archive" in manifest:
                archive = store.validate_file(manifest["archive"])
                committed_archives.add(manifest["archive"]["path"])
                expected = {
                    "v1": V1_SCHEMA,
                    "v2": V2_SCHEMA,
                }.get(manifest.get("source_file", {}).get("version"))
                if expected is None or not pq.ParquetFile(archive).schema_arrow.equals(
                    expected, check_metadata=False
                ):
                    raise ValueError("committed archive source schema mismatch")
            coverage.append(
                {
                    "manifest": str(path.relative_to(store.root)),
                    "source": manifest.get("source"),
                    "counts": manifest.get("counts", {}),
                }
            )
    inventories = []
    for path in sorted(store.path("inventories").glob("*.json")):
        if path.stem != sha256(path):
            raise ValueError("inventory filename does not match its content hash")
        value = json.loads(path.read_bytes())
        inventories.append(
            {
                "path": str(path.relative_to(store.root)),
                "start": value["start"],
                "end": value["end"],
                "counts": value["counts"],
                "estimated_listed_bytes": value["estimated_listed_bytes"],
            }
        )
    download_statuses = Counter()
    uncommitted_downloads = []
    for metadata in sorted(store.path("downloads").glob("*/*.json")):
        value = json.loads(metadata.read_bytes())
        status = value.get("status")
        if status not in {"partial", "ready_to_activate", "downloaded"}:
            raise ValueError("unknown archive cache status")
        download_statuses[status] += 1
        archive = metadata.with_suffix(".parquet")
        if status == "downloaded":
            if (
                not archive.is_file()
                or archive.stat().st_size != value["identity"]["bytes"]
                or sha256(archive) != value["sha256"]
            ):
                raise ValueError("downloaded archive cache metadata mismatch")
            relative = str(archive.relative_to(store.root))
            if relative not in committed_archives:
                uncommitted_downloads.append(relative)
    catalog = (
        active_catalog(store) if store.path("catalog-active.json").exists() else None
    )
    targets = catalog_targets(store) if catalog else {}
    sessions = []
    for _, session in sorted(committed_sessions.items()):
        if session["status"] not in {
            "duration_complete",
            "completed_with_internal_errors",
            "budget_stop",
            "interrupted",
            "failed",
        }:
            raise ValueError("unknown recording session status")
        if len(session["tokens"]) != session["admitted"]:
            raise ValueError("session token coverage count mismatch")
        if set(session["tokens"].values()) - {
            "recorded",
            "pending",
            "unavailable",
            "failed",
        }:
            raise ValueError("unknown session token coverage status")
        failed = {
            token for token, status in session["tokens"].items() if status == "failed"
        }
        if (
            "token_failure_reasons" in session
            and set(session["token_failure_reasons"]) != failed
        ):
            raise ValueError("session failed-token reasons do not reconcile")
        sessions.append(
            {
                "session": session["session"],
                "status": session["status"],
                "duration_seconds": session["duration_seconds"],
                "tokens": dict(Counter(session["tokens"].values())),
                "metrics": session["metrics"],
                "failure_reason_counts": dict(
                    Counter(
                        reason
                        for reasons in session.get("token_failure_reasons", {}).values()
                        for reason in reasons
                    )
                ),
                "exception_category_counts": dict(
                    Counter(
                        event["category"]
                        for event in session.get("exception_events", [])
                    )
                ),
            }
        )
    incomplete = [
        str(path.relative_to(store.root)) for path in store.root.rglob("*.partial")
    ]
    uncommitted = [
        str(path.relative_to(store.root))
        for path in store.root.rglob("*.parquet")
        if str(path.relative_to(store.root)) not in checked
        and "/downloads/" not in str(path)
    ]
    return {
        "status": "verified_committed_evidence"
        if catalog
        else "incomplete_no_active_catalog",
        "catalog_manifest_sha256": catalog["sha256"] if catalog else None,
        "admitted_tokens": len(targets),
        "files": len(checked),
        "counts": dict(counts),
        "sessions": sessions,
        "completed_recording_seconds": sum(
            session["duration_seconds"]
            for session in sessions
            if session["status"] == "duration_complete"
        ),
        "incomplete_sessions": sorted(
            recorded_sessions - {session["session"] for session in sessions}
        ),
        "uncommitted_session_summaries": [
            str(path.relative_to(store.root))
            for path in sorted(store.path("sessions").glob("*.json"))
            if str(path.relative_to(store.root)) not in committed_sessions
        ],
        "coverage": coverage,
        "inventories": inventories,
        "download_status_counts": dict(download_statuses),
        "uncommitted_downloads": uncommitted_downloads,
        "incomplete_files": incomplete,
        "incomplete_file_count": len(incomplete),
        "uncommitted_segments": uncommitted,
        "uncommitted_segment_count": len(uncommitted),
        "storage": store.check(),
        "continuity": "observed continuity only; provider limitations remain explicit",
    }

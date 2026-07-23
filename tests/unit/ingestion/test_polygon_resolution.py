from __future__ import annotations

from datetime import datetime, timezone

import pytest
import yaml
from tests.support.distribution_fixtures import write_synthetic_distribution_inputs

from oddsfox_pipeline.ingestion.polymarket.polygon_resolution import (
    load_polygon_resolution_attestation,
    write_polygon_resolution_attestation,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    load_polygon_market_seed,
)


def _valid_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "manifest_version": "1.0.0",
        "manifest_sha256": "a" * 64,
        "resolved_condition_count": 248,
        "verified_at_utc": "2026-07-22T12:00:00Z",
        "authoring_evidence_sha256": "b" * 64,
        "finalized_head_block_number": 123,
        "finalized_head_block_hash": "0x" + "c" * 64,
    }


def test_synthetic_resolution_attestation_matches_synthetic_manifest(
    tmp_path,
) -> None:
    seed_path, attestation_path = write_synthetic_distribution_inputs(tmp_path / "dbt")
    manifest = load_polygon_market_seed(seed_path)
    attestation = load_polygon_resolution_attestation(
        attestation_path,
        manifest=manifest,
    )

    assert attestation.resolved_condition_count == 248
    assert attestation.manifest_sha256 == manifest.sha256
    assert attestation.manifest_version == manifest.version
    assert attestation.public_summary() == {
        "manifest_version": "1.0.0",
        "manifest_sha256": manifest.sha256,
        "resolved_condition_count": 248,
        "verified_at_utc": "2026-08-01T00:00:00Z",
        "authoring_evidence_sha256": "b" * 64,
    }


def test_candidate_attestation_round_trips_and_keeps_internal_head(
    tmp_path,
) -> None:
    path = tmp_path / "resolution_attestation.yml"
    written = write_polygon_resolution_attestation(
        path,
        manifest_version="1.0.0",
        manifest_sha256="a" * 64,
        chain_evidence={
            "resolution_count": 248,
            "finalized_head": {
                "number": 123,
                "hash": "0x" + "b" * 64,
                "timestamp": datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
            },
        },
        authoring_evidence_sha256="c" * 64,
    )

    loaded = load_polygon_resolution_attestation(path)

    assert loaded == written
    assert loaded.finalized_head_block_number == 123
    assert loaded.finalized_head_block_hash == "0x" + "b" * 64
    assert "finalized_head_block_hash" not in loaded.public_summary()


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("schema_version", 2, "schema_version"),
        ("resolved_condition_count", 247, "cover 248"),
        ("manifest_sha256", "bad", "manifest_sha256"),
        ("authoring_evidence_sha256", "bad", "evidence SHA"),
        ("finalized_head_block_hash", "bad", "finalized head"),
    ],
)
def test_resolution_attestation_rejects_invalid_fields(
    tmp_path, field, value, message
) -> None:
    path = tmp_path / "resolution_attestation.yml"
    payload = _valid_payload()
    payload[field] = value
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_polygon_resolution_attestation(path)


def test_resolution_attestation_rejects_unreadable_or_invalid_documents(
    tmp_path,
) -> None:
    missing = tmp_path / "missing.yml"
    with pytest.raises(ValueError, match="Could not load"):
        load_polygon_resolution_attestation(missing)

    invalid = tmp_path / "invalid.yml"
    invalid.write_text("[]\n", encoding="utf-8")
    with pytest.raises(ValueError, match="fields are invalid"):
        load_polygon_resolution_attestation(invalid)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("not-a-time", "ISO-8601 UTC"),
        ("2026-07-22T12:00:00", "explicitly UTC"),
    ],
)
def test_resolution_attestation_rejects_invalid_verified_at(
    tmp_path, value, message
) -> None:
    path = tmp_path / "resolution_attestation.yml"
    payload = _valid_payload()
    payload["verified_at_utc"] = value
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_polygon_resolution_attestation(path)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("manifest_version", "1.0", "manifest_version"),
        ("schema_version", True, "must be an integer"),
    ],
)
def test_resolution_attestation_rejects_invalid_types_and_version(
    tmp_path, field, value, message
) -> None:
    path = tmp_path / "resolution_attestation.yml"
    payload = _valid_payload()
    payload[field] = value
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_polygon_resolution_attestation(path)


def test_resolution_attestation_rejects_manifest_mismatch(tmp_path) -> None:
    seed_path, _ = write_synthetic_distribution_inputs(tmp_path / "dbt")
    path = tmp_path / "resolution_attestation.yml"
    path.write_text(yaml.safe_dump(_valid_payload()), encoding="utf-8")

    with pytest.raises(ValueError, match="does not match"):
        load_polygon_resolution_attestation(
            path,
            manifest=load_polygon_market_seed(seed_path),
        )


@pytest.mark.parametrize(
    ("chain_evidence", "message"),
    [
        ({}, "missing finalized-head"),
        (
            {
                "resolution_count": 248,
                "finalized_head": {
                    "number": 123,
                    "hash": "0x" + "b" * 64,
                    "timestamp": "not-a-time",
                },
            },
            "timestamp is invalid",
        ),
        (
            {
                "resolution_count": 248,
                "finalized_head": {
                    "number": 123,
                    "hash": "0x" + "b" * 64,
                    "timestamp": "2026-07-22T12:00:00",
                },
            },
            "explicitly UTC",
        ),
    ],
)
def test_candidate_attestation_rejects_invalid_chain_evidence(
    tmp_path, chain_evidence, message
) -> None:
    with pytest.raises(ValueError, match=message):
        write_polygon_resolution_attestation(
            tmp_path / "resolution_attestation.yml",
            manifest_version="1.0.0",
            manifest_sha256="a" * 64,
            chain_evidence=chain_evidence,
            authoring_evidence_sha256="c" * 64,
        )

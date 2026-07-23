"""Load the reviewed WC2026 Polygon condition-resolution attestation."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import yaml

from oddsfox_pipeline.config.settings import DBT_PROJECT_DIR
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    EXPECTED_PROPOSITIONS,
    PolygonMarketManifest,
)

DEFAULT_POLYGON_RESOLUTION_ATTESTATION_PATH = (
    DBT_PROJECT_DIR.parent / "config" / "polygon-settlement-resolution-attestation.yml"
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BLOCK_HASH = re.compile(r"0x[0-9a-f]{64}\Z")
_SEMVER = re.compile(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\Z")
_FIELDS = {
    "schema_version",
    "manifest_version",
    "manifest_sha256",
    "resolved_condition_count",
    "verified_at_utc",
    "authoring_evidence_sha256",
    "finalized_head_block_number",
    "finalized_head_block_hash",
}


@dataclass(frozen=True)
class PolygonResolutionAttestation:
    """Reviewed aggregate evidence that every committed condition resolved."""

    schema_version: int
    manifest_version: str
    manifest_sha256: str
    resolved_condition_count: int
    verified_at_utc: datetime
    authoring_evidence_sha256: str
    finalized_head_block_number: int
    finalized_head_block_hash: str

    def public_summary(self) -> dict[str, Any]:
        """Return the locator-free fields safe for a technical export."""
        return {
            "manifest_version": self.manifest_version,
            "manifest_sha256": self.manifest_sha256,
            "resolved_condition_count": self.resolved_condition_count,
            "verified_at_utc": _utc_text(self.verified_at_utc),
            "authoring_evidence_sha256": self.authoring_evidence_sha256,
        }

    def as_mapping(self) -> dict[str, Any]:
        values = asdict(self)
        values["verified_at_utc"] = _utc_text(self.verified_at_utc)
        return values


def load_polygon_resolution_attestation(
    path: Path = DEFAULT_POLYGON_RESOLUTION_ATTESTATION_PATH,
    *,
    manifest: PolygonMarketManifest | None = None,
) -> PolygonResolutionAttestation:
    """Load and validate one strict reviewed attestation."""
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(
            f"Could not load Polygon resolution attestation: {path}"
        ) from exc
    if not isinstance(raw, Mapping) or set(raw) != _FIELDS:
        raise ValueError("Polygon resolution attestation fields are invalid")

    try:
        verified_at = datetime.fromisoformat(
            str(raw["verified_at_utc"]).replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError("Resolution verified_at_utc must be ISO-8601 UTC") from exc
    if verified_at.tzinfo is None or verified_at.utcoffset() is None:
        raise ValueError("Resolution verified_at_utc must be explicitly UTC")
    verified_at = verified_at.astimezone(timezone.utc)

    attestation = PolygonResolutionAttestation(
        schema_version=_strict_int(raw["schema_version"], "schema_version"),
        manifest_version=str(raw["manifest_version"]),
        manifest_sha256=str(raw["manifest_sha256"]),
        resolved_condition_count=_strict_int(
            raw["resolved_condition_count"], "resolved_condition_count"
        ),
        verified_at_utc=verified_at,
        authoring_evidence_sha256=str(raw["authoring_evidence_sha256"]),
        finalized_head_block_number=_strict_int(
            raw["finalized_head_block_number"], "finalized_head_block_number"
        ),
        finalized_head_block_hash=str(raw["finalized_head_block_hash"]),
    )
    _validate_attestation(attestation, manifest=manifest)
    return attestation


def write_polygon_resolution_attestation(
    path: Path,
    *,
    manifest_version: str,
    manifest_sha256: str,
    chain_evidence: Mapping[str, Any],
    authoring_evidence_sha256: str,
) -> PolygonResolutionAttestation:
    """Write a candidate attestation next to seed-authoring evidence."""
    finalized = chain_evidence.get("finalized_head")
    if not isinstance(finalized, Mapping):
        raise ValueError("Seed evidence is missing finalized-head metadata")
    attestation = PolygonResolutionAttestation(
        schema_version=1,
        manifest_version=manifest_version,
        manifest_sha256=manifest_sha256,
        resolved_condition_count=_strict_int(
            chain_evidence.get("resolution_count"), "resolution_count"
        ),
        verified_at_utc=_evidence_datetime(finalized.get("timestamp")),
        authoring_evidence_sha256=authoring_evidence_sha256,
        finalized_head_block_number=_strict_int(
            finalized.get("number"), "finalized_head.number"
        ),
        finalized_head_block_hash=str(finalized.get("hash", "")),
    )
    _validate_attestation(attestation)
    path.write_text(
        yaml.safe_dump(attestation.as_mapping(), sort_keys=False),
        encoding="utf-8",
        newline="\n",
    )
    return attestation


def _validate_attestation(
    attestation: PolygonResolutionAttestation,
    *,
    manifest: PolygonMarketManifest | None = None,
) -> None:
    if attestation.schema_version != 1:
        raise ValueError("Resolution attestation schema_version must be 1")
    if not _SEMVER.fullmatch(attestation.manifest_version):
        raise ValueError("Resolution attestation manifest_version is invalid")
    if not _SHA256.fullmatch(attestation.manifest_sha256):
        raise ValueError("Resolution attestation manifest_sha256 is invalid")
    if not _SHA256.fullmatch(attestation.authoring_evidence_sha256):
        raise ValueError("Resolution attestation evidence SHA-256 is invalid")
    if attestation.resolved_condition_count != EXPECTED_PROPOSITIONS:
        raise ValueError(
            f"Resolution attestation must cover {EXPECTED_PROPOSITIONS} conditions"
        )
    if attestation.finalized_head_block_number < 0 or not _BLOCK_HASH.fullmatch(
        attestation.finalized_head_block_hash
    ):
        raise ValueError("Resolution attestation finalized head is invalid")
    if manifest is not None and (
        attestation.manifest_version != manifest.version
        or attestation.manifest_sha256 != manifest.sha256
        or attestation.resolved_condition_count != len(manifest.markets)
    ):
        raise ValueError(
            "Resolution attestation does not match the committed Polygon manifest"
        )


def _strict_int(value: Any, field: str) -> int:
    if type(value) is not int:
        raise ValueError(f"Resolution attestation {field} must be an integer")
    return value


def _evidence_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("Seed evidence finalized timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Seed evidence finalized timestamp must be explicitly UTC")
    return parsed.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


__all__ = [
    "DEFAULT_POLYGON_RESOLUTION_ATTESTATION_PATH",
    "PolygonResolutionAttestation",
    "load_polygon_resolution_attestation",
    "write_polygon_resolution_attestation",
]

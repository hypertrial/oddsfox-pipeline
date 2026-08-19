"""Deterministic synthetic inputs for tests that require populated local seeds."""

from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path

from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    NEG_RISK_V2_EXCHANGE,
    SEED_COLUMNS,
    STANDARD_V2_EXCHANGE,
    parse_polygon_market,
    polygon_manifest_content_sha256,
)

REFERENCE_BUNDLE_ID = "synthetic-reference-v1"


def _polygon_stage(match_id: int) -> str:
    if match_id <= 72:
        return "group_stage"
    if match_id <= 88:
        return "round_of_32"
    if match_id <= 96:
        return "round_of_16"
    if match_id <= 100:
        return "quarterfinal"
    if match_id <= 102:
        return "semifinal"
    return "third_place" if match_id == 103 else "final"


def complete_polygon_seed_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    index = 0
    for match_id in range(1, 105):
        types = (
            ("home_win", "draw", "away_win")
            if match_id <= 72
            else (
                "home_win_third_place"
                if match_id == 103
                else "home_wins_final"
                if match_id == 104
                else "home_advances",
            )
        )
        kickoff = datetime(2026, 6, 1, tzinfo=timezone.utc) + timedelta(
            hours=match_id * 6
        )
        for proposition_type in types:
            index += 1
            duration = 150 if match_id <= 72 else 210
            structure = "neg_risk" if match_id <= 72 else "standard"
            rows.append(
                {
                    "proposition_id": f"m{match_id:03d}-{proposition_type}",
                    "fifa_match_id": str(match_id),
                    "stage": _polygon_stage(match_id),
                    "group_label": (
                        chr(65 + ((match_id - 1) % 12)) if match_id <= 72 else ""
                    ),
                    "home_team": f"Home {match_id}",
                    "away_team": f"Away {match_id}",
                    "kickoff_at_utc": kickoff.isoformat().replace("+00:00", "Z"),
                    "window_start_at_utc": kickoff.isoformat().replace("+00:00", "Z"),
                    "window_end_at_utc": (kickoff + timedelta(minutes=duration))
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "proposition_type": proposition_type,
                    "yes_represents": f"yes-{match_id}-{proposition_type}",
                    "no_represents": f"no-{match_id}-{proposition_type}",
                    "condition_id": f"0x{index:064x}",
                    "yes_token_id": str(index * 2 + 1000),
                    "no_token_id": str(index * 2 + 1001),
                    "market_structure": structure,
                    "exchange_address": (
                        STANDARD_V2_EXCHANGE
                        if structure == "standard"
                        else NEG_RISK_V2_EXCHANGE
                    ),
                    "reference_bundle_id": REFERENCE_BUNDLE_ID,
                    "reference_table": "wc2026_fixtures",
                    "reference_row_key": str(match_id),
                    "reference_row_sha256": f"{match_id:064x}",
                    "condition_init_tx_hash": f"0x{index + 1000:064x}",
                    "condition_init_log_index": str(index),
                    "question_init_tx_hash": f"0x{index + 2000:064x}",
                    "question_init_log_index": str(index + 1),
                    "ancillary_data_sha256": f"{index + 3000:064x}",
                    "token_verification_block_number": str(80_000_000 + index),
                    "token_verification_block_hash": f"0x{index + 4000:064x}",
                    "manifest_sha256": "0" * 64,
                    "manifest_version": "1.0.0",
                    "reviewed_at_utc": "2026-08-01T00:00:00Z",
                }
            )
    manifest_hash = polygon_manifest_content_sha256(
        parse_polygon_market(row) for row in rows
    )
    for row in rows:
        row["manifest_sha256"] = manifest_hash
    return rows


def _write_csv(
    path: Path,
    fieldnames: tuple[str, ...] | list[str],
    rows: list[dict[str, str]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_synthetic_distribution_inputs(dbt_root: Path) -> tuple[Path, Path]:
    """Populate a temporary dbt project without touching tracked seed shells."""
    seed_root = dbt_root / "seeds"
    polygon_rows = complete_polygon_seed_rows()
    polygon_path = seed_root / "polymarket_wc2026_polygon_settlement_markets.csv"
    _write_csv(polygon_path, list(SEED_COLUMNS), polygon_rows)

    attestation_path = (
        dbt_root.parent / "config/polygon-settlement-resolution-attestation.yml"
    )
    attestation_path.parent.mkdir(parents=True, exist_ok=True)
    attestation_path.write_text(
        "\n".join(
            (
                "schema_version: 1",
                "manifest_version: 1.0.0",
                f"manifest_sha256: {polygon_rows[0]['manifest_sha256']}",
                "resolved_condition_count: 248",
                'verified_at_utc: "2026-08-01T00:00:00Z"',
                f"authoring_evidence_sha256: {'b' * 64}",
                "finalized_head_block_number: 123",
                f'finalized_head_block_hash: "0x{"c" * 64}"',
            )
        )
        + "\n",
        encoding="utf-8",
    )
    return polygon_path, attestation_path

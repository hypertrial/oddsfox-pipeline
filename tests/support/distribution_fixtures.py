"""Deterministic synthetic inputs for tests that require populated local seeds."""

from __future__ import annotations

import csv
from datetime import date, datetime, timedelta, timezone
from itertools import combinations
from pathlib import Path

from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    NEG_RISK_V2_EXCHANGE,
    OPENFOOTBALL_REVISION,
    SEED_COLUMNS,
    STANDARD_V2_EXCHANGE,
    parse_polygon_market,
    polygon_manifest_content_sha256,
)


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
                    "openfootball_revision": OPENFOOTBALL_REVISION,
                    "openfootball_path": (
                        "2026--usa/cup.txt"
                        if match_id <= 72
                        else "2026--usa/cup_finals.txt"
                    ),
                    "openfootball_source_lines": f"{match_id}-{match_id + 1}",
                    "openfootball_line_hash": f"{match_id:064x}",
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

    schedule_fields = (
        "match_id",
        "stage",
        "group_label",
        "matchday",
        "match_date",
        "kickoff_time_et",
        "venue",
        "home_slot",
        "away_slot",
        "home_team",
        "away_team",
        "status",
        "source",
    )
    stage_by_match = (
        [(72, "Group Stage"), (88, "Round of 32"), (96, "Round of 16")]
        + [(100, "Quarter-final"), (102, "Semi-final")]
        + [(103, "Third-place"), (104, "Final")]
    )
    schedule_rows = []
    for match_id in range(1, 105):
        stage = next(name for ceiling, name in stage_by_match if match_id <= ceiling)
        match_date = date(2026, 6, 1) + timedelta(days=(match_id - 1) // 4)
        schedule_rows.append(
            {
                "match_id": str(match_id),
                "stage": stage,
                "group_label": (
                    chr(65 + ((match_id - 1) % 12)) if stage == "Group Stage" else ""
                ),
                "matchday": str(((match_id - 1) // 24) + 1),
                "match_date": match_date.isoformat(),
                "kickoff_time_et": "01:00 PM",
                "venue": f"Synthetic Venue {((match_id - 1) % 16) + 1}",
                "home_slot": f"Synthetic Home Slot {match_id}",
                "away_slot": f"Synthetic Away Slot {match_id}",
                "home_team": f"Synthetic Home {match_id}",
                "away_team": f"Synthetic Away {match_id}",
                "status": "scheduled",
                "source": "synthetic-test-fixture",
            }
        )
    _write_csv(
        seed_root / "wc2026_schedule_matches.csv", schedule_fields, schedule_rows
    )

    third_fields = (
        "option_id",
        "slot_1a_group",
        "slot_1b_group",
        "slot_1d_group",
        "slot_1e_group",
        "slot_1g_group",
        "slot_1i_group",
        "slot_1k_group",
        "slot_1l_group",
    )
    third_rows = []
    for option_id, groups in enumerate(combinations("ABCDEFGHIJKL", 8), start=1):
        third_rows.append(
            {"option_id": str(option_id)}
            | dict(zip(third_fields[1:], groups, strict=True))
        )
    _write_csv(
        seed_root / "wc2026_third_place_options.csv",
        third_fields,
        third_rows,
    )

    venue_fields = (
        "venue",
        "host_city",
        "host_country",
        "venue_lat",
        "venue_lon",
        "venue_timezone",
        "venue_altitude_m",
    )
    venue_rows = [
        {
            "venue": f"Synthetic Venue {index}",
            "host_city": f"Synthetic City {index}",
            "host_country": "Synthetic Country",
            "venue_lat": f"{30 + index / 10:.1f}",
            "venue_lon": f"{-100 + index / 10:.1f}",
            "venue_timezone": "UTC",
            "venue_altitude_m": str(index * 10),
        }
        for index in range(1, 17)
    ]
    _write_csv(seed_root / "wc2026_venues.csv", venue_fields, venue_rows)

    camp_fields = (
        "team_name_fifa",
        "team_name_model",
        "base_camp_market",
        "base_camp_country",
        "training_site_name",
        "training_site_lat",
        "training_site_lon",
        "training_site_timezone",
        "training_site_altitude_m",
        "geocode_quality",
        "geocode_source",
        "manual_review_status",
        "source_url",
        "source_updated_at",
        "notes",
    )
    camp_rows = [
        {
            "team_name_fifa": f"Synthetic Team {index}",
            "team_name_model": f"Synthetic Team {index}",
            "base_camp_market": f"Synthetic Camp {index}",
            "base_camp_country": "Synthetic Country",
            "training_site_name": f"Synthetic Training Site {index}",
            "training_site_lat": f"{35 + index / 100:.2f}",
            "training_site_lon": f"{-95 + index / 100:.2f}",
            "training_site_timezone": "UTC",
            "training_site_altitude_m": str(index),
            "geocode_quality": "synthetic",
            "geocode_source": "synthetic-test-fixture",
            "manual_review_status": "synthetic",
            "source_url": "https://example.invalid/synthetic",
            "source_updated_at": "2026-01-01",
            "notes": "Synthetic test fixture",
        }
        for index in range(1, 49)
    ]
    _write_csv(seed_root / "wc2026_base_camps_teams.csv", camp_fields, camp_rows)

    taxonomy_fields = (
        "tournament",
        "is_friendly",
        "is_competitive",
        "competition_family",
        "confederation_scope",
        "notes",
    )
    _write_csv(
        seed_root / "wc2026_tournament_classification.csv",
        taxonomy_fields,
        [
            {
                "tournament": "Synthetic Friendly",
                "is_friendly": "true",
                "is_competitive": "false",
                "competition_family": "friendly",
                "confederation_scope": "synthetic",
                "notes": "Synthetic test fixture",
            }
        ],
    )

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

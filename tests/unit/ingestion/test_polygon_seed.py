from __future__ import annotations

import csv
from dataclasses import replace
from datetime import timedelta

import pytest
from tests.support.distribution_fixtures import (
    complete_polygon_seed_rows as complete_seed_rows,
)

from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    NEG_RISK_V2_EXCHANGE,
    SEED_COLUMNS,
    STANDARD_V2_EXCHANGE,
    load_polygon_market_seed,
    parse_polygon_market,
    validate_polygon_market_manifest,
)


def test_load_polygon_seed_validates_complete_logical_hash(tmp_path) -> None:
    rows = complete_seed_rows()
    path = tmp_path / "seed.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SEED_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    manifest = load_polygon_market_seed(path)

    assert len(manifest.markets) == 248
    assert len(manifest.by_token) == 496
    assert manifest.version == "1.0.0"
    assert manifest.sha256 == rows[0]["manifest_sha256"]
    assert manifest.markets[0].exchange_address == NEG_RISK_V2_EXCHANGE


def test_seed_parser_rejects_bad_stage_window_token_and_exchange() -> None:
    row = complete_seed_rows()[0]
    mutations = (
        ({"stage": "final"}, "expected stage"),
        ({"window_end_at_utc": row["kickoff_at_utc"]}, "fixed 150-minute"),
        ({"yes_token_id": "01"}, "Invalid token ID"),
        ({"exchange_address": STANDARD_V2_EXCHANGE}, "Invalid V2 exchange"),
        ({"reviewed_at_utc": "2026-01-01T00:00:01Z"}, "minute-aligned"),
    )
    for changes, message in mutations:
        with pytest.raises(ValueError, match=message):
            parse_polygon_market({**row, **changes})


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"proposition_id": ""}, "must be non-empty"),
        ({"proposition_id": "Bad ID"}, "Invalid proposition_id"),
        ({"fifa_match_id": "-1"}, "non-negative integer"),
        ({"fifa_match_id": "１"}, "non-negative integer"),
        ({"kickoff_at_utc": "not-a-date"}, "ISO-8601"),
        ({"kickoff_at_utc": "2026-06-01T06:00:00"}, "explicitly UTC"),
        ({"kickoff_at_utc": "2026-06-01T07:00:00+01:00"}, "explicitly UTC"),
        ({"condition_id": "0x1234"}, "Invalid condition_id"),
        ({"yes_token_id": "0"}, "Invalid token ID"),
        ({"yes_token_id": str(2**256)}, "Invalid token ID"),
        ({"no_token_id": "1002"}, "token IDs must differ"),
        ({"market_structure": "other"}, "Invalid V2 exchange"),
        ({"condition_init_tx_hash": "0x1234"}, "canonical 32-byte hex"),
        ({"openfootball_line_hash": "A" * 64}, "lowercase SHA-256"),
        ({"openfootball_revision": "a" * 39}, "pinned OpenFootball"),
        ({"openfootball_path": "cup.txt"}, "pinned OpenFootball"),
        ({"openfootball_source_lines": "0"}, "source lines"),
        ({"openfootball_source_lines": "2-1"}, "source lines are reversed"),
        ({"manifest_version": "v1"}, "plain SemVer"),
        ({"away_team": "home 1"}, "teams must differ"),
        ({"no_represents": "YES-1-HOME_WIN"}, "semantics must differ"),
        ({"condition_init_log_index": str(2**63)}, "log index exceeds"),
        ({"question_init_log_index": str(2**63)}, "log index exceeds"),
        ({"token_verification_block_number": "0"}, "verification block is invalid"),
        (
            {"token_verification_block_number": str(2**63)},
            "verification block is invalid",
        ),
    ],
)
def test_seed_parser_rejects_each_provenance_and_semantic_boundary(
    changes, message
) -> None:
    row = complete_seed_rows()[0]
    with pytest.raises(ValueError, match=message):
        parse_polygon_market({**row, **changes})


def test_manifest_rejects_duplicate_inventory_and_declared_hash() -> None:
    parsed = tuple(parse_polygon_market(row) for row in complete_seed_rows())
    with pytest.raises(ValueError, match="token"):
        validate_polygon_market_manifest(
            (*parsed[:-1], replace(parsed[-1], yes_token_id=parsed[0].yes_token_id))
        )
    with pytest.raises(ValueError, match="manifest_sha256"):
        validate_polygon_market_manifest(
            (replace(parsed[0], manifest_sha256="f" * 64), *parsed[1:])
        )


def test_manifest_rejects_every_inventory_and_fixture_invariant() -> None:
    parsed = tuple(parse_polygon_market(row) for row in complete_seed_rows())
    cases = [
        (parsed[:-1], "Expected 248 propositions"),
        (
            (replace(parsed[0], proposition_id=parsed[1].proposition_id), *parsed[1:]),
            "proposition_id values must be unique",
        ),
        (
            (replace(parsed[0], condition_id=parsed[1].condition_id), *parsed[1:]),
            "condition_id values must be unique",
        ),
        (
            (
                replace(
                    parsed[0],
                    condition_init_tx_hash=parsed[1].condition_init_tx_hash,
                    condition_init_log_index=parsed[1].condition_init_log_index,
                ),
                *parsed[1:],
            ),
            "Condition initialization locators must be unique",
        ),
        (
            (
                replace(
                    parsed[0],
                    question_init_tx_hash=parsed[1].question_init_tx_hash,
                    question_init_log_index=parsed[1].question_init_log_index,
                ),
                *parsed[1:],
            ),
            "Question initialization locators must be unique",
        ),
        (
            (replace(parsed[0], fifa_match_id=105), *parsed[1:]),
            "match IDs must be exactly",
        ),
        (
            (replace(parsed[0], stage="final"), *parsed[1:]),
            "stage distribution",
        ),
        (
            (
                replace(parsed[0], openfootball_revision="a" * 40),
                *parsed[1:],
            ),
            "pinned OpenFootball revision",
        ),
        (
            (
                replace(
                    parsed[0],
                    reviewed_at_utc=parsed[0].reviewed_at_utc + timedelta(minutes=1),
                ),
                *parsed[1:],
            ),
            "one review timestamp",
        ),
        (
            (
                replace(
                    parsed[0],
                    market_structure="standard",
                    exchange_address=STANDARD_V2_EXCHANGE,
                ),
                *parsed[1:],
            ),
            "group propositions must use the neg-risk",
        ),
        (
            (
                *parsed[:216],
                replace(
                    parsed[216],
                    market_structure="neg_risk",
                    exchange_address=NEG_RISK_V2_EXCHANGE,
                ),
                *parsed[217:],
            ),
            "knockout propositions must use the standard",
        ),
        (
            (parsed[0], replace(parsed[1], home_team="Different"), *parsed[2:]),
            "Inconsistent repeated fixture facts",
        ),
        (
            (
                *(replace(row, group_label=None) for row in parsed[:3]),
                *parsed[3:],
            ),
            "three canonical propositions",
        ),
        (
            (
                *parsed[:216],
                replace(parsed[216], proposition_type="home_win"),
                *parsed[217:],
            ),
            "must have proposition",
        ),
        (
            (
                *parsed[:-1],
                replace(parsed[-1], manifest_version="2.0.0"),
            ),
            "one manifest_version",
        ),
        (
            tuple(replace(row, manifest_sha256="g" * 64) for row in parsed),
            "lowercase manifest_sha256",
        ),
        (
            tuple(replace(row, manifest_sha256="f" * 64) for row in parsed),
            "canonical logical seed content",
        ),
    ]
    for rows, message in cases:
        with pytest.raises(ValueError, match=message):
            validate_polygon_market_manifest(rows)


def test_manifest_rejects_group_and_knockout_row_counts() -> None:
    parsed = tuple(parse_polygon_market(row) for row in complete_seed_rows())
    match_two = parsed[3]
    moved_group = replace(
        parsed[0],
        fifa_match_id=2,
        stage=match_two.stage,
        group_label=match_two.group_label,
        home_team=match_two.home_team,
        away_team=match_two.away_team,
        kickoff_at_utc=match_two.kickoff_at_utc,
        window_start_at_utc=match_two.window_start_at_utc,
        window_end_at_utc=match_two.window_end_at_utc,
        openfootball_revision=match_two.openfootball_revision,
        openfootball_path=match_two.openfootball_path,
        openfootball_source_lines=match_two.openfootball_source_lines,
        openfootball_line_hash=match_two.openfootball_line_hash,
    )
    with pytest.raises(ValueError, match="three canonical propositions"):
        validate_polygon_market_manifest((moved_group, *parsed[1:]))

    match_74 = parsed[217]
    moved_knockout = replace(
        parsed[0],
        fifa_match_id=74,
        stage=match_74.stage,
        group_label=match_74.group_label,
        home_team=match_74.home_team,
        away_team=match_74.away_team,
        kickoff_at_utc=match_74.kickoff_at_utc,
        window_start_at_utc=match_74.window_start_at_utc,
        window_end_at_utc=match_74.window_end_at_utc,
        openfootball_revision=match_74.openfootball_revision,
        openfootball_path=match_74.openfootball_path,
        openfootball_source_lines=match_74.openfootball_source_lines,
        openfootball_line_hash=match_74.openfootball_line_hash,
        market_structure=match_74.market_structure,
        exchange_address=match_74.exchange_address,
    )
    with pytest.raises(ValueError, match="must have proposition"):
        validate_polygon_market_manifest((moved_knockout, *parsed[1:]))


def test_loader_rejects_unreviewed_schema(tmp_path) -> None:
    path = tmp_path / "bad.csv"
    path.write_text("proposition_id,gamma_slug\nx,y\n", encoding="utf-8")
    with pytest.raises(ValueError, match="headers"):
        load_polygon_market_seed(path)


@pytest.mark.parametrize(
    "prohibited_field",
    ["gamma_market_id", "event_slug", "market_title", "ui_question", "clob_token_id"],
)
def test_loader_rejects_gamma_slug_ui_and_clob_headers(
    tmp_path, prohibited_field
) -> None:
    assert prohibited_field not in SEED_COLUMNS
    path = tmp_path / f"bad-{prohibited_field}.csv"
    path.write_text(
        ",".join((*SEED_COLUMNS, prohibited_field)) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="headers"):
        load_polygon_market_seed(path)

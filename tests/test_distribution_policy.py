"""Repository distribution boundary checks."""

from __future__ import annotations

import csv
import re
import subprocess
from pathlib import Path

import pytest

from tests.support.distribution_fixtures import write_synthetic_distribution_inputs

pytestmark = pytest.mark.repo_check

REPO_ROOT = Path(__file__).resolve().parent.parent

DATA_LIKE_SUFFIXES = {
    ".csv",
    ".db",
    ".duckdb",
    ".json",
    ".parquet",
    ".pdf",
    ".sqlite",
    ".sqlite3",
    ".yaml",
    ".yml",
}

ALLOWED_DATA_LIKE_FILES = {
    ".github/ISSUE_TEMPLATE/bug_report.yml",
    ".github/ISSUE_TEMPLATE/documentation.yml",
    ".github/ISSUE_TEMPLATE/feature_request.yml",
    ".github/dependabot.yml",
    ".github/workflows/ci.yml",
    ".github/workflows/manual-full.yml",
    "config/live-smoke.yaml",
    "config/polygon-settlement-resolution-attestation.example.yml",
    "dagster_instance.yaml",
    "dbt/dbt_project.yml",
    "dbt/models/international_results_wc2026/intermediate/intermediate.yml",
    "dbt/models/international_results_wc2026/marts/international_results_wc2026.yml",
    "dbt/models/international_results_wc2026/observability/observability.yml",
    "dbt/models/international_results_wc2026/staging/staging.yml",
    "dbt/models/kalshi_wc2026/intermediate/intermediate.yml",
    "dbt/models/kalshi_wc2026/intermediate/match_odds.yml",
    "dbt/models/kalshi_wc2026/marts/kalshi_wc2026.yml",
    "dbt/models/openfootball_wc2026/staging/staging.yml",
    "dbt/models/polymarket_us_midterms_2026/intermediate/intermediate.yml",
    "dbt/models/polymarket_us_midterms_2026/marts/polymarket_us_midterms_2026.yml",
    "dbt/models/polymarket_us_midterms_2026/observability/observability.yml",
    "dbt/models/polymarket_us_midterms_2026/staging/staging.yml",
    "dbt/models/polymarket_wc2026/intermediate/intermediate.yml",
    "dbt/models/polymarket_wc2026/intermediate/match_minute.yml",
    "dbt/models/polymarket_wc2026/intermediate/match_odds.yml",
    "dbt/models/polymarket_wc2026/marts/polymarket_wc2026.yml",
    "dbt/models/polymarket_wc2026/observability/observability.yml",
    "dbt/models/polymarket_wc2026/polygon_settlement.yml",
    "dbt/models/polymarket_wc2026/staging/match_minute.yml",
    "dbt/models/polymarket_wc2026/staging/staging.yml",
    "dbt/models/sources/international_results_wc2026_sources.yml",
    "dbt/models/sources/kalshi_wc2026_sources.yml",
    "dbt/models/sources/openfootball_wc2026_sources.yml",
    "dbt/models/sources/polymarket_us_midterms_2026_sources.yml",
    "dbt/models/sources/polymarket_wc2026_sources.yml",
    "dbt/models/sources/wc2026_canonical_raw_sources.yml",
    "dbt/models/wc2026/intermediate/intermediate.yml",
    "dbt/models/wc2026/marts/wc2026.yml",
    "dbt/models/wc2026/observability/observability.yml",
    "dbt/profiles/profiles.yml",
    "dbt/seeds/international_results_wc2026_team_aliases.csv",
    "dbt/seeds/kalshi_wc2026_contract.csv",
    "dbt/seeds/polymarket_us_midterms_2026_contract.csv",
    "dbt/seeds/polymarket_wc2026_contract.csv",
    "dbt/seeds/polymarket_wc2026_polygon_settlement_markets.csv",
    "dbt/seeds/schema.yml",
    "dbt/seeds/wc2026_base_camps_teams.csv",
    "dbt/seeds/wc2026_schedule_matches.csv",
    "dbt/seeds/wc2026_team_canonical_aliases.csv",
    "dbt/seeds/wc2026_third_place_options.csv",
    "dbt/seeds/wc2026_tournament_classification.csv",
    "dbt/seeds/wc2026_venues.csv",
    "mkdocs.yml",
    "src/oddsfox_pipeline/ingestion/kalshi/seeds/market_scopes.yml",
    "src/oddsfox_pipeline/ingestion/polymarket/seeds/market_scopes.yml",
    "tests/fixtures/cassettes/international_results_revision.yml",
    "tests/fixtures/cassettes/kalshi_events_markets_candlesticks.yml",
    "tests/fixtures/cassettes/polymarket_clob_minute_history.yml",
    "tests/fixtures/cassettes/polymarket_gamma_market_event.yml",
    "tests/fixtures/golden/international_results_wc2026_team_status.csv",
    "tests/fixtures/golden/kalshi_wc2026_hourly_odds.csv",
    "tests/fixtures/golden/polymarket_us_midterms_2026_market_token_hourly_odds.csv",
    "tests/fixtures/golden/polymarket_wc2026_knockout_token_hourly_odds.csv",
    "tests/fixtures/golden/wc2026_knockout_match_hourly_odds.csv",
    "vercel.json",
    "workspace.yaml",
}

HEADER_ONLY_SEEDS = {
    "dbt/seeds/polymarket_wc2026_polygon_settlement_markets.csv": (
        "proposition_id,fifa_match_id,stage,group_label,home_team,away_team,"
        "kickoff_at_utc,window_start_at_utc,window_end_at_utc,proposition_type,"
        "yes_represents,no_represents,condition_id,yes_token_id,no_token_id,"
        "market_structure,exchange_address,openfootball_revision,"
        "openfootball_path,openfootball_source_lines,openfootball_line_hash,"
        "condition_init_tx_hash,condition_init_log_index,question_init_tx_hash,"
        "question_init_log_index,ancillary_data_sha256,"
        "token_verification_block_number,token_verification_block_hash,"
        "manifest_sha256,manifest_version,reviewed_at_utc"
    ),
    "dbt/seeds/wc2026_base_camps_teams.csv": (
        "team_name_fifa,team_name_model,base_camp_market,base_camp_country,"
        "training_site_name,training_site_lat,training_site_lon,"
        "training_site_timezone,training_site_altitude_m,geocode_quality,"
        "geocode_source,manual_review_status,source_url,source_updated_at,notes"
    ),
    "dbt/seeds/wc2026_schedule_matches.csv": (
        "match_id,stage,group_label,matchday,match_date,kickoff_time_et,venue,"
        "home_slot,away_slot,home_team,away_team,status,source"
    ),
    "dbt/seeds/wc2026_third_place_options.csv": (
        "option_id,slot_1a_group,slot_1b_group,slot_1d_group,slot_1e_group,"
        "slot_1g_group,slot_1i_group,slot_1k_group,slot_1l_group"
    ),
    "dbt/seeds/wc2026_tournament_classification.csv": (
        "tournament,is_friendly,is_competitive,competition_family,"
        "confederation_scope,notes"
    ),
    "dbt/seeds/wc2026_venues.csv": (
        "venue,host_city,host_country,venue_lat,venue_lon,venue_timezone,"
        "venue_altitude_m"
    ),
}


def _tracked_files() -> set[str]:
    completed = subprocess.run(
        ["git", "ls-files"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return set(completed.stdout.splitlines())


def _indexed_text(relative_path: str) -> str:
    completed = subprocess.run(
        ["git", "show", f":{relative_path}"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def test_tracked_data_like_files_match_the_reviewed_allowlist() -> None:
    actual = {
        name
        for name in _tracked_files()
        if Path(name).suffix.casefold() in DATA_LIKE_SUFFIXES
    }
    assert actual == ALLOWED_DATA_LIKE_FILES
    assert not {
        name
        for name in actual
        if Path(name).suffix.casefold()
        in {".db", ".duckdb", ".parquet", ".pdf", ".sqlite", ".sqlite3"}
    }


def test_external_seed_shells_contain_only_the_exact_header() -> None:
    for relative_path, expected_header in HEADER_ONLY_SEEDS.items():
        assert _indexed_text(relative_path) == expected_header + "\n"


def test_synthetic_factory_populates_only_a_temporary_project(tmp_path: Path) -> None:
    dbt_root = tmp_path / "dbt"
    polygon_path, attestation_path = write_synthetic_distribution_inputs(dbt_root)

    expected_rows = {
        polygon_path: 248,
        dbt_root / "seeds/wc2026_schedule_matches.csv": 104,
        dbt_root / "seeds/wc2026_third_place_options.csv": 495,
        dbt_root / "seeds/wc2026_base_camps_teams.csv": 48,
        dbt_root / "seeds/wc2026_venues.csv": 16,
        dbt_root / "seeds/wc2026_tournament_classification.csv": 1,
    }
    for path, expected_count in expected_rows.items():
        with path.open(encoding="utf-8", newline="") as handle:
            assert len(list(csv.DictReader(handle))) == expected_count
    assert attestation_path.is_file()


def test_reviewed_resolution_attestation_is_operator_local() -> None:
    example = REPO_ROOT / "config/polygon-settlement-resolution-attestation.example.yml"
    assert "config/polygon-settlement-resolution-attestation.yml" not in (
        _tracked_files()
    )
    assert "/config/polygon-settlement-resolution-attestation.yml" in (
        REPO_ROOT / ".gitignore"
    ).read_text(encoding="utf-8")
    text = example.read_text(encoding="utf-8")
    assert "REPLACE_WITH_LOCAL_" in text
    assert "resolved_condition_count: 0" in text
    assert not re.search(r"(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])", text)


def test_distribution_notices_and_package_scope_are_explicit() -> None:
    notices = (REPO_ROOT / "THIRD_PARTY_NOTICES.md").read_text(encoding="utf-8")
    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    fixtures = (REPO_ROOT / "tests/fixtures/README.md").read_text(encoding="utf-8")
    seeds = (REPO_ROOT / "dbt/seeds/README.md").read_text(encoding="utf-8")

    assert "Hypertrial is a project name, not a legal entity" in notices
    assert "contain no production datasets" in notices
    assert "No third-party material is relicensed" in notices
    assert "OddsFox name, logo, favicon" in notices
    assert 'license = "MIT"' in pyproject
    assert 'license-files = ["LICENSE", "THIRD_PARTY_NOTICES.md"]' in pyproject
    assert "LICENSE THIRD_PARTY_NOTICES.md" in dockerfile
    assert "synthetic, Hypertrial-authored inputs" in fixtures
    assert "Header-only schema shells" in seeds
    assert (REPO_ROOT / "docs/assets/fonts/INTER-OFL.txt").is_file()
    assert (REPO_ROOT / "docs/assets/fonts/JETBRAINS-MONO-OFL.txt").is_file()


def test_current_project_docs_do_not_frame_external_publication() -> None:
    paths = [
        REPO_ROOT / "README.md",
        REPO_ROOT / "AGENTS.md",
        REPO_ROOT / "CONTRIBUTING.md",
        REPO_ROOT / "CHANGELOG.md",
        *(REPO_ROOT / "docs").rglob("*.md"),
    ]
    forbidden = (
        "kaggle",
        "external publisher",
        "publisher identity",
        "dataset licensing",
        "dataset licence",
        "legal review",
        "rights review",
        "cc by",
    )
    violations = []
    for path in paths:
        text = path.read_text(encoding="utf-8").casefold()
        for phrase in forbidden:
            if phrase in text:
                violations.append(f"{path.relative_to(REPO_ROOT)}: {phrase}")
    assert not violations, "\n".join(violations)


def test_project_identity_is_not_assigned_to_an_external_organization() -> None:
    forbidden_name = "Tri" + "lemma"
    violations = []
    for relative_path in _tracked_files():
        path = REPO_ROOT / relative_path
        if not path.is_file() or b"\0" in path.read_bytes()[:8192]:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if forbidden_name.casefold() in text.casefold():
            violations.append(relative_path)
    assert not violations

"""Fail closed when retired terminology reappears in active first-party surfaces."""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

pytestmark = pytest.mark.repo_check

REPO_ROOT = Path(__file__).resolve().parent.parent

SCAN_SUFFIXES = {
    ".py",
    ".sql",
    ".md",
    ".yml",
    ".yaml",
    ".toml",
    ".csv",
    ".sh",
    ".txt",
}
SCAN_ROOT_PREFIXES = (
    "src/",
    "dbt/models/",
    "dbt/tests/",
    "dbt/seeds/",
    "dbt/macros/",
    "dbt/dbt_project.yml",
    "dbt/README.md",
    "scripts/",
    "docs/",
    "tests/",
    "config/",
    ".env.example",
    "README.md",
    "AGENTS.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "THIRD_PARTY_NOTICES.md",
    "mkdocs.yml",
    "Makefile",
    ".github/workflows/",
)
EXCLUDE_PATH_PREFIXES = (
    "uv.lock",
    "dbt/target/",
    "dbt/dbt_packages/",
    "tests/fixtures/cassettes/",
    "tests/fixtures/polymarket_wc2026_logical_v1/",
    "tests/test_terminology_policy.py",
    "CHANGELOG.md",
    "build/",
    ".venv/",
)
EXCLUDE_PATH_PARTS = {
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    "node_modules",
}


@dataclass(frozen=True)
class Rule:
    rule_id: str
    pattern: re.Pattern[str]
    prefer: str
    allow_path_substrings: tuple[str, ...] = ()
    allow_line_regexes: tuple[re.Pattern[str], ...] = ()


RULES: tuple[Rule, ...] = (
    Rule(
        "retired_graph_export_product",
        re.compile(
            r"\bgraph export\b|\bgraph_export\b|graph_token_hourly|graph_hourly"
        ),
        "logical-v1 bundle / polymarket-wc2026-logical-v1",
        allow_path_substrings=(
            "tests/test_naming_policy.py",
            "tests/dbt/test_project_files.py",
        ),
        allow_line_regexes=(
            re.compile(r"legacy graph export", re.IGNORECASE),
            re.compile(r"Breaking cutover from the legacy graph export"),
            re.compile(r"prior hourly graph mart/export"),
            re.compile(r"Avoid \| Prefer"),
            re.compile(r"graph odds / graph export / graph bundle"),
            re.compile(r"not in "),
            re.compile(r"assert .*not"),
        ),
    ),
    Rule(
        "ambiguous_graph_bundle",
        re.compile(r"\bgraph bundle\b"),
        "logical-v1 bundle",
        allow_line_regexes=(
            re.compile(r"logical[- ]graph bundle", re.IGNORECASE),
            re.compile(r"graph odds / graph export / graph bundle"),
        ),
    ),
    Rule(
        "internal_graph_contract_identifier",
        re.compile(r"\bgraph_contract\b|polymarket_wc2026_graph_contract"),
        "polymarket_wc2026_logical_contract",
        allow_line_regexes=(
            re.compile(r"graph contract \(internal\)"),
            re.compile(r"Avoid \| Prefer"),
        ),
    ),
    Rule(
        "intermediate_graph_model_names",
        re.compile(r"int_polymarket_wc2026_graph_(markets|market_events)"),
        "int_polymarket_wc2026_logical_*",
    ),
    Rule(
        "retired_logical_eligibility_flags",
        re.compile(
            r"\b(is_graph_event|event_graph_eligible|graph_usable|"
            r"graph_usable_market_count|graph_market_count|"
            r"graph_event_volume_min_usd)\b"
        ),
        "is_logical_event / event_logical_eligible / logical_usable / logical_*",
    ),
    Rule(
        "retired_schedule_fixture_names",
        re.compile(
            r"stg_openfootball_wc2026_knockout_fixtures|"
            r"openfootball_wc2026_raw_knockout_fixtures|"
            r"openfootball/wc2026/(raw|staging)/knockout_fixtures|"
            r"ingestion/openfootball/knockout_fixtures\.py"
        ),
        "schedule_fixtures",
        allow_line_regexes=(
            re.compile(r"int_wc2026_knockout_fixtures"),
            re.compile(r"knockout fixtures \(matches 1–104\)"),
            re.compile(r"Avoid \| Prefer"),
        ),
    ),
    Rule(
        "retired_scope_registry_module",
        re.compile(
            r"orchestration/scope_registry\.py|"
            r"from oddsfox_pipeline\.orchestration\.scope_registry|"
            r"import oddsfox_pipeline\.orchestration\.scope_registry"
        ),
        "shipped_scopes.py",
    ),
    Rule(
        "retired_job_suffix_market_registry_refresh",
        re.compile(
            r"\b(?:polymarket_wc2026|polymarket_us_midterms_2026|kalshi_wc2026)"
            r"_market_registry_refresh\b"
        ),
        "*_market_scope_registry_refresh",
    ),
    Rule(
        "retired_cli_registry_step",
        re.compile(
            r'ScopeStep\s*=\s*Literal\[[^\]]*"registry"[^\]]*\]|'
            r'SCOPE_STEPS[^\n]*"registry"|'
            r"--step\s+registry\b|"
            r'"registry":\s*self\.registry_job_name'
        ),
        'ScopeStep "market_registry"',
    ),
    Rule(
        "retired_membership_scope_class",
        re.compile(r"\bscope_class\b"),
        "membership_class",
        allow_line_regexes=(
            re.compile(r"scope class \(review taxonomy\)"),
            re.compile(r"Avoid \| Prefer"),
        ),
    ),
    Rule(
        "retired_metadata_backfill_asset",
        re.compile(
            r"market_metadata_backfill|MetadataBackfillConfig|"
            r"_materialize_metadata_backfill|\bbackfill_market_metadata\b"
        ),
        "market_metadata_enrichment / MetadataEnrichmentConfig / enrich_market_metadata",
        allow_line_regexes=(
            re.compile(r"metadata backfill \(non-historical\)"),
            re.compile(r"Avoid \| Prefer"),
        ),
    ),
    Rule(
        "retired_pipeline_run_events",
        re.compile(
            r"\bpipeline_run_events\b|append_pipeline_run_event|"
            r"seed_test_(?:kalshi_)?pipeline_run_event|"
            r"stg_\w+_pipeline_run_events|"
            r"polymarket_pipeline_run_events_sql|"
            r"pipeline_run_event_append_"
        ),
        "ingestion_run_events",
        allow_line_regexes=(
            re.compile(r"pipeline run events \(ops telemetry\)"),
            re.compile(r"Avoid \| Prefer"),
            re.compile(r"dlt\.pipeline"),
            re.compile(r"oddsfox_pipeline"),
            re.compile(r"_BATCH_PIPELINE_RUN_ID"),
            re.compile(r"_full_pipeline"),
            re.compile(r"pipeline_run_config"),
        ),
    ),
    Rule(
        "retired_sync_run_observability",
        re.compile(
            r"\w*_sync_run_observability\b|polymarket_sync_run_observability_sql|"
            r"assert_mart_sync_run_observability_"
        ),
        "*_ingestion_run_observability",
        allow_line_regexes=(
            re.compile(r"sync run observability"),
            re.compile(r"Avoid \| Prefer"),
            re.compile(r"sync_run_metrics"),
        ),
    ),
    Rule(
        "retired_threshold_contract_seeds",
        re.compile(
            r"\b(?:polymarket_wc2026|polymarket_us_midterms_2026|kalshi_wc2026)"
            r"_contract(?:\.csv)?\b|"
            r"ref\(['\"](?:polymarket_wc2026|polymarket_us_midterms_2026|"
            r"kalshi_wc2026)_contract['\"]\)"
        ),
        "*_pipeline_policy",
        allow_line_regexes=(
            re.compile(r"logical_contract"),
            re.compile(r"wc2026_contract_metadata"),
            re.compile(r"contract_metadata"),
            re.compile(r"CONTRACT_DEFAULTS"),
            re.compile(r"Avoid \| Prefer"),
        ),
    ),
    Rule(
        "retired_cadence_issue_type",
        re.compile(r"issue_type\s*=\s*['\"]cadence['\"]"),
        "observation_gap",
    ),
    Rule(
        "retired_minutely_config",
        re.compile(r"\bminutely\b|rebuild_minutely|minutely_backfill"),
        "minute-grain / match-minute",
        allow_path_substrings=("tests/test_naming_policy.py",),
        allow_line_regexes=(
            re.compile(r"Never use \*\*minutely\*\*"),
            re.compile(r"\| minutely \|"),
            re.compile(r"old minutely-oriented names are not accepted", re.IGNORECASE),
            re.compile(r"Avoid \| Prefer"),
        ),
    ),
    Rule(
        "inverted_namespace",
        re.compile(r"(?<!polymarket_)(?<!kalshi_)wc2026_polymarket|WC2026_POLYMARKET"),
        "polymarket_wc2026",
        allow_path_substrings=("tests/test_naming_policy.py",),
        allow_line_regexes=(
            re.compile(r"instead of"),
            re.compile(r"not in "),
            re.compile(r"assert .*not"),
            re.compile(r"OLD_"),
        ),
    ),
)


def _iter_scan_paths() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail("git ls-files failed; cannot enforce terminology policy")
    paths: list[Path] = []
    for line in result.stdout.splitlines():
        rel = line.strip()
        if not rel:
            continue
        if any(
            rel == prefix or rel.startswith(prefix) for prefix in EXCLUDE_PATH_PREFIXES
        ):
            continue
        if any(part in EXCLUDE_PATH_PARTS for part in Path(rel).parts):
            continue
        if not any(
            rel == prefix or rel.startswith(prefix) for prefix in SCAN_ROOT_PREFIXES
        ):
            continue
        path = REPO_ROOT / rel
        if path.suffix and path.suffix not in SCAN_SUFFIXES and path.name != "Makefile":
            continue
        if not path.is_file():
            continue
        try:
            with path.open("rb") as handle:
                if b"\x00" in handle.read(8192):
                    continue
        except OSError:
            continue
        paths.append(path)
    return paths


def _line_allowed(rule: Rule, line: str) -> bool:
    return any(pattern.search(line) for pattern in rule.allow_line_regexes)


def _path_allowed(rule: Rule, rel: str) -> bool:
    return any(token in rel for token in rule.allow_path_substrings)


def test_active_surfaces_avoid_retired_terminology():
    violations: list[str] = []
    for path in _iter_scan_paths():
        rel = path.relative_to(REPO_ROOT).as_posix()
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for line_number, line in enumerate(text.splitlines(), start=1):
            for rule in RULES:
                if _path_allowed(rule, rel):
                    continue
                if not rule.pattern.search(line):
                    continue
                if _line_allowed(rule, line):
                    continue
                violations.append(
                    f"{rel}:{line_number}: [{rule.rule_id}] prefer {rule.prefer!r}: {line.strip()}"
                )
    assert not violations, "Retired terminology found:\n" + "\n".join(violations[:80])


def test_terminology_reference_is_linked_from_glossary_and_naming():
    glossary = (REPO_ROOT / "docs/concepts/glossary.md").read_text(encoding="utf-8")
    naming = (REPO_ROOT / "docs/reference/naming.md").read_text(encoding="utf-8")
    terminology = (REPO_ROOT / "docs/reference/terminology.md").read_text(
        encoding="utf-8"
    )
    assert (
        "reference/terminology.md" in glossary
        or "../reference/terminology.md" in glossary
    )
    assert "terminology.md" in naming
    assert "Pipeline" in terminology
    assert "logical atlas" in terminology.lower()
    assert "wc2026.v1" in terminology

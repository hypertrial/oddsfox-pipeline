"""Keep all non-prediction-market acquisition out of Pipeline."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from oddsfox_pipeline.config.acquisition_ownership import ACQUISITION_SOURCES

pytestmark = pytest.mark.repo_check

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "src" / "oddsfox_pipeline"
NETWORK_IMPORTS = {"aiohttp", "curl_cffi", "httpx", "requests", "urllib.request"}
ALLOWED_NETWORK_PATHS = (
    "ingestion/polymarket/",
    "publishing/stage_execution_archive.py",
    "publishing/stage_execution.py",
    "resources/http.py",
    "resources/outbound_url.py",
    "contracts/reference_transport.py",
    "orchestration/transient_retry.py",
)
FORBIDDEN_PATHS = (
    SOURCE / "features" / "pre_match_elo",
    SOURCE / "ingestion" / "international_results",
    SOURCE / "ingestion" / "openfootball",
)
FORBIDDEN_RUNTIME_TEXT = (
    "github.com/openfootball",
    "github.com/martj42",
    "international_results_wc2026_raw",
    "openfootball_wc2026_raw",
)


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    names.update(
        node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    )
    return names


def test_only_prediction_market_sources_are_registered() -> None:
    assert set(ACQUISITION_SOURCES) == {"polymarket", "pmxt", "kalshi", "polygon"}
    assert all(
        source.owner == "oddsfox-pipeline" for source in ACQUISITION_SOURCES.values()
    )


def test_non_market_collectors_and_elo_are_absent() -> None:
    assert not [path for root in FORBIDDEN_PATHS for path in root.glob("*.py")]


def test_non_market_hosts_and_raw_schemas_are_absent_from_runtime() -> None:
    violations: list[str] = []
    for path in SOURCE.rglob("*.py"):
        text = path.read_text(encoding="utf-8").casefold()
        for forbidden in FORBIDDEN_RUNTIME_TEXT:
            if forbidden.casefold() in text:
                violations.append(f"{path.relative_to(SOURCE)}: {forbidden}")
    assert not violations, "non-market runtime ownership violations:\n" + "\n".join(
        violations
    )


def test_runtime_network_clients_are_confined_to_approved_modules() -> None:
    violations: list[str] = []
    for path in SOURCE.rglob("*.py"):
        imported = _imports(path)
        if not any(
            name == package or name.startswith(package + ".")
            for name in imported
            for package in NETWORK_IMPORTS
        ):
            continue
        relative = path.relative_to(SOURCE).as_posix()
        if not relative.startswith(ALLOWED_NETWORK_PATHS):
            violations.append(relative)
    assert not violations, "unapproved Pipeline network clients:\n" + "\n".join(
        violations
    )


def test_runtime_api_clients_cannot_bypass_the_registry() -> None:
    violations: list[str] = []
    for path in SOURCE.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = node.func.id if isinstance(node.func, ast.Name) else ""
            if name != "APIClient":
                continue
            keywords = {keyword.arg: keyword.value for keyword in node.keywords}
            if "source_id" not in keywords:
                violations.append(
                    f"{path.relative_to(SOURCE)}:{node.lineno}: missing source_id"
                )
            bypass = keywords.get("enforce_registry")
            if isinstance(bypass, ast.Constant) and bypass.value is False:
                violations.append(
                    f"{path.relative_to(SOURCE)}:{node.lineno}: registry bypass"
                )
    assert not violations, "registry-bypassable API clients:\n" + "\n".join(violations)


def test_removed_non_market_runtime_names_are_not_registered() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    definitions = (SOURCE / "orchestration" / "definitions.py").read_text(
        encoding="utf-8"
    )
    forbidden = (
        "pre-match-elo-acquire:",
        "pre-match-elo-release:",
        "international_results_historical_ingest",
        "international_results_wc2026_match_results_ingest",
        "openfootball_wc2026_schedule_fixtures_ingest",
    )
    assert not [name for name in forbidden if name in makefile or name in definitions]

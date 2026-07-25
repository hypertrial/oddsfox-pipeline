"""Mutation-testing dependency and scope policy checks."""

from __future__ import annotations

from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 CI
    import toml as tomllib

pytestmark = pytest.mark.repo_check

REPO_ROOT = Path(__file__).resolve().parent.parent

TARGETS = [
    "src/oddsfox_pipeline/resources/outbound_url.py",
    "src/oddsfox_pipeline/contracts/raw_snapshots.py",
    "src/oddsfox_pipeline/ingestion/polymarket/market_scope/predicates.py",
    "src/oddsfox_pipeline/ingestion/polymarket/odds/planning.py",
]
TESTS = [
    "tests/unit/resources/test_outbound_url.py",
    "tests/unit/contracts/test_raw_snapshots.py",
    "tests/unit/ingestion/test_market_scope_predicates.py",
    "tests/unit/ingestion/test_odds_planning.py",
]


def test_mutmut_dependency_and_scope_are_pinned() -> None:
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    dev = project["project"]["optional-dependencies"]["dev"]
    config = project["tool"]["mutmut"]

    assert "mutmut==3.6.0" in dev
    assert not any("great_expectations" in dependency for dependency in dev)
    assert config == {
        "source_paths": ["src/oddsfox_pipeline"],
        "only_mutate": TARGETS,
        "pytest_add_cli_args": ["--hypothesis-seed=20260725"],
        "pytest_add_cli_args_test_selection": TESTS,
    }


def test_gx_runner_is_removed() -> None:
    assert not (REPO_ROOT / "scripts/run_gx_data_quality.py").exists()

    for relative_path in [
        "pyproject.toml",
        "uv.lock",
        "Makefile",
        ".github/workflows/manual-full.yml",
        "AGENTS.md",
        "CONTRIBUTING.md",
        "docs/development/index.md",
        "tests/README.md",
    ]:
        text = (REPO_ROOT / relative_path).read_text().lower()
        assert "great_expectations" not in text, relative_path
        assert "great expectations" not in text, relative_path
        assert "gx-data-quality" not in text, relative_path
        assert "run_gx_data_quality" not in text, relative_path


def test_mutmut_generator_contract_workaround_remains_enabled() -> None:
    conftest = (REPO_ROOT / "tests/conftest.py").read_text()

    assert "_preserve_mutmut_generator_return_values()" in conftest
    assert "return (yield from trampoline(*args, **kwargs))" in conftest

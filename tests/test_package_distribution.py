"""Built-wheel licensing and distribution checks."""

from __future__ import annotations

import re
import subprocess
import zipfile
from importlib import metadata
from pathlib import Path

import pytest

pytestmark = pytest.mark.repo_check

REPO_ROOT = Path(__file__).resolve().parent.parent

EXPECTED_DIRECT_LICENSES = {
    "dagster": "Apache-2.0",
    "dagster-dbt": "Apache-2.0",
    "dagster-dlt": "Apache-2.0",
    "dagster-webserver": "Apache-2.0",
    "dbt-core": "Apache-2.0",
    "dbt-duckdb": "Apache-2.0",
    "dlt": "Apache-2.0",
    "duckdb": "MIT",
    "polars": "MIT",
    "pyarrow": "Apache-2.0",
    "python-dotenv": "BSD-3-Clause",
    "pyyaml": "MIT",
    "requests": "Apache-2.0",
    "tqdm": "MPL-2.0 AND MIT",
}
LICENSE_ALIASES = {
    "Apache-2": "Apache-2.0",
    "Apache-2.0": "Apache-2.0",
    "BSD-3-Clause": "BSD-3-Clause",
    "MIT": "MIT",
    "MPL-2.0 AND MIT": "MPL-2.0 AND MIT",
}
CLASSIFIER_LICENSES = {
    "License :: OSI Approved :: Apache Software License": "Apache-2.0",
    "License :: OSI Approved :: MIT License": "MIT",
}
REQUIREMENT_PATTERN = re.compile(
    r"""
    (?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)
    (?:\[[A-Za-z0-9._,-]+\])?
    (?:
        (?:~=|==|!=|<=|>=|<|>)[^;,\s]+
        (?:,(?:~=|==|!=|<=|>=|<|>)[^;,\s]+)*
    )?
    (?:;\s*extra\s*==\s*["'](?P<extra>dev)["'])?
    """,
    re.VERBOSE,
)


def _canonical_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).casefold()


def _direct_runtime_requirements() -> set[str]:
    requirements = metadata.requires("oddsfox-pipeline")
    assert requirements is not None
    names = set()
    for requirement in requirements:
        match = REQUIREMENT_PATTERN.fullmatch(requirement)
        assert match, f"unparseable direct requirement: {requirement}"
        if match.group("extra") == "dev":
            continue
        names.add(_canonical_name(match.group("name")))
    return names


def _normalized_license(package: str) -> str:
    package_metadata = metadata.metadata(package)
    expression = package_metadata.get("License-Expression")
    if expression is not None:
        assert expression in LICENSE_ALIASES, (
            f"{package} has unknown License-Expression {expression!r}"
        )
        return LICENSE_ALIASES[expression]

    classifiers = [
        classifier
        for classifier in package_metadata.get_all("Classifier") or []
        if classifier.startswith("License ::")
    ]
    if classifiers:
        assert set(classifiers) <= set(CLASSIFIER_LICENSES), (
            f"{package} has unknown licence classifiers {classifiers!r}"
        )
        normalized = {CLASSIFIER_LICENSES[classifier] for classifier in classifiers}
        assert len(normalized) == 1, (
            f"{package} has conflicting licence classifiers {classifiers!r}"
        )
        return normalized.pop()

    legacy = package_metadata.get("License")
    assert legacy in LICENSE_ALIASES, (
        f"{package} has missing or unknown legacy licence {legacy!r}"
    )
    return LICENSE_ALIASES[legacy]


def test_direct_runtime_dependency_licenses_match_reviewed_policy() -> None:
    assert _direct_runtime_requirements() == set(EXPECTED_DIRECT_LICENSES)
    for package, expected in EXPECTED_DIRECT_LICENSES.items():
        assert _normalized_license(package) == expected


def test_built_wheel_declares_mit_and_contains_notices(tmp_path: Path) -> None:
    completed = subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(tmp_path)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    wheels = list(tmp_path.glob("*.whl"))
    assert len(wheels) == 1

    with zipfile.ZipFile(wheels[0]) as archive:
        names = archive.namelist()
        metadata_name = next(name for name in names if name.endswith("/METADATA"))
        metadata = archive.read(metadata_name).decode()

    assert "License-Expression: MIT" in metadata
    assert any(name.endswith(".dist-info/licenses/LICENSE") for name in names)
    assert any(
        name.endswith(".dist-info/licenses/THIRD_PARTY_NOTICES.md") for name in names
    )
    assert not any(
        name.endswith((".csv", ".db", ".duckdb", ".parquet", ".pdf")) for name in names
    )

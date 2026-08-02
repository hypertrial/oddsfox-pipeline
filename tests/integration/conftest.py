"""Shared fixtures for integration tests (dbt profiles, temp DuckDB, DNS)."""

from __future__ import annotations

from pathlib import Path

import pytest
from tests.integration.dbt_cli import isolated_dbt_env


@pytest.fixture(autouse=True)
def _public_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "oddsfox_pipeline.resources.outbound_url.socket.getaddrinfo",
        lambda *a, **k: [(None, None, None, None, ("93.184.216.34", 443))],
    )


@pytest.fixture
def dbt_profiles_dir(tmp_path: Path) -> Path:
    """Empty profiles directory; tests write profiles.yml after choosing db_path."""
    profiles_dir = tmp_path / ".dbt"
    profiles_dir.mkdir()
    return profiles_dir


@pytest.fixture
def dbt_target_dir(tmp_path: Path) -> Path:
    """Per-test dbt target directory for parallel-safe artifact isolation."""
    path = tmp_path / "dbt-target"
    path.mkdir()
    return path


def write_dbt_profile(profiles_dir: Path, db_path: Path, *, threads: int = 1) -> None:
    (profiles_dir / "profiles.yml").write_text(
        f"""
oddsfox:
  outputs:
    dev:
      type: duckdb
      path: {db_path}
      schema: dbt
      threads: {threads}
  target: dev
""".strip()
        + "\n"
    )


def dbt_subprocess_env(
    *,
    db_path: Path,
    profiles_dir: Path,
    target_dir: Path,
    dbt_threads: int = 1,
) -> dict[str, str]:
    """Convenience wrapper around isolated_dbt_env for integration tests."""
    return isolated_dbt_env(
        db_path=db_path,
        profiles_dir=profiles_dir,
        target_dir=target_dir,
        dbt_threads=dbt_threads,
    )

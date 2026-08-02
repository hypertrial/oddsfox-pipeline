"""Focused coverage for shared dbt target-path resolution."""

from __future__ import annotations

from pathlib import Path

from oddsfox_pipeline.orchestration.dbt_project import resolve_dbt_target_path


def test_resolve_dbt_target_path_defaults_to_relative_target(monkeypatch):
    monkeypatch.delenv("DBT_TARGET_PATH", raising=False)
    assert resolve_dbt_target_path() == Path("target")


def test_resolve_dbt_target_path_uses_absolute_env(monkeypatch, tmp_path):
    target = tmp_path / "shared-target"
    monkeypatch.setenv("DBT_TARGET_PATH", str(target))
    assert resolve_dbt_target_path() == target.resolve()


def test_resolve_dbt_target_path_resolves_relative_env_against_project(
    monkeypatch, tmp_path
):
    from oddsfox_pipeline.config import settings

    monkeypatch.setenv("DBT_TARGET_PATH", "custom-target")
    assert (
        resolve_dbt_target_path()
        == (settings.DBT_PROJECT_DIR / "custom-target").resolve()
    )

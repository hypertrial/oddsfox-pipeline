"""Tests for scripts/profile_warehouse.py."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import duckdb


def _load_profile_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    import profile_warehouse

    return profile_warehouse


def test_main_sets_duckdb_path_env_before_refresh(tmp_path, monkeypatch) -> None:
    pw = _load_profile_module()
    custom = tmp_path / "warehouse.duckdb"
    out_dir = tmp_path / "out"
    captured: dict[str, object] = {}

    def capture_refresh(repo, duckdb_path, steps):
        captured["env"] = os.environ.get("DUCKDB_PATH")
        captured["arg"] = duckdb_path
        return []

    monkeypatch.setattr(pw, "run_refresh", capture_refresh)
    monkeypatch.setattr(
        "oddsfox_pipeline.storage.duckdb.open_duckdb_connection",
        lambda path, read_only=True: duckdb.connect(":memory:"),
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.storage.duckdb.profile.build_warehouse_profile_report",
        lambda *args, **kwargs: SimpleNamespace(as_json=lambda: "{}"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "profile_warehouse",
            "--duckdb-path",
            str(custom),
            "--refresh",
            "dbt",
            "--format",
            "json",
            "--output-dir",
            str(out_dir),
        ],
    )

    assert pw.main() == 0
    assert captured["env"] == str(custom.resolve())
    assert captured["arg"] == custom.resolve()


def test_run_refresh_polymarket_uses_active_duckdb_path(tmp_path, monkeypatch) -> None:
    pw = _load_profile_module()
    custom = tmp_path / "custom.duckdb"
    os.environ["DUCKDB_PATH"] = str(custom)

    from oddsfox_pipeline.storage.duckdb.connection import (
        active_duckdb_path,
        reset_duckdb_connection_state,
    )

    reset_duckdb_connection_state()
    observed: list[Path] = []

    def fake_sync_markets():
        observed.append(active_duckdb_path())
        return {}

    def fake_sync_odds():
        return {}

    with (
        patch(
            "oddsfox_pipeline.ingestion.polymarket.markets.sync.sync_markets",
            fake_sync_markets,
        ),
        patch(
            "oddsfox_pipeline.ingestion.polymarket.odds.sync.sync_odds",
            fake_sync_odds,
        ),
    ):
        results = pw.run_refresh(pw.REPO_ROOT, custom, ["polymarket"])

    assert len(observed) == 1
    assert observed[0] == custom.resolve()
    assert results[0].ok is True

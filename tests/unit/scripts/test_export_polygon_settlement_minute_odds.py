"""Tests for the sanitized Polygon settlement export CLI."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock


def _load_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    import export_polymarket_wc2026_polygon_settlement_minute_odds as script

    return importlib.reload(script)


def test_export_cli_passes_paths_and_reports_hash(monkeypatch, tmp_path, capsys):
    script = _load_module()
    export = MagicMock(
        return_value={
            "rows": 39_120,
            "release_dir": str(tmp_path / "exports" / "releases" / "1.2.3"),
            "csv_sha256": "a" * 64,
        }
    )
    monkeypatch.setattr(script, "export_polygon_settlement_minute_odds", export)
    audit = tmp_path / "audit" / "releases" / "1.2.3"
    output = tmp_path / "exports"

    assert (
        script.main(
            [
                "--audit-release",
                str(audit),
                "--output-root",
                str(output),
            ]
        )
        == 0
    )

    export.assert_called_once_with(audit, output, repo_root=script.REPO_ROOT)
    stdout = capsys.readouterr().out
    assert "39,120 rows" in stdout
    assert "a" * 64 in stdout


def test_export_cli_returns_one_for_validation_failure(monkeypatch, tmp_path, capsys):
    script = _load_module()
    monkeypatch.setattr(
        script,
        "export_polygon_settlement_minute_odds",
        MagicMock(side_effect=ValueError("invalid audit")),
    )

    assert (
        script.main(
            [
                "--audit-release",
                str(tmp_path / "missing"),
                "--output-root",
                str(tmp_path),
            ]
        )
        == 1
    )
    assert capsys.readouterr().err == "invalid audit\n"

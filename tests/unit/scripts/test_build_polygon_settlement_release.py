"""Tests for the standalone Polygon settlement release entrypoint."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock


def _load_release_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    import build_polymarket_wc2026_polygon_settlement_release as release

    return importlib.reload(release)


def _arguments(tmp_path: Path) -> list[str]:
    return [
        "--dataset-version",
        "1.2.3",
        "--publisher-name",
        "Publisher",
        "--duckdb-path",
        str(tmp_path / "warehouse.duckdb"),
        "--output-root",
        str(tmp_path / "releases"),
    ]


def _stub_bundle(monkeypatch, release, tmp_path: Path) -> MagicMock:
    conn = MagicMock()
    monkeypatch.setattr(release, "open_duckdb_connection", MagicMock(return_value=conn))
    monkeypatch.setattr(
        release,
        "build_polygon_settlement_release",
        MagicMock(return_value={"rows": 39_120, "release_dir": tmp_path / "1.2.3"}),
    )
    monkeypatch.setattr(release, "current_generator_commit", lambda _root: "a" * 40)
    return conn


def test_release_without_secondary_provider_stays_read_only(monkeypatch, tmp_path):
    release = _load_release_module()
    conn = _stub_bundle(monkeypatch, release, tmp_path)
    monkeypatch.setattr(release.settings, "POLYGON_VERIFY_RPC_URL", "")
    monkeypatch.setattr(release.settings, "POLYGON_VERIFY_RPC_PROVIDER_LABEL", "")
    verify = MagicMock()
    monkeypatch.setattr(release, "verify_polygon_settlement_scan", verify)
    monkeypatch.setattr(
        release,
        "load_polygon_settlement_release_provenance",
        MagicMock(return_value={"scan_id": "scan"}),
    )

    assert release.main(_arguments(tmp_path)) == 0

    release.open_duckdb_connection.assert_called_once_with(
        (tmp_path / "warehouse.duckdb").resolve(), read_only=True
    )
    verify.assert_not_called()
    conn.close.assert_called_once_with()


def test_release_runs_configured_secondary_verification(monkeypatch, tmp_path):
    release = _load_release_module()
    conn = _stub_bundle(monkeypatch, release, tmp_path)
    monkeypatch.setattr(
        release.settings, "POLYGON_VERIFY_RPC_URL", "https://verify.example"
    )
    monkeypatch.setattr(
        release.settings, "POLYGON_VERIFY_RPC_PROVIDER_LABEL", "secondary"
    )
    verify = MagicMock(return_value={"verification_status": "matched"})
    monkeypatch.setattr(release, "verify_polygon_settlement_scan", verify)
    monkeypatch.setattr(
        release,
        "load_polygon_settlement_release_provenance",
        MagicMock(return_value={"scan_id": "scan"}),
    )

    assert release.main(_arguments(tmp_path)) == 0

    release.open_duckdb_connection.assert_called_once_with(
        (tmp_path / "warehouse.duckdb").resolve(), read_only=False
    )
    verify.assert_called_once_with(
        conn,
        seed_path=release.DEFAULT_POLYGON_MARKET_SEED_PATH,
        rpc_url="https://verify.example",
        provider_label="secondary",
    )


def test_release_keeps_secondary_verification_failure_advisory(
    monkeypatch, tmp_path, capsys
):
    release = _load_release_module()
    conn = _stub_bundle(monkeypatch, release, tmp_path)
    monkeypatch.setattr(
        release.settings, "POLYGON_VERIFY_RPC_URL", "https://verify.example"
    )
    monkeypatch.setattr(
        release.settings, "POLYGON_VERIFY_RPC_PROVIDER_LABEL", "secondary"
    )
    monkeypatch.setattr(
        release,
        "verify_polygon_settlement_scan",
        MagicMock(side_effect=RuntimeError("provider failed")),
    )
    provenance = MagicMock(return_value={"scan_id": "scan"})
    monkeypatch.setattr(
        release, "load_polygon_settlement_release_provenance", provenance
    )
    set_status = MagicMock()
    monkeypatch.setattr(release, "set_polygon_verification_status", set_status)

    assert release.main(_arguments(tmp_path)) == 0

    set_status.assert_called_once_with(conn, "scan", "error")
    assert provenance.call_count == 2
    assert "continuing advisory release" in capsys.readouterr().err

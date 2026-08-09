"""Unit coverage for dbt long-query liveness fingerprints."""

from __future__ import annotations

from types import SimpleNamespace

from oddsfox_pipeline.orchestration.dbt_build import _dbt_liveness_fingerprint


def test_dbt_liveness_fingerprint_tracks_configured_temp(tmp_path, monkeypatch):
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    warehouse = tmp_path / "oddsfox.duckdb"
    warehouse.write_bytes(b"db")
    spill = tmp_path / "duckdb-temp"
    spill.mkdir()
    (spill / "spill.bin").write_bytes(b"x" * 2048)
    invocation = SimpleNamespace(process=SimpleNamespace(pid=123, poll=lambda: None))
    first = _dbt_liveness_fingerprint(invocation=invocation, warehouse_path=warehouse)
    assert first["dbt_alive"] is True
    assert first["duckdb_temp_bytes"] >= 2048
    (spill / "spill2.bin").write_bytes(b"y" * 4096)
    second = _dbt_liveness_fingerprint(invocation=invocation, warehouse_path=warehouse)
    assert second["duckdb_temp_bytes"] > first["duckdb_temp_bytes"]
    assert second["liveness_fingerprint"] != first["liveness_fingerprint"]

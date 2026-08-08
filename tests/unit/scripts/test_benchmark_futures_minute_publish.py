"""Smoke tests for the futures-minute publish benchmark harness."""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path


def _load_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    import benchmark_polymarket_wc2026_futures_minute_publish as benchmark

    return importlib.reload(benchmark)


def test_futures_minute_publish_benchmark_smoke_equals(tmp_path, monkeypatch):
    monkeypatch.setenv("ODDSFOX_RUNTIME_ROOT", str(tmp_path))
    benchmark = _load_module()
    output = tmp_path / "report.json"
    rc = benchmark.main(
        [
            "--tier",
            "smoke",
            "--output",
            str(output),
            "--shard-rows",
            "64",
            "--compression",
            "snappy",
        ]
    )
    assert rc == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["equality_all"] is True
    assert report["primary"]["equality"]["raw_identical"] is True
    assert report["primary"]["equality"]["audit_identical"] is True
    assert report["total_rows"] == 8 * 64
    assert report["speed_ratio_baseline_over_candidate"] > 0

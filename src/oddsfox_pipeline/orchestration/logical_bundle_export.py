"""In-process entry point for the WC2026 logical-v1 bundle exporter."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import duckdb

_EXPORTER: ModuleType | None = None
_EXPORTER_MODULE_NAME = "oddsfox_pipeline._wc2026_logical_bundle_export"


def _exporter_module() -> ModuleType:
    global _EXPORTER
    if _EXPORTER is not None:
        return _EXPORTER
    script_path = (
        Path(__file__).resolve().parents[3]
        / "scripts"
        / "export_polymarket_wc2026_logical_bundle.py"
    )
    spec = importlib.util.spec_from_file_location(_EXPORTER_MODULE_NAME, script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load logical bundle exporter from {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[_EXPORTER_MODULE_NAME] = module
    spec.loader.exec_module(module)
    _EXPORTER = module
    return module


def export_polymarket_wc2026_logical_bundle(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    *,
    require_clean_repo: bool = True,
) -> dict[str, Any]:
    exporter = _exporter_module()
    return exporter.export_polymarket_wc2026_logical_bundle(
        conn,
        output_dir,
        require_clean_repo=require_clean_repo,
    )


__all__ = ["export_polymarket_wc2026_logical_bundle"]

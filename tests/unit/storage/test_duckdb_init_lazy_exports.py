"""Lazy barrel exports on oddsfox_pipeline.storage.duckdb."""

from __future__ import annotations

import oddsfox_pipeline.storage.duckdb as duckdb_pkg


def test_duckdb_lazy_export_resolves_known_symbol() -> None:
    assert duckdb_pkg.save_odds_batch.__name__ == "save_odds_batch"


def test_duckdb_lazy_export_unknown_symbol_raises() -> None:
    try:
        duckdb_pkg.not_a_real_duckdb_export  # type: ignore[attr-defined]
    except AttributeError as exc:
        assert "not_a_real_duckdb_export" in str(exc)
    else:
        raise AssertionError("expected AttributeError")


def test_duckdb_dir_lists_public_exports() -> None:
    names = dir(duckdb_pkg)
    assert "open_duckdb_connection" in names
    assert "save_odds_batch" in names

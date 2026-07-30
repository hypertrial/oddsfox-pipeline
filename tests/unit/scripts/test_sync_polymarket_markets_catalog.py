"""Unit tests for platform-wide Polymarket catalog sync."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "scripts" / "sync_polymarket_markets_catalog.py"


def _load_script():
    spec = importlib.util.spec_from_file_location(
        "sync_polymarket_markets_catalog", SCRIPT
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_collect_high_volume_markets_uses_markets_keyset_and_both_closed_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod = _load_script()
    calls: list[dict] = []

    def fake_gamma_get(_client, endpoint, params=None):
        assert endpoint == "/markets/keyset"
        params = dict(params or {})
        calls.append(params)
        closed = params.get("closed")
        if params.get("after_cursor"):
            return {"markets": [], "next_cursor": None}
        if closed is False:
            return {
                "markets": [
                    {"id": "1", "volumeNum": 150_000, "question": "open"},
                    {"id": "2", "volumeNum": 50_000, "question": "below"},
                ],
                "next_cursor": "c1",
            }
        if closed is True:
            return {
                "markets": [
                    {"id": "1", "volumeNum": 200_000, "question": "dup"},
                    {"id": "3", "volumeNum": 100_000, "question": "closed"},
                ],
                "next_cursor": None,
            }
        raise AssertionError(f"unexpected params: {params}")

    monkeypatch.setattr(mod, "gamma_get", fake_gamma_get)
    rows = mod.collect_high_volume_markets(
        volume_min=100_000,
        keyset_closed=None,
        client=SimpleNamespace(),
    )
    assert [r["id"] for r in rows] == ["1", "3"]
    assert all(c.get("volume_num_min") == 100_000 for c in calls)
    assert {c.get("closed") for c in calls} == {False, True}
    assert any(c.get("after_cursor") == "c1" for c in calls)


def test_closed_passes() -> None:
    mod = _load_script()
    assert mod._closed_passes(None) == (False, True)
    assert mod._closed_passes(False) == (False,)
    assert mod._closed_passes(True) == (True,)


def test_land_catalog_markets_casts_all_null_optional_columns(
    tmp_path: Path,
) -> None:
    import duckdb

    mod = _load_script()
    duck = tmp_path / "catalog.duckdb"
    landed = mod.land_catalog_markets(
        [
            {
                "id": "1",
                "question": "Q?",
                "category": None,
                "description": "",
                "outcomes": '["Yes","No"]',
                "volume": 100_000.0,
                "active": True,
                "closed": False,
                "created_at": None,
                "scraped_at": None,
                "end_date": None,
                "slug": "q",
                "event_slug": "e",
                "event_id": "10",
                "event_title": None,
                "event_start_time": None,
                "event_finished_time": None,
                "event_game_id": None,
                "event_ended": None,
                "condition_id": None,
                "sports_market_type": None,
                "game_start_time": None,
                "group_item_title": None,
                "tags": None,
                "clob_token_ids": '["a","b"]',
                "is_resolved": None,
                "winning_outcome": None,
                "winning_clob_token_id": None,
            }
        ],
        duckdb_path=duck,
    )
    assert landed == 1
    conn = duckdb.connect(str(duck), read_only=True)
    try:
        types = {
            name: dtype
            for _cid, name, dtype, *_rest in conn.execute(
                f"pragma table_info('{mod.CATALOG_SCHEMA}.{mod.CATALOG_TABLE}')"
            ).fetchall()
        }
    finally:
        conn.close()
    assert types["game_start_time"] == "TIMESTAMP"
    assert types["event_start_time"] == "TIMESTAMP"
    assert types["tags"] == "VARCHAR"
    assert types["event_title"] == "VARCHAR"
    assert types["is_resolved"] == "BOOLEAN"

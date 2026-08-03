"""Regression checks for Arrow-backed odds stage loading."""

from __future__ import annotations

import time

import pytest
from tests.unit.storage.duckdb_storage_test_support import T_OH

from oddsfox_pipeline.storage.duckdb.dlt_batch import (
    load_odds_history_stage,
    load_stage_rows,
    merge_odds_history_stage,
    prepare_odds_history_stage,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket_raw_columns import (
    ODDS_HISTORY_COLUMNS,
)


def _odds_rows(count: int) -> list[dict[str, object]]:
    return [
        {
            "clobTokenId": f"token-{idx % 100}",
            "timestamp": 1_700_000_000 + idx,
            "price": 0.1 + (idx % 50) / 100.0,
            "ingested_at": "2026-06-11T00:00:00",
        }
        for idx in range(count)
    ]


def test_prepare_odds_history_stage_arrow_matches_merge_semantics(duck):
    rows = _odds_rows(3)
    with duck.get_connection() as conn:
        stage = prepare_odds_history_stage(rows, conn)
        merge_odds_history_stage(conn, stage)
        count = conn.execute(f"SELECT count(*) FROM {T_OH}").fetchone()[0]

    assert count == 3


def test_prepare_odds_history_stage_rejects_empty_rows(duck):
    with duck.get_connection() as conn:
        with pytest.raises(ValueError, match="rows must not be empty"):
            prepare_odds_history_stage([], conn)


@pytest.mark.slow
def test_arrow_odds_stage_faster_than_dlt_stage(duck):
    rows = _odds_rows(20_000)
    with duck.get_connection() as conn:
        dlt_start = time.perf_counter()
        dlt_stage = load_stage_rows(
            schema="polymarket_wc2026_raw",
            stage_table="stage_odds_history_dlt_bench",
            rows=[{**row, "row_order": idx} for idx, row in enumerate(rows)],
            columns=ODDS_HISTORY_COLUMNS,
        )
        dlt_elapsed = time.perf_counter() - dlt_start
        conn.execute(f"DROP TABLE IF EXISTS {dlt_stage}")

        arrow_start = time.perf_counter()
        arrow_stage = prepare_odds_history_stage(rows, conn)
        arrow_elapsed = time.perf_counter() - arrow_start
        conn.execute(f"DROP TABLE IF EXISTS {arrow_stage}")

    assert arrow_elapsed < dlt_elapsed / 5


def test_load_odds_history_stage_end_to_end(duck):
    with duck.get_connection() as conn:
        load_odds_history_stage(_odds_rows(2), conn)
        load_odds_history_stage(
            [
                {
                    "clobTokenId": "token-new",
                    "timestamp": 1_800_000_000,
                    "price": 0.42,
                    "ingested_at": "2026-06-12T00:00:00",
                }
            ],
            conn,
        )
        count = conn.execute(f"SELECT count(*) FROM {T_OH}").fetchone()[0]

    assert count == 3

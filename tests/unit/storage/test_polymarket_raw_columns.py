from __future__ import annotations

import pytest

from oddsfox_pipeline.storage.duckdb.schemas import polymarket as polymarket_schema
from oddsfox_pipeline.storage.duckdb.schemas.polymarket_raw_columns import (
    _DLT_COLUMNS_BY_RELATION,
    ddl_column_names,
    dlt_column_names,
)


@pytest.mark.parametrize("relation", sorted(_DLT_COLUMNS_BY_RELATION))
def test_dlt_and_ddl_column_names_match_per_raw_relation(relation: str) -> None:
    assert dlt_column_names(relation) == ddl_column_names(relation)


def test_bootstrap_odds_history_columns_match_dlt_contract(duck):
    with duck.get_connection() as conn:
        polymarket_schema.bootstrap_polymarket_tables(conn)
        ddl_columns = {
            str(description[0]).casefold()
            for description in conn.execute(
                "SELECT * FROM polymarket_wc2026_raw.odds_history LIMIT 0"
            ).description
        }

    assert ddl_columns == {name.casefold() for name in ddl_column_names("odds_history")}

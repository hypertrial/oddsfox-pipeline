from datetime import datetime
from unittest.mock import MagicMock

from oddsfox_pipeline.storage.duckdb import event_catalog_markets as ecm
from oddsfox_pipeline.storage.duckdb.event_catalog_markets import (
    _PAYLOAD_COLUMNS,
    _payload_row_to_gamma_dict,
    materialize_registry_markets_from_event_catalog,
)
from oddsfox_pipeline.storage.duckdb.market_scope_registry import (
    RegistryRow,
    upsert_registry_rows,
)
from oddsfox_pipeline.storage.duckdb.schemas.constants import polymarket_raw_tbl


def test_payload_row_to_gamma_dict_maps_snapshot_fields_to_gamma_shape():
    row = dict(
        zip(
            _PAYLOAD_COLUMNS,
            [
                "123",
                "Will team win?",
                "sports",
                "desc",
                "source",
                '["Yes", "No"]',
                250_000.0,
                True,
                False,
                datetime(2026, 4, 29, 18, 12, 44),
                datetime(2026, 8, 3, 12, 0, 0),
                datetime(2026, 7, 1, 0, 0, 0),
                "market-slug",
                "event-slug",
                "evt-1",
                "Event title",
                datetime(2026, 6, 1, 12, 0, 0),
                None,
                "game-1",
                False,
                "cond-1",
                "moneyline",
                datetime(2026, 6, 15, 18, 0, 0),
                "Team A",
                "0",
                1.5,
                None,
                '["tok-a", "tok-b"]',
                False,
                None,
                None,
                None,
                None,
                None,
            ],
            strict=True,
        )
    )
    values = tuple(row[column] for column in _PAYLOAD_COLUMNS)

    gamma = _payload_row_to_gamma_dict(values)

    assert gamma["id"] == "123"
    assert gamma["createdAt"] == "2026-04-29T18:12:44.000Z"
    assert gamma["volumeNum"] == 250_000.0
    assert gamma["clobTokenIds"] == ["tok-a", "tok-b"]
    assert gamma["events"] == [{"slug": "event-slug", "id": "evt-1"}]


def test_materialize_registry_markets_from_event_catalog_empty(duck):
    result = materialize_registry_markets_from_event_catalog(scope_name="wc2026")
    assert result == {"markets_materialized": 0, "token_rows_materialized": 0}


def test_materialize_registry_markets_from_event_catalog_runs_pipeline(
    monkeypatch, duck
):
    payloads = polymarket_raw_tbl("wc2026", "event_market_payload_snapshots")
    observed_at = datetime(2026, 8, 3, 12, 0, 0)
    upsert_registry_rows(
        [
            RegistryRow(
                market_id="123",
                event_slug="event-slug",
                event_id="evt-1",
                source="test",
                scope_name="wc2026",
                is_event_volume_eligible=True,
            )
        ]
    )
    with duck.get_connection() as conn:
        conn.execute(
            f"""
            INSERT INTO {payloads} (
                market_id, question, category, description, outcomes, volume,
                active, closed, created_at, scraped_at, end_date, slug,
                event_slug, event_id, condition_id, sports_market_type,
                clob_token_ids, is_resolved, observed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "123",
                "Will team win?",
                "sports",
                "desc",
                '["Yes", "No"]',
                250_000.0,
                True,
                False,
                datetime(2026, 4, 29, 18, 12, 44),
                observed_at,
                datetime(2026, 7, 1),
                "market-slug",
                "event-slug",
                "evt-1",
                "cond-1",
                "moneyline",
                '["tok-a", "tok-b"]',
                False,
                observed_at,
            ],
        )

    pipeline = MagicMock(has_pending_data=False)
    saved_tokens: list = []
    import oddsfox_pipeline.storage.duckdb.dlt_batch as dlt_batch_mod

    monkeypatch.setattr(
        dlt_batch_mod,
        "get_polymarket_dlt_pipeline",
        lambda **_kwargs: pipeline,
    )
    monkeypatch.setattr(
        ecm,
        "save_market_tokens_batch",
        lambda rows, **_kwargs: saved_tokens.extend(rows),
    )

    result = materialize_registry_markets_from_event_catalog(scope_name="wc2026")

    assert result["markets_materialized"] == 1
    assert result["token_rows_materialized"] >= 1
    assert pipeline.run.called
    assert saved_tokens

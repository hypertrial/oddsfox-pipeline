from datetime import datetime

from oddsfox_pipeline.storage.duckdb.event_catalog_markets import (
    _PAYLOAD_COLUMNS,
    _payload_row_to_gamma_dict,
)


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

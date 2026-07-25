"""Semantic tests for Polymarket market persistence rows."""

from datetime import datetime, timezone

import polars as pl
import pytest
from hypothesis import Phase, given, settings
from hypothesis import strategies as st

from oddsfox_pipeline.ingestion.polymarket.markets import persistence

FIXED_NOW = datetime(2026, 7, 25, 10, 11, 12, 123456)


def test_utc_now_is_current_utc_naive() -> None:
    before = datetime.now(timezone.utc).replace(tzinfo=None)
    actual = persistence._utc_now()
    after = datetime.now(timezone.utc).replace(tzinfo=None)

    assert actual.tzinfo is None
    assert before <= actual <= after


def test_prepare_batch_for_db_empty() -> None:
    assert persistence.prepare_batch_for_db(pl.DataFrame()) == ([], [])


def test_prepare_batch_for_db_maps_every_market_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(persistence, "_utc_now", lambda: FIXED_NOW)
    created_at = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
    end_date = datetime(2026, 7, 19, 18, 30, tzinfo=timezone.utc)
    frame = pl.DataFrame(
        {
            "id": ["market-1"],
            "question": ["Who wins?"],
            "category": ["sports"],
            "description": ["Synthetic market"],
            "outcomes_str": ['["Yes", "No"]'],
            "volumeNum": [12.5],
            "volume": [99.0],
            "active": [True],
            "closed": [True],
            "created_at": [created_at],
            "end_date": [end_date],
            "slug": ["who-wins"],
            "event_slug": ["world-cup"],
            "event_id": ["event-1"],
            "event_title": ["World Cup"],
            "event_start_time": ["2026-06-11T00:00:00Z"],
            "event_finished_time": ["2026-07-19T21:00:00Z"],
            "event_game_id": ["game-1"],
            "event_ended": [True],
            "condition_id": ["condition-1"],
            "sports_market_type": ["moneyline"],
            "game_start_time": ["2026-07-19T18:00:00Z"],
            "group_item_title": ["Final"],
            "tags_str": ['["football"]'],
            "clob_token_ids": ['["yes", "no"]'],
            "is_resolved": [True],
            "winning_outcome": ["Yes"],
            "winning_clob_token_id": ["yes"],
            "clobTokenIds_str": ['["yes", "no"]'],
        }
    )

    market_rows, token_rows = persistence.prepare_batch_for_db(frame)

    assert market_rows == [
        (
            "market-1",
            "Who wins?",
            "sports",
            "Synthetic market",
            '["Yes", "No"]',
            12.5,
            True,
            True,
            "2024-01-15 12:00:00",
            "2026-07-25T10:11:12.123456",
            "2026-07-19 18:30:00",
            "who-wins",
            "world-cup",
            "event-1",
            "World Cup",
            "2026-06-11T00:00:00Z",
            "2026-07-19T21:00:00Z",
            "game-1",
            True,
            "condition-1",
            "moneyline",
            "2026-07-19T18:00:00Z",
            "Final",
            '["football"]',
            '["yes", "no"]',
            True,
            "Yes",
            "yes",
        )
    ]
    assert len(market_rows[0]) == len(persistence.MARKET_RECORD_COLUMNS) == 28
    assert token_rows == [("market-1", '["yes", "no"]')]


def test_prepare_batch_for_db_uses_documented_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(persistence, "_utc_now", lambda: FIXED_NOW)

    market_rows, token_rows = persistence.prepare_batch_for_db(
        pl.DataFrame({"unused": ["value"]})
    )

    assert market_rows == [
        (
            "",
            "",
            "",
            "",
            "",
            0.0,
            False,
            False,
            "",
            "2026-07-25T10:11:12.123456",
            "",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    ]
    assert token_rows == []


def test_prepare_batch_for_db_uses_volume_when_volume_num_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(persistence, "_utc_now", lambda: FIXED_NOW)
    frame = pl.DataFrame({"id": ["market-1"], "volume": [7.25]})

    market_rows, _ = persistence.prepare_batch_for_db(frame)

    assert market_rows[0][5] == 7.25


def test_prepare_batch_for_db_preserves_explicit_nulls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(persistence, "_utc_now", lambda: FIXED_NOW)
    frame = pl.DataFrame(
        {
            "id": ["market-1"],
            "volumeNum": [None],
            "active": [None],
            "closed": [None],
            "created_at": [None],
            "end_date": [None],
            "event_ended": [None],
            "is_resolved": [None],
        },
        schema_overrides={
            "volumeNum": pl.Float64,
            "active": pl.Boolean,
            "closed": pl.Boolean,
            "created_at": pl.Datetime,
            "end_date": pl.Datetime,
            "event_ended": pl.Boolean,
            "is_resolved": pl.Boolean,
        },
    )

    market_rows, _ = persistence.prepare_batch_for_db(frame)

    assert market_rows[0][5:9] == (0.0, None, None, None)
    assert market_rows[0][10] is None
    assert market_rows[0][18] is None
    assert market_rows[0][25] is None


def test_prepare_batch_for_db_preserves_input_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(persistence, "_utc_now", lambda: FIXED_NOW)
    frame = pl.DataFrame(
        {
            "id": ["first", "second"],
            "clobTokenIds_str": ['["one"]', '["two"]'],
        }
    )

    market_rows, token_rows = persistence.prepare_batch_for_db(frame)

    assert [row[0] for row in market_rows] == ["first", "second"]
    assert token_rows == [("first", '["one"]'), ("second", '["two"]')]


@pytest.mark.parametrize("tokens", [None, "", "[]"])
def test_prepare_batch_for_db_omits_empty_tokens(
    monkeypatch: pytest.MonkeyPatch,
    tokens: str | None,
) -> None:
    monkeypatch.setattr(persistence, "_utc_now", lambda: FIXED_NOW)
    frame = pl.DataFrame(
        {"id": ["market-1"], "clobTokenIds_str": [tokens]},
        schema_overrides={"clobTokenIds_str": pl.String},
    )

    _, token_rows = persistence.prepare_batch_for_db(frame)

    assert token_rows == []


@given(
    volume_num=st.one_of(
        st.none(),
        st.floats(
            min_value=-1_000_000,
            max_value=1_000_000,
            allow_nan=False,
            allow_infinity=False,
        ),
    ),
    volume=st.floats(
        min_value=-1_000_000,
        max_value=1_000_000,
        allow_nan=False,
        allow_infinity=False,
    ),
    active=st.one_of(st.none(), st.booleans()),
    closed=st.one_of(st.none(), st.booleans()),
    token_payload=st.sampled_from(["", "[]", '["token"]']),
)
@settings(max_examples=8, phases=[Phase.generate], deadline=None)
def test_prepare_batch_preserves_volume_booleans_and_tokens(
    volume_num: float | None,
    volume: float,
    active: bool | None,
    closed: bool | None,
    token_payload: str,
) -> None:
    market_rows, token_rows = persistence.prepare_batch_for_db(
        pl.DataFrame(
            {
                "id": ["market-1"],
                "volumeNum": [volume_num],
                "volume": [volume],
                "active": [active],
                "closed": [closed],
                "clobTokenIds_str": [token_payload],
            }
        )
    )

    assert market_rows[0][5] == (float(volume_num) if volume_num is not None else 0.0)
    assert market_rows[0][6] is active
    assert market_rows[0][7] is closed
    assert token_rows == (
        [("market-1", token_payload)] if token_payload == '["token"]' else []
    )


def test_market_records_to_dicts_maps_columns_and_last_row_wins() -> None:
    first = ("market-a", *range(1, 28))
    other = ("market-b", *range(101, 128))
    replacement = ("market-a", *range(201, 228))

    assert persistence.market_records_to_dicts([first, other, replacement]) == [
        dict(zip(persistence.MARKET_RECORD_COLUMNS, replacement, strict=True)),
        dict(zip(persistence.MARKET_RECORD_COLUMNS, other, strict=True)),
    ]


@pytest.mark.parametrize("wrong_length", [27, 29])
def test_market_records_to_dicts_rejects_wrong_row_length(wrong_length: int) -> None:
    with pytest.raises(ValueError):
        persistence.market_records_to_dicts([tuple(range(wrong_length))])

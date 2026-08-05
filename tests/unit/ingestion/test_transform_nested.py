"""Branch coverage for nested JSON helpers used by process_markets_dataframe."""

from datetime import date, datetime, timedelta, timezone

import polars as pl

from oddsfox_pipeline.ingestion.polymarket.markets.transform import (
    _jsonify_nested_value,
    _normalize_nested_value,
    _parse_gamma_datetime_value,
)


def test_normalize_nested_value_series_tuple_and_scalar():
    assert _normalize_nested_value(pl.Series([1, 2])) == [1, 2]
    assert _normalize_nested_value((1, 2)) == [1, 2]
    assert _normalize_nested_value("plain") == "plain"


def test_jsonify_nested_value_list_and_scalar():
    assert _jsonify_nested_value([1, 2]) == "[1, 2]"
    assert _jsonify_nested_value(99) == "99"


def test_parse_gamma_datetime_value_variants():
    naive = datetime(2026, 1, 2, 3, 4, 5)
    aware = datetime(2026, 1, 2, 4, 4, 5, tzinfo=timezone(timedelta(hours=1)))

    assert _parse_gamma_datetime_value(None) is None
    assert _parse_gamma_datetime_value(naive) == naive
    assert _parse_gamma_datetime_value(aware) == naive
    assert _parse_gamma_datetime_value(date(2026, 1, 2)) == datetime(2026, 1, 2)
    assert _parse_gamma_datetime_value("  ") is None
    assert _parse_gamma_datetime_value("2026-01-02") == datetime(2026, 1, 2)
    assert _parse_gamma_datetime_value("2026-01-02T03:04:05.123456789Z") == datetime(
        2026, 1, 2, 3, 4, 5, 123456
    )
    assert _parse_gamma_datetime_value("2026-01-02 03:04:05.bad") == naive
    assert _parse_gamma_datetime_value("2026-01-02 03:04:05+bad") == naive
    assert _parse_gamma_datetime_value("not-a-date") is None

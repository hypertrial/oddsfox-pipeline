"""UTC-stable hourly odds timestamp bucketing (session TZ independent)."""

from __future__ import annotations

import duckdb


def test_hourly_odds_date_trunc_is_utc_stable_under_half_hour_session_tz():
    """Mirror stg odds + polymarket_token_hourly_odds_sql UTC-wall pattern."""
    epoch = 1719803400  # 2024-07-01 03:30:00 UTC → UTC hour 03:00
    expected = 1719802800
    sql = """
    select cast(
        epoch(
            date_trunc(
                'hour',
                to_timestamp(?) at time zone 'UTC'
            )
        ) as bigint
    )
    """
    for tz in ("UTC", "Asia/Kolkata", "America/St_Johns", "Europe/Warsaw"):
        con = duckdb.connect()
        con.execute(f"SET TimeZone='{tz}'")
        got = con.execute(sql, [epoch]).fetchone()[0]
        assert got == expected, tz

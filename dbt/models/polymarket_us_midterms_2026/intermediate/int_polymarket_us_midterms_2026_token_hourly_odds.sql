-- costguard: disable-file=SQLCOST007
-- costguard: disable-file=SQLCOST012
-- Window ordering defines OHLC open/close prices; SQLCOST012 is a false
-- positive on the close_rank expression after removing the one-row contract join.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['clob_token_id', 'odds_hour_epoch'],
        on_schema_change='fail',
        post_hook="
            delete from {{ this }}
            where odds_hour_utc < current_timestamp - (
                (
                    select hourly_window_days
                    from {{ ref('polymarket_us_midterms_2026_contract') }}
                    where scope_name = 'us_midterms_2026'
                ) * interval '1 day'
            )
        ",
    )
}}

{{ polymarket_token_hourly_odds_sql(
    ref('polymarket_us_midterms_2026_contract'),
    ref('stg_polymarket_us_midterms_2026_odds'),
    'us_midterms_2026',
) }}

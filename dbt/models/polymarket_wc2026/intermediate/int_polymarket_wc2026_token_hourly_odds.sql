-- costguard: disable-file=SQLCOST007
-- costguard: disable-file=SQLCOST012
-- Window ordering defines OHLC open/close prices; SQLCOST012 is a false
-- positive on the close_rank expression after removing the one-row pipeline policy join.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['clob_token_id', 'odds_hour_epoch'],
        on_schema_change='fail',
    )
}}

{{ polymarket_token_hourly_odds_sql(ref('stg_polymarket_wc2026_odds')) }}

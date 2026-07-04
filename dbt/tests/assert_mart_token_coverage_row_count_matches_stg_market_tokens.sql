-- polymarket_wc2026_token_coverage must include every expanded outcome token row from staging.
select
    (select count(*) from {{ ref('polymarket_wc2026_token_coverage') }}) as coverage_count,
    (select count(*) from {{ ref('stg_polymarket_wc2026_market_tokens') }}) as staging_count
where coverage_count <> staging_count

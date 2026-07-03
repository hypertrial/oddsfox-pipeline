-- wc2026_token_coverage must include every expanded outcome token row from staging.
select
    (select count(*) from {{ ref('wc2026_token_coverage') }}) as coverage_count,
    (select count(*) from {{ ref('stg_wc2026_polymarket_market_tokens') }}) as staging_count
where coverage_count <> staging_count

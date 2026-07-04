-- polymarket_wc2026_token_coverage must include every volume-scoped WC2026 outcome token.
select
    (select count(*) from {{ ref('polymarket_wc2026_token_coverage') }}) as coverage_count,
    (select count(*) from {{ ref('int_polymarket_wc2026_market_tokens') }}) as universe_count
where coverage_count <> universe_count

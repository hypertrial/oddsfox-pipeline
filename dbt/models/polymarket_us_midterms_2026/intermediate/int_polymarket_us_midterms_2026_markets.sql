{{ polymarket_markets_sql(
    ref('stg_polymarket_us_midterms_2026_markets'),
    source('polymarket_us_midterms_2026_ops', 'market_scope_registry'),
    'us_midterms_2026',
    ref('polymarket_us_midterms_2026_pipeline_policy')
) }}

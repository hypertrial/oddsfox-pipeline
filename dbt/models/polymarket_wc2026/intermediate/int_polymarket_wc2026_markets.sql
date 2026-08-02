{{ polymarket_markets_sql(
    ref('stg_polymarket_wc2026_markets'),
    source('polymarket_wc2026_ops', 'market_scope_registry'),
    'wc2026',
    ref('polymarket_wc2026_pipeline_policy')
) }}

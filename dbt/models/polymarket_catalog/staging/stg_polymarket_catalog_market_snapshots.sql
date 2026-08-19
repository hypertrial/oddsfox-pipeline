select * from {{ source('polymarket_catalog_raw', 'market_snapshots') }}

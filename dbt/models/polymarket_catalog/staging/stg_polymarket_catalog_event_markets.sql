select * from {{ source('polymarket_catalog_raw', 'event_market_snapshots') }}

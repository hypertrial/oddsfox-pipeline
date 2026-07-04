-- WC2026 public markets mart must respect the whale volume floor.
-- ponytail: keep in sync with POLYMARKET_WC2026_WHALE_MIN_VOLUME_USD (settings_polymarket.py).
select
    market_id,
    volume
from {{ ref('polymarket_wc2026_markets') }}
where coalesce(volume, 0) < 100000

select *
from {{ ref('polymarket_wc2026_knockout_data_quality') }}
where severity = 'error'

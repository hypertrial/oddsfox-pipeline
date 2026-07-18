{{ config(alias='third_place_slot_assignments') }}

with slot_codes as (
    select unnest(['1A', '1B', '1D', '1E', '1G', '1I', '1K', '1L'])
        as round_of_32_slot
)

select
    lookup.option_id,
    lookup.qualifying_group_set,
    slot_codes.round_of_32_slot,
    case slot_codes.round_of_32_slot
        when '1A' then lookup.slot_1a_group
        when '1B' then lookup.slot_1b_group
        when '1D' then lookup.slot_1d_group
        when '1E' then lookup.slot_1e_group
        when '1G' then lookup.slot_1g_group
        when '1I' then lookup.slot_1i_group
        when '1K' then lookup.slot_1k_group
        when '1L' then lookup.slot_1l_group
    end as third_place_group
from {{ ref('wc2026_third_place_lookup') }} as lookup
cross join slot_codes

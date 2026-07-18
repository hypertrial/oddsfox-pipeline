{{ config(alias='third_place_lookup') }}

select
    cast(option_id as integer) as option_id,
    'committed_static_seed' as source_provenance,
    upper(trim(cast(slot_1a_group as varchar))) as slot_1a_group,
    upper(trim(cast(slot_1b_group as varchar))) as slot_1b_group,
    upper(trim(cast(slot_1d_group as varchar))) as slot_1d_group,
    upper(trim(cast(slot_1e_group as varchar))) as slot_1e_group,
    upper(trim(cast(slot_1g_group as varchar))) as slot_1g_group,
    upper(trim(cast(slot_1i_group as varchar))) as slot_1i_group,
    upper(trim(cast(slot_1k_group as varchar))) as slot_1k_group,
    upper(trim(cast(slot_1l_group as varchar))) as slot_1l_group,
    array_to_string(
        list_sort([
            slot_1a_group, slot_1b_group, slot_1d_group, slot_1e_group,
            slot_1g_group, slot_1i_group, slot_1k_group, slot_1l_group
        ]),
        ''
    ) as qualifying_group_set
from {{ ref('wc2026_third_place_options') }}

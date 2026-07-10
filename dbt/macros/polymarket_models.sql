{% macro polymarket_token_hourly_odds_sql(contract_ref, odds_ref, scope_name) %}
with contract as (
    select hourly_window_days
    from {{ contract_ref }}
    where scope_name = '{{ scope_name }}'
),

source_odds as (
    select
        o.clob_token_id,
        o.odds_timestamp,
        o.odds_timestamp_epoch,
        o.price,
        o.ingested_at,
        cast(epoch(date_trunc('hour', o.odds_timestamp)) as bigint) as odds_hour_epoch,
        date_trunc('hour', o.odds_timestamp) as odds_hour_utc
    from {{ odds_ref }} as o
    where
        o.price is not null
        and o.odds_timestamp is not null
        and o.odds_timestamp_epoch is not null
        and o.odds_timestamp >= current_timestamp - (
            (select contract.hourly_window_days from contract) * interval '1 day'
        )
),

dirty_hours as (
    select distinct
        clob_token_id,
        odds_hour_utc,
        odds_hour_epoch
    from source_odds
    {% if is_incremental() %}
        where
            ingested_at is null
            or ingested_at >= (
                select coalesce(max(latest_ingested_at), timestamp '1970-01-01')
                from {{ this }}
            ) - interval '2 hour'
    {% endif %}
),

hourly_source as (
    select
        source_odds.clob_token_id,
        source_odds.odds_timestamp,
        source_odds.odds_timestamp_epoch,
        source_odds.price,
        source_odds.ingested_at,
        source_odds.odds_hour_utc,
        source_odds.odds_hour_epoch
    from source_odds
    inner join dirty_hours
        on
            source_odds.clob_token_id = dirty_hours.clob_token_id
            and source_odds.odds_hour_epoch = dirty_hours.odds_hour_epoch
),

ranked as (
    select
        clob_token_id,
        odds_timestamp,
        odds_timestamp_epoch,
        price,
        ingested_at,
        odds_hour_utc,
        odds_hour_epoch,
        row_number() over (
            partition by clob_token_id, odds_hour_epoch
            order by odds_timestamp_epoch asc, price asc
        ) as open_rank,
        row_number() over (
            partition by clob_token_id, odds_hour_epoch
            order by odds_timestamp_epoch desc, price desc
        ) as close_rank
    from hourly_source
)

select
    clob_token_id,
    odds_hour_utc,
    odds_hour_epoch,
    max(case when open_rank = 1 then price end) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    max(case when close_rank = 1 then price end) as close_price,
    round(avg(price), 8) as avg_price,
    count(*) as observed_points,
    min(odds_timestamp_epoch) as first_timestamp,
    min(odds_timestamp) as first_observed_at,
    max(odds_timestamp_epoch) as last_timestamp,
    max(odds_timestamp) as last_observed_at,
    max(ingested_at) as latest_ingested_at
from ranked
group by
    clob_token_id,
    odds_hour_utc,
    odds_hour_epoch
{% endmacro %}


{% macro polymarket_pipeline_run_events_sql(source_name) %}
select
    run_id,
    task_name,
    recorded_at,
    metrics_json,
    json_extract_string(metrics_json, '$.scope_name') as scope_name,
    json_extract_string(metrics_json, '$.discovery_mode') as discovery_mode,
    json_extract_string(metrics_json, '$.effective_keyset_tag_slugs')
        as effective_keyset_tag_slugs,
    try_cast(json_extract_string(metrics_json, '$.keyset_closed') as boolean)
        as keyset_closed,
    try_cast(json_extract_string(metrics_json, '$.keyset_volume_min') as double)
        as keyset_volume_min,
    try_cast(json_extract_string(metrics_json, '$.registry_refreshed') as boolean)
        as registry_refreshed,
    try_cast(json_extract_string(metrics_json, '$.events_pages') as bigint)
        as events_pages,
    try_cast(json_extract_string(metrics_json, '$.api_requests') as bigint)
        as api_requests,
    try_cast(json_extract_string(metrics_json, '$.markets_collected') as bigint)
        as markets_collected,
    try_cast(json_extract_string(metrics_json, '$.token_rows_collected') as bigint)
        as token_rows_collected,
    try_cast(json_extract_string(metrics_json, '$.total_fetched') as bigint)
        as total_fetched,
    try_cast(json_extract_string(metrics_json, '$.noop') as boolean) as is_noop,
    try_cast(json_extract_string(metrics_json, '$.duration_seconds') as double)
        as duration_seconds,
    try_cast(json_extract_string(metrics_json, '$.tokens') as bigint)
        as processed_tokens,
    try_cast(json_extract_string(metrics_json, '$.windows') as bigint)
        as windows_processed,
    try_cast(json_extract_string(metrics_json, '$.rows') as bigint) as rows_fetched,
    try_cast(json_extract_string(metrics_json, '$.empty') as bigint) as empty_tokens,
    try_cast(json_extract_string(metrics_json, '$.errors') as bigint) as error_tokens,
    try_cast(json_extract_string(metrics_json, '$.permanent_errors') as bigint)
        as permanent_error_tokens,
    try_cast(json_extract_string(metrics_json, '$.invalid_tokens') as bigint)
        as invalid_tokens,
    try_cast(json_extract_string(metrics_json, '$.saved_rows') as bigint) as saved_rows,
    try_cast(json_extract_string(metrics_json, '$.saved_daily_rows') as bigint)
        as saved_daily_rows,
    try_cast(json_extract_string(metrics_json, '$.sync_updates') as bigint)
        as sync_updates,
    try_cast(json_extract_string(metrics_json, '$.queue_high_watermark') as bigint)
        as queue_high_watermark,
    try_cast(json_extract_string(metrics_json, '$.http_requests') as bigint)
        as http_requests,
    try_cast(json_extract_string(metrics_json, '$.http_429') as bigint) as http_429,
    try_cast(json_extract_string(metrics_json, '$.http_errors') as bigint)
        as http_errors,
    try_cast(json_extract_string(metrics_json, '$.final_rps') as double) as final_rps,
    try_cast(json_extract_string(metrics_json, '$.token_rate') as double) as token_rate,
    try_cast(json_extract_string(metrics_json, '$.row_rate') as double) as row_rate,
    try_cast(json_extract_string(metrics_json, '$.planning.plans') as bigint)
        as planned_tokens,
    try_cast(
        json_extract_string(metrics_json, '$.planning.pre_clob_markets') as bigint
    ) as pre_clob_markets,
    try_cast(json_extract_string(metrics_json, '$.planning.invalid_token') as bigint)
        as invalid_tokens_from_planning,
    try_cast(json_extract_string(metrics_json, '$.planning.closed_done') as bigint)
        as closed_done_tokens,
    try_cast(json_extract_string(metrics_json, '$.planning.persisted_skip') as bigint)
        as persisted_skip_tokens,
    try_cast(json_extract_string(metrics_json, '$.planning.recent_skip') as bigint)
        as recent_skip_tokens,
    try_cast(
        json_extract_string(metrics_json, '$.planning.empty_cache_skip') as bigint
    ) as empty_cache_skip_tokens,
    try_cast(
        json_extract_string(metrics_json, '$.planning.already_current') as bigint
    ) as already_current_tokens,
    try_cast(json_extract_string(metrics_json, '$.planning.dup_token') as bigint)
        as duplicate_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.market_tokens_distinct_tokens'
        ) as bigint
    ) as market_tokens_distinct_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.odds_history_distinct_tokens'
        ) as bigint
    ) as odds_history_distinct_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.token_odds_daily_distinct_tokens'
        ) as bigint
    ) as token_odds_daily_distinct_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.ledger_distinct_tokens'
        ) as bigint
    ) as ledger_distinct_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.ledger_fully_checked_tokens'
        ) as bigint
    ) as ledger_fully_checked_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.token_sync_skips_distinct_tokens'
        ) as bigint
    ) as token_sync_skips_distinct_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.market_tokens_without_history'
        ) as bigint
    ) as market_tokens_without_history,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.history_tokens_without_market_tokens'
        ) as bigint
    ) as history_tokens_without_market_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.planned_vs_market_tokens'
        ) as double
    ) as planned_vs_market_tokens_ratio,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.history_coverage_vs_market_tokens'
        ) as double
    ) as history_coverage_vs_market_tokens_ratio,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.daily_coverage_vs_market_tokens'
        ) as double
    ) as daily_coverage_vs_market_tokens_ratio,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.ledger_coverage_vs_market_tokens'
        ) as double
    ) as ledger_coverage_vs_market_tokens_ratio,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.fully_checked_vs_market_tokens'
        ) as double
    ) as fully_checked_vs_market_tokens_ratio
from {{ source(source_name, 'pipeline_run_events') }}
{% endmacro %}


{% macro polymarket_sync_run_observability_sql(staging_ref) %}
select
    run_id,
    task_name,
    recorded_at,
    scope_name,
    discovery_mode,
    effective_keyset_tag_slugs,
    keyset_closed,
    keyset_volume_min,
    registry_refreshed,
    events_pages,
    api_requests,
    markets_collected,
    token_rows_collected,
    total_fetched,
    is_noop,
    duration_seconds,
    planned_tokens,
    processed_tokens,
    rows_fetched,
    windows_processed,
    empty_tokens,
    error_tokens,
    permanent_error_tokens,
    invalid_tokens,
    saved_rows,
    saved_daily_rows,
    sync_updates,
    queue_high_watermark,
    http_requests,
    http_429,
    http_errors,
    final_rps,
    token_rate,
    row_rate,
    market_tokens_distinct_tokens,
    odds_history_distinct_tokens,
    token_odds_daily_distinct_tokens,
    ledger_distinct_tokens,
    ledger_fully_checked_tokens,
    token_sync_skips_distinct_tokens,
    market_tokens_without_history,
    history_tokens_without_market_tokens,
    pre_clob_markets,
    invalid_tokens_from_planning,
    closed_done_tokens,
    persisted_skip_tokens,
    recent_skip_tokens,
    empty_cache_skip_tokens,
    already_current_tokens,
    duplicate_tokens,
    planned_vs_market_tokens_ratio,
    history_coverage_vs_market_tokens_ratio,
    daily_coverage_vs_market_tokens_ratio,
    ledger_coverage_vs_market_tokens_ratio,
    fully_checked_vs_market_tokens_ratio,
    case
        when coalesce(processed_tokens, 0) = 0 then null
        else rows_fetched::double / processed_tokens
    end as rows_per_processed_token
from {{ staging_ref }}
where task_name in ('sync_markets', 'sync_odds', 'reconcile_odds_ledger')
{% endmacro %}

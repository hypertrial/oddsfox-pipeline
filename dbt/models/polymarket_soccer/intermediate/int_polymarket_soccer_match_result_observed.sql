with eligible as (
    select
        registry.*,
        events.event_slug,
        events.event_title,
        events.event_subtitle,
        events.series_slugs_json
    from {{ ref('stg_polymarket_soccer_match_result_registry') }} as registry
    inner join {{ ref('stg_polymarket_soccer_event_latest') }} as events
        on registry.event_id = events.event_id
    inner join {{ ref('stg_polymarket_soccer_match_minute_audit_latest_published_success') }} as fetch_audit
        on
            registry.market_id = fetch_audit.market_id
            and registry.yes_token_id = fetch_audit.clob_token_id
            and registry.window_start_at = fetch_audit.exact_window_start_at
            and registry.window_end_at = fetch_audit.exact_window_end_at
)

select
    eligible.event_id,
    eligible.event_slug,
    eligible.event_title,
    eligible.event_subtitle as competition_label,
    eligible.series_slugs_json,
    eligible.market_id,
    eligible.result_role,
    eligible.home_team,
    eligible.away_team,
    eligible.yes_token_id as clob_token_id,
    eligible.window_start_at as match_started_at_utc,
    eligible.window_end_at as match_finished_at_utc,
    eligible.kickoff_source,
    eligible.timing_status,
    eligible.timing_confidence,
    eligible.coverage_tier,
    odds.odds_minute_epoch,
    odds.odds_minute_utc,
    odds.open_price as open_odds,
    odds.high_price as high_odds,
    odds.low_price as low_odds,
    odds.close_price as close_odds,
    odds.avg_price as avg_odds,
    odds.observed_points,
    odds.first_observed_at,
    odds.last_observed_at
from eligible
inner join {{ ref('stg_polymarket_soccer_match_primary_minute_ohlc') }} as odds
    on
        eligible.market_id = odds.market_id
        and eligible.yes_token_id = odds.clob_token_id
where
    odds.odds_minute_utc >= date_trunc('minute', eligible.window_start_at)
    and odds.odds_minute_utc <= date_trunc('minute', eligible.window_end_at)

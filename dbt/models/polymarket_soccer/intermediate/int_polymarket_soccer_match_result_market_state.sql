with eligible as (
    select
        registry.*,
        events.event_slug,
        events.event_title,
        events.event_subtitle,
        events.series_slugs_json,
        fetch_audit.fetch_run_id,
        fetch_audit.fetch_finished_at,
        fetch_audit.in_game_history_sha256
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
    *,
    md5(concat_ws(
        '|', event_id, coalesce(event_slug, ''), coalesce(event_title, ''),
        coalesce(event_subtitle, ''), coalesce(series_slugs_json, ''), market_id,
        result_role, home_team, away_team, yes_token_id, no_token_id,
        cast(window_start_at as varchar), cast(window_end_at as varchar),
        kickoff_source, timing_status, timing_confidence, coverage_tier,
        fetch_run_id, coalesce(in_game_history_sha256, '')
    )) as source_revision
from eligible

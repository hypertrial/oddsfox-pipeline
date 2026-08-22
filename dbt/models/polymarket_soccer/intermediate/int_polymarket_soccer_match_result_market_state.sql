with eligible as (
    select
        registry.*,
        events.event_slug,
        events.event_title,
        events.event_subtitle,
        events.series_slugs_json,
        yes_audit.fetch_run_id,
        yes_audit.fetch_finished_at,
        yes_audit.in_game_history_sha256,
        no_audit.fetch_run_id as no_fetch_run_id,
        no_audit.fetch_finished_at as no_fetch_finished_at,
        no_audit.in_game_history_sha256 as no_in_game_history_sha256
    from {{ ref('stg_polymarket_soccer_match_result_registry') }} as registry
    inner join {{ ref('stg_polymarket_soccer_event_latest') }} as events
        on registry.event_id = events.event_id
    inner join {{ ref('stg_polymarket_soccer_match_minute_audit_latest_published_success') }}
        as yes_audit
        on
            registry.market_id = yes_audit.market_id
            and registry.yes_token_id = yes_audit.clob_token_id
            and registry.window_start_at = yes_audit.exact_window_start_at
            and registry.window_end_at = yes_audit.exact_window_end_at
    left join {{ ref('stg_polymarket_soccer_match_minute_audit_latest_published_success') }}
        as no_audit
        on
            registry.market_id = no_audit.market_id
            and registry.no_token_id = no_audit.clob_token_id
            and registry.window_start_at = no_audit.exact_window_start_at
            and registry.window_end_at = no_audit.exact_window_end_at
)

select
    *,
    md5(concat_ws(
        '|', event_id, coalesce(event_slug, ''), coalesce(event_title, ''),
        coalesce(event_subtitle, ''), coalesce(series_slugs_json, ''), market_id,
        result_role, home_team, away_team, yes_token_id, no_token_id,
        cast(window_start_at as varchar), cast(window_end_at as varchar),
        kickoff_source, timing_status, timing_confidence, coverage_tier,
        fetch_run_id, coalesce(in_game_history_sha256, ''),
        coalesce(no_fetch_run_id, ''), coalesce(no_in_game_history_sha256, '')
    )) as source_revision
from eligible

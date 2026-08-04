# Query Cookbook

These examples use fully qualified DuckDB table names and assume the current
working directory contains `oddsfox.duckdb`. If `.env` sets `DUCKDB_PATH`, open
that file instead.

## WC2026 In-Game Minute Moneylines And Advance Odds

```sql
select
    odds_minute_utc,
    elapsed_window_minute,
    fifa_match_id,
    home_team,
    away_team,
    proposition_type,
    yes_close_price,
    no_close_price,
    minute_status,
    yes_no_close_deviation,
    pair_price_anomaly,
    results_source_revision
from polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds
where fifa_match_id = 104
order by elapsed_window_minute, market_id;
```

Rows span only Gamma's actual match window. A null price means the selected
token had no source point in that minute; it is not filled from another minute
or calculated from the other token. `elapsed_window_minute` is a zero-based,
uncapped wall-clock offset for aligning games; it includes delays, halftime,
extra time, and penalties and is not the official match clock.

Inspect current warning and structural issue details without changing the raw
price policy:

```sql
select
    severity,
    issue_type,
    fifa_match_id,
    market_id,
    clob_token_id,
    measured_value,
    threshold_value,
    issue_detail
from polymarket_wc2026_observability.polymarket_wc2026_match_minute_odds_quality_issues
order by severity, issue_type, fifa_match_id, market_id;
```

## WC2026 Polymarket Hourly Series

Yes-outcome hourly OHLC odds for one event:

```sql
select
    odds_hour_utc,
    event_slug,
    question,
    open_odds,
    high_odds,
    low_odds,
    close_odds,
    observed_points
from polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds
where event_slug = 'fifwc-2026-winner'
order by question, odds_hour_epoch;
```

Prices are raw Yes-outcome CLOB probabilities. Parse `outcomes` when you need
the literal outcome label for the selected token.

## Latest Hourly Close By Market

```sql
select
    market_id,
    event_slug,
    question,
    close_odds,
    odds_hour_utc,
    is_active,
    is_closed,
    is_resolved
from polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds
qualify row_number() over (
    partition by market_id
    order by odds_hour_epoch desc
) = 1
order by event_slug, question;
```

## WC2026 Fixtures And Team Status

Join hourly odds to tournament state manually when question text or event
metadata implies a team:

```sql
select
    odds.event_slug,
    odds.question,
    odds.close_odds,
    t.team_name,
    t.tournament_status,
    t.next_match_date
from polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds as odds
inner join international_results_wc2026_marts.international_results_wc2026_team_status as t
    on lower(odds.question) like '%' || lower(t.team_name) || '%'
qualify row_number() over (
    partition by odds.market_id
    order by odds.odds_hour_epoch desc
) = 1
order by t.team_name, odds.question;
```

Inspect fixtures directly:

```sql
select
    match_date,
    stage_key,
    home_team,
    away_team,
    home_score,
    away_score,
    match_status,
    advancing_team,
    advancer_inference_status
from international_results_wc2026_marts.international_results_wc2026_matches
order by match_date, match_id;
```

## Kalshi Stage Markets

Current actionable stage-of-elimination prices:

```sql
select
    canonical_team_name,
    stage_key,
    progression_outcome_label,
    progression_price,
    current_price_status,
    market_ticker
from kalshi_wc2026_marts.kalshi_wc2026_stage_markets
where is_actionable_live_market
order by canonical_team_name, stage_rank;
```

Hourly progression-side series:

```sql
select
    odds_hour_utc,
    canonical_team_name,
    stage_key,
    progression_open_price,
    progression_high_price,
    progression_low_price,
    progression_close_price,
    volume
from kalshi_wc2026_marts.kalshi_wc2026_stage_market_hourly_odds
where canonical_team_name = 'Argentina'
  and stage_key = 'round_of_16'
order by odds_hour_epoch;
```

For Kalshi stage markets, the raw Yes price and progression price can differ
when the source market is elimination-framed. Use `progression_*_price` for
team-progression analysis.

## Kalshi Group Winners

Current group-winner prices:

```sql
select
    group_letter,
    canonical_team_name,
    group_winner_price,
    current_price_status,
    market_ticker
from kalshi_wc2026_marts.kalshi_wc2026_group_winner_markets
where is_actionable_live_market
order by group_letter, canonical_team_name;
```

Hourly group-winner series:

```sql
select
    odds_hour_utc,
    group_letter,
    canonical_team_name,
    open_price,
    high_price,
    low_price,
    close_price,
    avg_price,
    volume
from kalshi_wc2026_marts.kalshi_wc2026_group_winner_market_hourly_odds
where group_letter = 'A'
order by canonical_team_name, odds_hour_epoch;
```

## Run Health And Freshness

Latest Polymarket WC2026 ingestion telemetry:

```sql
select *
from polymarket_wc2026_observability.polymarket_wc2026_ingestion_run_observability
order by recorded_at desc
limit 20;
```

Latest Kalshi ingestion telemetry:

```sql
select *
from kalshi_wc2026_observability.kalshi_wc2026_ingestion_run_observability
order by recorded_at desc
limit 20;
```

Latest hourly data available in each major time-series mart:

```sql
select
    'polymarket_wc2026' as mart,
    max(odds_hour_utc) as latest_hour
from polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds
union all
select
    'kalshi_stage',
    max(odds_hour_utc)
from kalshi_wc2026_marts.kalshi_wc2026_stage_market_hourly_odds
union all
select
    'kalshi_group_winner',
    max(odds_hour_utc)
from kalshi_wc2026_marts.kalshi_wc2026_group_winner_market_hourly_odds
```

## Python And Pandas

```python
import duckdb

con = duckdb.connect("oddsfox.duckdb", read_only=True)

df = con.sql("""
    select
        event_slug,
        question,
        close_odds,
        odds_hour_utc
    from polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds
    where is_active
      and not is_closed
    order by event_slug, question, odds_hour_epoch desc
""").df()
```

Export one query to CSV:

```python
con.sql("""
    copy (
        select *
        from polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds
        where is_active
          and not is_closed
    )
    to 'wc2026_active_market_hourly.csv' (header, delimiter ',')
""")
```

Export to Parquet:

```python
con.sql("""
    copy (
        select *
        from polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds
    )
    to 'wc2026_market_hourly.parquet'
    (format parquet)
""")
```

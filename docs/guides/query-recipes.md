# Query Cookbook

These examples use fully qualified DuckDB table names and assume the current
working directory contains `oddsfox.duckdb`. If `.env` sets `DUCKDB_PATH`, open
that file instead.

## Compare WC2026 Knockout Match Advance Prices

```sql
select
    odds_hour_utc,
    fifa_match_id,
    stage_key,
    home_team,
    away_team,
    polymarket_home_advance_price,
    kalshi_home_advance_price,
    polymarket_away_advance_price,
    kalshi_away_advance_price
from wc2026_marts.wc2026_knockout_match_hourly_odds
where both_sources_complete
order by fifa_match_id, odds_hour_epoch;
```

These are raw team-advance closes, including extra time and penalties. Missing
provider-side observations remain null and are not carried across hours.

## WC2026 In-Game Minute Moneylines And Advance Odds

```sql
select
    odds_minute_utc,
    fifa_match_id,
    home_team,
    away_team,
    proposition_type,
    yes_close_price,
    no_close_price,
    yes_observed_points,
    no_observed_points,
    minute_complete
from polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds
where fifa_match_id = 104
order by odds_minute_epoch, market_id;
```

Rows span only Gamma's actual match window. A null price means the selected
token had no source point in that minute; it is not filled from another minute
or calculated from the other token.

## Current WC2026 Polymarket Prices

Actionable live progression prices by team and stage:

```sql
select
    canonical_team_name,
    stage_key,
    progression_outcome_label,
    current_price,
    current_price_hour_utc,
    current_price_status
from polymarket_wc2026_marts.polymarket_wc2026_knockout_markets
where is_actionable_live_market
order by canonical_team_name, stage_rank;
```

Use `is_actionable_live_market` for current live analysis. Closed, resolved,
inactive, stale, and missing-price rows remain available for history and
diagnostics.

## WC2026 Polymarket Hourly Series

Progression-side hourly OHLC odds for one team and stage:

```sql
select
    odds_hour_utc,
    canonical_team_name,
    stage_key,
    progression_outcome_label,
    open_price,
    high_price,
    low_price,
    close_price,
    observed_points
from polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds
where canonical_team_name = 'United States'
  and stage_key = 'round_of_16'
  and price_represents = 'progression'
order by odds_hour_epoch;
```

Elimination-framed Polymarket markets may expose the No token as the public
progression side. Trust `price_represents` and `progression_outcome_label`
instead of inferring semantics from question text.

## Stale Or Missing WC2026 Live Markets

```sql
select
    canonical_team_name,
    stage_key,
    current_price_status,
    current_price_age_hours,
    question,
    market_id
from polymarket_wc2026_marts.polymarket_wc2026_knockout_markets
where is_active_team_live_market
  and current_price_status in ('stale_live', 'missing_live')
order by current_price_status, canonical_team_name, stage_rank;
```

Follow up with:

```sql
select *
from polymarket_wc2026_observability.polymarket_wc2026_knockout_data_quality
where severity in ('error', 'warn')
order by severity, issue_key;
```

## WC2026 Fixtures And Team Status

Join current market prices to tournament state:

```sql
select
    m.canonical_team_name,
    t.tournament_status,
    t.next_match_date,
    t.next_stage_key,
    m.stage_key,
    m.current_price
from polymarket_wc2026_marts.polymarket_wc2026_knockout_markets as m
inner join international_results_wc2026_marts.international_results_wc2026_team_status as t
    on m.canonical_team_name = t.team_name
where m.is_actionable_live_market
order by m.canonical_team_name, m.stage_rank;
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

## US Midterms Hourly Odds

```sql
select
    odds_hour_utc,
    event_slug,
    question,
    outcome_label,
    close_price,
    market_volume_usd,
    clob_token_id
from polymarket_us_midterms_2026_marts.polymarket_us_midterms_2026_market_token_hourly_odds
where event_slug is not null
order by event_slug, question, outcome_index, odds_hour_epoch;
```

Balance of Power combinations are independent binary Yes/No markets.
Probabilities across combinations do not necessarily sum to 1.0.

## Run Health And Freshness

Latest Polymarket WC2026 ingestion telemetry:

```sql
select *
from polymarket_wc2026_observability.polymarket_wc2026_sync_run_observability
order by recorded_at desc
limit 20;
```

Latest Kalshi ingestion telemetry:

```sql
select *
from kalshi_wc2026_observability.kalshi_wc2026_sync_run_observability
order by recorded_at desc
limit 20;
```

Latest hourly data available in each major time-series mart:

```sql
select
    'polymarket_wc2026' as mart,
    max(odds_hour_utc) as latest_hour
from polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds
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
union all
select
    'polymarket_us_midterms_2026',
    max(odds_hour_utc)
from polymarket_us_midterms_2026_marts.polymarket_us_midterms_2026_market_token_hourly_odds;
```

## Python And Pandas

```python
import duckdb

con = duckdb.connect("oddsfox.duckdb", read_only=True)

df = con.sql("""
    select
        canonical_team_name,
        stage_key,
        current_price
    from polymarket_wc2026_marts.polymarket_wc2026_knockout_markets
    where is_actionable_live_market
    order by canonical_team_name, stage_rank
""").df()
```

Export one query to CSV:

```python
con.sql("""
    copy (
        select *
        from polymarket_wc2026_marts.polymarket_wc2026_knockout_markets
        where is_actionable_live_market
    )
    to 'wc2026_actionable_prices.csv' (header, delimiter ',')
""")
```

Export to Parquet:

```python
con.sql("""
    copy (
        select *
        from polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds
    )
    to 'wc2026_knockout_hourly.parquet'
    (format parquet)
""")
```

# Query the warehouse

Use this page when you want to query OddsFox Pipeline data, not operate it.
OddsFox Pipeline ships code and local warehouse tooling, not a hosted dataset.
Analysts query the DuckDB file produced by a local or self-managed run. For the
full analyst map, start with [Analysts](../audiences/analysts.md). Term
definitions live in the [Glossary](../concepts/glossary.md).

!!! note "Reference ladder"

    Chooser → dictionary → public contracts → warehouse reference; do not treat
    staging/raw as APIs. This page is the chooser and trust guide.

## Shortest Path

=== "I already have a warehouse"

    Open `oddsfox.duckdb` (or the file in `DUCKDB_PATH`). See
    [Configuration](../reference/configuration.md) for path precedence.

=== "I need a warehouse first"

    Follow [Quickstart](../getting-started/index.md), or ask an operator to run
    a scope. See [Analysts](../audiences/analysts.md) for the full analyst map.

## Query Rules

- Query `*_marts` first. These are the supported public query surfaces.
- Use `*_observability` when checking freshness, coverage, run health, or data
  quality findings.
- Treat `*_raw`, `*_ops`, `*_staging`, and `*_intermediate` schemas as internal
  or debugging surfaces.
- Prefer fully qualified table names, such as
  `polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds`.
- Filter by `event_slug`, `event_id`, or market status fields when narrowing
  WC2026 hourly analysis. Closed and resolved markets remain in the mart.

Historical closed and resolved rows are intentionally retained. Do not assume a
row is live because it appears in a mart.

## Open With Python

```python
import duckdb

con = duckdb.connect("oddsfox.duckdb", read_only=True)
rows = con.sql("""
    select
        event_slug,
        question,
        odds_hour_utc,
        close_odds,
        event_volume_usd_lifetime_reported
    from polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds
    where is_active
      and not is_closed
    order by event_slug, question, odds_hour_epoch desc
""").df()
```

Use `read_only=True` for notebooks and analysis so you do not compete with a
running Dagster/dbt writer.

## Which Table Should I Use?

| Goal | Start Here | Notes |
| --- | --- | --- |
| WC2026 Polymarket hourly odds | `polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds` | One row per `market_id`, `odds_hour_epoch`; primary-outcome CLOB prices (`primary_outcome_label`) with market and event metadata. |
| WC2026 in-game match minutes | `polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds` | Dense minute series for all 104 matches; requires the match-minute path, not ordinary hourly ingest alone. |
| Argentina–Egypt historical L2 depth | `polymarket_wc2026_marts.polymarket_wc2026_match_order_book` | Long-form independent bid/ask levels for both PMXT outcome-token snapshot streams; requires the unscheduled PMXT backfill. |
| WC2026 fixtures and results | `international_results_wc2026_marts.international_results_wc2026_matches` | One row per `match_id`, with knockout advancer inference; requires Kalshi full pipeline or match-minute ingest, not the Polymarket golden-mart quickstart. |
| WC2026 team status | `international_results_wc2026_marts.international_results_wc2026_team_status` | Join on `canonical_team_name` or `team_name`; same ingest prerequisite as fixtures/results. |
| Current Kalshi stage prices | `kalshi_wc2026_marts.kalshi_wc2026_stage_markets` | Filter to `is_actionable_live_market`. |
| Kalshi stage hourly series | `kalshi_wc2026_marts.kalshi_wc2026_stage_market_hourly_odds` | Use `progression_*_price` for stage progression semantics. |
| Current Kalshi group-winner prices | `kalshi_wc2026_marts.kalshi_wc2026_group_winner_markets` | Use `group_winner_price`. |
| Kalshi group-winner hourly series | `kalshi_wc2026_marts.kalshi_wc2026_group_winner_market_hourly_odds` | One row per `market_ticker`, `odds_hour_epoch`. |
| WC2026 finalized Polygon settlement minutes (advanced) | `polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds` | Fixed 150/210-minute scheduled windows; empty sides remain null; fill counts are normalized economic legs. |

## Trust Before Analysis

For hourly odds:

1. Confirm the market's enclosing event is volume-eligible
   (`event_volume_usd_lifetime_reported >= 100000`).
2. Inspect market status fields (`is_active`, `is_closed`, `is_resolved`) when
   you need current vs historical rows.
3. If coverage looks sparse, inspect
   `polymarket_wc2026_observability.polymarket_wc2026_ingestion_run_observability`.
4. Keep historical rows when you explicitly want closed or resolved markets.

Useful observability tables:

| Source | Table | Use |
| --- | --- | --- |
| Polymarket WC2026 | `polymarket_wc2026_observability.polymarket_wc2026_ingestion_run_observability` | Ingestion run telemetry and request counts. |
| Polygon settlement WC2026 | `polymarket_wc2026_observability.polymarket_wc2026_polygon_settlement_data_quality` | Published scan/seed match, finalized chunk coverage, exact dense inventory, and hard publication state. |
| Polygon settlement WC2026 | `polymarket_wc2026_observability.polymarket_wc2026_polygon_settlement_quality_issues` | Sparse/no-fill, derived-leg, pair-deviation, secondary-RPC, and structural findings. |
| Kalshi WC2026 | `kalshi_wc2026_observability.kalshi_wc2026_data_quality` | Stage/group-winner stale or missing live odds and coverage findings. |
| Kalshi WC2026 | `kalshi_wc2026_observability.kalshi_wc2026_ingestion_run_observability` | Kalshi ingestion telemetry. |

Next: use [Query recipes](query-recipes.md) for examples, then the
[Data dictionary](../reference/data-dictionary.md) for table-by-table semantics.

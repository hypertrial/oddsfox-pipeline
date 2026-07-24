# Analyst Guide

Use this page when you want to query OddsFox Pipeline data, not operate it.
OddsFox Pipeline ships code and local warehouse tooling, not a hosted dataset.
Analysts query the DuckDB file produced by a local or self-managed run. For the
full analyst map, start with [Analysts](../audiences/analysts.md). Term
definitions live in the [Glossary](../concepts/glossary.md).

## Shortest Path

=== "I already have a warehouse"

    Open it directly:

    ```bash
    duckdb oddsfox.duckdb
    ```

    The default warehouse is `oddsfox.duckdb` in the repo root. If `.env` sets
    `DUCKDB_PATH`, query that file instead. See
    [Configuration](../reference/configuration.md)
    for path precedence. Prefer `read_only=True` in Python notebooks.

=== "I need a warehouse first"

    Ask an operator to run a scope, or run one yourself:

    ```bash
    uv sync --extra dev
    cp .env.example .env
    uv run python scripts/run_scope.py polymarket:wc2026 --step full
    ```

    Optional shipped scopes:

    ```bash
    uv run python scripts/run_scope.py kalshi:wc2026 --step full
    uv run python scripts/run_scope.py polymarket:us_midterms_2026 --step full
    ```

    Use [Quickstart](../getting-started/index.md) for the full operator path.
    Polygon settlement history is optional and advanced; ordinary mart queries
    do not need it.

## Query Rules

- Query `*_marts` first. These are the public analytics surfaces.
- Use `*_observability` when checking freshness, coverage, run health, or data
  quality findings.
- Treat `*_raw`, `*_ops`, `*_staging`, and `*_intermediate` schemas as internal
  or debugging surfaces.
- Prefer fully qualified table names, such as
  `polymarket_wc2026_marts.polymarket_wc2026_knockout_markets`.
- For current live analysis, prefer `is_actionable_live_market` when the mart
  exposes it, then inspect `current_price_status`.

Historical closed and resolved rows are intentionally retained. Do not assume a
row is live because it appears in a mart.

## Open With Python

```python
import duckdb

con = duckdb.connect("oddsfox.duckdb", read_only=True)
rows = con.sql("""
    select
        canonical_team_name,
        stage_key,
        progression_outcome_label,
        current_price,
        current_price_status
    from polymarket_wc2026_marts.polymarket_wc2026_knockout_markets
    where is_actionable_live_market
    order by canonical_team_name, stage_rank
""").df()
```

Use `read_only=True` for notebooks and analysis so you do not compete with a
running Dagster/dbt writer.

## Which Table Should I Use?

| Goal | Start Here | Notes |
| --- | --- | --- |
| Current WC2026 Polymarket progression prices | `polymarket_wc2026_marts.polymarket_wc2026_knockout_markets` | Filter to `is_actionable_live_market` for current live use. |
| WC2026 Polymarket progression hourly series | `polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds` | One row per `clob_token_id`, `odds_hour_epoch`. Prices are normalized to progression. |
| WC2026 graph or both-token analysis | `polymarket_wc2026_marts.polymarket_wc2026_graph_token_hourly_odds` | Keeps Yes and No tokens with `is_progression_token`. |
| Cross-platform knockout match hours | `wc2026_marts.wc2026_knockout_match_hourly_odds` | Compare Polymarket and Kalshi match-advance closes. |
| WC2026 in-game match minutes | `polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds` | Dense minute series for all 104 matches; requires the match-minute path, not ordinary hourly ingest alone. |
| WC2026 fixtures and results | `international_results_wc2026_marts.international_results_wc2026_matches` | One row per `match_id`, with knockout advancer inference. |
| WC2026 team status | `international_results_wc2026_marts.international_results_wc2026_team_status` | Join on `canonical_team_name` or `team_name`. |
| Current Kalshi stage prices | `kalshi_wc2026_marts.kalshi_wc2026_stage_markets` | Filter to `is_actionable_live_market`. |
| Kalshi stage hourly series | `kalshi_wc2026_marts.kalshi_wc2026_stage_market_hourly_odds` | Use `progression_*_price` for stage progression semantics. |
| Current Kalshi group-winner prices | `kalshi_wc2026_marts.kalshi_wc2026_group_winner_markets` | Use `group_winner_price`. |
| Kalshi group-winner hourly series | `kalshi_wc2026_marts.kalshi_wc2026_group_winner_market_hourly_odds` | One row per `market_ticker`, `odds_hour_epoch`. |
| US midterms Polymarket hourly odds | `polymarket_us_midterms_2026_marts.polymarket_us_midterms_2026_market_token_hourly_odds` | Balance of Power combos are independent binary markets. |
| WC2026 finalized Polygon settlement minutes (advanced) | `polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds` | Fixed 150/210-minute scheduled windows; empty sides remain null; fill counts are normalized economic legs. |

## Trust Before Analysis

For current prices:

1. Filter to rows that are meant for current use:
   `is_actionable_live_market = true`.
2. Check `current_price_status`; live rows should usually be `fresh_live`.
3. If rows are `stale_live` or `missing_live`, inspect the matching
   `*_data_quality` and `*_sync_run_observability` marts.
4. Keep historical rows only when you explicitly want closed or resolved market
   history.

Useful observability tables:

| Source | Table | Use |
| --- | --- | --- |
| Polymarket WC2026 | `polymarket_wc2026_observability.polymarket_wc2026_knockout_data_quality` | Source anomalies, sparse coverage, stale or missing live odds. |
| Polymarket WC2026 | `polymarket_wc2026_observability.polymarket_wc2026_sync_run_observability` | Ingestion run telemetry and request counts. |
| Polygon settlement WC2026 | `polymarket_wc2026_observability.polymarket_wc2026_polygon_settlement_data_quality` | Published scan/seed match, finalized chunk coverage, exact dense inventory, and hard publication state. |
| Polygon settlement WC2026 | `polymarket_wc2026_observability.polymarket_wc2026_polygon_settlement_quality_issues` | Sparse/no-fill, derived-leg, pair-deviation, secondary-RPC, and structural findings. |
| Kalshi WC2026 | `kalshi_wc2026_observability.kalshi_wc2026_data_quality` | Stage/group-winner stale or missing live odds and coverage findings. |
| Kalshi WC2026 | `kalshi_wc2026_observability.kalshi_wc2026_sync_run_observability` | Kalshi ingestion telemetry. |
| US midterms 2026 | `polymarket_us_midterms_2026_observability.polymarket_us_midterms_2026_sync_run_observability` | Midterms ingestion telemetry. |

Next: use [Query recipes](query-recipes.md) for examples, then the
[Data dictionary](../reference/data-dictionary.md) for table-by-table semantics.

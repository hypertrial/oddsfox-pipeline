# First query

<p class="of-personas" markdown><span class="of-persona of-persona--analyst">Analyst</span></p>

Use this page for a linear first session against an existing local warehouse.
OddsFox Pipeline ships software and local warehouse tooling, not a hosted
dataset. For table selection, trust rules, and the full chooser, continue with
[Query the warehouse](query-the-warehouse.md).

## 1. Open read-only

Prefer a read-only connection so analysis does not compete with a writer:

```python
import duckdb

con = duckdb.connect("oddsfox.duckdb", read_only=True)
```

Or open the CLI:

```bash
duckdb oddsfox.duckdb
```

The default path is `oddsfox.duckdb` in the repository root. If `.env` sets
`DUCKDB_PATH`, open that file instead. See
[Configuration](../reference/configuration.md) for path precedence.

If you do not have a warehouse yet, ask an operator to complete
[Quickstart](../getting-started/index.md), then return here.

## 2. List public schemas

```sql
show schemas;
```

Prefer schemas ending in `*_marts`. Use `*_observability` for freshness and
quality checks. Treat `*_raw`, `*_ops`, `*_staging`, and `*_intermediate` as
internal surfaces.

## 3. Pick a starting mart

| Goal | Start here |
| --- | --- |
| Polymarket WC2026 hourly odds | `polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds` |
| Current Kalshi stage prices | `kalshi_wc2026_marts.kalshi_wc2026_stage_markets` |

The full chooser lives in
[Query the warehouse](query-the-warehouse.md#which-table-should-i-use).

## 4. Run one query

Polymarket golden mart (after a Polymarket full run):

```python
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

Kalshi stage markets (after a Kalshi full run) — prefer
`is_actionable_live_market`:

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

## 5. Trust check

Before treating prices as live, inspect observability:

=== "Polymarket WC2026"

    ```sql
    select *
    from polymarket_wc2026_observability.polymarket_wc2026_ingestion_run_observability
    order by 1 desc
    limit 20;
    ```

=== "Kalshi WC2026"

    ```sql
    select *
    from kalshi_wc2026_observability.kalshi_wc2026_data_quality
    limit 50;
    ```

Historical closed and resolved rows are intentionally retained. Do not assume a
row is live because it appears in a mart. Term shortcuts live in
[Analyst shortcuts](../concepts/glossary.md).

## 6. Next steps

| Goal | Page |
| --- | --- |
| Copy-paste SQL and Python | [Query recipes](query-recipes.md) |
| Grain, filters, and common mistakes | [Data dictionary](../reference/data-dictionary.md) |
| Full table chooser and trust rules | [Query the warehouse](query-the-warehouse.md) |
| Analyst map and joins | [Analysts](../audiences/analysts.md) |

## See also

- [Analysts](../audiences/analysts.md)
- [Query the warehouse](query-the-warehouse.md)
- [Query recipes](query-recipes.md)
- [Data dictionary](../reference/data-dictionary.md)
- [Data contracts](../reference/data-contracts.md)

# OddsFox Pipeline dbt Project

This dbt project models Polymarket and Kalshi data and joins immutable
Scraper-owned reference tables in DuckDB.

See the operator docs for warehouse details:

- [Warehouse](../docs/reference/warehouse.md)
- [Orchestration](../docs/reference/orchestration.md)

Modeled layers:

- `polymarket_wc2026_staging`
- `polymarket_wc2026_intermediate`
- `polymarket_wc2026_marts`
- `polymarket_wc2026_observability`
- `kalshi_wc2026_staging`
- `kalshi_wc2026_intermediate`
- `kalshi_wc2026_marts`
- `kalshi_wc2026_observability`
- `wc2026_intermediate`
- `wc2026_marts`
- `wc2026_observability`

Run locally:

```bash
dbt parse --project-dir dbt --profiles-dir dbt/profiles
dbt build --full-refresh --project-dir dbt --profiles-dir dbt/profiles
```

WC2026 scoping is encoded in the dbt graph and
`polymarket_wc2026_ops.market_scope_registry`; real-team validation comes from
`oddsfox_reference.international_results_wc2026_team_status`, loaded from the
active Scraper bundle. There is no dbt scope-selection var.

Documented Polymarket WC2026 mart:

- `polymarket_wc2026_market_hourly_odds`

`polymarket_wc2026_market_hourly_odds` is the documented golden hourly odds
mart over the private incremental `int_polymarket_wc2026_token_hourly_odds`
fact.

The stable market strategy surface is contract version `wc2026.v1` in
`wc2026_marts.contract_metadata`. Pipeline publishes venue-market mapping,
current and historical price/liquidity, combined source provenance, and
strategy readiness in
`wc2026_observability.wc2026_strategy_input_readiness`.

Non-market reference acquisition, parsing, and transformation live only in
Scraper. Pipeline market models read the checksummed handoff directly from the
dedicated `oddsfox_reference` schema; no compatibility aliases are created.

If a local DuckDB file still has deleted schedule/catalog marts or older relation
types, reset the local warehouse or drop the affected dbt schemas before rebuilding.

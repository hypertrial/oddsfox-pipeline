# OddsFox Pipeline dbt Project

This dbt project models WC2026 Polymarket data plus FIFA World Cup
fixture/result data in DuckDB.

See the operator docs for warehouse details:

- [Warehouse](../docs/reference/warehouse.md)
- [Orchestration](../docs/reference/orchestration.md)

Modeled layers:

- `polymarket_wc2026_staging`
- `polymarket_wc2026_intermediate`
- `polymarket_wc2026_marts`
- `polymarket_wc2026_observability`
- `international_results_wc2026_staging`
- `international_results_wc2026_intermediate`
- `international_results_wc2026_marts`
- `international_results_wc2026_observability`

Run locally:

```bash
dbt parse --project-dir dbt --profiles-dir dbt/profiles
dbt build --full-refresh --project-dir dbt --profiles-dir dbt/profiles
```

WC2026 scoping is encoded in the model graph and
`polymarket_wc2026_ops.market_scope_registry`; real-team validation comes from
`international_results_wc2026_team_status`. There is no dbt scope-selection var.

Public knockout marts:

- `polymarket_wc2026_knockout_market_tokens`
- `polymarket_wc2026_knockout_token_hourly_odds`
- `polymarket_wc2026_knockout_markets`
- `international_results_wc2026_matches`
- `international_results_wc2026_team_status`

If a local DuckDB file still has deleted broad marts or older relation types,
reset the local warehouse or drop the affected dbt schemas before rebuilding.

`polymarket_wc2026_knockout_token_hourly_odds` is a public view over the private
incremental `int_polymarket_wc2026_token_hourly_odds` hourly price fact.

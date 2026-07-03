# OddsFox Pipeline dbt Project

This dbt project models Polymarket raw, ops, and selected-scope tables in DuckDB.

See the operator docs for warehouse details:

- [Warehouse](../docs/warehouse.md)
- [Operations](../docs/operations.md)

Modeled layers:

- `polymarket_staging`
- `polymarket_intermediate`
- `polymarket_marts`
- `polymarket_observability`

Run locally:

```bash
dbt parse --project-dir dbt --profiles-dir dbt/profiles
dbt build --full-refresh --project-dir dbt --profiles-dir dbt/profiles
```

Manual `dbt build` uses `active_market_scopes` from `dbt/dbt_project.yml` (default
`[wc2026]`), not `.env`. Keep it in sync with `POLYMARKET_MARKET_SCOPES` when
running dbt outside Dagster:

```bash
dbt build --full-refresh --project-dir dbt --profiles-dir dbt/profiles \
  --vars '{"active_market_scopes": ["wc2026", "nba"]}'
```

Dagster's `polymarket_dbt` asset passes `--vars` automatically.

# OddsFox Pipeline dbt Project

This dbt project models WC2026 Polymarket raw, ops, marts, and observability
tables in DuckDB.

See the operator docs for warehouse details:

- [Warehouse](../docs/warehouse.md)
- [Operations](../docs/operations.md)

Modeled layers:

- `polymarket_wc2026_staging`
- `polymarket_wc2026_intermediate`
- `polymarket_wc2026_marts`
- `polymarket_wc2026_observability`

Run locally:

```bash
dbt parse --project-dir dbt --profiles-dir dbt/profiles
dbt build --full-refresh --project-dir dbt --profiles-dir dbt/profiles
```

WC2026 scoping is encoded in the model graph and
`polymarket_wc2026_ops.market_scope_registry`; there is no dbt scope-selection var.

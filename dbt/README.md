# OddsFox Pipeline dbt Project

This dbt project models WC2026 Polymarket raw, ops, marts, and observability
tables in DuckDB.

See the operator docs for warehouse details:

- [Warehouse](../docs/warehouse.md)
- [Operations](../docs/operations.md)

Modeled layers:

- `wc2026_polymarket_staging`
- `wc2026_polymarket_intermediate`
- `wc2026_polymarket_marts`
- `wc2026_polymarket_observability`

Run locally:

```bash
dbt parse --project-dir dbt --profiles-dir dbt/profiles
dbt build --full-refresh --project-dir dbt --profiles-dir dbt/profiles
```

WC2026 scoping is encoded in the model graph and
`wc2026_polymarket_ops.market_scope_registry`; there is no dbt scope-selection var.

# Design Decisions

These are intentional `v0.2.x` product choices. Prefer deleting and replacing
over compatibility layers unless a change explicitly scopes legacy work.

## Local-First, No Hosted Data Service

Why: keeps MIT distribution, data ownership, and rate limits honest; avoids
shipping production data in the canonical tree.

See [Scope and non-goals](scope-and-non-goals.md).

## No Warehouse Migrations

Layout and mart contracts may break between `0.2.x` releases. Operators with an
older DuckDB file should delete `oddsfox.duckdb*` and rebuild.

Why: the project is too new to carry a migration surface; reset is smaller and
safer than dual-read shims.

## Fixed Scopes, Not Runtime Selectors

`run_scope.py` accepts only the shipped refs (`polymarket:wc2026`,
`polymarket:soccer`,
`kalshi:wc2026`). Dedicated advanced jobs such as Polygon settlement sit outside
that chooser. Dagster asset configs do not accept arbitrary runtime scope strings.

Why: keeps asset keys, dbt selectors, contracts, and docs aligned; prevents
half-wired scopes.

See [Choose a scope](../getting-started/choose-a-scope.md).

## Polygon Settlement Isolation

Why: historical on-chain evidence has different trust, privacy, and operational
boundaries than quote/CLOB history.

See [Scope and non-goals](scope-and-non-goals.md),
[Recreate Polygon settlement mart](../guides/recreate-polygon-settlement-mart.md),
and [Data contracts](../reference/data-contracts.md).

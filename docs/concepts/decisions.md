# Design Decisions

These are intentional `v0.1.x` product choices. Prefer deleting and replacing
over compatibility layers unless a change explicitly scopes legacy work.

## Local-First, No Hosted Data Service

OddsFox Pipeline is software and operator tooling. Hypertrial does not operate a
hosted production pipeline or dataset for this repository. Operators supply
inputs and own warehouses and exports.

Why: keeps MIT distribution, data ownership, and rate limits honest; avoids
shipping production data in the canonical tree.

See [Scope and non-goals](scope-and-non-goals.md).

## No Warehouse Migrations

Layout and mart contracts may break between `0.1.x` releases. Operators with an
older DuckDB file should delete `oddsfox.duckdb*` and rebuild.

Why: the project is too new to carry a migration surface; reset is smaller and
safer than dual-read shims.

## Fixed Scopes, Not Runtime Selectors

`run_scope.py` accepts only the shipped refs (`polymarket:wc2026`,
`polymarket:us_midterms_2026`, `kalshi:wc2026`). Dedicated advanced jobs such as
Polygon settlement and the cross-platform knockout pipeline sit outside that
chooser. Dagster asset configs do not accept arbitrary runtime scope strings.

Why: keeps asset keys, dbt selectors, contracts, and docs aligned; prevents
half-wired scopes.

See [Choose a scope](../getting-started/choose-a-scope.md).

## Polygon Settlement Isolation

The Polygon settlement-history flow uses an operator-local manifest, finalized
Polygon V2 logs, and its own unscheduled job and dbt tag. It must not call
Gamma, CLOB, the Polymarket UI, international-results, or OpenFootball at
runtime. Ordinary WC2026 odds pipelines do not depend on it.

Why: historical on-chain evidence has different trust, privacy, and operational
boundaries than quote/CLOB history.

See [Recreate local marts](../guides/recreate-local-marts.md) and
[Data contracts](../reference/data-contracts.md).

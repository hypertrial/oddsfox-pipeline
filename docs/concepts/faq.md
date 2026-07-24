# FAQ

## Where should I start?

| Role | Start page |
| --- | --- |
| Query an existing warehouse | [Analysts](../audiences/analysts.md) |
| Run or maintain the pipeline | [Operators](../audiences/operators.md) |
| Change code or dbt | [Contributors](../audiences/contributors.md) |
| Consume marts / graph parquet downstream | [Integrators](../audiences/integrators.md) |

## Is there a hosted OddsFox Pipeline dataset or API?

No. This project ships software and documentation. Operators run ingestion and
own the resulting warehouse. See [Scope and non-goals](scope-and-non-goals.md).

## Where is the documentation site?

[data.oddsfox.io](https://data.oddsfox.io/). Validate docs changes locally with
`uv run make docs-check`. While editing, use `uv run make docs-serve`.

## Do I need API keys?

Kalshi WC2026 uses the public trade API with no credentials. Polymarket public
flows work without CLOB credentials unless a selected live flow explicitly
requires authentication. Polygon settlement needs a finalized-capable JSON-RPC
endpoint and is optional.

## Can I use Postgres instead of DuckDB?

Not as a supported `v0.1.x` warehouse. The shipped stack targets local DuckDB.

## Are warehouse migrations supported?

No. If a checkout replaces an older layout, delete `oddsfox.duckdb*` and rerun
quickstart. See [Design decisions](decisions.md).

## Are schedules on by default?

No. Keep hourly schedules disabled until manual jobs and dbt builds are healthy.
See [Enable schedules](../guides/enable-schedules.md).

## Is Polygon settlement required for WC2026 analysis?

No. It is an isolated advanced historical flow with its own job and dbt tag.
Ordinary Polymarket/Kalshi WC2026 marts do not depend on it.

## How do pipeline outputs relate to trading?

Pipeline marts and graph parquet are analytics outputs. Order execution is a
separate concern in `oddsfox-execution`. See
[System overview](system-overview.md) and [Integration](integration.md).

## Are strategy and execution open source in this repo?

No. This repository is the warehouse component. Private strategy and parent
orchestration live elsewhere; public graph tooling is separate. See the
repository roles table in [System overview](system-overview.md).

## How do I reset a broken local warehouse?

Stop writers, then:

```bash
rm oddsfox.duckdb*
```

Rerun [Quickstart](../getting-started/index.md) or the relevant scope. Prefer
targeted recovery from [Validate and recover](../guides/validate-and-recover.md)
when a full reset is unnecessary.

# FAQ

## Where should I start?

| Role | Start page |
| --- | --- |
| Query an existing warehouse | [Analysts](../audiences/analysts.md) |
| Run or maintain the pipeline | [Operators](../audiences/operators.md) |
| Change code or dbt | [Contributors](../audiences/contributors.md) |
| Consume marts downstream | [Integrators](../audiences/integrators.md) |

## Is there a hosted OddsFox Pipeline dataset or API?

No — see [Scope and non-goals](scope-and-non-goals.md).

## Do I get rights in the data with the software?

No — see [Operator responsibilities](operator-responsibilities.md) and
[THIRD_PARTY_NOTICES.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/THIRD_PARTY_NOTICES.md).

## Is this trading or betting advice?

No — see [Scope and non-goals](scope-and-non-goals.md#what-it-does-not-ship-or-operate).

## May I redistribute my DuckDB file or Polygon export?

Only with independent rights — see
[Operator responsibilities](operator-responsibilities.md#export-and-redistribution-matrix).

## Are “validated” runs or exact row counts a Hypertrial certification?

No — see
[Operator responsibilities](operator-responsibilities.md#technical-success-is-not-certification).

## Where is the documentation site?

[data.oddsfox.io](https://data.oddsfox.io/) — validate with `uv run make docs-check`; edit with `uv run make docs-serve`.

## Do I need API keys?

Kalshi needs none; Polymarket CLOB credentials are optional; Polygon JSON-RPC is optional — see [Configuration](../reference/configuration.md).

## Can I use Postgres instead of DuckDB?

No — local DuckDB is the supported `v0.1.x` warehouse.

## Are warehouse migrations supported?

No — delete `oddsfox.duckdb*` and rerun quickstart; see [Design decisions](decisions.md).

## Are schedules on by default?

No — see [Enable schedules](../guides/enable-schedules.md).

## Is Polygon settlement required for WC2026 analysis?

No — isolated advanced pipeline; ordinary Polymarket/Kalshi marts do not depend on it.

## How do pipeline outputs relate to trading?

Analytics only — see [System overview](system-overview.md) and [Integration](integration.md).

## Are strategy and execution open source in this repo?

No — see repository roles in [System overview](system-overview.md).

## How do I reset a broken local warehouse?

Stop writers, then:

```bash
rm oddsfox.duckdb*
```

Rerun [Quickstart](../getting-started/index.md) or the relevant scope. Prefer
targeted recovery from [Validate and recover](../guides/validate-and-recover.md)
when a full reset is unnecessary.

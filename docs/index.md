---
hide:
  - navigation
  - toc
---

<div class="of-hero" markdown>

<div class="of-hero__copy" markdown>

<span class="of-eyebrow">Local-first prediction-market infrastructure</span>

# OddsFox Pipeline

Build inspectable local prediction-market warehouses with Dagster, dlt,
DuckDB, dbt, and Python.

Hypertrial-owned MIT software. No hosted service or bundled production data.
[Licence scope](concepts/scope-and-non-goals.md) ·
[Operator responsibilities](concepts/operator-responsibilities.md).

[Get started](getting-started/index.md){ .md-button .md-button--primary }
[Query the warehouse](guides/query-the-warehouse.md){ .md-button }

</div>

<div class="of-hero__mark">
  <img src="assets/images/oddsfox-white.png" alt="">
  <span>Pipeline</span>
</div>

</div>

<div class="of-install" markdown>

**Start in the repository**

```bash
uv sync --group dev
```

</div>

## Start with a task

<div class="of-task-grid" markdown>

<article class="of-task-card" markdown>

### Analyze the data

Open a local DuckDB warehouse, pick a documented mart, and start from tested SQL
and Python examples.

[Analysts hub](audiences/analysts.md)

</article>

<article class="of-task-card" markdown>

### Operate the pipeline

Install the project, keep schedules disabled, complete a validated scope run,
then keep the warehouse healthy.

[Operators hub](audiences/operators.md)

</article>

<article class="of-task-card" markdown>

### Contribute code

Change adapters, dbt marts, orchestration, or docs with the right quality gate.

[Contributors hub](audiences/contributors.md)

</article>

<article class="of-task-card" markdown>

### Integrate downstream

Consume documented marts without treating pipeline output
as execution. Strategy consumers use private `wc2026.v1` separately.

[Integrators hub](audiences/integrators.md)

</article>

</div>

## Supported local scopes

Version `0.2.x` supports Polymarket FIFA World Cup 2026 (golden mart:
`polymarket_wc2026_market_hourly_odds`) and Kalshi WC2026 stage and
group-winner markets. FIFA fixture/results ingestion runs in the Kalshi full
pipeline and isolated match-minute path, not the default Polymarket quickstart.

An isolated finalized Polygon WC2026 settlement-history mart with internal-audit
and operator-local technical-export paths is optional and advanced; it is not
required for ordinary odds analysis.

This site is software documentation and does not host datasets.

[Choose a scope](getting-started/choose-a-scope.md), read the
[FAQ](concepts/faq.md), or review the
[architecture](concepts/architecture.md) before extending the pipeline.

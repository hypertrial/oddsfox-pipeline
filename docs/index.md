---
hide:
  - navigation
  - toc
---

<div class="of-hero" markdown>

<div class="of-hero__copy" markdown>

<span class="of-eyebrow">Local-first prediction-market infrastructure</span>

# OddsFox Pipeline

Build inspectable prediction-market datasets with Dagster, dlt, DuckDB, dbt,
and Python.

OddsFox Pipeline ships the pipeline and operator tooling. You run ingestion
and retain the resulting warehouse on infrastructure you control.

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
uv sync --extra dev
```

</div>

## Start with a task

<div class="of-task-grid" markdown>

<article class="of-task-card" markdown>

### Run the pipeline

Install the project, keep schedules disabled, and complete a validated
Polymarket WC2026 run.

[Follow the quickstart](getting-started/index.md)

</article>

<article class="of-task-card" markdown>

### Analyze the data

Open the local DuckDB warehouse, find the supported marts, and start from
tested SQL and Python examples.

[Query the warehouse](guides/query-the-warehouse.md)

</article>

<article class="of-task-card" markdown>

### Operate and deploy

Run fixed scopes, validate freshness, recover failed jobs, or publish the
self-managed dashboard stack.

[Open the operator guides](guides/run-a-scope.md)

</article>

</div>

## Shipped data scopes

Version `0.1.x` ships Polymarket FIFA World Cup 2026 and US midterms 2026
pipelines, Kalshi WC2026 stage, group-winner, and exact match-advance markets,
a standardized cross-platform knockout match mart, and FIFA fixture/results
data for identity and real-team validation.

[Choose a scope](getting-started/choose-a-scope.md) or read the
[architecture](concepts/architecture.md) before extending the pipeline.

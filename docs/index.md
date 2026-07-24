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
[Licence scope](https://github.com/hypertrial/oddsfox-pipeline/blob/main/THIRD_PARTY_NOTICES.md).

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

Run fixed scopes, validate freshness, recover failed jobs, or publish portable
graph artifacts for offline analysis.

[Open the operator guides](guides/run-a-scope.md)

</article>

</div>

## Supported local scopes

Version `0.1.x` supports Polymarket FIFA World Cup 2026 and US midterms 2026
pipelines, Kalshi WC2026 stage, group-winner, and exact match-advance markets,
a standardized cross-platform knockout match mart, an isolated finalized
Polygon WC2026 settlement-history mart with internal-audit and operator-local
technical-export paths, and FIFA fixture/results ingestion for identity and
real-team validation.

This site is software documentation and does not host datasets.

[Choose a scope](getting-started/choose-a-scope.md) or read the
[architecture](concepts/architecture.md) before extending the pipeline.

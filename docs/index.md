<div class="of-home">

<section class="of-brand-lockup" aria-labelledby="oddsfox-pipeline-docs">
<h1 id="oddsfox-pipeline-docs" class="of-sr-only">OddsFox Pipeline Docs</h1>
<img class="of-brand-lockup__logo" src="assets/images/oddsfox-white.png" alt="OddsFox">
</section>

<div class="of-badges" aria-label="Project metadata">
<a class="of-badge of-badge--success" href="https://github.com/hypertrial/oddsfox-pipeline/actions/workflows/ci.yml">docs passing</a>
<a class="of-badge" href="https://github.com/hypertrial/oddsfox-pipeline/blob/main/pyproject.toml">python 3.10+</a>
<a class="of-badge" href="https://github.com/hypertrial/oddsfox-pipeline/blob/main/LICENSE">MIT</a>
<a class="of-badge of-badge--brand" href="https://github.com/hypertrial/oddsfox-pipeline/blob/main/CHANGELOG.md">v0.1.5</a>
</div>

<section class="of-home__intro" aria-label="Product introduction">
<p class="of-home__lead">Open-source prediction-market data pipeline, built for inspectable local and self-managed operation.</p>
<p>OddsFox combines Dagster orchestration, dlt and CSV ingestion, DuckDB storage, Python sync ledgers and retry logic, and dbt analytics models.</p>
<p class="of-home__scope">OddsFox ships code and operator tooling—not a hosted dataset. Operators ingest from source APIs and retain the resulting data in their own warehouse.</p>
</section>

<h2 class="of-section-heading">Choose your path</h2>

<div class="of-path-grid">
<article class="of-path-card">
<h3>Run the pipeline</h3>
<p>Install OddsFox, configure a scope, validate the warehouse, and operate scheduled jobs safely.</p>
<div class="of-path-card__links">
<a href="quickstart/">Getting Started</a>
<a href="operator-runbook/">Operator Runbook</a>
<a href="operations/">Operations</a>
</div>
</article>
<article class="of-path-card">
<h3>Query the data</h3>
<p>Find trusted public marts, understand analyst-facing fields, and start with useful DuckDB queries.</p>
<div class="of-path-card__links">
<a href="analyst-guide/">Analyst Guide</a>
<a href="query-cookbook/">Query Cookbook</a>
<a href="data-dictionary/">Data Dictionary</a>
<a href="warehouse/">Warehouse</a>
<a href="data-contracts/">Data Contracts</a>
</div>
</article>
<article class="of-path-card">
<h3>Understand and contribute</h3>
<p>Learn how the OddsFox repositories fit together, inspect the architecture, and make safe changes.</p>
<div class="of-path-card__links">
<a href="system-overview/">System Overview</a>
<a href="architecture/">Architecture</a>
<a href="development/">Development Guide</a>
</div>
</article>
</div>

<h2 class="of-section-heading">What the pipeline provides</h2>

<div class="of-capability-grid">
<article class="of-capability-card">
<h3>LOCAL-FIRST OPERATION</h3>
<p>Run the full pipeline on one machine, inspect raw, ops, staging, intermediate, mart, and observability schemas in one DuckDB warehouse, and opt in to schedules only after manual jobs and dbt builds are healthy.</p>
</article>
<article class="of-capability-card">
<h3>SUPPORTED MARKETS</h3>
<p>v0.1.x covers Polymarket FIFA World Cup 2026 (<code>wc2026</code>) and US midterms 2026 (<code>us_midterms_2026</code>), plus Kalshi WC2026 stage and group-winner markets.</p>
</article>
<article class="of-capability-card">
<h3>TESTED MARTS</h3>
<p>dbt-tested public marts provide WC2026 knockout odds, US midterms hourly odds, Kalshi stage and group-winner odds, and FIFA fixture and results validation.</p>
</article>
<article class="of-capability-card">
<h3>OBSERVABLE BY DESIGN</h3>
<p>Explicit Dagster assets and jobs, ledgered token sync, market health, freshness, coverage, and operator-first repair paths keep failures inspectable.</p>
</article>
</div>

<h2 class="of-section-heading of-quickstart">Quickstart</h2>

```bash
uv sync --extra dev
cp .env.example .env
uv run make dbt-parse
uv run make dagster-dev
```

Use [Configuration](configuration.md) to select a source and scope, then follow
[Getting Started](quickstart.md) for the first validated run.

<div class="of-compact-grid">
<section class="of-compact-card">
<h3>Philosophy</h3>
<p>OddsFox favors boring local operations over distributed infrastructure. The warehouse is inspectable, schedules are opt-in, and repair scripts are part of the operator surface.</p>
</section>
<section class="of-compact-card">
<h3>Community</h3>
<p>Focused issues and pull requests are welcome. Read <a href="community/">Community</a> and the <a href="development/">Development Guide</a>.</p>
</section>
<section class="of-compact-card">
<h3>License</h3>
<p>OddsFox is available under the <a href="https://github.com/hypertrial/oddsfox-pipeline/blob/main/LICENSE">MIT License</a>.</p>
</section>
</div>

</div>

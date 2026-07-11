<section class="of-hero">
<img class="of-hero__logo" src="assets/images/oddsfox-white.png" alt="OddsFox logo">
<div>
<h1 class="of-hero__title">OddsFox</h1>
<div class="of-hero__tagline">Open-source prediction-market data pipeline</div>
<div class="of-hero__subtitle">Dagster orchestration, dlt ingestion, local or self-managed DuckDB storage, dbt marts, and operator-first repair paths.</div>
</div>
</section>

<div class="of-badges">
<a class="of-badge of-badge--green" href="https://github.com/hypertrial/oddsfox-pipeline/actions/workflows/ci.yml">docs passing</a>
<a class="of-badge" href="https://github.com/hypertrial/oddsfox-pipeline/blob/main/pyproject.toml">python 3.10+</a>
<a class="of-badge" href="https://github.com/hypertrial/oddsfox-pipeline/blob/main/LICENSE">MIT</a>
<a class="of-badge of-badge--orange" href="https://github.com/hypertrial/oddsfox-pipeline/blob/main/CHANGELOG.md">v0.1.4</a>
</div>

OddsFox is an open-source, local-first data pipeline for prediction-market
data. It uses Dagster for orchestration, dlt/CSV ingestion for raw landing,
DuckDB for the local warehouse, Python for odds sync ledgers and retry logic,
and dbt for analytics models.

OddsFox ships code and operator tooling, not a hosted dataset. Operators run
ingestion against source APIs and keep the resulting data in their own local or
self-managed warehouse.

The current v0.1.x implementation ships two Polymarket scopes — FIFA World Cup
2026 (`wc2026`) and US midterms 2026 (`us_midterms_2026`) — plus Kalshi WC2026
stage and group-winner markets and a FIFA fixture/results source used to
validate WC2026 real-team scope. See [Configuration](configuration.md).

## Key features

- **Local-first:** run the pipeline and inspect the warehouse on one machine.
- **Dagster-orchestrated:** assets, jobs, and schedules are explicit and testable.
- **DuckDB-backed:** raw, ops, staging, intermediate, mart, and observability schemas live in one local file.
- **dbt-tested marts:** coverage, WC2026 knockout odds time series, US midterms hourly odds, health, and observability models build with dbt data tests.
- **Prediction-market focused:** token coverage, odds freshness, market health, and scoped marts are first-class outputs.
- **Current Polymarket adapter:** v0.1.x ships WC2026 knockout marts, US midterms generic market odds, CLOB odds sync, and dbt marts cleaned by FIFA fixture/results data for WC2026 team scope.
- **Current Kalshi adapter:** v0.1.x ships WC2026 stage and group-winner markets from the public Kalshi trade API.
- **Safe by default:** schedules are disabled until manual jobs and dbt builds are healthy.

## Example workflow

```bash
uv sync --extra dev
cp .env.example .env
uv run make dbt-parse
uv run make dagster-dev
```

## Philosophy

OddsFox favors boring local operations over distributed infrastructure. The
warehouse is inspectable, token sync is ledgered, schedules are opt-in, and
repair scripts are part of the operator surface.

## Start here

| Goal | Page |
| --- | --- |
| See how the four OddsFox repos work together | [System Overview](system-overview.md) |
| Run source refresh through the hosted dashboard | [Operator Runbook](operator-runbook.md) |
| Run OddsFox locally | [Getting Started](quickstart.md) |
| Query the warehouse as an analyst | [Analyst Guide](analyst-guide.md), [Query Cookbook](query-cookbook.md), and [Data Dictionary](data-dictionary.md) |
| Understand the system | [Architecture](architecture.md) |
| Operate Dagster jobs | [Operations](operations.md) |
| Inspect data outputs | [Warehouse](warehouse.md) and [Data Contracts](data-contracts.md) |
| Contribute safely | [Development Guide](development.md) |

## Community

We appreciate focused issues and pull requests. Start with
[Community](community.md) and [Development Guide](development.md).

## License

OddsFox is licensed under the
[MIT License](https://github.com/hypertrial/oddsfox-pipeline/blob/main/LICENSE).

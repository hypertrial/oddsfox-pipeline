# OddsFox Pipeline

[![CI](https://github.com/hypertrial/oddsfox-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/hypertrial/oddsfox-pipeline/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-00d7f7)](pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-00d7f7)](LICENSE)

OddsFox Pipeline is an open-source, local-first batch data warehouse for
prediction-market data.

It uses Dagster to orchestrate dlt/CSV ingestion, DuckDB storage, Python sync
ledgers, and dbt analytics models. Version `0.1.x` ships WC2026 Polymarket
odds, US midterms 2026 Polymarket odds, Kalshi WC2026 odds, and FIFA
fixture/results validation. Existing local warehouses from older layouts must
be reset with `rm oddsfox.duckdb*`.

## Project Scope

OddsFox is code and operator tooling, not a hosted dataset. The shipped market
adapters cover Polymarket WC2026, Polymarket US midterms 2026, and Kalshi
WC2026. The bundled `international_results` WC2026 CSV source is used only to
validate real FIFA World Cup team scope.

Every operator runs ingestion against source APIs and stores the resulting data
in their own local DuckDB file or self-managed warehouse.

## Part Of OddsFox

`oddsfox-pipeline` is the warehouse and orchestration repo. It ingests source
data, builds dbt marts, and exports graph parquet for `oddsfox-graph`, which
publishes artifacts for `oddsfox-live` and `oddsfox-dash`.

Read the cross-repo [System Overview](docs/system-overview.md) and
[Operator Runbook](docs/operator-runbook.md) for the end-to-end path from source
APIs to the dashboard.

## Start Here

| Reader | First step |
| --- | --- |
| Analysts querying the warehouse | Serve the docs with `uv run make docs-serve`, open `http://127.0.0.1:8000`, then start with the [Analyst Guide](docs/analyst-guide.md), [Query Cookbook](docs/query-cookbook.md), and [Data Dictionary](docs/data-dictionary.md). |
| Operators running ingestion | Use the [Getting Started guide](docs/quickstart.md) and local Dagster setup below. |
| Contributors changing code | Use the [Development guide](docs/development.md) and [CONTRIBUTING.md](CONTRIBUTING.md). |

## Quickstart

Read or query an existing warehouse:

```bash
uv sync --extra dev
uv run make docs-serve
```

Keep the docs server running and open `http://127.0.0.1:8000`. In another
terminal, query the default warehouse:

```bash
duckdb oddsfox.duckdb
```

The default warehouse is `oddsfox.duckdb` in the repo root. If `.env` sets
`DUCKDB_PATH`, open that file instead.

Run the local pipeline:

```bash
uv sync --extra dev
cp .env.example .env
uv run make dbt-parse
uv run make dagster-dev
```

Schedules are disabled by default. Keep them off until manual jobs and dbt
builds are healthy.

Read the full [Getting Started guide](docs/quickstart.md).

Analyst rules of thumb:

- Query `*_marts` first.
- Use `*_observability` for freshness, coverage, and trust checks.
- Treat `*_raw`, `*_ops`, staging, and intermediate schemas as internal or
  debugging surfaces.
- For current live analysis, prefer `is_actionable_live_market`, then inspect
  `current_price_status`.

## Architecture

OddsFox keeps the data stack local and inspectable:

- Prediction-market APIs and the WC2026 results CSV feed raw DuckDB tables;
  v0.1.x ships Polymarket Gamma/CLOB ingestion, Kalshi public trade API
  ingestion, and team-scope validation.
- Dagster coordinates market discovery, registry refresh, odds sync, and dbt.
- dbt models staging, intermediate, mart, and observability schemas.
- Operator scripts inspect, compact, prune, and repair local warehouse state.

See [Architecture](docs/architecture.md) and [Warehouse](docs/warehouse.md).

## Data Outputs

Main public analytics schemas:

- `polymarket_wc2026_marts`: WC2026 Polymarket knockout market snapshots,
  progression-side hourly odds, token classification, and graph exports.
- `international_results_wc2026_marts`: WC2026 fixtures, results, and canonical
  team status used to clean public odds marts.
- `polymarket_us_midterms_2026_marts`: scoped US midterms 2026 Polymarket
  hourly odds.
- `kalshi_wc2026_marts`: Kalshi WC2026 stage and group-winner market snapshots
  and hourly candlesticks.
- `*_observability`: ingestion telemetry, freshness, coverage, and data-quality
  checks for analyst trust and operator debugging.

Dagster registers source-first jobs:
`international_results_wc2026_match_results_ingest`,
`polymarket_wc2026_market_registry_refresh`,
`polymarket_wc2026_hourly_odds_ingest`, `polymarket_wc2026_dbt_build`, and
`polymarket_wc2026_full_pipeline`;
`polymarket_us_midterms_2026_market_registry_refresh`,
`polymarket_us_midterms_2026_hourly_odds_ingest`,
`polymarket_us_midterms_2026_dbt_build`, and
`polymarket_us_midterms_2026_full_pipeline`;
`kalshi_wc2026_market_registry_refresh`, `kalshi_wc2026_hourly_odds_ingest`,
`kalshi_wc2026_dbt_build`, and `kalshi_wc2026_full_pipeline`.

See [Data Dictionary](docs/data-dictionary.md) for analyst-facing table
semantics, [Data Contracts](docs/data-contracts.md) for formal guarantees, and
[Naming](docs/naming.md) for schema and asset naming.

## Development

```bash
uv run make lint
uv run make test-cov
uv run make integration-dagster-cov
uv run make integration-dbt-cov
uv run make coverage-report
uv run make docs-check
uv run make dbt-parse
uv run make dbt-build-ci
uv run make costguard
```

For local one-shot runs, `make test`, `make integration-dagster`,
`make integration-dbt`, and `make coverage` still work without the CI
coverage-accumulation split.

`dbt-build-ci` uses a disposable DuckDB database under `.cache/` for CI parity.
Costguard is a dbt/CI guardrail, not an odds ingestion runtime dependency.
Install the pinned local scanner with:

```bash
curl -fsSL https://raw.githubusercontent.com/hypertrial/costguard/main/scripts/install.sh | sh -s -- v2.5.0
```

See [Development](docs/development.md) and [CONTRIBUTING.md](CONTRIBUTING.md).

## Community

- [Docs](docs/index.md)
- [Contributing](CONTRIBUTING.md)
- [Security](SECURITY.md)
- [Changelog](CHANGELOG.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [License](LICENSE)

The v0.1.x repo intentionally excludes simulations, allocation tooling,
website integration, and generated historical docs.

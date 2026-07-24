# OddsFox Pipeline

[![CI](https://github.com/hypertrial/oddsfox-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/hypertrial/oddsfox-pipeline/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-00d7f7)](pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-00d7f7)](LICENSE)

OddsFox Pipeline is MIT-licensed, local-first batch data-pipeline software for
prediction-market analytics. It uses Dagster, dlt, DuckDB, dbt, and Python to
build inspectable local warehouses. The canonical repository and newly published
images contain no bundled production datasets. Hypertrial operates no hosted
production pipeline or data service. See
[Scope and non-goals](docs/concepts/scope-and-non-goals.md) and
[Third-Party Notices](THIRD_PARTY_NOTICES.md).

## Part Of OddsFox

`oddsfox-pipeline` is the open-source warehouse component of the private
`oddsfox` superproject. It ingests safe public sources, validates canonical
snapshots, builds dbt marts, and exports graph parquet for offline use.
Order execution belongs to `oddsfox-execution` and is not part of this runtime.

Read the [System Overview](docs/concepts/system-overview.md) for repository
boundaries.

## Start Here

| Reader | First step |
| --- | --- |
| Analysts | [Analysts hub](docs/audiences/analysts.md), then [Query the warehouse](docs/guides/query-the-warehouse.md), [Query recipes](docs/guides/query-recipes.md), and the [Data dictionary](docs/reference/data-dictionary.md). |
| Operators | [Operators hub](docs/audiences/operators.md), then [Quickstart](docs/getting-started/index.md). |
| Contributors | [Contributors hub](docs/audiences/contributors.md), [Development guide](docs/development/index.md), and [CONTRIBUTING.md](CONTRIBUTING.md). |
| Integrators | [Integrators hub](docs/audiences/integrators.md), [Integration](docs/concepts/integration.md), and [Data contracts](docs/reference/data-contracts.md). |

## Quickstart

Browse the docs site locally:

```bash
uv sync --extra dev
uv run make docs-serve
```

Open `http://127.0.0.1:8000`.

Query an existing warehouse (default `oddsfox.duckdb` in the repo root; use
`DUCKDB_PATH` from `.env` when set):

```bash
duckdb oddsfox.duckdb
```

Run the local pipeline:

```bash
uv sync --extra dev
cp .env.example .env
uv run make dbt-parse
uv run make dagster-dev
```

Schedules are disabled by default. Keep them off until manual jobs and dbt
builds are healthy. Read the full [Quickstart](docs/getting-started/index.md).

Analyst rules of thumb: query `*_marts` first; use `*_observability` for trust
checks; prefer `is_actionable_live_market`, then inspect `current_price_status`.

## Architecture

- Prediction-market APIs and WC2026 results feed raw DuckDB tables.
- Dagster coordinates discovery, registry refresh, odds sync, and dbt.
- dbt models staging, intermediate, mart, and observability schemas.
- Operator scripts inspect, compact, prune, and repair local warehouse state.

See [Architecture](docs/concepts/architecture.md) and the
[Warehouse reference](docs/reference/warehouse.md).

## Local Data Outputs

Supported local analytics schemas include
`polymarket_wc2026_marts`, `wc2026_marts`,
`international_results_wc2026_marts`, `polymarket_us_midterms_2026_marts`,
`kalshi_wc2026_marts`, and matching `*_observability` schemas. An optional
advanced Polygon settlement-minute mart is documented separately for operators
who supply a reviewed manifest and finalized RPC.

See the [Data dictionary](docs/reference/data-dictionary.md) and
[Data contracts](docs/reference/data-contracts.md).

## Development

```bash
uv run make ci-fast
uv run make release-gate
```

Run `ci-fast` before ordinary pushes and `release-gate` before releases or after
dependency, Docker, Dagster, dbt, or data-quality changes. The canonical gate
tables and layout guardrails live in [AGENTS.md](AGENTS.md). See also
[Development](docs/development/index.md) and [CONTRIBUTING.md](CONTRIBUTING.md).

## Documentation Website

Vercel publishes the MkDocs site from `main` at
[data.oddsfox.io](https://data.oddsfox.io/). Validate with
`uv run make docs-check`. During editing, leave `uv run make docs-serve`
running; MkDocs rebuilds after each saved change.

## Community

- [Docs](docs/index.md)
- [FAQ](docs/concepts/faq.md)
- [Operator responsibilities](docs/concepts/operator-responsibilities.md)
- [Contributing](CONTRIBUTING.md)
- [Security](SECURITY.md)
- [Changelog](CHANGELOG.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [License](LICENSE)
- [Third-Party Notices](THIRD_PARTY_NOTICES.md)

# OddsFox Pipeline

[![CI](https://github.com/hypertrial/oddsfox-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/hypertrial/oddsfox-pipeline/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-00d7f7)](pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-00d7f7)](LICENSE)

MIT-licensed, local-first prediction-market pipeline software (Dagster, dlt,
DuckDB, dbt, Python); no bundled production data or hosted service. See
[Third-Party Notices](THIRD_PARTY_NOTICES.md).

## Part Of OddsFox

`oddsfox-pipeline` is the open-source warehouse component of the private
`oddsfox` superproject. It ingests safe public sources, validates canonical
snapshots, builds dbt marts, and exports documented mart surfaces for offline use.
Order execution belongs to `oddsfox-execution` and is not part of this runtime.

Read the [System Overview](docs/concepts/system-overview.md) for repository
boundaries and [Terminology](docs/reference/terminology.md) for the compact
34-term vocabulary (including **working set**).

## Start Here

| Reader | First step |
| --- | --- |
| Analysts | [Analysts hub](docs/audiences/analysts.md), then [Query the warehouse](docs/guides/query-the-warehouse.md), [Query recipes](docs/guides/query-recipes.md), and the [Data dictionary](docs/reference/data-dictionary.md). |
| Operators | [Operators hub](docs/audiences/operators.md), then [Quickstart](docs/getting-started/index.md). |
| Contributors | [Contributors hub](docs/audiences/contributors.md), [Development guide](docs/development/index.md), and [CONTRIBUTING.md](CONTRIBUTING.md). |
| Integrators | [Integrators hub](docs/audiences/integrators.md), [Terminology](docs/reference/terminology.md), [Integration](docs/concepts/integration.md), and [Data contracts](docs/reference/data-contracts.md). |

## Quickstart

For a first warehouse, follow the full
[Quickstart](docs/getting-started/index.md)
(`uv run python scripts/run_scope.py polymarket:wc2026 --step full`).
After that install, you can inspect jobs with `uv run make dagster-dev`.
Schedules stay disabled until manual jobs and dbt builds are healthy.

Query an existing warehouse (default `oddsfox.duckdb` in the repo root; use
`DUCKDB_PATH` from `.env` when set):

```bash
duckdb oddsfox.duckdb
```

Analyst rules of thumb: query `*_marts` first; use `*_observability` for trust
checks; prefer `is_actionable_live_market`, then inspect `current_price_status`.

## Architecture

See [Architecture](docs/concepts/architecture.md) and the
[Warehouse reference](docs/reference/warehouse.md).

## Local Data Outputs

See the [Data dictionary](docs/reference/data-dictionary.md) and
[Data contracts](docs/reference/data-contracts.md).

## Development

Run `uv run make ci-fast` before ordinary pushes. Gate tables and layout
guardrails live in [AGENTS.md](AGENTS.md). See [Development](docs/development/index.md)
and [CONTRIBUTING.md](CONTRIBUTING.md).

## Documentation Website

Vercel publishes the MkDocs site from `main` at
[data.oddsfox.io](https://data.oddsfox.io/). Browse locally with
`uv run make docs-serve` (`http://127.0.0.1:8000`); validate with
`uv run make docs-check`.

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

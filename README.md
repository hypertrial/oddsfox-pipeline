# OddsFox Pipeline

[![CI](https://github.com/hypertrial/oddsfox-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/hypertrial/oddsfox-pipeline/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-00d7f7)](pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-00d7f7)](LICENSE)

OddsFox Pipeline is an open-source, local-first batch data warehouse for
prediction-market data.

It uses Dagster to orchestrate dlt/CSV ingestion, canonical Parquet snapshot
loading, DuckDB storage, Python sync ledgers, and dbt analytics models. The
public project ships Polymarket and Kalshi market data, GitHub-hosted football
feeds, a standardized `wc2026.v1` clean-data contract, and data-quality
telemetry. Optional private enrichments are accepted only through the canonical
raw snapshot contract.
Existing local warehouses from older layouts must
be reset with `rm oddsfox.duckdb*`.

## Project Scope

OddsFox Pipeline is code and operator tooling, not a hosted dataset. The shipped market
adapters cover Polymarket WC2026, Polymarket US midterms 2026, Kalshi WC2026,
OpenFootball, and the public 2006+ `international_results` files.

Every operator runs ingestion against source APIs and stores the resulting data
in their own local DuckDB file or self-managed warehouse.

## Part Of OddsFox

`oddsfox-pipeline` is the public warehouse component of the private `oddsfox`
superproject. It ingests safe public sources, validates canonical snapshots
produced by private collectors, builds dbt marts, and exports graph parquet for
offline use by `oddsfox-graph`. `oddsfox-strategy` consumes `wc2026.v1`
read-only; this repository never contains strategy or execution code.

The signed public container is published as
`ghcr.io/hypertrial/oddsfox-pipeline`. Release manifests include `linux/amd64`
and `linux/arm64` images, SBOMs, provenance, and GitHub OIDC signatures.

Read the cross-repo [System Overview](docs/concepts/system-overview.md) for the
data flow and repository boundaries. Order execution belongs to
`oddsfox-execution` and is not part of this runtime.

## Start Here

| Reader | First step |
| --- | --- |
| Analysts querying the warehouse | Serve the docs with `uv run make docs-serve`, open `http://127.0.0.1:8000`, then start with [Query the warehouse](docs/guides/query-the-warehouse.md), [Query recipes](docs/guides/query-recipes.md), and the [Data dictionary](docs/reference/data-dictionary.md). |
| Operators running ingestion | Use the [Quickstart](docs/getting-started/index.md) and local Dagster setup below. |
| Contributors changing code | Use the [Development guide](docs/development/index.md) and [CONTRIBUTING.md](CONTRIBUTING.md). |

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

Read the full [Quickstart](docs/getting-started/index.md).

Analyst rules of thumb:

- Query `*_marts` first.
- Use `*_observability` for freshness, coverage, and trust checks.
- Treat `*_raw`, `*_ops`, staging, and intermediate schemas as internal or
  debugging surfaces.
- For current live analysis, prefer `is_actionable_live_market`, then inspect
  `current_price_status`.

## Architecture

OddsFox Pipeline keeps the data stack local and inspectable:

- Prediction-market APIs and the WC2026 results CSV feed raw DuckDB tables;
  v0.1.x ships Polymarket Gamma/CLOB ingestion, Kalshi public trade API
  ingestion, and team-scope validation.
- Dagster coordinates market discovery, registry refresh, odds sync, and dbt.
- dbt models staging, intermediate, mart, and observability schemas.
- Operator scripts inspect, compact, prune, and repair local warehouse state.

See [Architecture](docs/concepts/architecture.md) and the [Warehouse reference](docs/reference/warehouse.md).

## Data Outputs

Main public analytics schemas:

- `polymarket_wc2026_marts`: WC2026 Polymarket in-game minute moneyline and
  advance odds for all 104 matches, knockout market snapshots,
  progression-side hourly odds, token classification, and graph exports.
- `wc2026_marts`: official FIFA-numbered knockout match hourly team-advance
  prices plus stable fixtures, results, identities, point-in-time features,
  venue/token identity, price/liquidity history, travel features, provenance,
  and `contract_metadata` for `wc2026.v1`.
- `international_results_wc2026_marts`: WC2026 fixtures, results, and canonical
  team status used to clean public odds marts.
- `polymarket_us_midterms_2026_marts`: scoped US midterms 2026 Polymarket
  hourly odds.
- `kalshi_wc2026_marts`: Kalshi WC2026 stage and group-winner market snapshots
  and hourly candlesticks.
- `*_observability`: ingestion telemetry, freshness, coverage, and data-quality
  checks for analyst trust and operator debugging.

Dagster registers source-first jobs:
`international_results_historical_ingest`,
`international_results_wc2026_match_results_ingest`,
`polymarket_wc2026_market_registry_refresh`,
`polymarket_wc2026_hourly_odds_ingest`,
`polymarket_wc2026_match_minute_odds_backfill`,
`polymarket_wc2026_dbt_build`, and
`polymarket_wc2026_full_pipeline`;
`polymarket_us_midterms_2026_market_registry_refresh`,
`polymarket_us_midterms_2026_hourly_odds_ingest`,
`polymarket_us_midterms_2026_dbt_build`, and
`polymarket_us_midterms_2026_full_pipeline`;
`kalshi_wc2026_market_registry_refresh`, `kalshi_wc2026_hourly_odds_ingest`,
`kalshi_wc2026_dbt_build`, and `kalshi_wc2026_full_pipeline`.
The atomic cross-platform job is
`wc2026_knockout_match_odds_full_pipeline`; its hourly schedule is stopped by
default.

See the [Data dictionary](docs/reference/data-dictionary.md) for analyst-facing table
semantics, [Data contracts](docs/reference/data-contracts.md) for formal guarantees, and
[Naming](docs/reference/naming.md) for schema and asset naming.

## Development

```bash
uv run make ci-fast
uv run make release-gate
```

Run `ci-fast` before ordinary pushes; GitHub runs the same lint, fast offline
tests, saved HTTP contracts, dbt parse, and strict docs build automatically.
Run `release-gate` before releases and after dependency, Docker, Dagster, dbt,
or data-quality changes. It reruns the fast checks, full coverage and
integration surface, Costguard, and a non-root container smoke. The full gate
is also available through the manual `Manual Full Validation` GitHub workflow.
Set that workflow's `publish` input only on `main` to publish the signed
AMD64/ARM64 image, SBOM, and provenance. For narrower local runs, `make test`,
`make integration-dagster`, `make integration-dbt`, `make data-quality`, and
`make coverage` still work.

`dbt-build-ci` uses a disposable DuckDB database under `.cache/` for release
parity. `gx-data-quality` checks that existing build; local `data-quality`
remains the safe wrapper that rebuilds first.
Costguard is a dbt/release guardrail, not an odds ingestion runtime dependency.
Install the pinned local scanner with:

```bash
curl -fsSL https://raw.githubusercontent.com/hypertrial/costguard/main/scripts/install.sh | sh -s -- v2.5.0
```

See [Development](docs/development/index.md) and [CONTRIBUTING.md](CONTRIBUTING.md).

## Documentation Website

Vercel builds the MkDocs site from `main` using [`vercel.json`](vercel.json) and
publishes it at [data.oddsfox.io](https://data.oddsfox.io/). Validate documentation
changes locally with `uv run make docs-check` before pushing. During editing,
leave `uv run make docs-serve` running; MkDocs rebuilds and refreshes the
browser after each saved change without a restart.

## Community

- [Docs](docs/index.md)
- [Contributing](CONTRIBUTING.md)
- [Security](SECURITY.md)
- [Changelog](CHANGELOG.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [License](LICENSE)

The v0.1.x repo intentionally excludes simulations, allocation tooling,
website integration, and generated historical docs.

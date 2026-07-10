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

## Quickstart

```bash
uv sync --extra dev
cp .env.example .env
uv run make dbt-parse
uv run make dagster-dev
```

Schedules are disabled by default. Keep them off until manual jobs and dbt
builds are healthy.

Read the full [Getting Started guide](docs/quickstart.md).

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

Current Polymarket WC2026 analytics outputs live in `polymarket_wc2026_marts`:

- `polymarket_wc2026_knockout_market_tokens`: progression-side token universe
  for knockout-related markets at or above the WC2026 contract volume floor
  (currently $5,000 USD), including derived live/historical status.
- `polymarket_wc2026_knockout_token_hourly_odds`: trailing 30-day hourly OHLC
  odds for the progression side of each knockout market, with market status
  metadata for live-only filtering.
- `polymarket_wc2026_graph_token_hourly_odds`: graph-build export surface with
  both Yes/No tokens per real-team knockout market plus dbt-clean team, stage,
  progression-side, and opposite-token semantics.
- `polymarket_wc2026_knockout_markets`: latest progression-side knockout
  snapshot with market/team/stage metadata and explicit current-price status.

WC2026 FIFA World Cup fixture/result outputs live in
`international_results_wc2026_marts`:

- `international_results_wc2026_matches`: clean fixture/result rows with stage
  mapping and tied-knockout advancer inference where possible.
- `international_results_wc2026_team_status`: canonical 48-team roster and
  current tournament status used to clean public odds marts.

Operational health outputs live in `polymarket_wc2026_observability`:

- `polymarket_wc2026_sync_run_observability`: run-level ingestion telemetry.
- `polymarket_wc2026_knockout_stage_coverage`: raw classified market coverage
  versus scoped tokens by stage, direction, and market status.
- `polymarket_wc2026_knockout_data_quality`: row-level DQ findings for source
  anomalies, sparse coverage, and stale or missing odds.

Polymarket US midterms 2026 outputs live in
`polymarket_us_midterms_2026_marts`:

- `polymarket_us_midterms_2026_market_token_hourly_odds`: trailing hourly OHLC
  odds for scoped midterms market tokens.

Kalshi WC2026 outputs live in `kalshi_wc2026_marts`:

- `kalshi_wc2026_stage_markets` and
  `kalshi_wc2026_stage_market_hourly_odds`: stage-of-elimination markets and
  hourly candlesticks.
- `kalshi_wc2026_group_winner_markets` and
  `kalshi_wc2026_group_winner_market_hourly_odds`: group-winner markets and
  hourly candlesticks.

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

See [Data Contracts](docs/data-contracts.md) and [Naming](docs/naming.md).

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

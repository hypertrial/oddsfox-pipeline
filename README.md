# OddsFox Pipeline

[![CI](https://github.com/hypertrial/oddsfox-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/hypertrial/oddsfox-pipeline/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-00d7f7)](pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-00d7f7)](LICENSE)

OddsFox Pipeline is MIT-licensed, local-first batch data-pipeline software for
prediction-market analytics. The canonical repository, source archives, Python
packages, documentation, and newly published images contain no production
datasets. Operators supply and control their own source data.

It uses Dagster to orchestrate dlt/CSV ingestion, canonical Parquet snapshot
loading, DuckDB storage, Python sync ledgers, and dbt analytics models. The
software supports Polymarket and Kalshi adapters, football-source ingestion, a
standardized `wc2026.v1` analytics contract, and data-quality telemetry.
Optional inputs are accepted only through the canonical raw snapshot contract.
Existing local warehouses from older layouts must
be reset with `rm oddsfox.duckdb*`.

Hypertrial is a project name, not a legal entity. MIT applies to
Hypertrial-authored software and associated documentation; operator data,
third-party fonts and dependencies, and OddsFox visual marks are outside that
grant. See [Third-Party Notices](THIRD_PARTY_NOTICES.md).

## Project Scope

OddsFox Pipeline is software and operator tooling, not a hosted dataset. The
market adapters cover Polymarket WC2026, Polymarket US midterms 2026, Kalshi
WC2026, OpenFootball, and `international_results` inputs.

The separate WC2026 Polygon settlement flow is an unscheduled historical
collector. It reads Polygon V2 settlement logs using a complete operator-local
market manifest and does not call Gamma, CLOB, the Polymarket website, or a
runtime football-results source. Its release job builds a local, immutable
internal audit bundle. A separate offline exporter copies the allowlisted CSV
byte-for-byte and produces an operator-local technical **WC2026 Polygon
Settlement Minute Aggregates** dossier. Neither path uploads data or determines
rights to use or distribute operator inputs or outputs. The v4
collector plans group and knockout ranges by their authored V2 exchange,
rejects discoveries outside exact token windows before receipt fetch, and
expands only matching finalized receipts in five bounded workers. Published
reruns short-circuit offline; live-smoke checkpoints, status, and tool caches
remain below the SSD-backed repository `.cache/` and resume by default.

Every operator runs ingestion against source APIs and stores the resulting data
in their own local DuckDB file or self-managed warehouse.

## Part Of OddsFox

`oddsfox-pipeline` is the open-source warehouse component of the private `oddsfox`
superproject. It ingests safe public sources, validates canonical snapshots
produced by private collectors, builds dbt marts, and exports graph parquet for
offline use by `oddsfox-graph`. `oddsfox-strategy` consumes `wc2026.v1`
read-only; this repository never contains strategy or execution code.

The signed software container is published as
`ghcr.io/hypertrial/oddsfox-pipeline`. Release manifests include `linux/amd64`
and `linux/arm64` images, SBOMs, provenance, and GitHub OIDC signatures.

Read the cross-repo [System Overview](docs/concepts/system-overview.md) for the
data flow and repository boundaries. Order execution belongs to
`oddsfox-execution` and is not part of this runtime.

## Start Here

| Reader | First step |
| --- | --- |
| Analysts querying the warehouse | Serve the docs with `uv run make docs-serve`, open `http://127.0.0.1:8000`, then start with [Query the warehouse](docs/guides/query-the-warehouse.md), [Query recipes](docs/guides/query-recipes.md), and the [Data dictionary](docs/reference/data-dictionary.md). |
| Operators running ingestion | Use the [Quickstart](docs/getting-started/index.md), then [recreate the WC2026 minute marts locally](docs/guides/recreate-local-marts.md). |
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

For a checkout on an SSD, keep temporary files and caches there too. Export
`ODDSFOX_RUNTIME_ROOT="$PWD/.cache/runtime"` and the `TMPDIR`, `UV_CACHE_DIR`,
and related paths shown in the
[local mart recreation guide](docs/guides/recreate-local-marts.md) before the
first `uv` command. The Makefile keeps child-process runtime state below that
root.

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
  ingestion, an isolated finalized Polygon settlement-log backfill, and
  team-scope validation.
- Dagster coordinates market discovery, registry refresh, odds sync, and dbt.
- dbt models staging, intermediate, mart, and observability schemas.
- Operator scripts inspect, compact, prune, and repair local warehouse state.

See [Architecture](docs/concepts/architecture.md) and the [Warehouse reference](docs/reference/warehouse.md).

## Local Data Outputs

Supported local analytics schemas:

- `polymarket_wc2026_marts`: WC2026 Polymarket in-game minute moneyline and
  advance odds for all 104 matches, knockout market snapshots,
  progression-side hourly odds, token classification, graph exports, and the
  independent `polymarket_wc2026_polygon_settlement_minute_odds` mart.
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
`polymarket_wc2026_polygon_settlement_backfill`,
`polymarket_wc2026_polygon_settlement_release`,
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

The Polygon mart contains exactly 39,120 proposition-minute rows over fixed
half-open scheduled windows: 150 minutes for group matches and 210 minutes for
knockout matches. Prices are finalized block-time settlement aggregates, not
quotes, order-book snapshots, order-match timestamps, or CLOB price history.
Empty minutes remain null. Normalized economic-leg counts can differ from unique
user trades, and derived MINT/MERGE counterparts are separately counted.

Both
`polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds` and
`polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds`
remain locally reproducible. Operators provide the schedule, reviewed Polygon
manifest and attestation at the existing paths, then use the live jobs or
`make local-marts-rebuild` for completed raw warehouses. See
[Recreate the WC2026 minute marts locally](docs/guides/recreate-local-marts.md).

The dbt mart and internal audit bundle retain condition/token IDs, exchange
addresses, and chain locators needed for verification; a direct mart export is
not the operator-local technical export. The standalone exporter copies only an
allowlisted CSV that omits wallets and transaction, log, block, provider,
order, raw-payload, condition, token, and exchange identifiers. This is
de-identification, not anonymity: sparse public blockchain aggregates can
still be reverse-linked. Operators remain responsible for their inputs and
outputs.

See the [Data dictionary](docs/reference/data-dictionary.md) for analyst-facing table
semantics, [Data contracts](docs/reference/data-contracts.md) for formal guarantees, and
[Naming](docs/reference/naming.md) for schema and asset naming.

## Development

```bash
uv run make ci-fast
uv run make release-gate
```

Run `ci-fast` before ordinary pushes. Local gates execute their Make targets
sequentially; GitHub parallelizes the equivalent Python/static/docs, fast
test/contract, and fail-closed dbt-lint work behind the stable `fast-gate`
aggregate.
Run `release-gate` before releases and after dependency, Docker, Dagster, dbt,
or data-quality changes. It runs the equivalent lint, contract, docs, full
coverage and integration surfaces, Costguard, and a non-root container smoke
without repeating the ordinary tests before coverage. The manual `Manual Full
Validation` GitHub workflow runs coverage, dbt/data quality, and
static/docs/container groups in parallel behind `full-gate`.
Set that workflow's `publish` input only on `main` to publish the signed
AMD64/ARM64 image, SBOM, and provenance. For narrower local runs, `make test`,
`make integration-dagster`, `make integration-dbt`, `make data-quality`, and
`make coverage` still work.

The ordinary `dbt-build` remains credential-free and excludes the isolated
`polygon_settlement` graph. `release-gate` additionally runs
`dbt-polygon-settlement-ci` against replay-only synthetic fixtures. Live Polygon
validation is explicitly opt-in through `make polygon-settlement-live-smoke`.

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
browser after each saved change without a restart. The site is software
documentation and does not host datasets.

## Community

- [Docs](docs/index.md)
- [Contributing](CONTRIBUTING.md)
- [Security](SECURITY.md)
- [Changelog](CHANGELOG.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [License](LICENSE)
- [Third-Party Notices](THIRD_PARTY_NOTICES.md)

The v0.1.x repo intentionally excludes simulations, allocation tooling,
website integration, and generated historical docs.

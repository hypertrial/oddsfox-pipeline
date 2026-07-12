# Contributing to OddsFox Pipeline

Thank you for your interest in contributing. OddsFox Pipeline is an open-source,
local-first prediction-market data pipeline built with Dagster, dlt, dbt, and
DuckDB. Version `0.1.x` ships WC2026 and US midterms 2026 Polymarket pipelines,
a Kalshi WC2026 pipeline, plus a small FIFA fixture/results source for
real-team scope validation.

## Development setup

```bash
uv sync --extra dev
cp .env.example .env
```

Documentation contributors should also install the browser used by the
responsive docs smoke tests:

```bash
uv run playwright install chromium
```

The default warehouse is `oddsfox.duckdb` in the repo root. Keep schedules disabled in local dev and CI unless you intentionally run live ingestion:

```dotenv
POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

See the [Quickstart](docs/getting-started/index.md) and
[Configuration reference](docs/reference/configuration.md) for full operator setup.

## Source adapter contributions

Polymarket WC2026, Polymarket US midterms 2026, and Kalshi WC2026 are the
shipped market adapters. The bundled `international_results` WC2026 source
validates team scope; it is not a bookmaker/market adapter. Contributions may
add or improve adapters for traditional bookmakers and other odds sources when
they keep the pipeline local-first and operator-owned.

Useful contribution areas include ingestion adapters, Dagster assets and jobs,
dbt models and tests, DuckDB storage, docs, and operator scripts. Adapter PRs
should include tests, docs, config examples, and source-specific data ownership
and rate-limit notes.

Do not assume centralized OddsFox Pipeline-hosted data. Operators must be able to run
ingestion and store data in their own local or self-managed warehouse.

## AI-assisted development

If you use Cursor, [Ponytail](https://github.com/DietrichGebert/ponytail) loads from [`.cursor/rules/ponytail.mdc`](.cursor/rules/ponytail.mdc). Repo-specific guardrails (layout, quality gate, orchestration limits) live in [AGENTS.md](AGENTS.md).

## Quality gate

Run these before opening a pull request (they mirror [`.github/workflows/ci.yml`](.github/workflows/ci.yml)):

```bash
uv run make lint
uv run make test-cov
uv run make integration-dagster-cov
uv run make integration-dbt-cov
uv run make dbt-unit
uv run make golden-dbt
uv run make dbt-source-freshness-ci
uv run make coverage-report
uv run make docs-check
uv run make dbt-parse
uv run make dbt-build-ci
uv run make data-quality
uv run make costguard
```

For local one-shot runs, `make test`, `make integration-dagster`,
`make integration-dbt`, and `make coverage` still work without the CI
coverage-accumulation split.

`dbt-build-ci` bootstraps a disposable DuckDB database under `.cache/` before
running dbt build. `contract-http` runs replay-only HTTP contract tests from
checked-in sanitized cassettes and is manual/nightly, not part of the default
CI gate.
Costguard is a dbt/CI guardrail, not an odds ingestion runtime dependency.
Install the pinned local scanner with:

```bash
curl -fsSL https://raw.githubusercontent.com/hypertrial/costguard/main/scripts/install.sh | sh -s -- v2.5.0
```

Additional targets are available in the [Makefile](Makefile) (`unit-core`, `unit-ingest`, etc.).

## Versioning expectations

OddsFox Pipeline is v0.1.x — the project is too new to carry backward-compatibility
burden by default.

- Breaking changes are acceptable when they simplify the pipeline.
- Update tests and docs with behavior changes; do not add backward-compat shims
  unless the PR explicitly scopes compat work.
- Document breaking changes in [CHANGELOG.md](CHANGELOG.md) and
  [Data contracts](docs/reference/data-contracts.md) when public marts or operator
  workflows change.
- AI agents should follow the no-legacy policy in [AGENTS.md](AGENTS.md).

## Pull requests

1. Branch from `main`.
2. Keep changes focused; match existing style (Ruff, sqlfluff for dbt SQL).
3. Add or update tests for behavior changes.
4. Do not commit secrets, `.env`, DuckDB files, parquet/CSV exports, or other local artifacts (see [`.gitignore`](.gitignore)).
5. Ensure the quality gate passes locally.

## Reporting issues

Use GitHub Issues for bugs and feature requests. For security vulnerabilities, see [SECURITY.md](SECURITY.md).

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating, you agree to uphold it.

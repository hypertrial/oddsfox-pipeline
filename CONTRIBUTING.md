# Contributing to OddsFox

Thank you for your interest in contributing. OddsFox is an open-source,
local-first prediction-market data pipeline built with Dagster, dlt, dbt, and
DuckDB. Version `0.1.x` ships a WC2026-only Polymarket pipeline for FIFA World
Cup 2026 markets and odds.

## Development setup

```bash
uv sync --extra dev
cp .env.example .env
```

The default warehouse is `oddsfox.duckdb` in the repo root. Keep schedules disabled in local dev and CI unless you intentionally run live ingestion:

```dotenv
WC2026_POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

See [docs/quickstart.md](docs/quickstart.md) and [docs/configuration.md](docs/configuration.md) for full operator setup.

## AI-assisted development

If you use Cursor, [Ponytail](https://github.com/DietrichGebert/ponytail) loads from [`.cursor/rules/ponytail.mdc`](.cursor/rules/ponytail.mdc). Repo-specific guardrails (layout, quality gate, orchestration limits) live in [AGENTS.md](AGENTS.md).

## Quality gate

Run these before opening a pull request (they mirror [`.github/workflows/ci.yml`](.github/workflows/ci.yml)):

```bash
uv run make lint
uv run make test
uv run make integration-dagster
uv run make integration-dbt
uv run make coverage
uv run make docs-check
uv run make dbt-parse
uv run make dbt-build-ci
uv run make costguard
```

`dbt-build-ci` bootstraps a disposable DuckDB database under `.cache/` before
running dbt build.
Costguard is a dbt/CI guardrail, not an odds ingestion runtime dependency.
Install the pinned local scanner with:

```bash
curl -fsSL https://raw.githubusercontent.com/hypertrial/costguard/main/scripts/install.sh | sh -s -- v2.5.0
```

Additional targets are available in the [Makefile](Makefile) (`unit-core`, `unit-ingest`, etc.).

## Versioning expectations

OddsFox is v0.1.x — the project is too new to carry backward-compatibility
burden by default.

- Breaking changes are acceptable when they simplify the pipeline.
- Update tests and docs with behavior changes; do not add backward-compat shims
  unless the PR explicitly scopes compat work.
- Document breaking changes in [CHANGELOG.md](CHANGELOG.md) and
  [docs/data-contracts.md](docs/data-contracts.md) when public marts or operator
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

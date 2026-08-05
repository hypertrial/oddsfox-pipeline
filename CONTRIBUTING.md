# Contributing to OddsFox Pipeline

Thank you for your interest in contributing. OddsFox Pipeline is an open-source,
local-first prediction-market data pipeline built with Dagster, dlt, dbt, and
DuckDB. Version `0.2.x` ships WC2026 Polymarket and Kalshi WC2026 pipelines,
plus a small FIFA fixture/results source for real-team scope validation.

## Development setup

See the [Quickstart](docs/getting-started/index.md) and
[Development guide](docs/development/index.md) for local setup. Use
[Terminology](docs/reference/terminology.md) for product vocabulary and the
[Configuration reference](docs/reference/configuration.md) for `.env` details.

## Source adapter contributions

Polymarket WC2026 and Kalshi WC2026 are the shipped market adapters. The
bundled `international_results` WC2026 source validates team scope; it is not a
bookmaker/market adapter. Contributions may
add or improve adapters for traditional bookmakers and other odds sources when
they keep the pipeline local-first and operator-owned.

Useful contribution areas include ingestion adapters, Dagster assets and jobs,
dbt models and tests, DuckDB storage, docs, and operator scripts. Adapter PRs
should include tests, docs, config examples, and source-specific data ownership
and rate-limit notes.

Do not assume centralized OddsFox Pipeline-hosted data. Operators must be able to run
ingestion and store data in their own local or self-managed warehouse.

## Data and IP hygiene

Do not contribute production datasets, scraped dumps, populated seed overlays,
reviewed attestations, source documents, or non-synthetic “real” warehouse rows.
Tracked seed shells must remain header-only. Synthetic fixtures belong under
`tests/fixtures/` with documentation in `tests/fixtures/README.md`.

Complete the provenance checklist in
[.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md) for every
proposed data-like file. See
[Operator responsibilities](docs/concepts/operator-responsibilities.md) and
[Third-Party Notices](THIRD_PARTY_NOTICES.md).

## Contribution licensing

Unless explicitly stated otherwise, any contribution intentionally submitted
for inclusion in OddsFox Pipeline is licensed under the project's MIT License.
Contributors retain copyright in their contributions and represent that they
have the rights needed to submit them. The project requires no contributor
licence agreement or copyright assignment.

See [Third-Party Notices](THIRD_PARTY_NOTICES.md) for the authoritative boundary
between first-party project material and independently governed data, code,
services, documents, dependencies, fonts, and marks.

## AI-assisted development

If you use Cursor, [Ponytail](https://github.com/DietrichGebert/ponytail) loads from [`.cursor/rules/ponytail.mdc`](.cursor/rules/ponytail.mdc). Repo-specific guardrails (layout, quality gate, orchestration limits) live in [AGENTS.md](AGENTS.md).
AI-assisted contributions still require that you have the rights needed to
submit the material and that it meets the data and IP hygiene rules above.

## Quality gate

Quality gates, targeted Make commands, and layout guardrails live in
[AGENTS.md](AGENTS.md). Contributor checklists are in the
[Development guide](docs/development/index.md) and
[Contributors hub](docs/audiences/contributors.md).

## Versioning expectations

OddsFox Pipeline is v0.2.x — the project is too new to carry backward-compatibility
burden by default.

- Breaking changes are acceptable when they simplify the pipeline.
- Update tests and docs with behavior changes; do not add backward-compat shims
  unless the PR explicitly scopes compat work.
- Document breaking changes in [CHANGELOG.md](CHANGELOG.md) and
  [Data contracts](docs/reference/data-contracts.md) when documented marts or operator
  workflows change.
- AI agents should follow the no-legacy policy in [AGENTS.md](AGENTS.md).

## Pull requests

See [AGENTS.md](AGENTS.md) for the PR checklist, quality gate, and artifact
policy. For every proposed data-like file, complete the pull-request provenance
checklist. State whether it is executable project configuration, a header-only
schema shell, a synthetic test fixture, or third-party material. Third-party
material must retain its original licence and a file-specific notice.

## Reporting issues

Use GitHub Issues for bugs and feature requests. For security vulnerabilities, see [SECURITY.md](SECURITY.md).

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating, you agree to uphold it.

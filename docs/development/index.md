# Development

Use this page when changing code, dbt models, docs, or orchestration behavior.
OddsFox Pipeline is a prediction-market pipeline; v0.2.x development touches the
Polymarket WC2026 and Kalshi WC2026 adapters, marts, and orchestration. For a short contributor map, start with
[Contributors](../audiences/contributors.md). For operator setup, start with
[Quickstart](../getting-started/index.md).

## Repo Layout

| Path | Purpose |
| --- | --- |
| `src/oddsfox_pipeline` | Python package for config, ingestion, storage, resources, and orchestration. |
| `dbt` | DuckDB dbt project, profiles, macros, models, and data tests. |
| `docs` | MkDocs content, self-hosted fonts, and the small OddsFox Pipeline theme extension. |
| `scripts` | Operator utilities for warehouse inspection, compaction, pruning, repair, and WC2026 exports. |
| `tests` | Unit, integration, dbt, Dagster, and repo policy tests. |

## Local Setup

See [Quickstart](../getting-started/index.md) for `uv sync`, `.env`, schedule
flags, and docs-browser setup.

## Which Quality Gate?

Quality gates, targeted Make commands, Costguard install, coverage rules, and
layout guardrails live in
[AGENTS.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/AGENTS.md).

Dagster dbt assets enable dbt source tests as asset checks. Row-count and
column metadata fetching is available through `DbtBuildConfig` but stays
opt-in because DuckDB in-process integration tests share local database
connections.

Costguard high findings must be fixed or justified with an inline suppression
and dbt grain tests that prove the intended shape. Medium/low findings are
measured dbt debt, not automatic materialization work. Before changing dbt
materializations or adding incremental models, capture the failing advisory,
dbt build runtime, and warehouse/profile size evidence that justifies the
change.

## Add A Market Adapter

1. Keep the pipeline local-first and operator-owned; do not assume hosted data.
2. Add fetch/sync code under `src/oddsfox_pipeline` with rate-limit and ownership
   notes in docs/config examples.
3. Wire Dagster assets/jobs with source-first asset keys; register jobs in the
   existing orchestration surface.
4. Add unit and orchestration tests; mark live network checks local-only.
5. Update configuration examples, [Choose a scope](../getting-started/choose-a-scope.md)
   or runbooks when operator behavior changes, and
   [Data contracts](../reference/data-contracts.md) / the
   [Data dictionary](../reference/data-dictionary.md) when documented marts change.
6. Run the gate tree in [AGENTS.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/AGENTS.md).

## Add a documented mart

1. Add dbt models under the correct source-first schema layers and tags.
2. Define grain, null policy, and intended use in
   [Data contracts](../reference/data-contracts.md) and the
   [Data dictionary](../reference/data-dictionary.md).
3. Add dbt unit tests and, for stable public shapes, golden fixtures.
4. Expose the mart through existing Dagster dbt selectors/jobs; do not invent a
   runtime scope selector.
5. Add or update query guidance in
   [Query the warehouse](../guides/query-the-warehouse.md) when analysts need a
   new starting table.
6. Run `dbt-unit` / `golden-dbt` as relevant, then `ci-fast`.

## dbt Materialization Debt

Treat Costguard medium/low advisories as measurement prompts. Current measured
debt includes:

- `int_polymarket_wc2026_token_working_set` is materialized as a table because profiling
  showed it is reused heavily by WC2026 marts and the dbt build stayed
  neutral or faster after the change.
- `int_polymarket_wc2026_market_tokens` is materialized as a table because it
  feeds multiple WC2026 intermediate joins. Costguard now tracks its remaining
  incremental-conversion question as `SQLCOST040`.
- `int_polymarket_wc2026_token_hourly_odds` is an incremental private fact that
  reprocesses dirty hourly buckets from raw odds `ingested_at` overlap.
- `SQLCOST040`: `int_polymarket_wc2026_token_working_set` and
  `int_polymarket_wc2026_market_tokens` still track remaining materialization
  questions. Keep collecting row-volume profiling before further conversions.
- Low advisories may still flag `ORDER BY` without `LIMIT` in table-building
  marts; treat them as profiling prompts, not automatic refactors.
- Remaining medium/low Costguard advisories are known dbt debt and do not make
  the gate fail while the scanner exits successfully.

Do not change materializations on advisory text alone. Capture dbt build
runtime, relevant relation sizes from `scripts/profile_warehouse.py`, and the
Costguard finding before switching a model to table or incremental.

## Adding A Scope

OddsFox Pipeline v0.2.x ships fixed scopes, not a runtime scope selector. Add a scope by
making the static surfaces explicit and letting the guard tests catch drift:

1. Add the source discovery seed entry, for example in the Polymarket or Kalshi
   `market_scopes.yml`.
2. Add a `ScopeSpec` in `oddsfox_pipeline.orchestration.shipped_scopes` with the
   source/scope ref, namespace alias, fixed jobs, and dbt selector.
3. Add explicit Dagster assets/jobs in the source module; keep asset keys and op
   names source-first and scope-first.
4. Add dbt source YAML, model folder tags, and a pipeline policy seed when the scope
   ships analytics.
5. Update the quickstart, scope guide, orchestration reference, scripts, and
   this checklist when operator behavior changes.
6. Run the market-scope registry, dbt-structure, orchestration, and docs tests before the
   broader quality gate.

## Local `.env` And Tests

`DUCKDB_PATH` in `.env` overrides `DUCKDB_NAME` and can leak into unit tests
when settings reload from disk. See
[Configuration](../reference/configuration.md#local-development) and
[Troubleshooting](../guides/troubleshooting.md#tests-writing-to-production-warehouse).

- Use the shared `duck` fixture from `tests/unit/storage/duckdb_storage_test_support.py`
  for storage tests that need a disposable warehouse.
- Call `isolate_duckdb_test_env(monkeypatch, db_path)` in ingestion or
  orchestration tests that reload settings but cannot use the `duck` fixture
  directly.

## Pull Request Expectations

- Keep PRs focused and update docs for behavior or operator workflow changes.
- Breaking changes are OK in v0.2.x; document them in CHANGELOG and data
  contracts — do not add legacy fallbacks unless the PR explicitly scopes compat
  work.
- Add or update tests for changed behavior.
- Do not commit `.env`, local DuckDB files, generated dbt targets, `site/`, or
  data exports.
- Follow [CONTRIBUTING](https://github.com/hypertrial/oddsfox-pipeline/blob/main/CONTRIBUTING.md)
  for the full contribution workflow.

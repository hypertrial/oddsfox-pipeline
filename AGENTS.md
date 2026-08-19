# AGENTS.md

OddsFox Pipeline is MIT-licensed, local-first prediction-market pipeline
software owned and licensed by Hypertrial. The canonical repository and its
release artifacts contain no bundled production datasets or operator data.
Hypertrial operates no hosted production pipeline or data service; operators
supply and control their own data. `THIRD_PARTY_NOTICES.md` is the authoritative
scope statement.
Version `0.2.x` ships WC2026 Polymarket and Kalshi pipelines. Runtime
acquisition is deny-by-default: only Polymarket, PMXT, Kalshi, and Polygon are
owned here. Every other collector, source-native parser, reference
transformation, identity workflow, and Elo publisher belongs in
`oddsfox-scraper`. Pipeline may only validate and transport immutable
checksummed Scraper bundles; it must not acquire or parse their upstream data.
Stack: **Dagster** (orchestration), **dlt** (market landing), **dbt** +
**DuckDB** (warehouse/analytics), **uv** (deps), **Ruff** + **sqlfluff**
(lint), **pytest** (tests). Prefer **Polars** over Pandas for dataframe work;
Pandas is banned (Ruff `TID251` + `tests/repository/test_dependency_policy.py`).

## Setup

```bash
uv sync --group dev
cp .env.example .env
```

Default warehouse: `oddsfox.duckdb` in the repo root. Keep schedules disabled in local dev and CI unless intentionally running live ingestion:

```dotenv
KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

## AI agent guidance

Cursor loads [Ponytail](https://github.com/DietrichGebert/ponytail) from [`.cursor/rules/ponytail.mdc`](.cursor/rules/ponytail.mdc) (`alwaysApply: true`). It complements this file: reuse existing code, prefer stdlib and installed deps, and keep diffs minimal. Do not cut validation, error handling, security, or tests that cover real behavior. Mark intentional shortcuts with a `ponytail:` comment (name the ceiling and upgrade path).

Other agents should read this `AGENTS.md` at the repo root.

**Permanent source boundary:** before adding any runtime network adapter, add
it to the acquisition ownership registry and its boundary tests. Non-market
hosts, parsers, raw schemas, jobs, schedules, and feature publishers are not
permitted in this repository. See
[`docs/architecture/source-ownership.md`](docs/architecture/source-ownership.md).

**Repo change review:** for in-session audits and ordinary dev validation, run
`make test-dev` (not `ci-fast`, not `release-gate`). Reserve `ci-fast` for
explicit pre-push checks and `release-gate` for major-version publish prep only.
Add focused Make targets (for example `make check-terminology`) when the diff
touches those surfaces.

**Terminology:** [docs/reference/terminology.md](docs/reference/terminology.md)
is the normative **34-term** vocabulary (pipeline, job/run, scope, catalog,
registry, working set, contract, grain language, and related terms). Machine
rules live in
[`config/terminology_policy.toml`](config/terminology_policy.toml). Prefer those
terms in new identifiers, docs, CLI help, metrics, and comments. Before finishing
work that touches naming or prose, run `make check-terminology` (also part of
`make lint` / `ci-fast`). Do not reintroduce retired first-party “flow,”
“graph odds/export,” universe phrases, or other forbidden identifiers listed
there.

## No legacy support (v0.2.x)

OddsFox Pipeline is v0.2.x — too new for a supported legacy surface, migration path, or
backward-compatibility layer unless the task explicitly requests one.

- **Remove and replace** old APIs, config values, warehouse layouts, and marts;
  do not add adapters, aliases, deprecation periods, or dual code paths.
- **Warehouse reset over migration:** operators with pre-layout DuckDB files
  should delete the warehouse (`rm oddsfox.duckdb*`) and rerun quickstart.
- **Public contracts:** [Data contracts](docs/reference/data-contracts.md) marts,
  Dagster asset keys, and the read-only `oddsfox.reference.v1` handoff are the
  current API. Breaking changes belong in [CHANGELOG.md](CHANGELOG.md), not
  hidden compat layers.
- **Ponytail alignment:** deletion over addition applies here — prefer removing
  dead paths over preserving them.

Do not add `legacy`, `compat`, `deprecated`, or `migration` code paths unless
the user explicitly asks. Do not preserve removed config values (e.g.
`wc2026_legacy`) or reintroduce removed marts/APIs. Do not document long-term
semver stability; v0.2.x may break between releases.

## Quality gate (run before finishing work)

Run the automatic code-safety gate before ordinary pushes:

```bash
uv run make ci-fast
```

Run `uv run make release-gate` only before publishing a major version. Ordinary
PRs and dependency, Dagster, dbt, or data-quality work use `ci-fast` (plus
focused Make targets as needed). `release-gate` runs the lint, contract, docs,
100%-coverage and integration surfaces, focused mutation testing, and Costguard
without repeating the ordinary test pass before coverage. Local `ci-fast` and
`release-gate` launch isolated parallel lanes that mirror GitHub's worker
topology via one Make jobserver (`GATE_JOBS`) over a prerequisite DAG;
`release-gate` also parallelizes coverage shards and dbt-quality sub-lanes, and
caps Mutmut via `MUTMUT_MAX_CHILDREN`. Use `ci-fast-core` / `release-gate-core`
for a true `-j1` sequential diagnosis of the same graph. GitHub parallelizes
the equivalent automatic surface across `static-docs`, `tests`, `dbt`, and a
Python 3.13 package/test compatibility worker, then reports the stable
`fast-gate` aggregate. Python 3.10 remains the supported floor and full-release
runtime. The manual `Manual Full Validation` workflow is the GitHub equivalent
of `release-gate` for a major-version publish: it parallelizes coverage,
dbt/data quality, focused mutation, and static/docs validation behind the
stable `full-gate` aggregate. OddsFox Pipeline is macOS-first and does not ship
or gate on Docker images. For narrower local runs, `make test`, `make
integration-dagster`, `make integration-dbt`, `make data-quality`, `make
mutation`, and `make coverage` still work.

`dbt-build-ci` bootstraps a disposable DuckDB database under the active
`ODDSFOX_RUNTIME_ROOT` before running the ordinary dbt graph, which excludes
`tag:polygon_settlement` and `tag:pmxt_order_book`.
`dbt-polygon-settlement-ci` runs Polygon-tagged dbt unit tests (every hard
blocker key and normalization-pair branch) then a slim integration suite that
keeps the 39,120-row mart contract and representative publication-gate failures.
`data-quality` is the safe dbt-only wrapper that rebuilds the disposable
database before validation. `mutation` resumes focused Mutmut work, while
`mutation-ci` starts from an empty mutation cache and enforces zero unresolved
mutants across outbound URL safety, reference-bundle contracts, market-scope
predicates, market persistence, and odds planning. Incremental dbt tests compare
all five incremental odds models with a full refresh; seeded Dagster tests prove
repeat-run stability and Polymarket writer recovery. `contract-http` is
replay-only and part of both gates, while the
default `make test` still excludes the `contract` marker.
Costguard is a dbt/release guardrail, not an odds ingestion runtime dependency.
Install the pinned local scanner with:

```bash
curl -fsSL https://raw.githubusercontent.com/hypertrial/costguard/main/scripts/install.sh | sh -s -- v2.5.0
```

### Targeted commands

| Target | Purpose |
|--------|---------|
| `make format` | Ruff format + fail-closed sqlfluff fix |
| `make unit-core` | Config, resources, storage unit tests |
| `make unit-ingest` | Ingestion unit tests |
| `make unit-orchestration` | Orchestration/Dagster unit tests |
| `make dagster-jobs-smoke` | Headless smoke for every registered public job with mocked externals |
| `make dagster-jobs-smoke-cov` | Coverage version of registered public job smoke |
| `make dagster-refresh-cov` | Coverage for scoped Dagster E2E, writer recovery, and dbt wiring |
| `make test-cov` | Unit tests with coverage accumulation (`-n auto`) |
| `make test-dev` | Dev-only fast loop: skips dbt deps/parse when unchanged, skips Polygon-settlement tests when untouched, reduced Hypothesis budget — not a CI gate substitute |
| `make integration-dbt` | DuckDB + dbt integration smoke |
| `make integration-dagster` | Dagster integration smoke |
| `make integration-dbt-cov` | DuckDB + dbt integration with coverage append |
| `make integration-dagster-cov` | Local wrapper for both split Dagster coverage targets |
| `make pipelines-deterministic` | Dev-only all-pipelines offline validation across Dagster, dbt integration, Polygon, match-minute, and production dbt build |
| `make dbt-unit` | dbt unit tests for high-risk SQL branches |
| `make golden-dbt` | Standalone exact-output dbt mart fixtures (also covered by `integration-dbt`) |
| `make dbt-prepare` | Shared dbt deps/parse into `DBT_TARGET_PATH` before xdist test imports |
| `make gate-timing` | Opt-in gate timing JSON under `.cache/runtime/benchmarks/` |
| `make dbt-source-freshness-ci` | Seed fresh temp source rows and run dbt source freshness |
| `make coverage-report` | Coverage report gate (`--fail-under=100`) |
| `make check-secrets` | Repo policy check for tracked secret leakage |
| `make check-repository` | Ownership suite for Make/workflow/naming/distribution/terminology/secrets/static dbt checks |
| `make check-terminology` | Enforce [terminology](docs/reference/terminology.md); fail on retired vocabulary and identifier regressions |
| `make runtime-dirs` | Create SSD-local temp, cache, dbt, Python, DuckDB-extension, and browser directories below `.cache/runtime` |
| `make dbt-build-ci` | Bootstrap disposable DuckDB + dbt build |
| `make dbt-polygon-settlement-ci` | Build the isolated Polygon settlement graph against replay fixtures |
| `make dbt-match-order-book-ci` | Build the isolated PMXT order-book graph against replay fixtures |
| `make dbt-match-minute-ci` | Build the isolated match-minute graph against a synthetic 104/248/496 contract |
| `make dbt-market-portrait-ci` | Build the isolated market-portrait graph and bundle contract against replay fixtures |
| `make market-portrait-target-validate` | Validate the committed synthetic match-95 portrait target manifest |
| `make data-quality` | Safe local dbt build-and-test wrapper against disposable state |
| `make mutation` | Resume the focused Mutmut run and enforce its exported statistics |
| `make mutation-ci` | Delete cached mutants and run the deterministic focused mutation gate |
| `make contract-http` | Replay-only HTTP contract tests; included in the fast GitHub gate |
| `make match-order-book-live-smoke` | Opt-in resumable PMXT backfill; consumes PMXT credits |
| `make event-catalog-recall-audit` | Opt-in exhaustive WC2026 event-catalog slug-prefix recall audit |
| `make polymarket-catalog-refresh` | Manual four-pass global Gamma event/market crawl plus graph-mart build |
| `make polymarket-catalog-dbt-build` | Offline rebuild of the cumulative global graph mart |
| `make polymarket-catalog-release RELEASE_VERSION=<semver>` | Offline immutable Parquet publication of the graph mart |
| `make match-minute-inputs-validate` | Validate the loaded Scraper bundle's 104-match fixture inventory |
| `make minute-odds-live-smoke` | Disposable unified minute-odds live smoke (5%/leg sample, futures 24h tail; external CLOB/Gamma) |
| `make futures-minute-publish-benchmark` | Disposable baseline/candidate futures-minute publish speed + equality report |
| `make local-marts-rebuild` | Full-refresh and verify both WC2026 minute marts from completed operator-local raw warehouses |
| `make polygon-settlement-live-smoke` | Opt-in finalized Polygon settlement backfill against a disposable warehouse |
| `make polygon-settlement-benchmark` | Exact optional v3/v4 fill+mart comparison; requires two completed warehouses |
| `make polygon-settlement-seed-candidate` | Author an evidence-backed candidate below ignored `artifacts/`; never promotes the dbt seed |
| `make polygon-settlement-seed-validate` | Validate an operator-local 248-proposition seed and resolution attestation |
| `make polygon-settlement-release` | Build an immutable internal Polygon settlement audit bundle |
| `make polygon-settlement-export` | Build an offline allowlisted technical export from an audit bundle |
| `make costguard-scan` | Run the dbt cost guardrail against an existing dbt build |
| `make costguard` | Safe local wrapper that rebuilds disposable dbt state before Costguard |
| `make dagster-dev` | Local Dagster UI |
| `make docs-serve` | MkDocs dev server |

## Project layout

```
.cursor/rules/     # Cursor agent rules (ponytail)
src/oddsfox_pipeline/
  config/          # Settings barrel (settings.py re-exports warehouse + polymarket)
  ingestion/       # dlt sources, Polymarket fetch/sync/backfill, odds engine
  orchestration/   # Dagster assets, jobs, schedules, dbt wiring
  resources/       # HTTP, outbound URL, progress guardrails
  storage/duckdb/  # Connection, schemas, markets/odds persistence, profiling
dbt/
  models/sources/   # Read-only oddsfox.reference.v1 and market source declarations
  models/polymarket_catalog/{staging,marts}/
  models/polymarket_wc2026/{staging,intermediate,marts,observability}/
  models/kalshi_wc2026/{staging,intermediate,marts,observability}/
  models/wc2026/{intermediate,marts,observability}/
  tests/           # Singular dbt data tests (assert_*)
tests/
  unit/            # Mocked config, ingestion, storage, orchestration
  integration/     # DuckDB/dbt/Dagster smoke (temp databases)
  dbt/             # dbt project structure checks
docs/              # Project docs (MkDocs)
scripts/           # Warehouse audits, repairs, profiling (not CI gate)
```

Imports use src-layout paths: `from oddsfox_pipeline.config.settings import …`, not relative imports across package boundaries.

## Code style

**Python** ([pyproject.toml](pyproject.toml)):

- Ruff: `target-version = "py310"`, line length 88, double quotes, isort (`extend-select = ["I"]`).
- Run `make format` before committing Python changes; `make lint` checks format + ruff check.
- Match existing module boundaries; avoid drive-by refactors outside the task scope.

**dbt SQL**:

- sqlfluff: DuckDB dialect, dbt templater, max line length 130.
- Lint/fix: `dbt/models`, `dbt/tests` only (see Makefile).
- Layer naming: source-first schemas such as `polymarket_wc2026_staging`,
  `polymarket_wc2026_marts`, and `kalshi_wc2026_marts`. Established
  non-market relation names may appear only in source-neutral reference bridges.

**Tests** ([tests/README.md](tests/README.md)):

- Default `make test` excludes `integration`, `performance`, `slow`, `repo_check`.
- Default `make test` and `make test-cov` also exclude `contract` replay tests.
- Mark slow/external tests with the appropriate pytest marker; do not widen default test scope without reason.
- Add or update tests for behavior changes in the matching `tests/unit/` or `tests/integration/` subtree.

## Orchestration guardrails

Asset key order (routine pipeline; flat op names use the same subject order):

1. `polymarket/catalog/raw/crawl` (manual global catalog only)
2. `polymarket/catalog/{staging,marts}/...`
3. `polymarket/catalog/release/polymarket_graph_catalog` (manual only)
4. `polymarket/wc2026/raw/markets`
5. `polymarket/wc2026/raw/markets_snapshot`
6. `polymarket/wc2026/raw/event_catalog`
7. `polymarket/wc2026/raw/event_snapshots`
8. `polymarket/wc2026/raw/event_market_memberships`
9. `polymarket/wc2026/ops/market_scope_registry`
10. `polymarket/wc2026/raw/market_metadata_enrichment`
11. `polymarket/wc2026/raw/token_odds_history_hourly`
12. `polymarket/wc2026/raw/match_token_odds_history_minute` (dedicated backfill only)
13. `polymarket/wc2026/raw/match_order_book_snapshots` (dedicated PMXT backfill only)
14. `polymarket/wc2026/raw/polygon_settlement_fills` (dedicated finalized backfill only)
15. `polymarket/soccer/raw/event_catalog`
16. `polymarket/soccer/raw/event_snapshots`
17. `polymarket/soccer/raw/event_market_memberships`
18. `polymarket/soccer/ops/match_result_registry`
19. `polymarket/soccer/raw/match_result_token_odds_history_minute`
20. `kalshi/wc2026/raw/events` (dlt sibling landed with markets)
21. `kalshi/wc2026/raw/markets`
22. `kalshi/wc2026/raw/markets_snapshot`
23. `kalshi/wc2026/ops/market_scope_registry`
24. `kalshi/wc2026/raw/market_candlesticks_hourly`
25. external `oddsfox/reference/...` source assets loaded from an immutable
   Scraper `oddsfox.reference.v1` bundle
26. dbt model assets under `polymarket/wc2026/{staging,intermediate,marts,observability}/...`,
   `polymarket/soccer/{staging,intermediate,marts,observability}/...`,
   `kalshi_wc2026/{staging,intermediate,marts,observability}/...`, and
   market-owned `wc2026/{intermediate,marts,observability}/...`; Scraper
   reference tables remain external assets with read-only bridge views
27. `polymarket/wc2026/release/polygon_settlement_odds_bundle` (immutable internal audit release only)

Global catalog jobs: `polymarket_catalog_full_pipeline`,
`polymarket_catalog_dbt_build`, and `polymarket_catalog_release`. They are
manual-only and have no schedule.

Key jobs: `polymarket_wc2026_market_scope_registry_refresh`,
`polymarket_wc2026_event_catalog_recall_audit`, `polymarket_wc2026_hourly_odds_ingest`,
`polymarket_wc2026_match_minute_odds_backfill`,
`polymarket_wc2026_minute_odds_backfill`,
`polymarket_wc2026_minute_odds_live_smoke`,
`polymarket_wc2026_match_order_book_backfill`,
`polymarket_wc2026_polygon_settlement_backfill`,
`polymarket_wc2026_polygon_settlement_release`,
`polymarket_wc2026_dbt_build`, `polymarket_wc2026_full_pipeline`,
`kalshi_wc2026_market_scope_registry_refresh`, `kalshi_wc2026_hourly_odds_ingest`,
`kalshi_wc2026_dbt_build`, `kalshi_wc2026_full_pipeline`.
Soccer jobs: `polymarket_soccer_market_scope_registry_refresh`,
`polymarket_soccer_match_result_minute_odds_ingest`,
`polymarket_soccer_dbt_build`, and `polymarket_soccer_full_pipeline`.

Schedules target `kalshi_wc2026_hourly_odds_ingest`; it is **stopped by default**.
The daily `polymarket_soccer_daily_schedule` targets the soccer full pipeline
at 04:00 UTC and is stopped by default.
Polymarket WC2026 has no Dagster schedule; use manual jobs when needed.
The PMXT order-book backfill, Polygon settlement backfill, and audit-release
jobs are unscheduled and have no schedule-enable environment flags. The
event-catalog recall-audit job is also unscheduled. The minute-odds live
smoke job is also unscheduled. The
technical exporter is standalone and unscheduled.
Do not enable live/hourly schedules in code or `.env` unless the task explicitly requires it.

**Market scope:** v0.2.x ships fixed Dagster/dbt graphs for `wc2026` and
`soccer` on Polymarket and `wc2026` on Kalshi. Polymarket scope helpers may load other
slug-like seed entries for tests and future work, but Dagster asset configs do
not accept a runtime scope selector. See
[Configuration](docs/reference/configuration.md).

**Kalshi env vars:** `KALSHI_REQUESTS_PER_SECOND`,
`KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED`. Kalshi uses the public trade API;
no API credentials are required for local docs, dbt, or mocked tests.

DuckDB is local-only runtime state. For read-only inspection prefer `scripts/profile_warehouse.py` over opening the warehouse read-write.

**Polygon settlement isolation:** this historical pipeline uses an operator-local
`polymarket_wc2026_polygon_settlement_markets.csv` seed and finalized Polygon V2
logs. The tracked seed is a header-only schema shell. Fixture evidence must come
from a validated Scraper reference bundle; the pipeline must not call Gamma,
CLOB, the Polymarket UI, or any non-market source at runtime. Never log or persist RPC URLs, wallet addresses, order hashes,
signatures, raw topics/data, calldata, or oracle prose. The optional second RPC
is advisory only. The release asset writes an internal audit bundle below
`artifacts/polygon_settlement/audit/`; the standalone exporter reads that
bundle offline and writes only the allowlisted operator-local technical dossier
below `artifacts/polygon_settlement/exports/`. Neither path uploads data.

**Distribution policy:** keep production data, generated exports, reviewed
resolution attestations, databases, Parquet, and source documents untracked.
Header-only external seed shells must remain empty in commits. Retained
pipeline policy constants and aliases are executable project configuration; test
fixtures must be synthetic and documented in `tests/fixtures/README.md`.
Third-party material keeps its original licence and must not be described as
MIT-licensed project data. See `THIRD_PARTY_NOTICES.md`.

## Do not

- Commit `.env`, secrets, operator seed rows, reviewed attestations, `*.duckdb`
  / WAL/SHM files, parquet/CSV exports, source documents, or other local
  artifacts (see [`.gitignore`](.gitignore)).
- Invent commands outside the Makefile; if a check is missing, add a Makefile target rather than documenting one-off scripts as the gate.
- Add runtime scope such as soccer context, simulations, allocation, or web integration without explicit product direction; v0.2.x ships fixed WC2026 Polymarket and Kalshi ingest and warehouse graphs only.
- Add legacy, compat, deprecated, or migration shims unless the task explicitly requests backward compatibility.

## Pull requests

1. Branch from `main`.
2. Keep changes focused; one concern per PR when possible.
3. Ensure `uv run make ci-fast` passes locally (use `release-gate` only before
   a major-version publish).
4. Update docs in `docs/` when behavior, configuration, or project positioning changes.

## Further reading

- [OddsFox Pipeline docs](docs/index.md) — overview, audience hubs, runbooks
- [Terminology](docs/reference/terminology.md) — normative repository vocabulary
- [Contributors hub](docs/audiences/contributors.md) and
  [Integration](docs/concepts/integration.md) — human docs entry points
- [CONTRIBUTING.md](CONTRIBUTING.md) — contributor and release workflow
- [Orchestration](docs/reference/orchestration.md) — assets, jobs, and schedules
- [Configuration](docs/reference/configuration.md) — `.env` reference

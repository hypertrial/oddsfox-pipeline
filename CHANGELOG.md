# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Removed

- **Breaking:** Polymarket WC2026 logical atlas — the seven
  `polymarket_wc2026_logical_*` marts, `polymarket-wc2026-logical-v1` export
  bundle, `polymarket_wc2026_logical_atlas` job, `release/logical_bundle` asset,
  `raw/reviewed_event_membership` asset, related dbt models/seeds/tests, and
  scripts `export_polymarket_wc2026_logical_bundle.py`,
  `materialize_polymarket_wc2026_logical_fixture.py`, and
  `build_hosted_artifacts.py`. Shared event-catalog ingestion
  (`raw/event_catalog`, `raw/event_snapshots`, `raw/event_market_memberships`)
  remains for market scope registry refresh.

### Changed

- `polymarket_wc2026_market_scope_registry_refresh` now materializes the
  `event_catalog` multi-asset neighborhood (plus OpenFootball schedule fixtures)
  so registry refresh aligns with the declared `event_catalog` dependency.
- Polymarket odds writer flushes now stage rows with Arrow bulk loads on the
  writer connection instead of per-flush `dlt.pipeline.run` replace loads.
- Kalshi hourly candlestick sync now fetches concurrently and persists candlestick
  rows plus ledger state in one batched write after the worker pool joins.
- Split `polygon_settlement` ingest into `polygon_settlement_{types,normalize,scan,sync}`
  modules with a facade preserving public imports; widened focused mutation coverage
  to `polygon_settlement_normalize.py`.
- Split `market_portrait` into `market_portrait_story` and `market_portrait_export`
  modules with a facade preserving public imports.
- Extracted `merge_event_catalog_batch` into `dlt_batch_event_catalog.py`.
- Deduplicated polygon audit/export JSON writes via `publishing._bundle_io.write_json`.
- Extracted shared registry helpers (`registry_common.py`) and lifted Kalshi
  registry skip-if-refreshed materialization into `kalshi_asset_helpers.py`.
- Added a bootstrap lock around DuckDB schema initialization in `connection.py`.
- Extracted `int_polymarket_wc2026_logical_team_groups` and rewired logical
  markets/propositions/entities marts to stop referencing OpenFootball staging
  directly; propositions now share markets' ambiguous team-group guard.
- Removed `[project.optional-dependencies].dev`; contributor tooling installs
  exclusively through uv dependency groups (`uv sync --group dev`).
- Split the root `Makefile` into include fragments (`Makefile.gates`,
  `Makefile.dbt`, `Makefile.lint`, `Makefile.test`, `Makefile.ops`).
- Removed the pre-atlas legacy rollback path from
  `scripts/build_hosted_artifacts.py`; activation now always requires a
  logical-atlas release manifest plus Graph acceptance and browser-smoke
  receipt validation.

- Split the former "Advanced match analysis (experimental)" registry row into
  three mature, isolated pipelines: match-minute odds, match order book, and
  market portrait. Added `dbt-match-order-book-ci`, `dbt-market-portrait-ci`,
  and `market-portrait-target-validate` release-gate lanes plus synthetic
  portrait/trades replay fixtures.

### Fixed

- `init_duck_db()` again syncs the active DuckDB path before the initialized
  fast-path return, so `DUCKDB_PATH` / `DUCKDB_NAME` swaps still re-bootstrap
  under the schema bootstrap lock.
- `profile_warehouse.py` `--refresh` now propagates `--duckdb-path` to Polymarket
  sync and dbt subprocesses instead of mutating the settings-default warehouse.
- `build_hosted_artifacts.py` default `--duckdb-path` now follows
  `settings.DUCKDB_PATH` (`ODDSFOX_PIPELINE_ROOT` / `DUCKDB_NAME`).
- Polygon settlement scan status JSON is stored under
  `BASE_DIR/.cache/polygon_settlement/status/` instead of a hardcoded checkout
  path.
- `polymarket_wc2026_full_pipeline` (and other jobs using `_merge_run_configs`)
  now unions `dbt_select` and `dbt_exclude` when combining `oddsfox_dbt` run
  configs instead of last-write-wins over the whole op config.
- In-process logical-bundle export reloads the script-backed exporter when its
  source mtime changes, so long-lived Dagster workers pick up exporter updates
  without restart.
- Kalshi hourly candlestick sync now honors Dagster `history_backfill_days` and
  `routine_interval_hours` run-config fields instead of ignoring them.
- `parse_created_at()` accepts ISO-8601 timestamps ending in `Z` or `+00:00`
  without fractional seconds.
- Empty `DUCKDB_PATH` now falls back to `DUCKDB_NAME` like the connection
  resolver, instead of treating the empty string as a literal path.
- Unrecognized `POLYMARKET_WC2026_SCOPE_KEYSET_CLOSED` values now omit the
  closed filter instead of coercing to `false`.
- Kalshi `map_bounded` now skips per-item transient failures instead of
  aborting the whole candlestick sync or registry refresh batch.
- Polymarket market-discovery progress guardrails now read `events_page` and
  record per-page deltas instead of always reporting zero work.
- Kalshi and Polymarket progress guardrails now record per-callback work
  deltas instead of inflating cumulative totals.
- `wc2026_results` now tolerates a one-day fixture/result date drift when
  joining international results.
- `fetch_token_history()` honors explicit `start_ts=0` epoch timestamps.
- Warehouse profiler numeric classification now covers DuckDB unsigned integer
  types (`USMALLINT`, `UINTEGER`, `UBIGINT`).
- Match-minute knockout working-set joins now allow up to one day of kickoff
  drift instead of a 60-second cutoff that silently dropped mapped markets.
- Kalshi `hour_start_utc` now stores the hourly candlestick bucket start
  (`end_period_ts - 3600`) instead of the inclusive period end timestamp.
- `wc2026_results` keeps one-day date tolerance for team-identity joins but
  requires exact dates for knockout city-only attribution.
- Kalshi QF/SF/FL `progression_outcome_label` values now use the
  `not_eliminated_in_*` convention aligned with price inversion.
- Order-book backfill progress callbacks now record progress before enforcing
  hard no-progress timeouts.
- Release-gate match-order-book and market-portrait lanes now use isolated
  dbt runtime roots instead of sharing one target directory.
- `count_polymarket_wc2026_gamma_tag_events.py` now forwards
  `keyset_related_tags` to Gamma keyset requests.
- `release-gate-coverage-prep` now prepares dbt state under the coverage
  runtime root used by coverage shards.
- `join_under_base()` now rejects relative hrefs that escape the base path via
  `../` segments.

### Removed

- Polymarket US midterms 2026 pipeline (`polymarket_us_midterms_2026_*` jobs,
  schedules, dbt graph, and `POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED`).
- Cross-platform WC2026 knockout match pipeline (`wc2026_knockout_match_odds_full_pipeline`,
  `wc2026_marts.wc2026_knockout_match_hourly_odds`, related observability, and
  `WC2026_KNOCKOUT_MATCH_ODDS_HOURLY_SCHEDULE_ENABLED`).
- Docker packaging and publication: Dockerfile(s), `.dockerignore`, container
  smoke Make targets, GHCR multi-arch publish/sign steps, and the Docker image
  guide. OddsFox Pipeline is macOS-first; distribution smoke stays on
  `make package-smoke`.
- Retired first-party terminology identifiers and prose from the compact
  cutover (see [Terminology](docs/reference/terminology.md) deprecated table):
  `token_universe` / `match_market_universe` model names and “market/token/
  validated universe” phrases, ScopeStep `market_registry`, `publish_current`,
  `scope_class`, and related graph-export product names. Operators with older
  warehouses delete `oddsfox.duckdb*` and rebuild.

### Changed

- Routine scoped dbt jobs (`polymarket_wc2026_dbt_build`,
  `kalshi_wc2026_dbt_build`, and
  `polymarket_wc2026_logical_atlas`) default to incremental builds
  (`full_refresh=False`). Set `full_refresh=True` in Dagster run config when a
  one-off full rebuild is required.
- Polymarket market-scope discovery and registry refresh now apply the scan
  helper's built-in page-budget guard (25 pages without progress) when Dagster
  run config leaves `max_pages_without_progress` unset, instead of disabling the
  guard.
- Kalshi market-scope registry refresh and hourly candlestick sync use bounded
  concurrent fetch with the shared `KALSHI_REQUESTS_PER_SECOND` rate limiter.
- `int_polymarket_wc2026_logical_markets` and
  `int_polymarket_wc2026_fixture_events` materialize as tables; Polymarket
  markets and token-working-set twin models share parameterized dbt macros.
- Warehouse observability snapshots batch raw-table counts and scope dbt model
  snapshots to the models selected by the active `dbt_select` /
  `dbt_exclude`.
- `release/logical_bundle` calls the exporter in-process instead of shelling
  out to `scripts/export_polymarket_wc2026_logical_bundle.py` (the script
  remains the CLI wrapper).
- Removed unused single-field Polymarket metadata backfill entry points
  (`backfill_tokens`, `backfill_slugs`, `backfill_end_dates`,
  `backfill_event_slugs`); `enrich_market_metadata` is the sole path.
- Consolidated Polymarket raw dlt column specs with DuckDB DDL, shared
  publishing bundle I/O helpers, and orchestration raw-snapshot / dlt-cache
  helpers; decomposed the highest-complexity ingestion, contracts, publishing,
  and odds-planning functions without behavior change.

- Pipeline clarity docs: [Pipeline registry](docs/reference/orchestration.md#pipeline-registry)
  with maturity tiers (production, mature composed/isolated, experimental);
  entry-point jobs vs steps in [Terminology](docs/reference/terminology.md#execution);
  advanced match-analysis family (order book → market portrait; minute odds
  optional and independent) grouped in operator and scope guides. Docs-only; no job or code changes.
- Compact terminology cutover: normative vocabulary is exactly **34** core
  terms in `docs/reference/terminology.md`, gated by
  `config/terminology_policy.toml` and `make check-terminology`. Breaking
  renames include ScopeStep / job surface `market_scope_registry`, dbt
  `*_working_set` models, `int_wc2026_advancement_fixtures`, and symlink
  activation via `activate_current` (replacing `publish_current`).
- `release-gate` (and Manual Full Validation) is required only before publishing
  a major version; ordinary PRs, including dependency/Dagster/dbt/data-quality
  work, use `ci-fast` plus focused Make targets.
- Local `ci-fast` and `release-gate` use one Make jobserver (`GATE_JOBS`) over a
  prerequisite DAG; `ci-fast-core` / `release-gate-core` run the same graph with
  `-j1`. Coverage shards write distinct `COVERAGE_FILE`s and combine once;
  subprocess pools are capped with `RELEASE_PYTEST_WORKERS`, `DBT_TEST_WORKERS`,
  and `MUTMUT_MAX_CHILDREN`. dbt profile threads are environment-configurable
  (`DBT_THREADS`). PR CI and Manual Full Validation install narrower uv
  dependency groups per worker (`--no-default-groups`) while keeping full
  Python 3.10 and 3.13 suites. Manual Full Validation's static lane is
  `static-docs` (no container worker).
- Test ownership is path-based: `tests/repository/`, `tests/docs/`, and
  `tests/package/` hold policy checks; `make check-repository` is the canonical
  repo-check entrypoint wired into lint and CI static lanes. Ordinary unit
  collection ignores those directories.
- Orchestration unit fixtures no longer reload settings modules per test; market
  scope tests live under `tests/unit/ingestion/market_scope/`. Polygon audit
  release validation uses focused helpers and minimal row factories, with one
  full 39,120-row aggregate check and one complete audit-bundle build.
- Polygon settlement data-quality SQL is split into tagged seed/scan/raw/minute
  summary seams. `dbt-polygon-settlement-ci` runs Polygon-tagged dbt unit tests
  covering every hard blocker key and all seven normalization-pair branches,
  then a slim integration suite (exact dense mart, representative scan failure,
  representative raw-pair failure).
- Dagster integration is layered: mocked registered-job smoke, recording-dbt
  wiring for the twelve shipped scoped jobs, one real disposable-DuckDB/dbt E2E
  per shipped scope, and writer recovery without repeated real dbt builds.
- Golden mart fixtures remain available via `make golden-dbt` but are no longer
  duplicated in the release / Manual Full `dbt-quality` sequence because
  `integration-dbt-cov` already executes them.

### Added

- `scripts/bootstrap_dbt_ci_duckdb.py`, `scripts/gate_timing.py`, Playwright
  browser caching in Manual Full Validation, and unified uv cache path
  `.cache/runtime/uv`.

## [0.1.13] - 2026-08-02

### Added

- The manifest-bound `polymarket-wc2026-logical-v1` event, market, membership,
  proposition, entity, and scope bundle for the local WC2026 Logical Market
  Atlas. Event admission uses Polymarket's source-reported lifetime event volume
  with a reviewed final-tournament membership policy and sticky eligibility.
- Append-only Polymarket event, tag, and event-market snapshots; reviewed
  membership decisions; bundle quality checks; and atomic paired Pipeline/Graph
  release publication with exact source revisions and file hashes. Builds are
  shadow-only; activation additionally requires Graph's manifest-bound browser
  smoke receipt and validates it under the activation lock before repointing
  `current`. Content-sealed pre-atlas rollback releases remain activatable
  without fabricating unavailable historical code revisions.
- Logical market membership is derived from each event's latest complete
  catalog observation, so source corrections and zero-child removals do not
  leave stale relationships in the served logical atlas while raw history remains
  append-only.
- Public Polymarket market catalog marts `polymarket_wc2026_markets` and
  `polymarket_us_midterms_2026_markets`: one row per platform-wide Gamma market
  with volume at or above $100,000 USD (`/markets/keyset` catalog sync; no
  tag/registry filter), exposing event/market identity, question, description, outcomes,
  CLOB token IDs, reported USD volume, start/end times (nulling `start_time` when
  Gamma reports a start after `end_time`), category, and tags.
- `scripts/sync_polymarket_markets_catalog.py` to land that platform-wide catalog
  into `polymarket_catalog_raw.markets`.
- `scripts/export_polymarket_markets.py` to export those catalog marts to parquet
  under `artifacts/polymarket_markets_exports/` with fail-closed grain, volume-floor,
  timing, and outcomes/CLOB JSON checks.

### Changed

- Breaking terminology cutover (no aliases): canonical vocabulary in
  `docs/reference/terminology.md` — product-path **pipeline** (not flow);
  **logical atlas / logical bundle / logical contract** (not graph odds/export);
  public marts as the supported query API; **`wc2026.v1` is the private strategy
  clean-data contract only**; minute-grain / match-minute (not minutely);
  ingestion-run observability (not sync-run); schedule fixtures for OpenFootball
  1–104; market scope registry refresh / step `market_registry`; metadata
  enrichment; pipeline policy for threshold seeds. Identifier renames:
  `shipped_scopes`; `*_market_scope_registry_refresh`; ScopeStep
  `market_registry`; `membership_class`; `market_metadata_enrichment`;
  `ingestion_run_events` / `*_ingestion_run_observability`; match-minute
  `observation_gap`. Delete `oddsfox.duckdb*` and rebuild before using the new
  layout.

- Breaking: logical-atlas identifiers rename `graph_*` eligibility/usability
  fields and models to `logical_*` (`int_polymarket_wc2026_logical_markets`,
  `polymarket_wc2026_logical_contract`, `logical_usable`,
  `event_logical_eligible`, tag `wc2026_logical_atlas`). No compatibility
  aliases.

- Breaking: the WC2026 release path now exports the logical-v1 bundle and
  invokes `oddsfox-graph discover --input-profile polymarket-wc2026-logical-v1`.
  The prior hourly graph mart/export is removed. Recreate local DuckDB
  warehouses before running the new full-pipeline job.

## [0.1.12] - 2026-07-29

### Fixed

- Football stories now use exact half-open 60-second UTC bands, infer missing
  stoppage labels from actual period duration with a one-millisecond tolerance,
  and clamp only the final band to the actual period boundary. Validation
  tolerates only the sanitizer's possible two-microsecond inversion between
  equal final-period and game-end timestamps.
- Football annotations now carry chronological post-event scores. Scoring
  transitions are validated one goal at a time, final scores must agree with
  `MatchFacts`, and score checkpoints become effective at the uncertain
  event-minute band's end.
- Minute-aligned reaction windows now select the last strictly pre-window and
  first guaranteed post-window observations, return null when unavailable, and
  never cross a halftime or extra-time break. Directional millisecond rounding
  uses ceilings for `< S` and `>= E` thresholds and floors for same-period
  upper bounds, preserving those predicates at sanitized micro-epsilon
  boundaries.
- Story publication now fails closed on derived band, annotation, score, and
  reaction invariant violations without changing the
  `oddsfox.market-portrait.v1` wire shape.

## [0.1.11] - 2026-07-29

### Changed

- Market portraits now begin at actual kickoff and use a continuous 45-second
  regulation timeline with uniform football-minute pacing.
- Extra time extends portraits to 60 seconds and penalty shootouts add one
  five-second phase without changing the v1 bundle contract.

## [0.1.10] - 2026-07-29

### Changed

- Added sanitized UTC kickoff to the neutral market-portrait fact boundary.
- Market-portrait export now rejects implausible or shifted period timelines,
  inconsistent validated-universe timing, and PMXT root windows that do not
  strictly cover the complete football interval before serializing a bundle.
- Preserved the byte-stable `oddsfox.market-portrait.v1` bundle contract.

## [0.1.9] - 2026-07-28

### Added

- Added the private `oddsfox.market-portrait.v1` contract, neutral football
  fact API, group/knockout target generation, resumable PMXT trade acquisition,
  exact complete-state marts, and explicit portrait backfill workflow.
- Renamed the optional public match-event placeholder and asset identity to the
  provider-neutral `private_match_events` boundary.

### Fixed

- `wc2026_team_ratings_current` now selects only EloRatings
  `snapshot_scope = current` (live World scrape). It previously preferred
  year-end rows via `snapshot_year desc nulls last`, so the “current” mart and
  latest freeze CSV mostly echoed 2025 year-end ratings.

### Added

- Added an unscheduled, resumable hosted-PMXT historical L2 backfill for the
  pinned Argentina–Egypt WC2026 round-of-16 team-to-advance market, including
  both outcome-token snapshot streams, conservative credit accounting,
  fail-closed adaptive range splitting, dlt landing, transactional publication,
  and the long-form
  `polymarket_wc2026_marts.polymarket_wc2026_match_order_book` mart.
- Added PMXT order-book identity/completeness/depth observability, an isolated
  Dagster/dbt graph, replay and integration coverage, and the opt-in
  `make match-order-book-live-smoke` operator path.
- Added `wc2026_marts.team_ratings_pre_match`: match×team pre-match EloRatings
  reconstructed from canonical `wc2026_raw.eloratings__match_results`
  (`pre = post ∓ change`, all competitions on/after 2026-01-01). Requires a
  fresh EloRatings collector snapshot that publishes `match_results.parquet`
  alongside `team_ratings.parquet`.
- Added `scripts/export_eloratings_wc2026_team_ratings_freezes.py` and
  `make export-wc2026-elo-freezes` to export national-team Elo CSV freezes
  (`pre_kickoff` = year-end 2025 history; `latest_current` = live World scrape)
  under `artifacts/wc2026_elo_exports/`.
- Added strict focused Mutmut coverage for outbound URL safety, raw snapshot
  contracts, Polymarket scope predicates, market persistence, and odds
  planning, enforced by ordinary policy tests plus local and manual full
  release gates.
- Added incremental/full-refresh equivalence coverage for all five incremental
  odds models, repeat-run tests for all shipped refresh paths, and transactional
  Polymarket writer failure/recovery validation.
- Added a required Python 3.13 package and ordinary-test compatibility worker
  while retaining Python 3.10 as the supported floor and full-release runtime.
- Added an operator guide for the cross-platform knockout job
  (`wc2026_knockout_match_odds_full_pipeline`) and linked it from Choose a
  scope, Operators, Enable schedules, and Orchestration.
- Split private `oddsfox.raw.v1` / strategy clean-data docs into
  `docs/reference/strategy-contracts.md`; public mart contracts remain on
  `docs/reference/data-contracts.md`.
- Split the WC2026 minute-mart recreation runbook into an index plus
  match-minute and Polygon settlement child guides.
- Expanded the Integrators hub with a consume/pin/export/Polygon-boundary
  checklist.
- Documented audience hubs (analysts, operators, contributors, integrators),
  FAQ, glossary, scope and non-goals, design decisions, integration guidance,
  day-two operations, and the signed Docker image as an advanced path.
- Added an operator-responsibilities page covering data-rights checklist,
  non-advice and non-venue disclaimers, export redistribution matrix, privacy
  caveats, and non-authoritative third-party terms pointers.
- Expanded `SECURITY.md` scope notes for RPC secrets, wallet material, and
  operator-local Polygon audit artifacts.

### Changed

- Removed the unused Great Expectations-style report layer; dbt build/tests and
  observability relations remain the data-quality authority.
- Moved the Polygon settlement complete column contract from the data
  dictionary into public data contracts; dictionary keeps analyst summary.
- Slimmed the Analysts hub to a warehouse branch and join map; added a shared
  reference-ladder note on the chooser, dictionary, contracts, and warehouse
  pages.
- Aligned Development schedule snippets with all four disabled-by-default
  hourly flags and pointed README first-run readers at Quickstart.
- Reshaped the docs site and README toward progressive disclosure: thinner
  README portal, quality-gate SSOT in `AGENTS.md`, contributor checklists in the
  Development guide, and Polygon settlement framed as optional/advanced.
- Reordered the analyst data dictionary so common WC2026 marts lead and the
  Polygon settlement mart follows.
- Clarified that local success checks are technical verification, not
  Hypertrial certification of data rights or trading fitness, and added a docs
  site copyright footer linking Scope, operator responsibilities, and
  Third-Party Notices.
- Strengthened contribution IP hygiene in `CONTRIBUTING.md` and the
  Contributors hub.

### Fixed

- Corrected the pinned Neg Risk adapter provenance notice: its repository
  contains no licence file, so the project infers no licence permission.
- Extended ownership and production-use wording enforcement across the complete
  tracked tree.

## [0.1.8] - 2026-07-24

### Added

- Enforced the reviewed licence mapping for every direct runtime dependency
  while retaining the release SBOM for transitive and base-image inventory.

### Changed

- Established `THIRD_PARTY_NOTICES.md` as the authoritative statement that
  Hypertrial owns and MIT-licenses the first-party project, operates no hosted
  production pipeline or data service, and does not restrict recipients' MIT
  rights.
- Documented independently authored Polygon event-interface provenance,
  third-party source and Contributor Covenant attribution, nominative use of
  third-party marks, and contributor-retained MIT licensing.
- Made GitHub Private Vulnerability Reporting the sole private security channel
  and scoped the container's MIT label to the Hypertrial-owned application.
- Split automatic validation into parallel static/docs, fast-test/contract, and
  dbt-lint workers behind the stable `fast-gate` check. Fast unit tests now use
  xdist while DuckDB, Dagster, dbt integration, replay contract, and browser
  suites remain serial.
- Split manual full validation into parallel coverage, dbt/data-quality, and
  static/docs/container workers behind `full-gate`, and removed the duplicate
  ordinary test pass before coverage.
- Made SQLFluff dbt compilation fail closed and removed redundant automatic dbt
  parsing after mutation checks proved malformed project/schema YAML, undefined
  macros, missing refs, and invalid SQL all fail lint.

## [0.1.7] - 2026-07-23

### Added

- Independent WC2026 Polygon V2 settlement-log flow with an operator-supplied
  248-row on-chain market manifest, resumable finalized-block backfill, wallet- and
  order-payload-redacted fill snapshot, isolated Dagster/dbt graph, and dense
  39,120-row
  `polymarket_wc2026_polygon_settlement_minute_odds` mart.
- Immutable internal Polygon settlement audit releases with complete
  source/provenance/quality evidence and checksums, plus a standalone offline
  exporter for the allowlisted **WC2026 Polygon Settlement Minute Aggregates**
  CSV and operator-local technical quality dossier.
- Developer-only Polygon seed authoring and validation commands, replay-backed
  Polygon dbt validation, and an opt-in live Polygon smoke target.
- `polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds`, a dense,
  null-preserving in-game minute mart for all 104 FIFA World Cup 2026 matches,
  with 248 selected markets, 496 literal source tokens, minute Yes/No OHLC, and
  primary Gamma event timing.
- Dedicated, unscheduled
  `polymarket_wc2026_match_minute_odds_backfill` ingestion/publication job and
  match-minute inventory/completeness observability.
- A DuckDB-native script to export and summarize the match-minute mart as
  Parquet.
- Immutable `international_results` revision and payload provenance, append-only
  per-token minute-fetch audits, token-cadence coverage, and detailed current
  match-minute quality issues.
- `THIRD_PARTY_NOTICES.md`, PEP 639 MIT package metadata, distribution-policy
  checks, and explicit synthetic-fixture provenance.
- An SSD-rooted runtime contract plus `local-marts-rebuild`, which full-refreshes
  and verifies both WC2026 minute marts from operator-local raw warehouses.

### Changed

- Polygon settlement audit releases now live below
  `artifacts/polygon_settlement/audit/`; allowlisted technical exports live below
  `artifacts/polygon_settlement/exports/`. The exporter verifies the immutable
  audit bundle and copies the primary CSV byte-for-byte without querying the
  warehouse
  or calling a network service.
- Polygon settlement collection now uses the `polygon-v2-settlement-v4`
  pipeline: ranges are planned per authored exchange, exact token/block bounds
  reject irrelevant discoveries before receipt fetch, and complete leaves run
  concurrently through receipt/header validation and normalization. Adaptive
  250–20,000-block and 5–50-receipt work records safe per-chunk RPC metrics,
  writes atomic redacted status, and inserts fills through explicit Arrow
  batches. Published compatible reruns short-circuit before credentials or RPC
  construction; all live-smoke state remains below the SSD-backed `.cache`.
- Ordinary Polymarket/dbt jobs and `make dbt-build` exclude the isolated
  `polygon_settlement` graph; the full release gate validates it independently
  with synthetic replay fixtures. The new backfill and release jobs have no
  schedules and do not modify the existing Gamma/CLOB flow.
- Match-minute CLOB publication now replaces the complete raw snapshot in one
  transaction only after all 496 token fetches succeed; failed runs retain audit
  evidence and leave the previous raw snapshot and public mart unchanged.
- The minute mart now exposes scheduled-versus-actual timing, boundary status,
  a zero-based uncapped `elapsed_window_minute` wall-clock axis, raw Yes/No
  close-pair diagnostics, and pinned results provenance. Source-price, cadence,
  timing, and incomplete interior-minute anomalies are nonblocking warnings;
  structural contract failures still block atomic publication.
- Match-minute Parquet export now validates a temporary artifact before atomic
  replacement and reports structural inventory, completeness, boundary, pair,
  provenance, size, and SHA-256 metrics. Existing local warehouses must be reset
  for the new results-provenance and fetch-audit schemas; no migration shim is
  provided.
- Make child processes now place temporary files, uv/XDG/browser caches, Python
  bytecode, and dbt output below `.cache/runtime` by default. Polygon and local
  mart rebuild targets also use SSD-local DuckDB extension directories. Local
  overlays are permitted in the working tree while distribution checks continue
  to validate the committed header shells.

### Fixed

- Polygon V2 normalization now accounts for the exchange's post-settlement
  refund of unused active maker assets in mixed MINT/MERGE matches while still
  requiring exact passive-leg and received-asset conservation.
- Match-minute team names and home/away orientation now follow the latest
  international-results snapshot. The dedicated backfill refreshes that source
  and blocks publication unless all 104 FIFA-numbered games reconcile uniquely.
- Corrected FIFA match 31's scheduled kickoff to June 19 at 11:00 PM ET
  (`2026-06-20 03:00 UTC`).
- Strategy-facing WC2026 marts now use only each private source's latest
  ledger-declared complete snapshot. Repeated full snapshots no longer duplicate
  model grains or retain rows removed by a newer complete snapshot, and an empty
  latest payload now blocks readiness instead of inheriting older raw rows.
- Completed knockout outcomes now align to FIFA match IDs by the schedule's
  unique date and host city when bracket-slot fixtures do not yet contain team
  names.

### Removed

- Removed populated external/reference seeds and the reviewed Polygon
  resolution attestation from repository distributions. Existing seed paths
  are retained as header-only schema shells, and the attestation is
  operator-local.
- Removed external-publication framing from current project documentation. No
  repository command uploads local audit or export artifacts.

## [0.1.6] - 2026-07-17

### Added

- Manual live-readiness workflow for offline source contracts and a disposable
  live WC2026 cross-platform pipeline smoke. The smoke uses a bounded 24-hour
  odds window and no historical backfill without changing production defaults.
- Static Vercel deployment for the MkDocs documentation site at
  `https://data.oddsfox.io/`.
- `wc2026_marts.wc2026_knockout_match_hourly_odds`, keyed by published FIFA
  match number, with dense nullable raw team-advance closes from Polymarket and
  Kalshi for matches 73–102 and 104.
- OpenFootball WC2026 knockout fixture ingestion, permanent incremental
  platform match-hour facts, cross-provider coverage/data-quality relations,
  `wc2026_knockout_match_odds_full_pipeline`, and the stopped-by-default
  `wc2026_knockout_match_odds_hourly_schedule`.

### Changed

- Consolidated GitHub Actions into one offline runner capped at five minutes
  total; it runs lint, fast tests, saved HTTP contracts, dbt parse, and a
  strict documentation build. The exhaustive release gate remains local.
- Reorganized the documentation into operator-first getting-started, guide,
  reference, concept, and development sections.
- Replaced the oversized landing page with a compact responsive homepage, a
  consistent dark palette, rendered Mermaid diagrams, and
  permanent redirects for moved public documentation URLs.
- Refreshed the MkDocs site with the OddsFox website palette, locally hosted
  Inter and JetBrains Mono fonts, a fox favicon, responsive task navigation,
  and a logo-led technical homepage.
- Breaking: added neutral `wc2026_intermediate`, `wc2026_marts`, and
  `wc2026_observability` schemas plus source match facts. Existing local
  warehouses must be reset; no compatibility aliases or migration are provided.
- Kalshi WC2026 scope now includes `KXWCADVANCE`; Polymarket discovery includes
  `fifwc-` exact match events. Exact `soccer_team_to_advance` match markets
  bypass the progression-futures volume floor without changing existing marts.

### Removed

- Removed GitHub-hosted live ingestion. `make live-smoke` remains the
  operator-owned local readiness path.

### Fixed

- Combined WC2026 Dagster/dbt runs now preserve the exact Dagster model subset
  and leave indirect tests to dbt's `buildable` policy, preventing unselected
  model emissions and relationship tests against unbuilt staging relations.
- Documentation browser policy tests fulfill GitHub metadata requests locally,
  so rate limits cannot make otherwise offline render checks fail.

## [0.1.5] - 2026-07-11

### Added

- Kalshi WC2026 pipeline for winner, knockout stage-of-elimination, and group-winner
  markets (`kalshi/wc2026/...` Dagster assets, `kalshi_wc2026_*` DuckDB schemas,
  and public `kalshi_wc2026_marts` relations). Additive; Polymarket contracts are
  unchanged.

### Removed

- Unused CLOB authenticated-request path (`ClobAuth`, `CLOB_API_KEY` /
  `CLOB_API_SECRET` / `CLOB_API_PASSPHRASE`). Polymarket and Kalshi ingestion
  use unauthenticated public API endpoints only.
- `scripts/build_hosted_artifacts.py --refresh-command`; hosted artifact refreshes
  now use the fixed Dagster pipeline command or `--skip-refresh` only.

### Fixed

- Kalshi market-registry-refresh job now lands the `events` raw table (previously
  omitted, breaking standalone registry refreshes).
- Scoped `kalshi_wc2026_full_pipeline` dbt builds exclude `cross_domain` tests
  that reference Polymarket or international-results observability models outside
  the Kalshi ancestor closure (prevents missing-relation errors under
  `indirect_selection: buildable`). Kalshi jobs pass `dbt_exclude=tag:cross_domain`
  at dbt CLI time so schema tests pulled in via indirect selection are skipped too.
  Scoped builds also exclude `tag:polymarket` nodes outside the Kalshi ancestor
  closure.
- Scoped Polymarket market queries respect `active_polymarket_scope` through full
  query execution (midterms metadata backfill).
- DuckDB unit test isolation when local `.env` sets `DUCKDB_PATH`.
- Dagster dbt build syncs `DUCKDB_PATH` to the active warehouse path before
  `dbt build`.
- Scoped `pipeline_run_events` writes for US midterms 2026 ingestion runs.

### Changed

- Breaking: renamed the shared Dagster dbt asset/op and run-config key from
  `polymarket_wc2026_dbt` to `oddsfox_dbt`. Existing job names such as
  `polymarket_wc2026_dbt_build` are unchanged.
- Docs: midterms quickstart path, warehouse schemas, operations validation SQL,
  data-contract analyst caveats, and local `.env`/test isolation guidance.
- Breaking: renamed WC2026 knockout observability columns
  `raw_classified_markets_ge_5000` to `raw_classified_markets_ge_floor` and
  `minimum_raw_markets_ge_5000` to `minimum_raw_markets_ge_floor`.
- Added `international_results_wc2026_match_results_ingest`, raw
  `international_results_wc2026_raw.match_results`, and public
  `international_results_wc2026_matches` / `international_results_wc2026_team_status`
  marts from the `martj42/international_results` FIFA World Cup CSV slice.
- Breaking: public Polymarket WC2026 knockout marts now require extracted teams
  to match the FIFA World Cup 2026 fixture/result roster, removing non-team
  regional futures and non-participant teams such as Italy from public odds
  output while retaining live and historical real-team rows.
- WC2026 knockout classification now recognizes Polymarket elimination-framed
  Round of 16/32 questions (`Will % be eliminated in the Round of X of the World Cup?`),
  so `round_of_16` and `round_of_32` rows populate knockout marts when those markets
  cross the volume floor.
- Breaking: public WC2026 marts now expose only knockout-related Polymarket markets
  with reported `volume >= $5,000` USD. The public mart surface is
  `polymarket_wc2026_knockout_market_tokens`,
  `polymarket_wc2026_knockout_token_hourly_odds`,
  `polymarket_wc2026_knockout_markets`, and run observability.
- Breaking: removed the broad public WC2026 marts for all-token hourly odds, daily odds,
  token coverage, market coverage, market universe, and market-token universe.
  Operators should reset local DuckDB warehouses or drop old dbt schemas before rebuilding.
- Knockout public odds now normalize to the progression side: winner/reach markets
  use the Yes token, elimination-framed markets use the No token, and marts expose
  `market_direction` plus `source_outcome_label` for source-framing metadata.
- Default Gamma keyset discovery now uses `keyset_volume_min=5000`.
- `polymarket_wc2026_hourly_odds_ingest` and `polymarket_wc2026_full_pipeline` now
  default odds sync to the trailing 30 days (`history_backfill_days=30`,
  `window_hours=720`, `min_volume=5000`).
- Breaking: removed minutely ingestion, minutely odds marts, minutely schedule
  flags, the standalone knockout Dagster job, and the unused Dagster odds repair
  asset. `polymarket_wc2026_dbt_build` and `polymarket_wc2026_full_pipeline` still build the knockout
  dbt marts.
- Breaking v0.1.x namespace reset: source/scope names now use source-first
  `polymarket_wc2026` instead of `wc2026_polymarket`. Dagster asset keys are
  hierarchical under `polymarket/wc2026/...`; jobs, schedules, op config keys,
  env vars, scripts, DuckDB/dbt schemas, and marts use flat
  `polymarket_wc2026_*` names. Delete old local warehouses with
  `rm oddsfox.duckdb*` and rerun quickstart.
- Removed the remaining list-shaped Dagster scope config surface and fixed all
  orchestration ingestion/backfill/odds calls to the single `wc2026` market
  scope.
- `int_polymarket_wc2026_market_tokens` now materializes as a dbt table to avoid
  repeated high-fanout downstream view expansion.
- Breaking: `polymarket_wc2026_knockout_token_hourly_odds` is now a dbt view
  over the private incremental `int_polymarket_wc2026_token_hourly_odds` fact
  table, preserving public columns while keeping hourly prices incremental and
  market/team metadata current at query time. Operators with old local relation
  types should reset their DuckDB warehouse or drop the affected dbt schemas
  before rebuilding.
- Breaking: odds history run config renamed `rebuild_minutely` to
  `rebuild_history` and `minutely_backfill_days` to `history_backfill_days`.
  No aliases are provided.
- Polymarket scope helpers now load any slug-like scope present in
  `market_scopes.yml`; the packaged v0.1.x Dagster/dbt graph remains fixed to
  WC2026.
## [0.1.4] - 2026-07-03

### Added

- Live-current hourly odds mart and export option for graph-ready OddsGraph
  inputs.

### Fixed

- Hourly forced sync planning keeps ended-market grace filters instead of
  re-planning stale ended markets.

## [0.1.3] - 2026-07-03

### Fixed

- Python 3.10 CI coverage now exercises the dbt profiles fallback and
  `polymarket_wc2026_raw.markets` index creation branches, restoring the required
  100% coverage gate after the `v0.1.2` release.

## [0.1.2] - 2026-07-02

### Added

- Generic dbt test macros for grain uniqueness and price bounds (replacing
  duplicated singular tests).
- Regression tests ensuring dbt source `meta.dagster.asset_key` values match
  Dagster asset keys and that resolved dbt model deps wire to ingestion assets.
- Schedule mutual-exclusion guard when both minutely odds schedule env flags
  are enabled.
- `outcome_label` on selected-scope minutely, daily, and whale odds marts so
  analysts can interpret `outcome_index` without joining to `polymarket_wc2026_markets`.
- Companion markdown data spec written alongside selected-scope minutely odds
  parquet exports (`export_selected_minutely_odds.py`; use `--no-spec` to skip).
- Seven new Polymarket scope presets: `us-politics`, `geopolitics`, `crypto`,
  `economy`, `nba`, `nfl`, `champions-league`.

### Changed

- `PolymarketDagsterDbtTranslator` now honors `meta.dagster.asset_key` on dbt
  sources (with duplicate-source keys enabled) so the shared dbt asset waits for
  ingestion assets instead of running immediately after `polymarket_wc2026_raw_markets`.
- Removed tautological dbt tests on selected-scope minutely/daily/whale marts
  (`mart_matches_selected_scope`, redundant `no_duplicate_grain`, whale subset
  singular test) that scanned ~54M view rows and added ~10 minutes to local dbt
  builds; grain and reconciliation coverage remains on sources and upstream
  models.
- Breaking: `POLYMARKET_MARKET_SCOPE` replaced by CSV
  `POLYMARKET_WC2026_MARKET_SCOPES` (one or more preset names). dbt var
  `active_market_scope` replaced by `active_market_scopes` (list).
  `polymarket_wc2026_markets` grain is now `(scope_name, market_id)`. Dagster-run dbt
  passes `active_market_scopes` from env automatically. Warehouse reset
  recommended (`rm oddsfox.duckdb*`).
- Breaking v0.1.x warehouse and orchestration contract change: WC2026-specific
  marts, registry tables, env vars, scripts, assets, and jobs were replaced by
  generic selected-market-scope surfaces. WC2026 remains the default preset in
  `market_scopes.yml`; operators with old local DuckDB files should delete
  `oddsfox.duckdb*` and rerun quickstart.
- GitHub Actions CI now runs `integration-dagster` and `make coverage`
  alongside the existing lint, test, dbt, docs, and costguard gates.
- selected-scope full-keyset discovery now defaults `keyset_volume_min` to
  `POLYMARKET_WC2026_SCOPE_KEYSET_VOLUME_MIN` (10_000) for both dlt and markets sync
  entrypoints.
- dlt Dagster asset name aligned to `polymarket_wc2026_raw_markets` (matches deps and
  dbt sources).
- CLOB odds HTTP retries happen only in the app-level backoff loop (urllib3
  status retries disabled for the CLOB client).
- Settings consumers in `market_scope` predicates/scan and DuckDB connection
  read config lazily so `reload_all_settings_modules()` propagates without
  extra `importlib.reload` per module.
- Orchestration ops facade collapsed through `polymarket_wc2026_ops.py`.
- Orphan `market_tokens` cleanup now runs after metadata/token backfill instead
  of inside the dbt asset, keeping the shared dbt asset read-only against raw
  tables.
- DuckDB market storage internals split into query and mutation modules while
  preserving the `oddsfox_pipeline.storage.duckdb.markets` facade.
- Odds sync now exposes `default_odds_sync_runtime()` as the supported runtime
  factory for tests and injected callables.
- selected-scope keyset scan tag-closure queueing moved into a pure helper with
  the same strict scope gates and telemetry output.
- `int_polymarket_wc2026_token_universe` now materializes as a dbt table after
  profile-backed validation showed neutral-or-better build behavior.

### Fixed

- Circular import between `scope_sql` and `storage.duckdb._market_queries` that
  prevented `dagster dev` from loading definitions.
- Minutely odds sync no longer re-fetches full history for already-closed,
  fully-checked tokens on every run when `force=True`; only explicit rebuild
  (`rebuild_minutely` or `minutely_backfill_days`) reopens them.
- Pool worker exceptions now enqueue skip/state ledger updates instead of
  silently dropping tokens.
- Writer flush wraps odds + ledger upserts in one transaction (dlt stage load
  happens before `BEGIN`).
- Markets sync progress guardrail now calls `.check()` during discovery.
- dbt build raises on non-zero process exit code after stream completion.
- Due-token count queries apply consistent volume/ended-market filters.
- `market_tokens` backfill and sync share one dlt-batch write path.
- Multi-statement DuckDB writes wrapped in transactions (`save_event_slugs_batch`,
  `delete_orphan_market_tokens`, `refresh_token_odds_daily`).
- Latest sync metrics now surface `pipeline_run_event_append_failed` and
  `pipeline_run_event_append_error` when append-only run telemetry cannot land.

### Removed

- WC2026-specific public marts, dbt intermediates, Dagster asset/job names,
  env-var names, and operator scripts. No compatibility views, env aliases, or
  migration shims were added.
- Dead parallel odds planning/fetch module (`process.py`, `build_token_plans`,
  `set_status_hook`).
- Unused snapshot HTTP client/cache surface and degraded snapshot result helper.

## [0.1.1] - 2026-07-01

### Added

- Full WC2026 odds time-series marts: `polymarket_wc2026_token_minutely_odds` and
  `polymarket_wc2026_token_daily_odds` (dbt views).
- `scripts/prune_odds_history.py` and `make prune-odds-history` for raw
  minutely retention (default 365 days).
- MkDocs Material theme; architecture, data-contracts, community, and
  development docs.
- GitHub issue and PR templates; AGENTS.md and Ponytail Cursor rule.
- Shared transient HTTP retry helper (`http_retry.py`).

### Changed

- `wc2026_whale_minutely_odds` is now a filtered view over
  `polymarket_wc2026_token_minutely_odds`.
- dlt is the sole owner of `polymarket_wc2026_raw.markets` rows; snapshot upserts
  populate dlt metadata columns.
- Due-token SQL deduplicated; backfill progress and slug handling aligned with
  shared storage helpers.

### Fixed

- dlt-owned markets snapshot upserts in DuckDB (`idx_markets_id`, metadata
  columns).
- Backfill slug tuple order, empty scheduler snapshot returns, and post-save
  progress accounting.
- Daily `avg_price` float drift breaking OHLC dbt test.

### Removed

- `token_latest_odds` mart (use time-series marts; see
  `docs/reference/data-contracts.md`).
- Redundant `odds_history` indexes (~1.45 GiB legacy index footprint on
  upgrade).
- Dead `wc2026_event_tags` dbt var.

## [0.1.0] - 2026-06-30

### Added

- Local Python pipeline foundation for prediction-market data, initially
  focused on FIFA World Cup 2026 Polymarket markets and odds.
- Dagster orchestration with WC2026 ingest, minutely odds, and dbt refresh jobs.
- dlt landing for Polymarket Gamma markets into DuckDB raw schemas.
- Python odds sync engine with ledgers, retries, and token-level planning.
- dbt staging, intermediate, mart, and observability models for WC2026 scope.
- DuckDB warehouse bootstrap, ops schemas, and profiling utilities.
- MkDocs documentation site with CI `docs-check` validation.
- GitHub Actions CI: lint, tests, docs build, dbt parse, and dbt build.
- Schedules disabled by default; opt-in via `.env` for live ingestion.

[Unreleased]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.13...HEAD
[0.1.13]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.12...v0.1.13
[0.1.12]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.11...v0.1.12
[0.1.11]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.10...v0.1.11
[0.1.10]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.9...v0.1.10
[0.1.9]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.8...v0.1.9
[0.1.8]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/hypertrial/oddsfox-pipeline/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/hypertrial/oddsfox-pipeline/releases/tag/v0.1.1
[0.1.0]: https://github.com/hypertrial/oddsfox-pipeline/releases/tag/v0.1.0

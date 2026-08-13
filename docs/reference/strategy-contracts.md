# Strategy Contracts

Use this page when consuming private canonical snapshots (`oddsfox.raw.v1`) or
the strategy clean-data relation set under `wc2026.v1`. A **contract** is a
named guarantee; see [Terminology](terminology.md#guarantee). Ordinary mart
queries, and open-source integrator work should start
with [Data contracts](data-contracts.md) instead. `wc2026.v1` is not the
analytics mart contract; documented marts are the supported query API.

## Canonical raw snapshots

Private collectors do not write implementation-specific tables into this
warehouse. They publish one immutable directory per source and snapshot:

```text
.runtime/raw/<source>/<snapshot_id>/
  manifest.json
  <table>.parquet
```

The `oddsfox.raw.v1` manifest records the source and snapshot ID, UTC collection
time, collector Git SHA and container digest, credential-free upstream
revision/request provenance, predecessor snapshot, and each file's SHA-256,
Arrow schema fingerprint, row count, and byte size. Both `status` and
`completeness` must be `complete`.

Collectors publish payloads into a temporary directory and publish
`manifest.json` last. The pipeline refuses missing manifests or payloads,
unknown versions/tables, unregistered schemas, unsafe paths, duplicate IDs,
predecessor mismatches, timestamp regressions, hash/size/row/schema mismatches,
and credential-bearing provenance. A successful load appends the Parquet rows
and `wc2026_ops.raw_snapshot_ledger` record in one DuckDB transaction.
Raw rows remain append-only for auditability, but each private source publishes
a complete replacement snapshot: strategy-facing marts use only the latest
ledger-declared snapshot. Rows omitted from a newer complete snapshot therefore
do not leak forward from an older load.

Public tests use synthetic Parquet snapshots only. HTML, selectors, cached
pages, discretionary URLs, and real scrape fixtures are not part of this
repository.

## Strategy clean-data contract

`wc2026_marts.contract_metadata` publishes contract version `wc2026.v1` and a
fingerprint of the stable relation set. There are no legacy compatibility
views.

| Relation | Purpose |
| --- | --- |
| `fixtures`, `results`, `team_identities` | Official schedule, completed outcomes, and canonical team identity. |
| `team_ratings_current`, `team_ratings_history` | Current World scrape (`snapshot_scope = current`) and point-in-time national-team ratings. |
| `team_ratings_pre_match` | Match×team pre-match Elo from EloRatings `{year}_results.tsv` (`pre = post ∓ change`), all competitions on/after 2026-01-01. Not a freeze and not a calendar-day series. |
| `player_features`, `squad_player_features` | FIFAIndex features and official-squad matches. |
| `club_strength_current`, `club_strength_history`, `club_strength_snapshot` | Current and point-in-time club strength. |
| `base_camp_venues`, `travel_features` | Venue, base-camp, rest, distance, timezone, and altitude features. |
| `venue_markets` | Venue event/market identity, Polymarket `condition_id`, outcomes, and token IDs. |
| `price_liquidity_current`, `price_liquidity_history` | Current and historical token price/liquidity data. |
| `event_state_timing` | Optional point-in-time match event state. |
| `international_matches` | Public 2006+ scorelines, tournament taxonomy, shootouts, and goal-event counts. |
| `third_place_slot_assignments` | FIFA Annexe C knockout-slot mapping. |
| `source_provenance` | Canonical snapshot provenance. |

Operator CSV freezes from those marts (`make export-wc2026-elo-freezes`) write
`artifacts/wc2026_elo_exports/team_ratings_pre_kickoff.csv` (year-end
`snapshot_year = 2025` and `snapshot_scope = '2025'`, designated pre-WC2026 freeze
— not a recovered June-2026 `World.tsv` scrape) and
`team_ratings_latest_current.csv` (latest `team_ratings_current` World scrape).
Both CSVs use mart columns `rank, team_code, team_name, rating` plus export
metadata `freeze_label, as_of, snapshot_id, collected_at`.

## WC2026 stage-minute strategy inputs

Operators can build an immutable, untracked stage-market price input release
from the canonical minute snapshots and deterministic logical artifacts. First
run `make minute-odds-snapshot-rebuild` against an existing operator warehouse;
this validates and registers `CURRENT` without calling Gamma or CLOB, then
rebuilds the isolated minute mart and quality checks.

After producing clean deterministic `nodes.parquet` and `edges.parquet` with
the graph utility's proposition compiler and rule engine (no LLM inference),
run:

```bash
make stage-minute-input-release \
  GRAPH_NODES_PATH=/absolute/path/nodes.parquet \
  GRAPH_EDGES_PATH=/absolute/path/edges.parquet \
  GRAPH_REVISION=<40-character-clean-revision>
```

Release `1.0.0` contains token-minute OHLC, 576 outcome identities, 528 direct
stage implications, complete candidate coverage, schemas, provenance, and
checksums below ignored `artifacts/strategy-inputs/`. It has no forward-filled
prices, execution costs, order-book liquidity, fill assumptions, or strategy
returns; those belong in the private research/backtest consumer.

## WC2026 stage-execution evidence

The isolated `oddsfox.polymarket_wc2026.stage_execution.v1` release targets
historical PMXT books and trades for every close-qualified signal in the pinned
stage-minute report. Planning is offline and must precede acquisition:

```bash
make stage-execution-plan \
  STAGE_EXECUTION_MINUTE_RELEASE=/absolute/path/to/stage-minute/releases/1.0.0 \
  STAGE_EXECUTION_OHLC_REPORT=/absolute/path/to/ohlc-report
```

The planner coalesces only overlapping windows for the same token and rejects a
plan whose minimum book-plus-trade requests exceed
`STAGE_EXECUTION_REQUEST_BUDGET` (20,000 by default). The release target uses
the same arguments plus `PMXT_API_KEY`, resumes from ignored local state, and
atomically writes reconstructed L2 snapshots, levels, diagnostic trades, and
coverage below ignored `artifacts/strategy-inputs/`.

Source timestamps are PMXT exchange-time reconstruction timestamps in
milliseconds. Ingestion timestamps record the backfill, not historical feed
receipt latency. Completed empty books and zero-trade windows are retained as
negative evidence. Trades are diagnostic and cannot grant a simulated fill.

`team_ratings_pre_match` is a separate match×team reconstruction from
`eloratings__match_results` (collector `{year}_results.tsv`). It is not covered
by the freeze export. Query the mart directly after a snapshot that includes
`match_results.parquet`.

Completed group results align by date and canonical home/away team identity.
Knockout schedule rows contain bracket slots until participants resolve, so
completed knockout results use the schedule's unique `(match_date, host_city)`
key and retain the source's actual teams when deriving the winner.

Private FIFAIndex, Wikipedia squad, EloRatings, ClubElo, and match-event inputs
are optional for a documented mart build. The on-run-start contract macro creates
schema-correct empty raw tables when they are absent, so every public model
still builds. Missing optional inputs are surfaced as warnings and blocking
reasons rather than hidden. A ledger record alone is not availability: the
latest snapshot must contain canonical rows, and the source-availability model
publishes that latest payload's `row_count`.

`wc2026_observability.wc2026_strategy_input_readiness` evaluates required-source
availability, freshness, point-in-time interval integrity, and blocking reasons
per strategy. Strategy consumers must open DuckDB read-only and fail closed
unless the required contract version and readiness row both pass.

See [System overview](../concepts/system-overview.md) for repository roles and
[Integration](../concepts/integration.md) for the public-vs-strategy boundary.

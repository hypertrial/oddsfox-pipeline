# Strategy Contracts

Use this page when consuming Scraper reference bundles (`oddsfox.reference.v1`)
or the market strategy relation set under `wc2026.v1`. A **contract** is a
named guarantee; see [Terminology](terminology.md#guarantee). Ordinary mart
queries, and open-source integrator work should start
with [Data contracts](data-contracts.md) instead. `wc2026.v1` is not the
analytics mart contract; documented marts are the supported query API.

## Scraper reference bundles

Non-market collectors do not run in Pipeline. Scraper publishes one immutable
directory per reference bundle:

```text
.runtime/reference-bundles/<bundle_id>/
  manifest.json
  <reference-table>.parquet
  checksums.sha256
```

The `oddsfox.reference.v1` manifest records the bundle ID, Scraper Git SHA and
image digest, source revisions and licenses, predecessor bundle, and each
table's checksum, schema fingerprint, grain, row count, and date range.

Pipeline verifies the complete manifest and all checksums before loading any
table. Activation is transactional, replay-idempotent, rejects mutated bundle
IDs, unsupported schemas, missing tables, and duplicate keys, and preserves the
last known-good `oddsfox_reference` schema on failure. Pipeline contains no
source-specific parser, endpoint, or private raw-snapshot loader.

Public tests use synthetic reference bundles only. HTML, selectors, cached
pages, discretionary source URLs, and real scrape fixtures are not part of this
repository.

## Strategy clean-data contract

`wc2026_marts.contract_metadata` publishes contract version `wc2026.v1` and a
fingerprint of the stable relation set. There are no legacy compatibility
views.

| Relation | Purpose |
| --- | --- |
| `venue_markets` | Venue event/market identity, Polymarket `condition_id`, outcomes, and token IDs. |
| `price_liquidity_current`, `price_liquidity_history` | Current and historical token price/liquidity data. |
| `source_provenance` | Combined Scraper bundle and Pipeline market provenance. |
| `source_availability` | Combined reference-bundle and market-input availability. |
| `strategy_input_readiness` | Fail-closed strategy readiness and blocking reasons. |

These non-market tables and their export/publication workflows are owned by
OddsFox Scraper. Pipeline receives only their validated Parquet representations
inside `oddsfox.reference.v1` and does not expose source-specific export commands.

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

## Scraper-owned soccer features

The event-grain `oddsfox.scraper.soccer.pre_match_elo.v1` contract is produced
and published by `oddsfox-scraper`, then consumed directly by Trading. Pipeline
does not acquire its sources, resolve identities, calculate ratings, publish the
release, or act as an Elo compatibility layer.

Pipeline consumes non-market strategy references only through complete,
checksummed `oddsfox.reference.v1` bundles. Bundle activation is transactional,
idempotent, and preserves the last known-good reference schema on failure.

## WC2026 stage-execution evidence

The isolated `oddsfox.polymarket_wc2026.stage_execution.v1` release targets
historical PMXT books and trades for every close-qualified signal in the pinned
stage-minute report. Planning is offline and must precede acquisition:

```bash
make stage-execution-plan \
  STAGE_EXECUTION_MINUTE_RELEASE=/absolute/path/to/stage-minute/releases/1.0.0 \
  STAGE_EXECUTION_OHLC_REPORT=/absolute/path/to/ohlc-report
```

The default `archive-v2` planner coalesces only overlapping windows for the
same token, downloads each required public PMXT v2 hourly object once, and
budgets one historical seed request per token-hour. Seed requests reconstruct
the state at each UTC-hour boundary but are never published and never directly
grant a hypothetical fill. Set `STAGE_EXECUTION_SOURCE=api-range` only for the
older explicit range-query path. Both modes reject plans whose minimum API
requests exceed `STAGE_EXECUTION_REQUEST_BUDGET` (20,000 by default).

Release mode reserves every seed or range attempt against the same UTC-month
PMXT ledger used by other acquisition jobs, resumes from ignored local state,
deletes each temporary archive object after validation and processing, and
atomically writes reconstructed L2 snapshots, levels, diagnostic trades, and
coverage below ignored `artifacts/strategy-inputs/`.
Release mode checks the exact dataset version, unused output target, and a clean
pipeline Git tree—including untracked files—before making any paid request.

Archive receipt timestamps govern when reconstructed evidence becomes
available to a consumer. Source timestamps govern age and latency; ingestion
timestamps record this backfill and are not historical feed receipt latency.
Completed missing hourly objects, empty books, and zero-trade windows are
retained as verified negative evidence. Trades are diagnostic and cannot grant
a simulated fill. Upstream archive terms and attribution remain documented by
PMXT.

`wc2026_observability.wc2026_strategy_input_readiness` evaluates required-source
availability, freshness, reference-bundle validity, and blocking reasons per
strategy. Strategy consumers must open DuckDB read-only and fail closed
unless the required contract version and readiness row both pass.

See [System overview](../concepts/system-overview.md) for repository roles and
[Integration](../concepts/integration.md) for the public-vs-strategy boundary.

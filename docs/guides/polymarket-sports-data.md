# Polymarket sports catalog and displayed-book history

This standalone command acquires generic public market data into an
operator-controlled directory. It installs no service or schedule, uses no
paid PMXT API, and cannot submit orders. Existing graph, soccer and WC2026
contracts are unchanged. Captured data and cache remain local and gitignored.
A data root inside a Git repository must already be ignored; the command
refuses an unignored location rather than changing Git policy automatically.

## Commands

```bash
uv run python scripts/polymarket_sports_data.py --data-root data/polymarket_sports catalog-refresh
uv run python scripts/polymarket_sports_data.py --data-root data/polymarket_sports record --duration-seconds 7200
uv run python scripts/polymarket_sports_data.py --data-root data/polymarket_sports backfill --start-utc 2026-04-14T00:00:00Z --end-utc 2026-04-15T00:00:00Z --dry-run
uv run python scripts/polymarket_sports_data.py --data-root data/polymarket_sports backfill --start-utc 2026-04-14T00:00:00Z --end-utc 2026-04-15T00:00:00Z --max-files 1
uv run python scripts/polymarket_sports_data.py --data-root data/polymarket_sports build --start-utc 2026-04-14T00:00:00Z --end-utc 2026-04-15T00:00:00Z
uv run python scripts/polymarket_sports_data.py --data-root data/polymarket_sports verify
uv run python scripts/polymarket_sports_data.py --data-root data/polymarket_sports quote --source pmxt-v2 --token-id 123 --side BUY --quantity 10 --as-of 2026-04-14T12:00:00Z
```

The quote token is a placeholder; substitute an exact native catalog token.
Bounds are explicit UTC and half-open. No selected accounts or historical
period are built into collection. Recording requires an explicit positive
duration. Restart creates a new session, never reuses an old in-memory book.

Matching Make targets: `sports-data-catalog`, `sports-data-record`
(`DURATION_SECONDS`), `sports-data-backfill` (`START_UTC`, `END_UTC`, optional
`DRY_RUN=true`, `MAX_FILES`), `sports-data-build`, `sports-data-verify`, and
`sports-data-quote` (`SOURCE`, `TOKEN_ID`, `SIDE`, `QUANTITY`, `AS_OF`). Override
`SPORTS_DATA_ROOT` to select another local directory.

## Resource limits and recovery

Defaults: 100 GiB retained data/cache/temp, 10 GiB downloaded per invocation
including retries, and 50 GiB free-disk reserve. Global CLI options
`--retained-gib`, `--download-gib`, `--reserve-gib` configure these limits.
Exhaustion stops/checkpoints; it is not complete coverage. Retained evidence
is never automatically pruned. Failed work and partial downloads count too.

One writer lock protects a root. Atomic manifests are the reader boundary;
unlisted segments are incomplete work, not automatically admissible evidence.
SQL preparation uses a single worker and a 4 GB workspace limit, with bounded
temporary spill space charged to the managed root. This is not a total-process
RSS promise; Arrow batches, catalog identities and sockets are measured separately.
`verify` validates hashes, counts, schemas and input pins and reports partial
and uncommitted files, including a non-success status when the first catalog
has not activated. Interrupted downloads resume only against consistent
object identity; changed upstream files fail without overwriting old evidence.

## Catalog contract

`oddsfox.polymarket.sports-catalog.v1` shares the existing global Gamma keyset
page implementation. Activation requires independent open/closed event and
market passes to reach natural completion. Failure leaves the previous
activation intact. Completion does not mean an atomic upstream snapshot or
recovery of deleted records.
The complete acquisition is checkpointed separately before materialization,
including source-pass completion, taxonomy and every observation-file checksum.
A materialization failure retains that checkpoint and the prior activation.

Classification uses explicit sports tags, source sports/series relationships
and sports-market fields, not titles or liquidity. Source-labelled esports,
props, periods and futures are included. Ambiguities and orphan markets remain
in raw observations and coverage. Catalog retention and book admission are
separate; identity conflicts quarantine only affected markets.
Prior sports evidence keeps a market in the cumulative catalog. Recording
admission uses its last-observed direct sports evidence or current membership;
an explicitly removed classification is retained as historical-only evidence.

Manifests bind these relations:

| Relation | Grain and meaning |
| --- | --- |
| `observation_files` | Source entity/membership observation; receipt, endpoint, sequence, payload hash and raw fields |
| `relations` | Latest event/market state; raw and normalized metadata, classification and missing reasons |
| `outcomes` | Market/condition/native token/outcome index and label; admission status |
| `memberships` | Source-observed event/market association |
| `identity_scope` | All observed market/condition identities with sports classification |

Taxonomy, pass completion, activation time and previous-manifest checksum are
pinned. Native endpoint records outrank nested summaries. Explicit null and
absence remain distinguishable; old non-null values are not carried forward.
Missing records do not imply cancellation. Untrusted source prose is retained,
not executed; external resolution URLs are never fetched.

`startDate` remains ambiguous source metadata, not an asserted listing or
contest start. `createdAt`, `gameStartTime` and `eventStartTime` stay separate;
listing time remains unknown unless explicitly supplied. Group labels are
not inferred participants or subjects. Missing participants, period, line or
collateral mechanics stay unknown. Normalized broad types retain raw types
alongside them.
Source market `version` remains source market metadata; it is not relabeled as
a condition-contract or collateral version. Archive versions/dates never
establish collateral versions.

`observed_as_of` uses actual receipt availability, not source `updatedAt`.
Newly retrieved metadata is never backdated into archive periods. Recording
requires activation; open membership becomes due every five minutes without
overlap, full reconciliation becomes due every 24 hours, and lifecycle messages
request targeted refreshes. A global crawl may take longer than its target
interval; overdue work starts immediately and its delay is reported rather than
hidden. Lifecycle notifications are not an exhaustive listing feed. Refresh
ages survive recorder restarts and are measured from source-pass completion,
not later catalog activation.

## Order-book contract

`oddsfox.polymarket.sports-order-books.v1` stores compressed immutable raw and
normalized Parquet segments by UTC date/hour and source/session, not per token.
Raw bytes retain subscription, heartbeat, ancillary, reconnect and loss events.
Normalized rows retain native token/condition, source and receipt time,
receive sequence, item ordinal, exact depth and unverified provider hashes.

Subscribe to every admitted native token, including temporarily non-accepting
markets. Keep condition outcomes together; 500 tokens per connection is an
operational default, not an asserted provider maximum. Report all pending and
initialized subscriptions. Never derive a NO quote as one minus YES.
The terminal session distinguishes tokens that were recorded, pending,
unavailable or failed and preserves per-token exception classes for failures.
Pending or failed coverage makes the record command exit nonzero even though
completed evidence remains committed and resumable.
Transport, source-data, overflow and unexpected-internal exception categories
remain separate. Sanitized exception messages and local stack-frame identities
are retained with boundary evidence; any internal exception produces an
explicit non-success session status even when reconnect later succeeds.

Full snapshots replace depth. Deltas set quantity; zero removes a level.
Receive order is authoritative; equal timestamps are not duplicates. Sort
normalized levels, reject duplicate snapshot prices, and never initialize from
a delta. Disconnect, restart, overflow or parsing uncertainty invalidate state
until another authoritative snapshot. REST depth is never spliced into a live
delta stream. Empty, one-sided and crossed books remain explicit observations.
One-sided, crossed and uninitialized books cannot quote; the recorder never
invents the absent side. Quiet markets alone are not gaps.
Observed market resolution invalidates both named native outcomes immediately;
it is not treated as an ordinary price update. Initial and changed tick sizes
remain explicit in replayed states.

Input values fit DECIMAL(38,18) exactly; derived arithmetic uses wider fixed
precision where required. Dimensionless imbalance may use floating point;
money and quantities do not. `build` emits BBO, spread, midpoint, depth,
displayed imbalance and changes between valid consecutive states. No changes
are computed across resets. Replenishment means displayed liquidity change,
not proven maker action. No universal exchange-sequence guarantee is claimed.

## Free archives and quotes

`backfill` first inventories every requested hour from paginated listings,
including estimated sizes and declared schemas. Dry-run downloads no Parquet.
Missing, empty, unavailable, unsupported and failed objects are distinct;
none implies zero activity. Bounded ranges, TLS, upstream identity and SHA-256
protect download/resume. Schema drift fails that file without replacing prior
committed output.

Native v1 JSON envelopes and v2 typed columns are parsed in bounded batches.
Exact catalog identities filter rows; non-sports, unknown, malformed, ancillary
and uncertain-order counts reconcile to source rows. Exclusion evidence pins
file/row-group/exact contiguous row spans. Provider receipt and local
download time stay distinct. Archive versions and local recording remain
independent. Only adjacent committed non-empty files of one archive version
share observed continuity; a missing, empty or failed hour breaks the chain.
Conflicting tied updates invalidate the affected book until a later snapshot.
The `non_sports_rows` count means known catalog conditions without observed
sports classification; it is not proof that missing upstream sports tags were
complete. Unclassified catalog observations remain retained for later review.

[PMXT's specification](https://archive.pmxt.dev/docs/v2-data-overview) documents
v2 beginning April 13, 2026 and material missing-market coverage in v1. This
cannot recover complete earlier history. Local bundles preserve PMXT's
source attribution, source/licence links and transformation notice. The
authoritative licence statement is in `THIRD_PARTY_NOTICES.md` at the repository root.

Quotes walk actual asks/bids and report requested/available quantity,
gross cost/proceeds, average/marginal price, partial depth, state identity,
age and quality. Pair amounts require verified complementary identities and
known collateral mechanics; missing mechanics produce unavailable results.
The low-level pair calculation requires an explicitly verified two-token set
and collateral-per-pair amount. The CLI does not infer these mechanics from
dates or archive versions and reports them unavailable when not proven.
When those inputs are supplied, it reports the opposite acquisition cost,
gross merge proceeds and net synthetic-exit proceeds separately so acquisition
cost is never mislabeled as gross proceeds.
These amounts exclude fees, latency, queues and transaction costs and are
not fill guarantees or recommendations.

## Local acceptance

Run `make integration-sports-data`, `make test-dev`, `make check-terminology`,
`make check-repository` and replay-only `make contract-http` locally. Before
push: `uv run make ci-fast`. Operational acceptance additionally requires a
two-hour all-sports recording soak with restart, both archive-format samples,
full requested-period inventory and bounded-memory replay at twice measured
live peak. Passing fixtures alone does not establish live acceptance.

After recording, `make sports-data-scale` reprocesses the measured busiest
receipt seconds by message count and bytes through exact normalization and both
raw and normalized Parquet writes. Each case requires at least twice its
observed rate, no parse losses, and at most 512 MiB additional
peak resident memory. Its local report pins the raw manifests. This benchmark
does not replace the live transport/reconnect soak or prove exchange continuity.

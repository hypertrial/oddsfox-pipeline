# Build the WC2026 logical atlas

Use this runbook to produce the static `polymarket-wc2026-logical-v1` bundle
consumed by `oddsfox-graph`, build an `oddsfox-graph` release, and switch the
local dashboard atomically. The logical bundle intentionally contains no odds. Raw
hourly odds collection is an independent temporal-foundation branch for a
future dashboard version.

This page owns **logical atlas**, **logical marts**, **logical bundle**,
**atlas node**, **membership class**, and the Pipeline↔`oddsfox-graph`
handoff. Global vocabulary stays in [Terminology](../reference/terminology.md).

## Logical atlas vocabulary

| Term | Meaning |
| --- | --- |
| **Logical atlas** | Reviewed static WC2026 inventory product (events through scopes). |
| **Logical marts** | The seven `polymarket_wc2026_logical_*` warehouse relations that feed export. |
| **Logical bundle** | Versioned seven-Parquet + `manifest.json` package `polymarket-wc2026-logical-v1`. |
| **Atlas node** | One row in the atlas hierarchy (tournament, stage, group, fixture, or award). Physical contract keeps relation `logical_scopes` / `polymarket_wc2026_logical_scopes` and column `scope_id`; those identifiers are frozen and are not the product **scope** term. |
| **Membership class** | Review taxonomy for event admission (`sporting`, adjacent classes, and so on). Prefer this over the retired `scope class` label. |
| **oddsfox-graph** | External product that builds releases from the logical bundle; not part of this repository’s ontology. |

## Contract and admission policy

Admission is decided at the Polymarket **event** grain:

- The source field is cumulative `event.volume` and the boundary is
  `>= 100000` USD.
- Eligibility is sticky. Once an event is observed at the boundary, it remains
  eligible even if a later source correction reports less volume.
- Eligibility is effective from the event creation time. The bundle includes
  every child market in each event's latest complete catalog observation; raw
  membership history remains append-only, but corrected or removed links are
  not served as current relationships.
- An included event that crossed the volume boundary but has no source
  creation timestamp fails dbt validation and bundle publication. It is never
  silently reassigned to its first observation time or omitted from review.
- There is no child-market volume floor. A zero-volume child of an admitted
  event is still part of the inventory.
- Five discovery sources cover open and closed events: exact 2026 tag, related
  2026-tag recall, broad FIFA World Cup tag, `soccer-fifwc` series, and WC2026
  event-slug-prefix recall. All ten scan partitions must converge before
  publication.
- Exact fixture mappings are admitted automatically. Every other eligible
  candidate requires an explicit operator-reviewed inclusion or exclusion.
  The tracked `dbt/seeds/polymarket_wc2026_event_membership_overrides.csv` is a
  header-only schema shell and must not contain review decisions.

Malformed or tokenless child markets remain in `markets.parquet` with
`logical_usable = false` and an explicit `quarantine_reason`. They are auditable,
but they do not produce unsupported proposition claims.

The versioned bundle contains exactly these seven Parquet files plus
`manifest.json`:

| File | Grain |
| --- | --- |
| `events.parquet` | One row per reviewed logical event, plus required audit context |
| `markets.parquet` | One row per current child market of an admitted event |
| `market_events.parquet` | One row per current market/event membership, with one deterministic primary event per market |
| `propositions.parquet` | One row per parseable source outcome |
| `entities.parquet` | One row per canonical team, player, fixture, group, stage, award, or tournament entity |
| `proposition_entities.parquet` | One row per proposition/entity/role mapping |
| `scopes.parquet` | One row per atlas node (tournament, stage, group, fixture, or award); physical `scope_id` retained |

The manifest pins the contract and taxonomy versions, producer Git SHA,
threshold policy, scan-partition inventories, every input hash, every file
hash and row count, data-quality results, and topology, semantic, and source
snapshot fingerprints. The exporter validates exact column order, nullable
physical Parquet types, grain, referential integrity, one-primary-event
membership, and scan stability before replacing the output directory.
Negative reported event or market volume also fails publication; invalid source
values are normalized to null and remain distinguishable from zero.

## Build the Pipeline bundle

Run the focused job from a clean Pipeline checkout:

```bash
export DUCKDB_PATH="$PWD/.runtime/warehouse/oddsfox.duckdb"
export ODDSFOX_WC2026_REVIEWED_MEMBERSHIP_PATH="/absolute/path/to/reviewed-membership.csv"
uv run python -m dagster job execute \
  -m oddsfox_pipeline.orchestration.definitions \
  -j polymarket_wc2026_logical_atlas
```

This job refreshes the event catalog, builds `+tag:wc2026_logical_atlas`, runs
its dbt tests, and validates the seven logical marts. It does **not** fetch
hourly odds, Polygon data, or order books.

Export those marts atomically:

```bash
uv run python scripts/export_polymarket_wc2026_logical_bundle.py \
  --duckdb-path "$DUCKDB_PATH" \
  --output-dir .runtime/exports/polymarket-wc2026-logical-v1
```

The exporter refuses an existing output directory, an unstable catalog scan,
a dirty producer checkout, missing or empty marts, and contract or quality
violations. Use a new output path instead of modifying a published bundle.
The manifest's semantic inputs include the complete 104-row staged fixture
relation. The parser pins OpenFootball `2026--usa/cup.txt` and
`2026--usa/cup_finals.txt`, validates their exact file hashes, binds the 72
group fixtures to reviewed source-slice hashes and official FIFA match IDs,
and records the pinned FIFA schedule document. A fixture-source change therefore
produces a different input fingerprint. The raw/staging relation is
`schedule_fixtures` and contains matches 1–104.
Child-market metadata is fingerprinted from
`stg_polymarket_wc2026_event_market_payload_latest`, backed by append-only
event-catalog payload snapshots. The logical producer never side-loads the
dlt-owned `markets` table.

For an offline consumer check, materialize the pinned synthetic fixture:

```bash
uv run python scripts/materialize_polymarket_wc2026_logical_fixture.py \
  --output-dir /tmp/oddsfox-logical-v1-fixture
```

The default lock check verifies the fixture source, seven Parquet schemas and
hashes, row counts, and the expected topology and semantic fingerprints.

## Review newly eligible events

The dbt build fails closed when an ever-eligible non-fixture event has no
review decision. Inspect the candidates left by the failed build:

```sql
select
    event_id,
    event_title,
    event_slug,
    event_volume_usd_lifetime_reported,
    membership_class,
    membership_basis
from polymarket_wc2026_intermediate.int_polymarket_wc2026_event_membership
where ever_eligible and membership_status = 'review_required'
order by event_volume_usd_lifetime_reported desc nulls last, event_id;
```

For each candidate, inspect its resolution terms and all child markets. Add one
row to the operator-local CSV named by
`ODDSFOX_WC2026_REVIEWED_MEMBERSHIP_PATH`, with an `included` or `excluded`
decision, membership class, tournament part, concise reason, non-placeholder
reviewer, and timezone-qualified review time. Included rows must be sporting
and use one of the locked final-tournament parts. Do not edit the tracked
header-only seed, and do not admit entertainment, political attendance,
qualification, squad-selection, administrative, or other adjacent markets as
final-tournament sporting outcomes.

Then run:

```bash
uv run make dbt-unit
uv run python -m dbt.cli.main build \
  --project-dir dbt \
  --profiles-dir dbt/profiles \
  --select +tag:wc2026_logical_atlas \
  --exclude tag:polygon_settlement tag:pmxt_order_book
```

Publication remains blocked until the review-completeness test is empty.

## Preserve the temporal foundation

The normal `polymarket_wc2026_full_pipeline` owns the independent legacy
raw-odds branch. The logical event catalog does not register its children in
that legacy market scope registry, and a routine
`polymarket_wc2026_market_scope_registry_refresh` does not run the five-source
logical event-catalog crawl. Supporting temporal odds for every atlas
proposition will require a dedicated observation producer joined through
proposition/token identity.

`polymarket_wc2026_raw.odds_history` is append-only at
`(clobTokenId, timestamp)`: a replay cannot overwrite a previously observed
point. The existing 30-day hourly dbt marts remain presentation windows, not
the raw-retention policy.

The reserved future graph-observation grain is
`(logical_proposition_id, clob_token_id, observed_at)`.
It keeps logical identity, source-token identity, and observation time explicit;
the current static logical-v1 bundle does not publish this relation.

Do not prune this raw history during the tournament or its 90-day review
window. `scripts/prune_odds_history.py` permanently exempts observations from
2026-06-11 00:00:00 through 2026-10-18 23:59:59 UTC, inclusive, from deletion;
`--dry-run` remains available. There is no protected-window override.

## Build and publish a complete release

The release builder runs the focused Pipeline refresh, exports logical-v1,
invokes `oddsfox-graph`, validates both manifests, all content hashes, and the
Graph-owned deterministic acceptance suite. Every build is a shadow release;
building never switches the `current` symlink:

```bash
export ODDSFOX_DATA_DIR="${ODDSFOX_DATA_DIR:-.runtime}"
uv run python scripts/build_hosted_artifacts.py \
  --artifact-dir "$ODDSFOX_DATA_DIR/artifacts" \
  --duckdb-path "$DUCKDB_PATH" \
  --graph-repo ../oddsfox-graph
```

Inspect the shadow release, validate it, and run Graph's browser smoke against
that exact candidate. Keep the receipt outside the immutable release tree:

```bash
(cd ../oddsfox-graph && uv sync --extra browser)
../oddsfox-graph/.venv/bin/python -m playwright install chromium

export RELEASE_ID=REPLACE_ME
export RELEASE_DIR="$ODDSFOX_DATA_DIR/artifacts/releases/$RELEASE_ID"
export BROWSER_RECEIPT="$ODDSFOX_DATA_DIR/artifacts/browser-smoke-receipts/$RELEASE_ID.json"

uv run python scripts/build_hosted_artifacts.py \
  --artifact-dir "$ODDSFOX_DATA_DIR/artifacts" \
  --graph-repo ../oddsfox-graph \
  --validate-release "$RELEASE_ID"

mkdir -p "$(dirname "$BROWSER_RECEIPT")"
../oddsfox-graph/.venv/bin/python -m oddsfox_graph.cli \
  atlas-browser-smoke \
  --graph-dir "$RELEASE_DIR/graph" \
  --receipt "$BROWSER_RECEIPT" \
  --output-format json

uv run python scripts/build_hosted_artifacts.py \
  --artifact-dir "$ODDSFOX_DATA_DIR/artifacts" \
  --graph-repo ../oddsfox-graph \
  --activate-release "$RELEASE_ID"
```

Activation re-runs deterministic Graph acceptance and validates the
manifest-bound browser receipt immediately before a locked atomic symlink
replacement. Missing, stale, malformed, or failed receipts block activation.
The prior target is retained as `previous`; activating that release ID performs
a rollback after full validation. A legacy pre-atlas release is content-sealed
automatically before it becomes `previous`. Its rollback revalidates that seal
but skips atlas-only acceptance and browser-receipt gates that the legacy format
cannot satisfy; unavailable historical Git SHAs remain null rather than being
invented.

## Breaking cutover from the retired hourly export

`polymarket-wc2026-logical-v1` replaces the retired one-file hourly export.
Do not mix those schemas, manifests, Graph databases, or release
directories.

For a deployment created with Pipeline 0.1.12 or earlier:

1. Stop Dagster and every DuckDB writer.
2. Preserve the existing database and current graph release as read-only
   rollback material.
3. Create a fresh warehouse for the first logical-v1 build. The additive raw
   table compatibility path is not a promise that a complete legacy warehouse
   is a supported in-place graph migration.
4. Build a shadow logical-v1 release and compare its coverage report and
   manifest counts.
5. Activate only after Pipeline and Graph validation both pass.

Never delete the preserved warehouse or legacy release as part of the first
cutover. Remove them only under a separate, reviewed retention decision after
the rollback period ends.

# World Cup market portrait

The pipeline owns the public `oddsfox.market-portrait.v1` file contract and all
prediction-market semantics. Production bundles are private operator artifacts,
not repository inputs.

## Acquire an approved target

Build the normal World Cup market universe, then create a review candidate:

```bash
python scripts/generate_polymarket_wc2026_market_portrait_target.py \
  --fifa-match-id 95
```

Generation calls Gamma for fresh identities but does not consume PMXT credits.
Review the ignored YAML, then authorize the resumable book-and-trade scan:

```bash
make market-portrait-live-backfill \
  TARGET_MANIFEST=/absolute/path/to/match-95.yml
```

The manifest must resolve exactly three literal `Yes` tokens for a group match,
or the named home and away tokens of one advance/win market for a knockout
match. Ambiguity, changed Gamma identity, unfinished adaptive windows, a
total-zero trade result, invalid decimal values, or conflicting provider order
blocks publication. Books and trades share the UTC-month PMXT credit counter.

## Build a bundle

`build_market_portrait_bundle` accepts a read-only DuckDB connection, a FIFA
match ID, sanitized `MatchFacts`, ordered sanitized `FootballEvent` values, an
output root, and a `RenderProfile`. It never imports collector code, names a
private upstream provider, or accepts raw provider relations. The scraper is
responsible for sanitizing and mapping its private sidecar to these neutral
types.

The output directory contains:

- `manifest.json`
- `book_states.ndjson.gz`
- `trades.ndjson.gz`
- `story.json`

JSON keys and stream order are stable, gzip uses `mtime=0`, and the bundle ID is
derived from content. A byte-identical rerun is a verified no-op. Existing
content at the same immutable path is never overwritten.
The manifest records book and trade aggregate scan hashes alongside each file
hash and record count.

Timeline mapping requires actual start and end timestamps for every played
period. `MatchFacts.kickoff_at_utc` and every period boundary must be
timezone-aware and sanitized. Before story construction, export verifies the
kickoff against the validated match universe and verifies that each required
role's published root scan window strictly contains the complete football
timeline. Scheduled kickoff is a validation anchor and is never substituted
for missing actual boundaries. Event reactions are explicitly labelled
`minute-aligned`; shootout events receive annotations but no reaction metric.

## Recovery and retention

Interrupted PMXT work resumes from terminal window leaves. Preserve a completed
warehouse until the bundle is verified. For this pre-1.0 release, do not migrate
old warehouse layouts: preserve the old database separately, rebuild a clean
warehouse, and reacquire only operator-approved targets.

Keep source bundles inside the scraper private export root. Copy an MP4 out only
after the operator completes rights and provenance review.

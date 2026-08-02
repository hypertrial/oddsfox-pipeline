# World Cup market portrait

For market portraits, this public repository owns the PMXT acquisition,
prediction-market semantics, provider-neutral story construction, and the
`oddsfox.market-portrait.v1` file contract. Production bundles are private
operator artifacts, not repository inputs.

Private collection, source-native schemas, sanitation implementation, and
rendering do not belong here. The public API accepts only neutral, sanitized
facts; it must not import a private collector package, name a private source,
query a private source relation, or contain plotting and video-rendering
implementation.

## Acquire an approved target

Build the normal World Cup market working set, then create a review candidate:

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
private upstream provider, or accepts raw provider relations. The private
adapter is responsible for sanitizing and mapping source facts to these neutral
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
kickoff against the validated match working set and verifies that each required
role's published root scan window strictly contains the complete football
timeline. Scheduled kickoff is a validation anchor and is never substituted
for missing actual boundaries. The declared deterministic sanitizer can move
equal raw timestamps independently by one microsecond, so `match_ended_at` may
precede the final period boundary by at most two microseconds; any larger
inversion blocks publication.

Each played period is tiled with half-open UTC bands `[S, E)`. Regular bands
are exactly 60 seconds; only the last band is clamped to the remaining positive
period duration so that it ends at the actual boundary. The one-millisecond
tolerance prevents a timestamp micro-epsilon from creating another band.
Missing stoppage labels are inferred as
`ceil(max(0, actual duration - nominal duration - 1 ms) / 60 seconds)`, then
combined with any explicit event stoppage by taking the greater count. An event
whose labelled band has no positive source duration blocks publication.

Sanitized event scores are post-event facts. The builder derives display scores
in football-timeline order, accepts only non-revoked, non-shootout `Goal`,
`Own goal`, or `Penalty scored` events as scoring transitions, and requires
each transition to add exactly one goal to one team. Non-scoring annotations
receive the derived chronological score rather than trusting a possibly stale
source score. A score checkpoint becomes effective at its event band's end,
and the derived terminal score must agree with `MatchFacts` when supplied.

Event reactions are explicitly labelled `minute-aligned`. For an event band
`[S, E)`, `before` is the last observation strictly before `S`, and primary
`after` is the first observation at or after `E`. Extended `after` uses the
following band end. Observations cannot cross a halftime or extra-time break;
missing qualifying observations are serialized as null. Shootout events
receive annotations but no reaction metric. Because market observations use
integer milliseconds while sanitized football boundaries may retain
microseconds, both the bisect threshold for `< S` and the lower bound for
`>= E` use the ceiling millisecond; the same-period upper bound uses the floor
millisecond. These directional rules preserve the real datetime predicates
rather than truncating them. Producer validation checks the derived band
tiling, annotation mapping, score checkpoints, reaction event-role inventory,
bounds, and observation predicates before publication.

The default story begins at the actual first-half boundary with elapsed minute
zero. Every football band has equal video weight. Regulation flows continuously
for 45 seconds; extra time extends the story to 60 seconds and a shootout adds
one five-second `PENS` phase. Pre-match, halftime, and post-match remain
zero-valued render defaults and are not emitted as timeline segments. The
source clock jumps over each validated period break rather than interpolating
through it.

## Recovery and retention

Interrupted PMXT work resumes from terminal window leaves. Preserve a completed
warehouse until the bundle is verified. For this pre-1.0 release, do not migrate
old warehouse layouts: preserve the old database separately, rebuild a clean
warehouse, and reacquire only operator-approved targets.

Keep source bundles inside the caller-managed private export root. Copy an MP4
out only after the operator completes rights and provenance review.

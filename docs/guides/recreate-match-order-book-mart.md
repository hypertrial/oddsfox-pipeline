# Recreate the PMXT order-book mart

Build `polymarket_wc2026_marts.polymarket_wc2026_match_order_book` with the
dedicated, unscheduled PMXT backfill. Complete
[shared setup](recreate-local-marts.md#shared-setup-every-route) first.

## Before you run

You need a PMXT API key and authorization to acquire and use the returned data.
The default configuration reserves at most 20,000 locally counted attempts per
UTC month and paces at 50 requests per minute. Retries count again before each
HTTP attempt. PMXT provider limits and terms remain independently governed.

The initial reviewed target is the Spain–Argentina FIFA final (match 104)
team-to-advance market and both outcome tokens. This path is not part of
ordinary WC2026 ingestion or any schedule.

Copy `.env.example` to `.env` and set:

```dotenv
PMXT_API_KEY=...
```

The key is sent only as a Bearer header. Do not place it in Dagster run config,
logs, issue reports, or committed files.

## Run the resumable live acceptance

```bash
uv run make match-order-book-live-smoke
```

This command explicitly warns that it consumes PMXT credits and uses
`.cache/match_order_book_live_smoke.duckdb`. It preserves that warehouse by
default so paused or interrupted scans resume outstanding adaptive windows.
To intentionally discard the disposable scan and start again:

```bash
MATCH_ORDER_BOOK_LIVE_SMOKE_RESET=true \
  uv run make match-order-book-live-smoke
```

The backfill performs one exact Gamma identity check before PMXT calls. It then
splits every saturated 1,000-snapshot range until each terminal range is
demonstrably short, lands bounded batches through dlt, checkpoints completed
windows, publishes only after both token trees are complete, and builds the
isolated `+tag:pmxt_order_book` dbt graph with `tag:match_minute` excluded.

## Verify the result

```sql
select
    count(*) as levels,
    count(distinct clob_token_id) as tokens,
    min(snapshot_at_utc) as first_snapshot,
    max(snapshot_at_utc) as last_snapshot
from polymarket_wc2026_marts.polymarket_wc2026_match_order_book;

select *
from polymarket_wc2026_observability
    .polymarket_wc2026_match_order_book_data_quality;
```

Expect one match, one market, two tokens, at least one level, and zero blocking
errors. Empty books, crossed books, and large snapshot gaps are warnings and do
not create synthetic rows.

Run the same Make target again. A compatible published manifest must complete
without Gamma or PMXT calls and leave raw hashes and mart rows unchanged.

## Pause and recovery

- Local monthly budget or repeated upstream quota exhaustion marks the scan
  `paused`; completed dlt loads and window checkpoints remain available.
- A persistent PMXT 5xx response fails the asset without publishing or
  discarding checkpoints. Re-run the same target after the hosted historical
  service recovers.
- Re-run after quota capacity is available. The expired lease may be taken over
  and only pending windows are requested.
- Authentication, identity, response-schema, timestamp, or level-validation
  failures mark the scan failed and block dbt. Correct the cause and rerun; do
  not edit raw or ops rows manually.
- `force=true` is an expert Dagster run-config option that creates a separate
  scan. It does not overwrite the previous published scan. Do not use
  it merely to resume.

Downloaded snapshots, DuckDB/dlt state, and exports are operator-local and
ignored by Git. See
[Data contracts](../reference/data-contracts.md#documented-marts)
for the exact grain and publication guarantees.

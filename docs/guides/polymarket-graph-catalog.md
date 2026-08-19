# Global Polymarket graph catalog

The global catalog is a manual pipeline for building a cumulative, textual
inventory of Polymarket events, tradable markets, and their memberships. It is
separate from the WC2026 and soccer scopes and has no acquisition or release
schedule.

## Completeness boundary

A complete crawl reaches natural keyset completion for four independent Gamma
passes: open events, closed events, open markets, and closed markets. The mart
retains every qualifying record exposed by a completed crawl from the first
successful crawl onward. Records deleted before that first crawl cannot be
recovered or claimed.

A market qualifies only when an observation contains durable evidence that it
was tradable: CLOB token IDs, order-book enablement, accepting-orders time,
funding time, or a deployed condition ID paired with `ready=true` or
`funded=true`. Volume, active state, and a bare condition ID do not qualify it.

## Run the pipeline

Use an operator-local DuckDB path. The refresh command acquires all four passes,
activates the crawl only after they complete, and builds the mart:

```bash
make polymarket-catalog-refresh DUCKDB_NAME=/absolute/path/catalog.duckdb
```

Rebuild the mart without network access:

```bash
make polymarket-catalog-dbt-build DUCKDB_NAME=/absolute/path/catalog.duckdb
```

Publish an immutable SemVer release without network access:

```bash
make polymarket-catalog-release \
  DUCKDB_NAME=/absolute/path/catalog.duckdb \
  RELEASE_VERSION=1.0.0
```

The release command requires a clean Git working tree so its Pipeline commit
is reproducible. The default ignored release root is
`artifacts/polymarket_catalog/releases/`; set
`POLYMARKET_CATALOG_RELEASE_ROOT` to use another operator-local location.

## Failure and recovery

Page checkpoints and incomplete attempts remain audit state. A failed or
truncated crawl does not write active observations and cannot change the mart.
Rerun the refresh: completed passes resume from their checkpoints; a pass whose
cursor can no longer be continued is restarted within the same crawl attempt.
Observations are never combined across crawl IDs.

If Gamma returns conflicting non-null identities or malformed identifiers, the
crawl fails instead of guessing. Correct the source or implementation issue and
rerun. The last completed catalog remains queryable.

## Mart and release

`polymarket_catalog_marts.polymarket_graph_catalog` has one row per namespaced
`record_id` and exactly three record types:

- `event:<event_id>` for included event nodes;
- `market:<market_id>` for qualifying market nodes;
- `event_market:<event_id>:<market_id>` for membership edges.

Events require at least one retained edge; qualifying orphan markets remain.
Rows expose raw source text, stable JSON attributes, deterministic labeled
`content_text`, content-text SHA-256, latest state, and cumulative observation
provenance. Derived text strips unsafe controls but preserves meaningful
Markdown, URLs, and full source length. It is data, never instructions.

Each release contains:

```text
polymarket_graph_catalog.parquet
manifest.json
schema.json
quality_report.json
checksums.sha256
```

Publication sorts rows deterministically, writes into a temporary directory,
validates the entire contract, and atomically installs the version. Existing
versions are never overwritten. The catalog does not collect prices, order
books, trades, comments, chats, profiles, or non-Polymarket enrichment.

# Test Fixture Provenance

Files in this directory are synthetic, Hypertrial-authored inputs created only
to exercise deterministic parsing, ingestion, dbt, and output contracts. They
are not captured production datasets and do not represent live market state.

Any future fixture derived from a third-party source must document its origin
and governing licence next to the file before it is committed.

`cassettes/pmxt_order_book.yml` is a fully synthetic PMXT-shaped response
authored by Hypertrial. It tests replay parsing and credential filtering; it
does not contain a captured PMXT order book or API key.

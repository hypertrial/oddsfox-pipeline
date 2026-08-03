# Test Fixture Provenance

Files in this directory are synthetic, Hypertrial-authored inputs created only
to exercise deterministic parsing, ingestion, dbt, and output contracts. They
are not captured production datasets and do not represent live market state.

Any future fixture derived from a third-party source must document its origin
and governing licence next to the file before it is committed.

`cassettes/pmxt_order_book.yml` is a fully synthetic PMXT-shaped response
authored by Hypertrial. It tests replay parsing and credential filtering; it
does not contain a captured PMXT order book or API key.

`cassettes/pmxt_trades.yml` is a fully synthetic PMXT-shaped trades response
authored by Hypertrial for the same reviewed match-95 manifest. It tests replay
parsing only; it does not contain a captured PMXT trade stream or API key.

`market_portrait/match-95-target.yml` is a fully synthetic operator-review
target manifest for the market-portrait pipeline. It mirrors the committed
order-book target identities for match 95 and is used by
`dbt-market-portrait-ci`; it is not an operator-approved production manifest.

The match-minute dbt integration contract is generated in-test by
`tests/integration/match_minute_seed.py` (104 games, 248 markets, 496
tokens, 24,304 mart rows). No operator minute-history or schedule rows are
committed.

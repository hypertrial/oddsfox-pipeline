# Seed Distribution Policy

The repository distributes prediction-market software configuration and a
header-only Polygon market manifest shell:

- Small Hypertrial-authored pipeline policy constants used as executable
  software configuration.
- A header-only shell for operator-reviewed Polygon market data.

The following files intentionally contain one header row and no records:

- `polymarket_wc2026_polygon_settlement_markets.csv`

Operators may populate this path locally with data they are entitled to use.
The Polygon candidate
generator writes below ignored `artifacts/`; review its output before copying a
manifest to the seed path and supplying the matching local resolution
attestation. The source and authoring steps for the two WC2026 minute marts are
documented in
[`Recreate local marts`](../../docs/guides/recreate-local-marts.md)
([match-minute](../../docs/guides/recreate-match-minute-mart.md),
[Polygon settlement](../../docs/guides/recreate-polygon-settlement-mart.md)).

Local overlays make a checkout dirty and must never be committed. Restore the
tracked shell with `git restore dbt/seeds` after local work. Non-market
references arrive only through a validated Scraper `oddsfox.reference.v1`
bundle.

See [`THIRD_PARTY_NOTICES.md`](../../THIRD_PARTY_NOTICES.md) for licence scope.

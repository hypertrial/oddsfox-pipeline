# Seed Distribution Policy

The repository distributes two kinds of dbt seeds:

- Small Hypertrial-authored contract constants and aliases used as executable
  software configuration.
- Header-only schema shells for operator-supplied reference and Polygon market
  data.

The following files intentionally contain one header row and no records:

- `polymarket_wc2026_polygon_settlement_markets.csv`
- `wc2026_base_camps_teams.csv`
- `wc2026_schedule_matches.csv`
- `wc2026_third_place_options.csv`
- `wc2026_tournament_classification.csv`
- `wc2026_venues.csv`

Operators may populate these paths locally with data they are entitled to use,
or mount populated files over the paths in a container. The Polygon candidate
generator writes below ignored `artifacts/`; review its output before copying a
manifest to the seed path and supplying the matching local resolution
attestation. The source and authoring steps for the two WC2026 minute marts are
documented in
[`Recreate the WC2026 minute marts locally`](../../docs/guides/recreate-local-marts.md).

Local overlays make a checkout dirty and must never be committed. Restore the
tracked shells with `git restore dbt/seeds` after local work. The ordinary dbt
graph remains parseable with the shells and produces empty dependent relations;
data-dependent validation fails closed until complete local inputs are present.

See [`THIRD_PARTY_NOTICES.md`](../../THIRD_PARTY_NOTICES.md) for licence scope.

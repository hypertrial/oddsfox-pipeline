# Source ownership boundary

Status: accepted; hard cutover with no compatibility aliases.

Pipeline owns runtime acquisition only for Polymarket, PMXT, Kalshi, and
Polygon. The acquisition registry is deny-by-default and every runtime client
must validate its source and host through that registry.

Scraper owns every non-prediction-market collector, source-native parser,
normalization and reference transformation, team identity workflow, Elo
calculation, and immutable reference/Elo publication. Pipeline may consume a
complete `oddsfox.reference.v1` bundle through its source-neutral loader. That
loader verifies contract version, inventory, schemas, primary keys, checksums,
and immutable bundle identity before a transactional replacement; a failed load
leaves the prior bundle active.

Source names may appear in contract columns, license attribution, and
provenance. They must not reintroduce source-specific network, parsing, raw
storage, Dagster collectors, schedules, or Make targets.

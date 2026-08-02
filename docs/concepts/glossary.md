# Glossary

Short analyst and operator shortcuts. Normative definitions live in
[Terminology](../reference/terminology.md).

## Analyst Semantics

**progression** — Price or label normalized to a team advancing or reaching a
stage, not necessarily the venue's raw Yes token wording.

**price_represents** — Column that states what price columns mean. For public
Polymarket WC2026 knockout marts, expect `progression`.

**progression_outcome_label** — Human-readable progression outcome tied to the
normalized price side.

**is_actionable_live_market** — Prefer this filter for current live analysis when
the mart exposes it. Historical closed and resolved rows remain in marts on
purpose.

**current_price_status** — Freshness and lifecycle bucket such as `fresh_live`,
`stale_live`, `missing_live`, `historical_closed`, `historical_resolved`, or
`inactive`.

**temporal grain** — What one row uniquely represents (for example one
token-hour or one FIFA match-minute). See
[Terminology](../reference/terminology.md#grain-and-cadence).

**null policy** — How missing observations appear. For match and settlement
minute marts, dense empty slots usually keep null prices with no forward-fill or
pair renormalization.

**both_sources_complete** — Cross-platform match-mart flag that both providers
have usable closes for that row's comparison.

## Operator And Integration Terms

**pipeline** — A coherent source-to-output data path such as the Polymarket
WC2026 match-minute odds pipeline. See
[Terminology](../reference/terminology.md#orchestration-ladder).

**scope** — A fixed shipped product slice such as `polymarket:wc2026`. Dagster
asset configs do not accept arbitrary runtime scope selectors in `v0.1.x`. See
[Terminology](../reference/terminology.md#identity).

**wc2026.v1** — The private strategy clean-data contract exposed through
`wc2026_marts` relation aliases. Ordinary public-mart consumers start with
[Data contracts](../reference/data-contracts.md). See
[Terminology](../reference/terminology.md#contract).

**logical atlas** — Static WC2026 inventory product exported as
`polymarket-wc2026-logical-v1`. See
[Terminology](../reference/terminology.md#logical-atlas-chain).

**asset key** — Dagster asset identity, written source-first (for example
`polymarket/wc2026/raw/markets`).

**seed shell** — A tracked CSV header (and empty body) that defines schema only.
Complete operator rows stay local and untracked.

**attestation** — Operator-reviewed resolution or evidence file required by some
advanced pipelines (notably Polygon settlement). Not committed to the canonical
repo.

**observability schema** — `*_observability` relations for freshness, coverage,
ingestion runs, and data-quality findings used before trusting prices.

## See Also

- [Terminology](../reference/terminology.md)
- [Naming](../reference/naming.md)
- [Data dictionary](../reference/data-dictionary.md)
- [Data contracts](../reference/data-contracts.md)
- [Strategy contracts](../reference/strategy-contracts.md)

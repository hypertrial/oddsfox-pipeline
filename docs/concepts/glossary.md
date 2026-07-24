# Glossary

Short definitions for analyst and operator terms used across the docs.

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

**grain** — What one row uniquely represents (for example one token-hour or one
FIFA match-hour).

**null policy** — How missing observations appear. For match and settlement
minute marts, dense empty slots usually keep null prices with no forward-fill or
pair renormalization.

**both_sources_complete** — Cross-platform match-mart flag that both providers
have usable closes for that row's comparison.

## Operator And Integration Terms

**scope** — A fixed shipped source and market graph such as
`polymarket:wc2026`. Dagster asset configs do not accept arbitrary runtime scope
selectors in `v0.1.x`.

**wc2026.v1** — The public WC2026 analytics contract exposed primarily through
`wc2026_marts` (and related documented marts). Downstream tools should depend on
these public surfaces, not raw or intermediate schemas.

**asset key** — Dagster asset identity, written source-first (for example
`polymarket/wc2026/raw/markets`).

**seed shell** — A tracked CSV header (and empty body) that defines schema only.
Complete operator rows stay local and untracked.

**attestation** — Operator-reviewed resolution or evidence file required by some
advanced flows (notably Polygon settlement). Not committed to the canonical
repo.

**observability schema** — `*_observability` relations for freshness, coverage,
sync runs, and data-quality findings used before trusting prices.

## See Also

- [Data dictionary](../reference/data-dictionary.md)
- [Data contracts](../reference/data-contracts.md)
- [Naming](../reference/naming.md)

# Integrators

Use this hub when another repository or offline tool consumes OddsFox Pipeline
outputs. Public marts and the documented logical-v1 bundle are analytics inputs,
not execution orders. Canonical vocabulary lives in
[Terminology](../reference/terminology.md).

## Checklist

1. **Consume public surfaces only** — public `*_marts` relations and the
   documented `polymarket-wc2026-logical-v1` logical bundle. Do not treat
   `*_raw`, `*_ops`, staging, or intermediate schemas as APIs. Start with
   [Data contracts](../reference/data-contracts.md) and the
   [Data dictionary](../reference/data-dictionary.md).
2. **Pin versions** — read `wc2026_marts.contract_metadata` where present and
   track
   [CHANGELOG.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/CHANGELOG.md).
   `v0.1.x` mart layouts may break between releases.
3. **Produce logical-atlas input intentionally** — export with
   `scripts/export_polymarket_wc2026_logical_bundle.py` and require the
   `polymarket-wc2026-logical-v1` manifest (see the
   [logical-atlas runbook](../guides/build-wc2026-logical-atlas.md)). Do not
   scrape intermediate tables.
4. **Strategy / raw.v1 consumers** — if you load private canonical snapshots or
   the strategy clean-data set (`wc2026.v1`), fail closed on readiness using
   [Strategy contracts](../reference/strategy-contracts.md). Ordinary public
   mart consumers do not need that page.
5. **Polygon boundary** — operator-local Polygon audit bundles and technical
   exports are not `wc2026.v1` signal inputs and must not feed intents or
   execution.
6. **Execution stays elsewhere** — order admission belongs to
   `oddsfox-execution`. See [Integration](../concepts/integration.md) and
   [System overview](../concepts/system-overview.md) for repository roles.

## Boundary

- Consume public `*_marts` and the documented logical-v1 bundle only.
- Do not treat `*_raw`, `*_ops`, staging, or intermediate schemas as APIs.
- Order execution belongs to `oddsfox-execution` and is outside this runtime.
- Polygon technical exports are operator-local and do not feed `wc2026.v1`
  signals or intents.
- `v0.1.x` mart layouts may break between releases; pin to CHANGELOG and
  contracts when integrating.

See [Scope and non-goals](../concepts/scope-and-non-goals.md) for what this
repository ships and what it does not host.

# Integrators

Use this hub when another repository or offline tool consumes OddsFox Pipeline
outputs. Pipeline marts and graph parquet are analytics inputs, not execution
orders.

## Start Here

1. [Integration guide](../concepts/integration.md) — public contracts, allowed
   schemas, versioning expectations.
2. [System overview](../concepts/system-overview.md) — repository boundaries
   across the OddsFox superproject.
3. [Data contracts](../reference/data-contracts.md) — formal grains and
   guarantees for `wc2026.v1` and sibling marts.

## Boundary

- Consume public `*_marts` and documented graph export parquet only.
- Do not treat `*_raw`, `*_ops`, staging, or intermediate schemas as APIs.
- Order execution belongs to `oddsfox-execution` and is outside this runtime.
- Polygon technical exports are operator-local and do not feed `wc2026.v1`
  signals or intents.
- `v0.1.x` mart layouts may break between releases; pin to CHANGELOG and
  contracts when integrating.

See [Scope and non-goals](../concepts/scope-and-non-goals.md) for what this
repository ships and what it does not host.

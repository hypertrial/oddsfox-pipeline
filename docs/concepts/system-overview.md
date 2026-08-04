# OddsFox Pipeline system overview

OddsFox Pipeline is local-first prediction-market pipeline software. Operators
supply source data, run ingestion into their own warehouse, and may create
immutable internal WC2026 Polygon settlement audit bundles and derive
allowlisted technical CSV dossiers from them entirely offline. Distribution
boundaries and non-goals live in
[Scope and non-goals](scope-and-non-goals.md); the operator checklist in
[Operator responsibilities](operator-responsibilities.md).

```text
Public sources and private canonical snapshots
  -> oddsfox-pipeline: DuckDB warehouse (documented *_marts per pipeline; optional private wc2026.v1 from routine WC2026 paths)
  -> oddsfox-strategy: versioned signal batches
  -> oddsfox parent: policy-capped explicit intent plans
  -> oddsfox-execution: paper orders and trades
```

The independent operator-local artifact branch is:

```text
Polygon/dbt settlement mart
  -> internal audit bundle
  -> allowlisted technical export
  -> operator-controlled local use
```

It does not feed `wc2026.v1`, signals, intents, or execution.

Public mart grains live in
[Data contracts](../reference/data-contracts.md). Private snapshot and strategy
clean-data relations live in
[Strategy contracts](../reference/strategy-contracts.md). Vocabulary lives in
[Terminology](../reference/terminology.md).

## Repository Roles

| Repository | Role | Input | Output |
| --- | --- | --- | --- |
| private `oddsfox` | Superproject, private collectors, orchestration, policy, dispatch, deployment, and monitoring. | Private/public source changes and signal batches. | Canonical raw snapshots and effective intent plans. |
| `oddsfox-pipeline` | Ingests operator-configured sources and validated canonical snapshots, then builds stable dbt marts. | Source APIs, finalized Polygon logs, operator-supplied CSV/TXT inputs, and `oddsfox.raw.v1`. | Local DuckDB `*_marts` per pipeline; private `wc2026.v1` strategy clean-data only from routine Polymarket/Kalshi WC2026 paths; telemetry; internal Polygon audit bundles; optional allowlisted technical exports. |
| private `oddsfox-strategy` | Runs WC2026 discovery, models, arbitrage, and allocation. | Read-only `wc2026.v1` strategy clean-data. | Immutable `oddsfox.signal.v1` batches. |
| `oddsfox-execution` | Executes externally generated order intents under durable risk controls. | Authenticated strategy intents and current venue state. | Orders, trades, positions, audit events, and operator controls. |
| `oddsfox-dash` | Archived historical WC2026 graph client. | Retired `/api/v0` contract. | No supported deployment. |

## Which Repo Do I Touch?

| Goal | Repo |
| --- | --- |
| Change safe-source ingestion, canonical snapshot validation, DuckDB schemas, or dbt marts. | `oddsfox-pipeline` |
| Change private collection, end-to-end orchestration, policy, dispatch, deployment, or monitoring. | private `oddsfox` |
| Change models, discovery, allocation, or signal generation. | private `oddsfox-strategy` |
| Change order admission, risk, signing, reconciliation, or execution controls. | `oddsfox-execution` |
| Inspect the retired graph UI. | `oddsfox-dash` |

## Operator Path

Run ingestion independently. Strategies may consume routine-path `wc2026.v1`
outputs; execution is separate — see [Integration](integration.md).

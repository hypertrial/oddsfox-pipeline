# OddsFox Pipeline system overview

OddsFox Pipeline is local-first prediction-market pipeline software. Operators
supply source data, run ingestion into their own warehouse, and may create
logical-atlas artifacts for offline analysis. The software can also create an
immutable internal WC2026 Polygon settlement audit bundle and derive an
allowlisted technical CSV dossier from it entirely offline. It does not host
datasets or upload outputs. Trade execution is a separate concern owned by
`oddsfox-execution`.

Hypertrial owns and licenses the first-party project under MIT and operates no
continuous live ingestion, hosted production pipeline, or hosted data service.
See the
[authoritative licence scope](https://github.com/hypertrial/oddsfox-pipeline/blob/main/THIRD_PARTY_NOTICES.md).

```text
Public sources and private canonical snapshots
  -> oddsfox-pipeline: DuckDB warehouse (public marts + private wc2026.v1 strategy contract)
  -> oddsfox-pipeline: polymarket-wc2026-logical-v1 bundle
  -> oddsfox-graph: logical graph database, edge proofs, coverage, and dashboard
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

## Local-First Data

OddsFox Pipeline ships software and operator tooling, not production datasets.
Each operator supplies inputs and controls the resulting DuckDB file or
self-managed warehouse.

Pipeline and logical-atlas outputs are not execution inputs unless the private
strategy and parent control plane convert them into an admitted explicit intent
for `oddsfox-execution`.

## Repository Roles

| Repository | Role | Input | Output |
| --- | --- | --- | --- |
| private `oddsfox` | Superproject, private collectors, orchestration, policy, dispatch, deployment, and monitoring. | Private/public source changes and signal batches. | Canonical raw snapshots and effective intent plans. |
| `oddsfox-pipeline` | Ingests operator-configured sources and validated canonical snapshots, then builds stable dbt marts. | Source APIs, finalized Polygon logs, operator-supplied CSV/TXT inputs, and `oddsfox.raw.v1`. | Local public DuckDB marts, private `wc2026.v1` strategy clean-data, telemetry, the seven-file `polymarket-wc2026-logical-v1` bundle, internal Polygon audit bundles, and optional allowlisted technical exports. |
| private `oddsfox-strategy` | Runs WC2026 discovery, models, arbitrage, and allocation. | Read-only `wc2026.v1` strategy clean-data. | Immutable `oddsfox.signal.v1` batches. |
| `oddsfox-graph` | Converts the versioned static logical bundle into analyst-facing graph artifacts and a filterable dashboard. | Pipeline `polymarket-wc2026-logical-v1` bundle. | Logical graph DuckDB, proposition/market edges and proofs, coverage reports, and dashboard assets. |
| `oddsfox-execution` | Executes externally generated order intents under durable risk controls. | Authenticated strategy intents and current venue state. | Orders, trades, positions, audit events, and operator controls. |
| `oddsfox-dash` | Archived historical WC2026 graph client. | Retired `/api/v0` contract. | No supported deployment. |

## Which Repo Do I Touch?

| Goal | Repo |
| --- | --- |
| Change safe-source ingestion, canonical snapshot validation, DuckDB schemas, dbt marts, or logical-atlas export. | `oddsfox-pipeline` |
| Change private collection, end-to-end orchestration, policy, dispatch, deployment, or monitoring. | private `oddsfox` |
| Change models, discovery, allocation, or signal generation. | private `oddsfox-strategy` |
| Change graph logic, artifact schemas, conditional probabilities, coherence, or build reports. | `oddsfox-graph` |
| Change order admission, risk, signing, reconciliation, or execution controls. | `oddsfox-execution` |
| Inspect the retired graph UI. | `oddsfox-dash` |

## Operator Path

Run ingestion and logical-atlas generation independently. Strategies may consume
those outputs, but they communicate with execution only through the
`oddsfox-execution` `/v1` intent API.

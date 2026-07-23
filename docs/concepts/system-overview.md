# OddsFox Pipeline system overview

OddsFox Pipeline is a local-first prediction-market data system. Operators run
source ingestion into their own warehouse and may publish graph artifacts for
offline analysis. The repository can also create an immutable internal WC2026
Polygon settlement audit bundle and derive a sanitized technical CSV dossier
from it entirely offline. Hosting, publisher identity, dataset licensing, legal
review, and distribution are external concerns. Trade execution is a separate
concern owned by `oddsfox-execution`.

```text
Public sources and private canonical snapshots
  -> oddsfox-pipeline: DuckDB warehouse and wc2026.v1 dbt marts
  -> oddsfox-strategy: versioned signal batches
  -> oddsfox parent: policy-capped explicit intent plans
  -> oddsfox-execution: paper orders and trades
  -> oddsfox-graph: graph_snapshot.json and knockout_artifacts.json
```

The independent historical export branch is:

```text
Polygon/dbt settlement mart
  -> internal audit bundle
  -> sanitized technical export
  -> external publisher process
```

It does not feed `wc2026.v1`, signals, intents, or execution.

## Local-First Data

OddsFox Pipeline ships code and operator tooling, not a shared hosted dataset. Each
operator runs ingestion against source APIs and owns the resulting DuckDB file
or self-managed warehouse.

Pipeline and graph outputs are not execution inputs unless the private strategy
and parent control plane convert them into an admitted explicit intent for
`oddsfox-execution`.

## Repository Roles

| Repository | Role | Input | Output |
| --- | --- | --- | --- |
| private `oddsfox` | Superproject, private collectors, orchestration, policy, dispatch, deployment, and monitoring. | Private/public source changes and signal batches. | Canonical raw snapshots and effective intent plans. |
| `oddsfox-pipeline` | Ingests safe public sources and validated canonical snapshots, then builds stable dbt marts. | Source APIs, finalized Polygon logs, public CSV/TXT feeds, and `oddsfox.raw.v1`. | `wc2026.v1` DuckDB marts, telemetry, graph export parquet, internal Polygon audit bundles, and optional sanitized technical exports. |
| private `oddsfox-strategy` | Runs WC2026 discovery, models, arbitrage, and allocation. | Read-only `wc2026.v1` marts. | Immutable `oddsfox.signal.v1` batches. |
| `oddsfox-graph` | Converts token-hour odds into graph-ready artifacts. | Pipeline graph export parquet. | `graph_snapshot.json`, `knockout_artifacts.json`, parquet artifacts, and reports. |
| `oddsfox-execution` | Executes externally generated order intents under durable risk controls. | Authenticated strategy intents and current venue state. | Orders, trades, positions, audit events, and operator controls. |
| `oddsfox-dash` | Archived historical WC2026 graph client. | Retired `/api/v0` contract. | No supported deployment. |

## Which Repo Do I Touch?

| Goal | Repo |
| --- | --- |
| Change safe-source ingestion, canonical snapshot validation, DuckDB schemas, dbt marts, or graph export. | `oddsfox-pipeline` |
| Change private collection, end-to-end orchestration, policy, dispatch, deployment, or monitoring. | private `oddsfox` |
| Change models, discovery, allocation, or signal generation. | private `oddsfox-strategy` |
| Change graph logic, artifact schemas, conditional probabilities, coherence, or build reports. | `oddsfox-graph` |
| Change order admission, risk, signing, reconciliation, or execution controls. | `oddsfox-execution` |
| Inspect the retired graph UI. | `oddsfox-dash` |

## Operator Path

Run ingestion and graph generation independently. Strategies may consume those
outputs, but they communicate with execution only through the
`oddsfox-execution` `/v1` intent API.

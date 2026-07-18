# OddsFox Pipeline system overview

OddsFox Pipeline is a local-first prediction-market data system. Operators run
source ingestion into their own warehouse and may publish graph artifacts for
offline analysis. Trade execution is a separate concern owned by
`oddsfox-execution`.

```text
Polymarket and FIFA sources
  -> oddsfox-pipeline: DuckDB warehouse and dbt marts
  -> oddsfox-graph: graph_snapshot.json and knockout_artifacts.json

Strategy repositories
  -> oddsfox-execution: risk-controlled Polymarket order execution
```

## Local-First Data

OddsFox Pipeline ships code and operator tooling, not a shared hosted dataset. Each
operator runs ingestion against source APIs and owns the resulting DuckDB file
or self-managed warehouse.

The former `oddsfox-live` JSON/SSE backend and `oddsfox-dash` deployment have
been retired. Pipeline and graph outputs are not execution inputs unless a
strategy repository explicitly consumes them and submits a validated intent to
`oddsfox-execution`.

## Repository Roles

| Repository | Role | Input | Output |
| --- | --- | --- | --- |
| `oddsfox-pipeline` | Ingests Polymarket WC2026 and US midterms 2026 odds plus FIFA team/result data, then builds dbt marts. | Source APIs and CSV feeds. | DuckDB warehouse and graph export parquet. |
| `oddsfox-graph` | Converts token-hour odds into graph-ready artifacts. | Pipeline graph export parquet. | `graph_snapshot.json`, `knockout_artifacts.json`, parquet artifacts, and reports. |
| `oddsfox-execution` | Executes externally generated order intents under durable risk controls. | Authenticated strategy intents and current venue state. | Orders, trades, positions, audit events, and operator controls. |
| `oddsfox-dash` | Archived historical WC2026 graph client. | Retired `/api/v0` contract. | No supported deployment. |

## Which Repo Do I Touch?

| Goal | Repo |
| --- | --- |
| Change ingestion, DuckDB schemas, dbt marts, Dagster jobs, or artifact refresh orchestration. | `oddsfox-pipeline` |
| Change graph logic, artifact schemas, conditional probabilities, coherence, or build reports. | `oddsfox-graph` |
| Change order admission, risk, signing, reconciliation, or execution controls. | `oddsfox-execution` |
| Inspect the retired graph UI. | `oddsfox-dash` |

## Operator Path

Run ingestion and graph generation independently. Strategies may consume those
outputs, but they communicate with execution only through the
`oddsfox-execution` `/v1` intent API.

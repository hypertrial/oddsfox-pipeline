# OddsFox Pipeline system overview

OddsFox Pipeline is a local-first WC2026 prediction-market system with an additional US
midterms 2026 Polymarket pipeline (warehouse and dbt marts only — no hosted
graph or dashboard path today). Operators run source ingestion into their own
warehouse, publish graph artifacts from that warehouse,
serve those artifacts with a small live backend, and view them in the dashboard.

```text
Polymarket and FIFA sources
  -> oddsfox-pipeline: DuckDB warehouse and dbt marts
  -> oddsfox-graph: graph_snapshot.json and knockout_artifacts.json
  -> oddsfox-live: JSON/SSE API plus live token state
  -> oddsfox-dash: WC2026 probability network dashboard
```

## Local-First Data, Hosted Runtime

OddsFox Pipeline ships code and operator tooling, not a shared hosted dataset. Each
operator runs ingestion against source APIs and owns the resulting DuckDB file
or self-managed warehouse.

The hosted graph deployment is a runtime pattern, not a centralized data
service. It packages the artifact builder, `oddsfox-live`, and `oddsfox-dash`
so an operator can host their own API and dashboard from self-managed artifacts.

## Repository Roles

| Repository | Role | Input | Output |
| --- | --- | --- | --- |
| `oddsfox-pipeline` | Ingests Polymarket WC2026 and US midterms 2026 odds plus FIFA team/result data, then builds dbt marts. | Source APIs and CSV feeds. | DuckDB warehouse and graph export parquet. |
| `oddsfox-graph` | Converts token-hour odds into graph-ready artifacts. | Pipeline graph export parquet. | `graph_snapshot.json`, `knockout_artifacts.json`, parquet artifacts, and reports. |
| `oddsfox-live` | Serves artifacts and live Polymarket token state. | Current graph artifacts plus optional public market WebSocket updates. | `/api/v0/*` JSON endpoints and SSE stream. |
| `oddsfox-dash` | Visualizes the live API. | `oddsfox-live` JSON/SSE API. | WC2026 probability network UI. |

## Which Repo Do I Touch?

| Goal | Repo |
| --- | --- |
| Change ingestion, DuckDB schemas, dbt marts, Dagster jobs, or hosted refresh orchestration. | `oddsfox-pipeline` |
| Change graph logic, artifact schemas, conditional probabilities, coherence, or build reports. | `oddsfox-graph` |
| Change API endpoints, artifact reload behavior, replay, or SSE events. | `oddsfox-live` |
| Change the dashboard, network layout, UI state, or API client parsing. | `oddsfox-dash` |

## Operator Path

For the shortest end-to-end path from source APIs to a visible dashboard, use
the [hosted-stack guide](../guides/deploy-hosted-stack.md). For the live API contract, see the
[`oddsfox-live` API documentation](https://github.com/hypertrial/oddsfox-live/blob/main/docs/api.md).

# Operators

Use this hub to run, validate, and maintain a local OddsFox Pipeline warehouse.
Schedules stay disabled until manual jobs and dbt builds are healthy.

## Path

1. **First run** — [Quickstart](../getting-started/index.md) (Polymarket WC2026).
   That full scope also refreshes FIFA results used for real-team validation.
2. **Other scopes** — [Choose a scope](../getting-started/choose-a-scope.md) and
   [Run a scope](../guides/run-a-scope.md).
3. **Day-two** — [Day-two operations](../guides/day-two-operations.md).
4. **Recover** — [Validate and recover](../guides/validate-and-recover.md) and
   [Troubleshooting](../guides/troubleshooting.md).

## Credentials And Inputs

| Scope or flow | Network / credentials | Operator-local inputs |
| --- | --- | --- |
| `polymarket:wc2026` | Public Gamma/CLOB; CLOB auth optional unless a live flow requires it | `.env` only for the ordinary full run |
| `polymarket:us_midterms_2026` | Public Gamma/CLOB; auth optional for public flows | `.env` only |
| `kalshi:wc2026` | Public trade API; no API credentials | `.env` only |
| FIFA / international results | Public CSV feeds pulled by WC2026 jobs | `.env` only |
| Match-minute mart recreation | Live APIs or a completed raw warehouse | Populated schedule overlay at the documented seed path (tracked file is a header-only shell) |
| Polygon settlement (advanced) | Finalized-capable Polygon JSON-RPC | Reviewed 248-row manifest + resolution attestation (tracked seed is a header-only shell) |

Never commit `.env`, operator seed rows, reviewed attestations, DuckDB files, or
exports. See [Operator responsibilities](../concepts/operator-responsibilities.md),
[Scope and non-goals](../concepts/scope-and-non-goals.md), and
[dbt/seeds/README.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/dbt/seeds/README.md).

## Confirm Success

After a first Polymarket WC2026 full run you should have `oddsfox.duckdb` with
relations under `polymarket_wc2026_marts` and
`international_results_wc2026_marts`. Those local checks verify technical shape;
they are not Hypertrial certification of data rights or fitness for trading.
See [Operator responsibilities](../concepts/operator-responsibilities.md).
Query with [Query the warehouse](../guides/query-the-warehouse.md) or hand off
to an [analyst](analysts.md).

## Advanced

These are optional. They are not part of the default quickstart.

| Topic | Page |
| --- | --- |
| Enable hourly schedules | [Enable schedules](../guides/enable-schedules.md) |
| Cross-platform knockout full pipeline | [Orchestration](../reference/orchestration.md) (`wc2026_knockout_match_odds_full_pipeline`) |
| Recreate WC2026 minute marts locally | [Recreate local marts](../guides/recreate-local-marts.md) |
| Isolated Polygon settlement history | [Run a scope](../guides/run-a-scope.md#run-the-isolated-polygon-settlement-history) |
| Graph / knockout parquet exports | [Scripts](../reference/scripts.md) |
| Signed GHCR image | [Docker image](../guides/docker-image.md) |
| Configuration reference | [Configuration](../reference/configuration.md) |

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

| Scope or pipeline | Network / credentials | Operator-local inputs |
| --- | --- | --- |
| `polymarket:wc2026` | Public Gamma/CLOB; CLOB auth optional unless a live job requires it | `.env` only for the ordinary full run |
| `kalshi:wc2026` | Public trade API; no API credentials | `.env` only |
| FIFA / international results | Public CSV feeds pulled by WC2026 jobs | `.env` only |
| Match-minute odds (mature, isolated) | Live APIs or completed raw warehouse | Populated schedule overlay (tracked shell) |
| Match order book (mature, isolated) | Live APIs or completed raw warehouse; PMXT API key | Reviewed target manifest for match 95 |
| Market portrait (mature, isolated) | Completed order-book + trades scan; PMXT API key | Reviewed `TARGET_MANIFEST` for one approved match |
| Polygon settlement (advanced) | Finalized-capable Polygon JSON-RPC | Reviewed 248-row manifest + resolution attestation (tracked seed is a header-only shell) |

Never commit `.env`, operator seed rows, reviewed attestations, DuckDB files, or
exports. See [Operator responsibilities](../concepts/operator-responsibilities.md),
[Scope and non-goals](../concepts/scope-and-non-goals.md), and
[dbt/seeds/README.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/dbt/seeds/README.md).

## Confirm Success

After a first Polymarket WC2026 full run you should have `oddsfox.duckdb` with
`polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds`. Those local
checks verify technical shape;
they are not Hypertrial certification of data rights or fitness for trading.
See [Operator responsibilities](../concepts/operator-responsibilities.md).
Query with [Query the warehouse](../guides/query-the-warehouse.md) or hand off
to an [analyst](analysts.md).

## Advanced

These are optional. They are not part of the default quickstart.

| Topic | Page |
| --- | --- |
| Enable hourly schedules | [Enable schedules](../guides/enable-schedules.md) |
| Match-minute odds (mature, isolated) | [Recreate local marts](../guides/recreate-local-marts.md); [Pipeline registry](../reference/orchestration.md#pipeline-registry) |
| Match order book (mature, isolated) | [Recreate PMXT order-book mart](../guides/recreate-match-order-book-mart.md); [Pipeline registry](../reference/orchestration.md#pipeline-registry) |
| Market portrait (mature, isolated) | [Market portrait](../reference/market-portrait.md); [Pipeline registry](../reference/orchestration.md#pipeline-registry) |
| Isolated Polygon settlement history | [Recreate Polygon settlement mart](../guides/recreate-polygon-settlement-mart.md) |
| Knockout parquet exports | [Scripts](../reference/scripts.md) |
| Configuration reference | [Configuration](../reference/configuration.md) |

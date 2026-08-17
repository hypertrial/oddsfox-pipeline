# Operators

Use this hub to run, validate, and maintain a local OddsFox Pipeline warehouse.
Schedules stay disabled until manual jobs and dbt builds are healthy.

## Path

1. **First run** — [Quickstart](../getting-started/index.md) (Polymarket or Kalshi
   WC2026 tabs).
2. **Other scopes** — [Choose a scope](../getting-started/choose-a-scope.md) and
   [Run a scope](../guides/run-a-scope.md).
3. **Day-two** — [Day-two operations](../guides/day-two-operations.md).
4. **Recover** — [Validate and recover](../guides/validate-and-recover.md) and
   [Troubleshooting](../guides/troubleshooting.md).

## Credentials And Inputs

| Scope or pipeline | Network / credentials | Operator-local inputs |
| --- | --- | --- |
| `polymarket:wc2026` | Public Gamma/CLOB; CLOB auth optional unless a live job requires it | `.env` only for the ordinary full run |
| `polymarket:soccer` | Public Gamma/CLOB | `.env` only; the daily schedule is stopped by default |
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
`polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds`. After a Kalshi
full run, confirm the stage and group-winner marts plus shared FIFA fixtures.
Those local checks verify technical shape;
they are not Hypertrial certification of data rights or fitness for trading.
See [Operator responsibilities](../concepts/operator-responsibilities.md).
Query with [Query the warehouse](../guides/query-the-warehouse.md) or hand off
to an [analyst](analysts.md).

## Advanced

These are optional. They are not part of the default quickstart. Start at
[Advanced pipelines](../guides/advanced-pipelines.md) for the decision tree and
maturity tiers.

| Topic | Page |
| --- | --- |
| Isolated WC2026 paths (minute, order book, portrait, Polygon) | [Advanced pipelines](../guides/advanced-pipelines.md) |
| Enable hourly schedules | [Enable schedules](../guides/enable-schedules.md) |
| Shared rebuild setup (minute + Polygon) | [Recreate local marts](../guides/recreate-local-marts.md) |
| Golden mart Parquet export | [Scripts](../reference/scripts.md) (`export_polymarket_wc2026_market_hourly_odds.py`) |
| Registry hygiene cleanup | [Scripts](../reference/scripts.md) / [Troubleshooting](../guides/troubleshooting.md#tests-writing-to-production-warehouse) (`cleanup-polymarket-wc2026-registry-hygiene`) |
| Configuration reference | [Configuration](../reference/configuration.md) |

## See also

- [Quickstart](../getting-started/index.md)
- [Advanced pipelines](../guides/advanced-pipelines.md)
- [Day-two operations](../guides/day-two-operations.md)
- [Troubleshooting](../guides/troubleshooting.md)
- [Operator responsibilities](../concepts/operator-responsibilities.md)

# Choose a scope

Use this page to select one of the two fixed source and market scopes shipped
by OddsFox Pipeline `v0.1.x`. Dagster asset configs do not accept arbitrary runtime
scope selectors.

| Scope | Source | Public output | Credentials |
| --- | --- | --- | --- |
| `polymarket:wc2026` | Polymarket Gamma/CLOB plus FIFA results | Knockout snapshots, progression odds, logical atlas, schedule fixtures, and team status | Optional for public pipelines |
| `kalshi:wc2026` | Kalshi public trade API plus FIFA results | Stage-of-elimination and group-winner snapshots and hourly odds | None |

The manual WC2026 Polygon settlement-history pipeline is not a third
`run_scope.py` scope. It is an isolated historical backfill that needs a
complete operator-local market manifest at the tracked seed path (header-only
in git), a configured Polygon JSON-RPC, and its own unscheduled job and dbt
tag. See
[Run a scope](../guides/run-a-scope.md#run-the-isolated-polygon-settlement-history).

## Run a full scope

=== "Polymarket WC2026"

    ```bash
    uv run python scripts/run_scope.py polymarket:wc2026 --step full
    ```

=== "Kalshi WC2026"

    ```bash
    uv run python scripts/run_scope.py kalshi:wc2026 --step full
    ```

    Kalshi uses the public trade API and requires no API credentials.

List the accepted refs and aliases at any time:

```bash
uv run python scripts/run_scope.py --list
```

## Beyond `run_scope.py`

These fixed jobs are not chooser refs:

| Pipeline / job | Guide |
| --- | --- |
| Isolated Polygon settlement history | [Recreate Polygon settlement mart](../guides/recreate-polygon-settlement-mart.md) |
| Advanced match analysis (experimental): minute odds (optional); order book → market portrait | [Recreate local marts](../guides/recreate-local-marts.md), [Recreate PMXT order-book mart](../guides/recreate-match-order-book-mart.md), [Market portrait](../reference/market-portrait.md); [Pipeline registry](../reference/orchestration.md#pipeline-registry) |

Next, read [Run a scope](../guides/run-a-scope.md) for staged execution or
[Data contracts](../reference/data-contracts.md) for the exact documented marts.

# Choose a scope

Use this page to select one of the two fixed source and market scopes shipped
by OddsFox Pipeline `v0.2.x`. Dagster asset configs do not accept arbitrary runtime
scope selectors. For step-by-step first runs, see the
[Quickstart](index.md) tabs.

| Scope | Source | Public output | Credentials |
| --- | --- | --- | --- |
| `polymarket:wc2026` | Polymarket Gamma/CLOB | `polymarket_wc2026_market_hourly_odds` (golden mart) | Optional for public pipelines |
| `kalshi:wc2026` | Kalshi public trade API plus FIFA results | Stage-of-elimination and group-winner snapshots and hourly odds | None |

The manual WC2026 Polygon settlement-history pipeline is not a third
`run_scope.py` scope. It is an isolated historical backfill that needs a
complete operator-local market manifest at the tracked seed path (header-only
in git), a configured Polygon JSON-RPC, and its own unscheduled job and dbt
tag. See [Advanced pipelines](../guides/advanced-pipelines.md).

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

Match-minute odds, PMXT order-book history, market portrait, and Polygon
settlement history are not chooser refs. Start at
[Advanced pipelines](../guides/advanced-pipelines.md) for the decision tree,
maturity tiers, and links to each recreate guide. The
[Pipeline registry](../reference/orchestration.md#pipeline-registry) lists
entry jobs and CI gates.

Next, read [Run a scope](../guides/run-a-scope.md) for staged execution or
[Data contracts](../reference/data-contracts.md) for the exact documented marts.

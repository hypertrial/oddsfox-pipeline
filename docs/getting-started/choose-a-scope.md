# Choose a scope

Use this page to select one of the three fixed source and market scopes shipped
by OddsFox Pipeline `v0.1.x`. Dagster asset configs do not accept arbitrary runtime
scope selectors.

| Scope | Source | Public output | Credentials |
| --- | --- | --- | --- |
| `polymarket:wc2026` | Polymarket Gamma/CLOB plus FIFA results | Knockout snapshots, progression odds, graph odds, fixtures, and team status | Optional for public flows |
| `polymarket:us_midterms_2026` | Polymarket Gamma/CLOB | Balance of Power, Senate control, and House control hourly odds | Optional for public flows |
| `kalshi:wc2026` | Kalshi public trade API plus FIFA results | Stage-of-elimination and group-winner snapshots and hourly odds | None |

The manual WC2026 Polygon settlement-history flow is not a fourth
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

=== "Polymarket US midterms"

    ```bash
    uv run python scripts/run_scope.py polymarket:us_midterms_2026 --step full
    ```

    This scope has no FIFA results or candidate/race validation layer. Its dbt
    job builds only `tag:us_midterms_2026`.

=== "Kalshi WC2026"

    ```bash
    uv run python scripts/run_scope.py kalshi:wc2026 --step full
    ```

    Kalshi uses the public trade API and requires no API credentials.

List the accepted refs and aliases at any time:

```bash
uv run python scripts/run_scope.py --list
```

Next, read [Run a scope](../guides/run-a-scope.md) for staged execution or
[Data contracts](../reference/data-contracts.md) for the exact public marts.

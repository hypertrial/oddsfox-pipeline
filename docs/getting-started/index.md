# Quickstart

Use this guide to complete a safe first Polymarket WC2026 run in a local
DuckDB warehouse. Schedules stay disabled until the manual pipeline and dbt
models are healthy.

## Install

From the repository root:

```bash
uv sync --extra dev
cp .env.example .env
```

The default warehouse is `oddsfox.duckdb` in the repository root.

!!! warning "Reset warehouses from older layouts"

    OddsFox Pipeline `v0.1.x` does not maintain warehouse migrations. If this checkout
    replaces an older layout, delete the local warehouse before continuing:

    ```bash
    rm oddsfox.duckdb*
    ```

## Keep schedules disabled

Confirm these values in `.env`:

```dotenv
POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

Kalshi uses the public trade API. Polymarket CLOB credentials are optional
unless the selected live flow explicitly requires authentication.

## Validate the project

Parse the dbt project before making live requests:

```bash
uv run make dbt-parse
```

To build dbt models before live dlt ingestion, initialize the DuckDB bootstrap
tables first:

```bash
uv run python - <<'PY'
import oddsfox_pipeline.storage.duckdb.connection as connection

connection.reset_duckdb_connection_state()
connection.init_duck_db()
PY
uv run make dbt-build
```

The ordinary build intentionally excludes the manual
`tag:polygon_settlement` graph, so no Polygon RPC configuration is needed for
quickstart. Validate that graph offline with
`uv run make dbt-polygon-settlement-ci`; run its live backfill only when you
explicitly want the historical settlement dataset.

## Run the first pipeline

Run the fixed WC2026 pipeline from discovery through dbt:

```bash
uv run python scripts/run_scope.py polymarket:wc2026 --step full
```

The full run refreshes FIFA results, discovers WC2026 markets, syncs the
trailing hourly odds window, and builds the public dbt marts.

For a staged run or a dry-run preview, use [Run a scope](../guides/run-a-scope.md).

## Start Dagster

Start the local Dagster UI when you want to inspect or launch individual jobs:

```bash
uv run make dagster-dev
```

Open the URL printed in the terminal. Leave the hourly schedules disabled
until the manual jobs are healthy.

## Confirm success

The first run should create `oddsfox.duckdb`, complete
`polymarket_wc2026_full_pipeline`, and build relations under
`polymarket_wc2026_marts` and `international_results_wc2026_marts`.

Next, [choose another shipped scope](choose-a-scope.md),
[query the warehouse](../guides/query-the-warehouse.md), or
[validate and recover a run](../guides/validate-and-recover.md).

# Quickstart

Use this page for the first local run. It keeps schedules off until the
warehouse, dbt project, and manual Dagster jobs are healthy. The v0.1.x
quickstart runs the WC2026-only Polymarket pipeline.

## 1. Install

```bash
uv sync --extra dev
```

The default warehouse is `oddsfox.duckdb` in the repo root.
If you have a warehouse from an older layout, reset it first:

```bash
rm oddsfox.duckdb*
```

## 2. Configure

```bash
cp .env.example .env
```

For a local dry run, keep schedules disabled:

```dotenv
POLYMARKET_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

CLOB credentials are optional unless a live authenticated flow requires them.

## 3. Validate dbt

```bash
uv run make dbt-parse
```

To build models before live dlt ingestion, initialize the DuckDB bootstrap tables first:

```bash
uv run python - <<'PY'
import oddsfox_pipeline.storage.duckdb.connection as connection
connection.reset_duckdb_connection_state()
connection.init_duck_db()
PY
uv run make dbt-build
```

Standalone `make dbt-build` uses the fixed WC2026 dbt model graph.

## 4. Start Dagster

```bash
uv run make dagster-dev
```

Open the Dagster UI shown in the terminal. Materialize
`polymarket/wc2026/raw/markets` before
`polymarket/wc2026/raw/markets_snapshot`.

## 5. Run the Pipeline

For a full manual run, launch `polymarket_wc2026_full_pipeline`.

For a safer staged run:

1. `polymarket_wc2026_market_registry_refresh`
2. `polymarket_wc2026_hourly_odds_ingest`
3. `polymarket_wc2026_dbt_build`

Leave schedules off until these jobs complete successfully.

Next: read [Operations](operations.md) before enabling schedules, and use
[Troubleshooting](troubleshooting.md) if Dagster, DuckDB, or dbt fails.

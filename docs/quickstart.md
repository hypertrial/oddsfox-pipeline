# Quickstart

Use this page for the first local run. It keeps schedules off until the
warehouse, dbt project, and manual Dagster jobs are healthy. The v0.1.x
quickstart runs the current Polymarket/selected-scope pipeline.

## 1. Install

```bash
uv sync --extra dev
```

The default warehouse is `oddsfox.duckdb` in the repo root.

## 2. Configure

```bash
cp .env.example .env
```

For a local dry run, keep schedules disabled:

```dotenv
POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED=false
POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED=false
POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

Set `POLYMARKET_MARKET_SCOPES` in `.env` to choose one or more scope presets
(default `wc2026`). See [Configuration](configuration.md) for the preset catalog
and multi-scope examples.

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

Standalone `make dbt-build` uses `active_market_scopes` from `dbt/dbt_project.yml`
(default `[wc2026]`), not your `.env`, unless you pass `--vars`. Dagster's
`polymarket_dbt` asset syncs `active_market_scopes` from `POLYMARKET_MARKET_SCOPES`
automatically.

## 4. Start Dagster

```bash
uv run make dagster-dev
```

Open the Dagster UI shown in the terminal. Materialize `dlt_polymarket_markets` before `polymarket_markets_snapshot`.

## 5. Run the Pipeline

For a full manual run, launch `polymarket_selected_scope_full_pipeline`.

For a safer staged run:

1. `polymarket_ingest_full_refresh_events`
2. `polymarket_minutely_odds_ingest`
3. `polymarket_hourly_odds_ingest` (optional hourly-grain refresh)
4. `dbt_full_refresh`

Leave schedules off until these jobs complete successfully.

Next: read [Operations](operations.md) before enabling schedules, and use
[Troubleshooting](troubleshooting.md) if Dagster, DuckDB, or dbt fails.

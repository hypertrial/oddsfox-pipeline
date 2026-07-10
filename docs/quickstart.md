# Quickstart

Use this page for the first local run. It keeps schedules off until the
warehouse, dbt project, and manual Dagster jobs are healthy. The v0.1.x
quickstart runs the WC2026 Polymarket pipeline plus the FIFA World Cup
fixture/result source used to clean team scope.

For the shortest source-to-dashboard flow across all four OddsFox repos, use
the [End-to-End Operator Runbook](operator-runbook.md).

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
KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

CLOB credentials are optional unless a live authenticated Polymarket flow requires
them. Kalshi uses the public trade API and requires no credentials for local runs.

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

For a full manual run, use the scoped runner:

```bash
uv run python scripts/run_scope.py polymarket:wc2026 --step full
```

`polymarket_wc2026` is accepted as an alias for `polymarket:wc2026`.

For a safer staged run:

```bash
.venv/bin/python -m dagster job execute -m oddsfox_pipeline.orchestration.definitions -j international_results_wc2026_match_results_ingest
uv run python scripts/run_scope.py polymarket:wc2026 --step registry
uv run python scripts/run_scope.py polymarket:wc2026 --step odds
uv run python scripts/run_scope.py polymarket:wc2026 --step dbt
```

The underlying jobs are `polymarket_wc2026_market_registry_refresh`,
`polymarket_wc2026_hourly_odds_ingest`, and `polymarket_wc2026_dbt_build`.

Leave schedules off until these jobs complete successfully.

## Kalshi WC2026 (optional)

Kalshi uses the public trade API; no API credentials are required. Keep the
Kalshi schedule disabled for the first manual run:

```dotenv
KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

For a full manual run:

```bash
uv run python scripts/run_scope.py kalshi:wc2026 --step full
```

`kalshi_wc2026` is accepted as an alias for `kalshi:wc2026`.

For a safer staged run:

```bash
.venv/bin/python -m dagster job execute -m oddsfox_pipeline.orchestration.definitions -j international_results_wc2026_match_results_ingest
uv run python scripts/run_scope.py kalshi:wc2026 --step registry
uv run python scripts/run_scope.py kalshi:wc2026 --step odds
uv run python scripts/run_scope.py kalshi:wc2026 --step dbt
```

The underlying jobs are `kalshi_wc2026_market_registry_refresh`,
`kalshi_wc2026_hourly_odds_ingest`, and `kalshi_wc2026_dbt_build`.

## US midterms 2026 (optional)

The midterms path uses the same install, configure, and `dbt-parse` steps above.
Keep the midterms schedule disabled for the first manual run:

```dotenv
POLYMARKET_US_MIDTERMS_2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

There is no FIFA results ingest for this scope. The full pipeline builds only
`tag:us_midterms_2026` dbt models.

For a full manual run:

```bash
uv run python scripts/run_scope.py polymarket:us_midterms_2026 --step full
```

`polymarket_us_midterms_2026` is accepted as an alias for
`polymarket:us_midterms_2026`.

For a safer staged run:

```bash
uv run python scripts/run_scope.py polymarket:us_midterms_2026 --step registry
uv run python scripts/run_scope.py polymarket:us_midterms_2026 --step odds
uv run python scripts/run_scope.py polymarket:us_midterms_2026 --step dbt
```

The underlying jobs are `polymarket_us_midterms_2026_market_registry_refresh`,
`polymarket_us_midterms_2026_hourly_odds_ingest`, and
`polymarket_us_midterms_2026_dbt_build`.

To inspect all shipped scope refs before running:

```bash
uv run python scripts/run_scope.py --list
```

Next: read [Operations](operations.md) before enabling schedules, and use
[Troubleshooting](troubleshooting.md) if Dagster, DuckDB, or dbt fails.

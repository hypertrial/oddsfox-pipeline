# Run a scope

Use `scripts/run_scope.py` to preview or execute the fixed Dagster jobs for one
or more shipped scopes without navigating the Dagster UI.

## Inspect the command surface

```bash
uv run python scripts/run_scope.py --list
uv run python scripts/run_scope.py polymarket_us_midterms_2026 --step odds --dry-run
```

Supported refs are `polymarket:wc2026` (`polymarket_wc2026`),
`polymarket:us_midterms_2026` (`polymarket_us_midterms_2026`), and
`kalshi:wc2026` (`kalshi_wc2026`). Supported steps are `registry`, `odds`,
`dbt`, and `full`.

The command maps known refs to fixed jobs. It is not a runtime market-scope
selector and does not accept arbitrary dbt selectors.

## Run one stage at a time

=== "Polymarket WC2026"

    ```bash
    .venv/bin/python -m dagster job execute -m oddsfox_pipeline.orchestration.definitions -j international_results_wc2026_match_results_ingest
    uv run python scripts/run_scope.py polymarket:wc2026 --step registry
    uv run python scripts/run_scope.py polymarket:wc2026 --step odds
    uv run python scripts/run_scope.py polymarket:wc2026 --step dbt
    ```

=== "Polymarket US midterms"

    ```bash
    uv run python scripts/run_scope.py polymarket:us_midterms_2026 --step registry
    uv run python scripts/run_scope.py polymarket:us_midterms_2026 --step odds
    uv run python scripts/run_scope.py polymarket:us_midterms_2026 --step dbt
    ```

=== "Kalshi WC2026"

    ```bash
    .venv/bin/python -m dagster job execute -m oddsfox_pipeline.orchestration.definitions -j international_results_wc2026_match_results_ingest
    uv run python scripts/run_scope.py kalshi:wc2026 --step registry
    uv run python scripts/run_scope.py kalshi:wc2026 --step odds
    uv run python scripts/run_scope.py kalshi:wc2026 --step dbt
    ```

For WC2026 scopes, refresh
`international_results_wc2026_match_results_ingest` before a staged dbt run so
real-team validation inputs are current.

## Run multiple dbt scopes

```bash
uv run python scripts/run_scope.py polymarket:wc2026 kalshi:wc2026 --step dbt
```

Unlike the deterministic job smoke, real scope execution may call configured
external sources and write to the selected warehouse.

## Run the isolated Polygon settlement history

The Polygon settlement flow is not a `run_scope.py` step and is never scheduled.
After configuring the required primary RPC URL and non-secret provider label,
run its dedicated job:

```bash
uv run python -m dagster job execute \
  -m oddsfox_pipeline.orchestration.definitions \
  -j polymarket_wc2026_polygon_settlement_backfill
```

This job reads the committed static market seed, scans finalized Polygon V2
logs, and builds only the dedicated `polygon_settlement` dbt ancestors. It does
not refresh Gamma, CLOB, international-results, or OpenFootball. For a
disposable warehouse plus exact 39,120-row assertion, use
`uv run make polygon-settlement-live-smoke`. That target uses
`.cache/polygon_settlement/benchmarks/v4/live_smoke.duckdb` and resumes its
SSD-local checkpoint by default; opt into a clean disposable scan with
`POLYGON_SETTLEMENT_LIVE_SMOKE_RESET=true`.

Next, [validate the run](validate-and-recover.md). The Polygon settlement flow
remains manual-only.

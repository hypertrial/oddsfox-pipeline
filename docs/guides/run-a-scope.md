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

Next, [validate the run](validate-and-recover.md) before
[enabling schedules](enable-schedules.md).

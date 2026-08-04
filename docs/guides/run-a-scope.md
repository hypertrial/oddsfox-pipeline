# Run a scope

Use `scripts/run_scope.py` to preview or execute the fixed jobs for one
or more shipped scopes without navigating the Dagster UI. For the operator map,
start with [Operators](../audiences/operators.md). Day-two cadence lives in
[Day-two operations](day-two-operations.md).

## Inspect the command surface

```bash
uv run python scripts/run_scope.py --list
uv run python scripts/run_scope.py polymarket_wc2026 --step odds --dry-run
```

Supported refs are `polymarket:wc2026` (`polymarket_wc2026`) and
`kalshi:wc2026` (`kalshi_wc2026`). Supported steps are `market_scope_registry`, `odds`,
`dbt`, and `full`.

The command maps known refs to fixed jobs. It is not a runtime market-scope
selector and does not accept arbitrary dbt selectors.

## Run one stage at a time

=== "Polymarket WC2026"

    ```bash
    uv run python scripts/run_scope.py polymarket:wc2026 --step market_scope_registry
    uv run python scripts/run_scope.py polymarket:wc2026 --step odds
    uv run python scripts/run_scope.py polymarket:wc2026 --step dbt
    ```

=== "Kalshi WC2026"

    ```bash
    .venv/bin/python -m dagster job execute -m oddsfox_pipeline.orchestration.definitions -j international_results_wc2026_match_results_ingest
    uv run python scripts/run_scope.py kalshi:wc2026 --step market_scope_registry
    uv run python scripts/run_scope.py kalshi:wc2026 --step odds
    uv run python scripts/run_scope.py kalshi:wc2026 --step dbt
    ```

For Kalshi and match-minute scopes, refresh
`international_results_wc2026_match_results_ingest` before a staged dbt run so
real-team validation inputs are current. The Polymarket golden-mart path does not
require that ingest step.

## Run multiple dbt scopes

```bash
uv run python scripts/run_scope.py polymarket:wc2026 kalshi:wc2026 --step dbt
```

Unlike the deterministic job smoke, real scope execution may call configured
external sources and write to the selected warehouse.

## Run the isolated Polygon settlement history

The Polygon settlement pipeline is not a `run_scope.py` step and is never scheduled.
After configuring the required primary RPC URL and non-secret provider label,
run its dedicated job:

```bash
uv run python -m dagster job execute \
  -m oddsfox_pipeline.orchestration.definitions \
  -j polymarket_wc2026_polygon_settlement_backfill
```

This job reads a complete operator-local market manifest at the tracked seed
path (the committed file is a header-only shell), scans finalized Polygon V2
logs, and builds only the dedicated `polygon_settlement` dbt ancestors. It does
not refresh Gamma, CLOB, international-results, or OpenFootball. For a
disposable warehouse plus exact 39,120-row assertion, use
`uv run make polygon-settlement-live-smoke`. That target uses
`.cache/polygon_settlement/benchmarks/v4/live_smoke.duckdb` and resumes its
SSD-local checkpoint by default; opt into a clean disposable scan with
`POLYGON_SETTLEMENT_LIVE_SMOKE_RESET=true`.

For the full seed-authoring and disposable-smoke path, see
[Recreate Polygon settlement mart](recreate-polygon-settlement-mart.md).
Next, [validate the run](validate-and-recover.md). The Polygon settlement
pipeline remains manual-only.

## Run the isolated PMXT order-book history

The PMXT pipeline is also outside `run_scope.py` and is never scheduled. Configure
`PMXT_API_KEY`, then use the credit-consuming disposable/resumable acceptance
target:

```bash
uv run make match-order-book-live-smoke
```

For identity, credit, resume, and warehouse checks, see
[Recreate the PMXT order-book mart](recreate-match-order-book-mart.md).

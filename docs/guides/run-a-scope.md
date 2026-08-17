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

Supported refs are `polymarket:wc2026` (`polymarket_wc2026`),
`polymarket:soccer` (`polymarket_soccer`), and `kalshi:wc2026`
(`kalshi_wc2026`). Supported steps are `market_scope_registry`, `odds`, `dbt`,
and `full`.

For Polymarket WC2026, `dbt` and `full` build only the golden-mart closure
(`+polymarket_wc2026_market_hourly_odds`), not match-minute, order-book,
portrait, or Polygon settlement marts. Use dedicated backfill jobs for those
paths.

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
    uv run python -m dagster job execute -m oddsfox_pipeline.orchestration.definitions -j international_results_wc2026_match_results_ingest
    uv run python scripts/run_scope.py kalshi:wc2026 --step market_scope_registry
    uv run python scripts/run_scope.py kalshi:wc2026 --step odds
    uv run python scripts/run_scope.py kalshi:wc2026 --step dbt
    ```

For Kalshi and the isolated WC2026 match-minute pipeline, refresh
`international_results_wc2026_match_results_ingest` before a staged dbt run so
real-team validation inputs are current. The Polymarket golden-mart path does not
require that ingest step.

=== "Polymarket Soccer"

    ```bash
    uv run python scripts/run_scope.py polymarket:soccer --step market_scope_registry
    uv run python scripts/run_scope.py polymarket:soccer --step odds
    uv run python scripts/run_scope.py polymarket:soccer --step dbt
    ```

    The registry step proves converged open and closed exact-tag scans and then
    rebuilds the strict three-role match-result registry. The odds step is
    incremental and may publish successful tokens while retaining failures in
    observability.

## Run multiple dbt scopes

```bash
uv run python scripts/run_scope.py polymarket:wc2026 kalshi:wc2026 --step dbt
```

Unlike the deterministic job smoke, real scope execution may call configured
external sources and write to the selected warehouse.

## Isolated advanced pipelines

Match-minute odds, PMXT order-book history, market portrait, and Polygon
settlement history are outside `run_scope.py` and are never scheduled. Start at
[Advanced pipelines](advanced-pipelines.md) for the decision tree and recreate
guides, then [validate the run](validate-and-recover.md).

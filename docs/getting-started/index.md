# Quickstart

<p class="of-personas" markdown><span class="of-persona of-persona--operator">Operator</span></p>

Use this guide to complete a safe first WC2026 run in a local DuckDB warehouse.
Schedules stay disabled until the manual pipeline and dbt models are healthy.
For step-by-step first runs of either shipped scope, use the tabs below. To
compare scopes or pick advanced pipelines, see
[Choose a scope](choose-a-scope.md).

## Install

From the repository root:

```bash
uv sync --group dev
cp .env.example .env
```

The default warehouse is `oddsfox.duckdb` in the repository root.

!!! warning "Reset warehouses from older layouts"

    OddsFox Pipeline `v0.2.x` does not maintain warehouse migrations. If this checkout
    replaces an older layout, delete the local warehouse before continuing:

    ```bash
    rm oddsfox.duckdb*
    ```

## Keep schedules disabled

Confirm this value in `.env`:

```dotenv
KALSHI_WC2026_HOURLY_ODDS_SCHEDULE_ENABLED=false
```

Polymarket WC2026 has no Dagster schedule. Use manual jobs such as
`polymarket_wc2026_full_pipeline` when you need a one-off refresh.

Kalshi uses the public trade API. Polymarket CLOB credentials are optional
unless the selected live job explicitly requires authentication.

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
`tag:polygon_settlement` and `tag:pmxt_order_book` dbt graphs, so no Polygon RPC or
PMXT configuration is needed for quickstart.

!!! note "Advanced: Polygon settlement history"

    Polygon settlement is an optional isolated historical pipeline, not part of
    this first run. Validate it offline with
    `uv run make dbt-polygon-settlement-ci`, or follow
    [Recreate Polygon settlement mart](../guides/recreate-polygon-settlement-mart.md)
    when you explicitly want that dataset. See
    [Advanced pipelines](../guides/advanced-pipelines.md) for other isolated paths.

## Choose your first scope

=== "Polymarket WC2026"

    ### Run the first pipeline

    Run the fixed WC2026 pipeline from discovery through dbt:

    ```bash
    uv run python scripts/run_scope.py polymarket:wc2026 --step full
    ```

    The full run discovers WC2026 markets, syncs the trailing hourly odds window,
    and builds `polymarket_wc2026_market_hourly_odds`.

    For a staged run or a dry-run preview, use [Run a scope](../guides/run-a-scope.md).

    ### Confirm success

    The first run should create `oddsfox.duckdb`, complete
    `polymarket_wc2026_full_pipeline`, and build
    `polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds`.
    Those local checks verify technical shape; they are not Hypertrial
    certification of data rights or fitness for trading. See
    [Operator responsibilities](../concepts/operator-responsibilities.md).

=== "Kalshi WC2026"

    ### Run the first pipeline

    Kalshi needs no API credentials. The full run refreshes FIFA results inputs,
    syncs stage and group-winner markets, and builds the Kalshi marts:

    ```bash
    uv run python scripts/run_scope.py kalshi:wc2026 --step full
    ```

    For a staged run or a dry-run preview, use [Run a scope](../guides/run-a-scope.md).

    ### Confirm success

    The first run should create `oddsfox.duckdb` and build:

    - `kalshi_wc2026_marts.kalshi_wc2026_stage_markets`
    - `kalshi_wc2026_marts.kalshi_wc2026_group_winner_markets`
    - `international_results_wc2026_marts.international_results_wc2026_matches`

    Those local checks verify technical shape; they are not Hypertrial
    certification of data rights or fitness for trading. See
    [Operator responsibilities](../concepts/operator-responsibilities.md).

    ### First analyst query

    Prefer live rows with `is_actionable_live_market`:

    ```sql
    select
        canonical_team_name,
        stage_key,
        progression_outcome_label,
        progression_price,
        current_price_status,
        market_ticker
    from kalshi_wc2026_marts.kalshi_wc2026_stage_markets
    where is_actionable_live_market
    order by canonical_team_name, stage_rank;
    ```

    More examples live in [Query recipes](../guides/query-recipes.md#kalshi-stage-markets).

## Start Dagster

Start the local Dagster UI when you want to inspect or launch individual jobs:

```bash
uv run make dagster-dev
```

The first load (or a load after dbt model, seed, or yml edits) may run `dbt
parse` into `.cache/runtime/dbt-target`. Warm restarts reuse that manifest when
inputs are unchanged. Force a refresh with `ODDSFOX_DBT_FORCE_PREPARE=1`, or
prefer `make dbt-prepare` when iterating on SQL outside Dagster.

Open the URL printed in the terminal. Leave the hourly schedules disabled
until the manual jobs are healthy.

Next, return to the [Operators](../audiences/operators.md) hub,
[choose another shipped scope](choose-a-scope.md),
[query the warehouse](../guides/query-the-warehouse.md), or
[validate and recover a run](../guides/validate-and-recover.md).

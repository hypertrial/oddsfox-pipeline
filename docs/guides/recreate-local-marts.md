# Recreate local marts

This index covers three optional advanced marts:

- `polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds`
- `polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds`
- `polymarket_wc2026_marts.polymarket_wc2026_match_order_book`

The repository contains the complete software path but no production rows.
Operator-local inputs must be supplied:

| Input | How to obtain it | Required for |
| --- | --- | --- |
| `dbt/seeds/wc2026_schedule_matches.csv` | Author 104 rows from the [official FIFA schedule](https://www.fifa.com/en/tournaments/mens/worldcup/canadamexicousa2026/articles/match-schedule-fixtures-results-teams-stadiums), preserving the tracked header. | Match-minute |
| `dbt/seeds/polymarket_wc2026_polygon_settlement_markets.csv` | Generate and review it with the repository's Polygon candidate tool. | Polygon settlement |
| `config/polygon-settlement-resolution-attestation.yml` | Generate it with the same candidate tool and install it only with its matching reviewed manifest. | Polygon settlement |

Do not commit these inputs, generated warehouses, or authoring evidence.
See [Operator responsibilities](../concepts/operator-responsibilities.md).

!!! important "A clean clone is necessary, but not sufficient"

    The clone supplies the pipeline, schemas, validators, and rebuild commands.
    It does not supply the 104-row schedule, the reviewed 248-proposition
    Polygon manifest, its matching attestation, or historical raw observations.

    Source-fetch routes create raw observations only while the required APIs and
    Polygon archive history remain available. For a repeatable rebuild after
    that availability ends, preserve completed raw DuckDB warehouses and use the
    [completed-warehouse route](#alternative-rebuild-completed-raw-warehouses).

## Choose a route

| Goal | Guide |
| --- | --- |
| Match-minute mart only | [Recreate match-minute mart](recreate-match-minute-mart.md) |
| Polygon settlement mart only | [Recreate Polygon settlement mart](recreate-polygon-settlement-mart.md) |
| PMXT order-book mart only | [Recreate match-order-book mart](recreate-match-order-book-mart.md) |
| Both minute marts from public sources | Complete shared setup below, then both minute child guides |
| Both minute marts from completed raw warehouses | Shared setup + inputs, then [completed-warehouse route](#alternative-rebuild-completed-raw-warehouses) |

## Shared setup (every route)

### Clone onto the SSD

```bash
git clone https://github.com/hypertrial/oddsfox-pipeline.git
cd oddsfox-pipeline
```

Confirm that `pwd` points to the intended SSD before continuing. All commands
assume the repository itself is on that SSD.

### Put temporary and cached state below the clone

Run this block in every new shell used for the rebuild:

```bash
export ODDSFOX_STORAGE_ROOT="$PWD"
export ODDSFOX_RUNTIME_ROOT="$PWD/.cache/runtime"
export TMPDIR="$ODDSFOX_RUNTIME_ROOT/tmp"
export TMP="$TMPDIR"
export TEMP="$TMPDIR"
export XDG_CACHE_HOME="$ODDSFOX_RUNTIME_ROOT/xdg"
export UV_CACHE_DIR="$ODDSFOX_RUNTIME_ROOT/uv"
export UV_PYTHON_INSTALL_DIR="$ODDSFOX_RUNTIME_ROOT/uv-python"
export PYTHONPYCACHEPREFIX="$ODDSFOX_RUNTIME_ROOT/pycache"
export PLAYWRIGHT_BROWSERS_PATH="$ODDSFOX_RUNTIME_ROOT/ms-playwright"

mkdir -p \
  "$TMPDIR" \
  "$XDG_CACHE_HOME" \
  "$UV_CACHE_DIR" \
  "$UV_PYTHON_INSTALL_DIR" \
  "$PYTHONPYCACHEPREFIX" \
  "$PLAYWRIGHT_BROWSERS_PATH"
```

The Makefile sends its DuckDB extensions, dbt targets and logs, Dagster state,
and child-process caches below the same SSD-backed checkout.

### Install the project

```bash
uv sync --group dev
uv run make runtime-dirs
cp .env.example .env
```

Leave every schedule flag in `.env` set to `false`. The historical jobs in
these guides are unscheduled and run only when invoked.

```bash
git status --short
```

The new `.env` is ignored and should not appear. Populated schedule overlays,
reviewed Polygon manifests, and real attestations must also stay untracked.
Never run `git add -A` in this operator checkout.

## Alternative: rebuild completed raw warehouses

Use this route only when network ingestion already succeeded and you have
SSD-backed copies of both completed raw DuckDB warehouses.

`local-marts-rebuild` always requires **both** warehouse paths (match-minute
and Polygon settlement); it does not rebuild a single mart from one file.

Install and validate the schedule overlay and the reviewed Polygon
manifest/attestation first (see the child guides). Then place both warehouse
copies below `ODDSFOX_STORAGE_ROOT` and run:

```bash
mkdir -p "$PWD/.cache/operator-marts"

# Replace these two source paths with the operator's preserved warehouses.
cp "/path/to/completed-match-minute-raw.duckdb" \
  "$PWD/.cache/operator-marts/match.duckdb"
cp "/path/to/completed-polygon-settlement-raw.duckdb" \
  "$PWD/.cache/operator-marts/polygon.duckdb"

uv run make local-marts-rebuild \
  MATCH_MINUTE_REBUILD_DUCKDB_PATH="$PWD/.cache/operator-marts/match.duckdb" \
  POLYGON_SETTLEMENT_REBUILD_DUCKDB_PATH="$PWD/.cache/operator-marts/polygon.duckdb"
```

Use copies because the target full-refreshes both current dbt graphs in place.
The two files must exist at those exact paths before the command starts. The
target then verifies:

- match minute: more than zero rows, 104 matches, 248 markets, unique grain,
  and no blocking issue; and
- Polygon settlement: 39,120 rows, 104 matches, 248 propositions, unique grain,
  and publication ready.

## Final checklist

Inputs must always validate:

- `uv run make match-minute-inputs-validate` passes;
- `uv run make polygon-settlement-seed-validate` passes.

For the source-fetch route, both disposable smokes must also succeed:

- `uv run make match-minute-live-smoke` exits zero with
  `.cache/match_minute_live_smoke.duckdb`;
- `uv run make polygon-settlement-live-smoke` exits zero with
  `.cache/polygon_settlement/benchmarks/v4/live_smoke.duckdb`;
- the match-minute mart satisfies the 104-match/248-market unique-grain
  contract (30,936 rows for the reviewed completed-WC2026 source state); and
- the Polygon mart satisfies the
  39,120-row/104-match/248-proposition unique-grain contract.

For the completed-warehouse route, `uv run make local-marts-rebuild` must exit
zero against the operator-preserved raw warehouses instead of the live smokes.

The two minute marts intentionally live in separate source-specific warehouses.
The commands do not upload either warehouse or mart.

Operators must obtain and use each source under terms that apply to them.
Successful local rebuilds and exact row-count checks verify technical shape;
they are not Hypertrial certification of data rights or fitness for trading.
Public availability or an unauthenticated endpoint does not itself grant
permission to access, retain, or redistribute data. See the
[authoritative licence scope](https://github.com/hypertrial/oddsfox-pipeline/blob/main/THIRD_PARTY_NOTICES.md).

## Troubleshooting (all routes)

| Failure | What to check |
| --- | --- |
| A dbt publication/readiness assertion fails | Inspect the named quality relation. Do not bypass the gate or manually publish the candidate table. |
| A warehouse path is rejected | Keep it below the SSD-backed `ODDSFOX_STORAGE_ROOT` and make sure the file already exists for `local-marts-rebuild`. |

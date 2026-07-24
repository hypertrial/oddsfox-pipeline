# Recreate both WC2026 minute marts locally

This runbook starts with a clean clone and finishes with:

- `polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds`;
- `polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds`.

The repository contains the complete software path but no production rows.
Three operator-local inputs must be supplied:

| Input | How to obtain it |
| --- | --- |
| `dbt/seeds/wc2026_schedule_matches.csv` | Author 104 rows from the [official FIFA schedule](https://www.fifa.com/en/tournaments/mens/worldcup/canadamexicousa2026/articles/match-schedule-fixtures-results-teams-stadiums), preserving the tracked header. |
| `dbt/seeds/polymarket_wc2026_polygon_settlement_markets.csv` | Generate and review it with the repository's Polygon candidate tool. |
| `config/polygon-settlement-resolution-attestation.yml` | Generate it with the same candidate tool and install it only with its matching reviewed manifest. |

Do not commit these inputs, generated warehouses, or authoring evidence.
Operators must obtain and use each source under terms that apply to them.
Successful local rebuilds and exact row-count checks verify technical shape;
they are not Hypertrial certification of data rights or fitness for trading.
See [Operator responsibilities](../concepts/operator-responsibilities.md).

!!! important "A clean clone is necessary, but not sufficient"

    The clone supplies the pipeline, schemas, validators, and rebuild commands.
    It does not supply the 104-row schedule, the reviewed 248-proposition
    Polygon manifest, its matching attestation, or historical raw observations.

    The source-fetch route below can create the raw observations only while the
    required APIs and Polygon archive history remain available. For a
    repeatable rebuild after that availability ends, the operator must preserve
    the completed raw DuckDB warehouses and use the
    [completed-warehouse route](#alternative-rebuild-completed-raw-warehouses).

## Before you begin

You need:

- Git and [`uv`](https://docs.astral.sh/uv/getting-started/installation/);
- an SSD-backed working directory with enough space for two DuckDB warehouses;
- network access to FIFA, GitHub, Polymarket Gamma, and Polymarket CLOB;
- an authorized Polygon RPC endpoint with chain ID 137, `finalized` block
  support, historical logs, and archive-state contract calls; and
- the ability to review the generated Polygon evidence before installing it.

Docker and Colima are not required to create either mart.

There are two supported routes:

1. **From public sources:** follow Steps 1–9 below.
2. **From completed raw warehouses:** follow Steps 1–4 and 6–8 to install and
   validate the local inputs, then use the shorter
   [completed-warehouse route](#alternative-rebuild-completed-raw-warehouses).

## From a clean clone

### Step 1: clone onto the SSD

```bash
git clone https://github.com/hypertrial/oddsfox-pipeline.git
cd oddsfox-pipeline
```

Confirm that `pwd` points to the intended SSD before continuing. All commands
below assume the repository itself is on that SSD.

### Step 2: put temporary and cached state below the clone

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

### Step 3: install the project

```bash
uv sync --extra dev
uv run make runtime-dirs
cp .env.example .env
```

Leave every schedule flag in `.env` set to `false`. The two historical jobs in
this guide are unscheduled and run only when invoked.

Checkpoint:

```bash
git status --short
```

The new `.env` is ignored and should not appear.

### Step 4: create and validate the 104-match schedule overlay

Open the
[official FIFA schedule](https://www.fifa.com/en/tournaments/mens/worldcup/canadamexicousa2026/articles/match-schedule-fixtures-results-teams-stadiums)
and populate:

```text
dbt/seeds/wc2026_schedule_matches.csv
```

Keep its existing header unchanged. Supply exactly one row for each
`match_id` from 1 through 104. The repository validates this file but does not
download or redistribute a populated copy.

Use this exact header:

```text
match_id,stage,group_label,matchday,match_date,kickoff_time_et,venue,home_slot,away_slot,home_team,away_team,status,source
```

Fill the columns as follows:

| Column | Required format |
| --- | --- |
| `match_id` | Official integer match number, exactly 1–104 with no gaps or duplicates. |
| `stage` | One of `Group Stage`, `Round of 32`, `Round of 16`, `Quarter-final`, `Semi-final`, `Third-place`, or `Final`. |
| `group_label` | Group letter for group matches; empty for knockout matches. |
| `matchday` | Integer matchday from the reviewed schedule. |
| `match_date` | `YYYY-MM-DD`. |
| `kickoff_time_et` | Eastern Time using `HH:MM AM` or `HH:MM PM`, for example `03:00 PM`. |
| `venue` | Schedule venue label. |
| `home_slot`, `away_slot` | Published schedule slots; retain them even after teams are known. |
| `home_team`, `away_team` | Resolved team names used by the source markets. |
| `status` | Operator-maintained schedule status. |
| `source` | Source URL or revision used to author the row. |

The safest editing workflow is to copy the header shell to an untracked
working file, populate and review it in a spreadsheet, export it as UTF-8 CSV,
then replace the local shell at the same path. Do not add, remove, or reorder
columns.

Validate it:

```bash
uv run make match-minute-inputs-validate
```

Do not continue until the command prints:

```text
104 operator-local schedule rows
```

### Step 5: create the match-minute mart

```bash
uv run make match-minute-live-smoke
```

The job obtains:

- market inventory from the public
  [Polymarket Gamma API](https://docs.polymarket.com/api-reference/introduction);
- minute token history from the public
  [Polymarket CLOB API](https://docs.polymarket.com/market-data/overview);
- CC0 knockout fixture identity from
  [OpenFootball](https://github.com/openfootball/worldcup); and
- CC0 result validation from
  [`martj42/international_results`](https://github.com/martj42/international_results).

The job fails closed unless it maps 104 matches, 248 markets, and 496 tokens
with no blocking issue. On success, the first real mart is in:

```text
.cache/match_minute_live_smoke.duckdb
```

The relation is:

```text
polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds
```

Expected contract:

- 30,936 rows for the reviewed completed-WC2026 source state;
- 104 distinct matches;
- 248 distinct markets; and
- unique `(odds_minute_epoch, market_id)` grain.

The Make target prints and asserts the inventory and quality result. Do not
continue if it exits nonzero.

Historical API availability is not guaranteed. If Gamma or CLOB no longer
returns the complete interval, use an operator's previously completed raw
warehouse through the
[completed-warehouse route](#alternative-rebuild-completed-raw-warehouses).
The repository does not host one.

### Step 6: configure the Polygon RPC

Export the endpoint and a non-secret provider/plan label:

```bash
export POLYGON_RPC_URL="https://your-authorized-archive-rpc"
export POLYGON_RPC_PROVIDER_LABEL="provider-plan"
```

The endpoint value may contain credentials. Do not paste it into a tracked
file, terminal transcript, issue, or log. The software records only the label
and sanitized HTTPS origin.

The RPC must support:

- Polygon PoS mainnet, chain ID 137;
- `eth_getBlockByNumber` with the `finalized` block tag;
- historical `eth_getLogs` over the fixed authoring ranges; and
- historical `eth_call` at the referenced event blocks.

A free public endpoint may not provide the required archive history.

### Step 7: generate and review the Polygon inputs

Choose an unused plain SemVer and record the UTC review minute:

```bash
export POLYGON_SEED_MANIFEST_VERSION="1.0.0"
export POLYGON_SEED_REVIEWED_AT="$(date -u +%Y-%m-%dT%H:%M:00Z)"
```

Generate the candidate:

```bash
uv run make polygon-settlement-seed-candidate
```

The command refuses to overwrite an existing candidate directory. If the
version already exists, choose another unused SemVer; do not amend an existing
candidate.

The tool automatically:

1. downloads the pinned FIFA schedule and CC0 OpenFootball fixtures;
2. verifies their exact content hashes;
3. discovers the relevant Polygon question and condition events;
4. derives and verifies Yes/No token orientation;
5. verifies the standard and neg-risk contract relationships;
6. verifies finalized resolution evidence; and
7. writes a candidate manifest, attestation, and evidence report below:

```text
artifacts/polygon_settlement_seed_candidates/<version>/
```

Set a convenient path to that directory:

```bash
export POLYGON_CANDIDATE_DIR="$PWD/artifacts/polygon_settlement_seed_candidates/$POLYGON_SEED_MANIFEST_VERSION"
```

Before installing anything, confirm:

```bash
test ! -f "$POLYGON_CANDIDATE_DIR/FAILED"
test -f "$POLYGON_CANDIDATE_DIR/EVIDENCE.json"
test -f "$POLYGON_CANDIDATE_DIR/resolution_attestation.yml"
test "$(wc -l < "$POLYGON_CANDIDATE_DIR/polymarket_wc2026_polygon_settlement_markets.csv" | tr -d ' ')" = "249"
```

Review `EVIDENCE.json` and the 248 candidate rows. Approve them only when the
fixture inventory, proposition semantics, source revisions and hashes,
question/condition locators, token orientation, duplicate overrides, and
resolution evidence match the intended WC2026 scope.

### Step 8: install and validate the reviewed Polygon inputs

After review approval, copy the matching pair:

```bash
cp \
  "$POLYGON_CANDIDATE_DIR/polymarket_wc2026_polygon_settlement_markets.csv" \
  dbt/seeds/polymarket_wc2026_polygon_settlement_markets.csv

cp \
  "$POLYGON_CANDIDATE_DIR/resolution_attestation.yml" \
  config/polygon-settlement-resolution-attestation.yml
```

Validate the installed pair:

```bash
uv run make polygon-settlement-seed-validate
```

Do not continue unless it reports:

- 248 propositions;
- 248 resolved conditions;
- the selected manifest version; and
- a manifest SHA-256 matching the reviewed evidence.

### Step 9: create the Polygon settlement mart

For a new disposable warehouse, run:

```bash
POLYGON_SETTLEMENT_LIVE_SMOKE_RESET=true \
  uv run make polygon-settlement-live-smoke
```

The job validates the installed manifest and attestation, scans finalized
Polygon V2 settlement logs, publishes the raw snapshot, builds the isolated dbt
graph, and fails closed on incomplete coverage or invalid output.

On success, the second real mart is in:

```text
.cache/polygon_settlement/benchmarks/v4/live_smoke.duckdb
```

The relation is:

```text
polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds
```

Expected contract:

- exactly 39,120 rows;
- 104 distinct matches;
- 248 distinct propositions;
- unique `(proposition_id, settlement_minute_epoch)` grain; and
- `publication_ready = true`.

The Make target asserts the 39,120-row contract and exits nonzero on failure.

## Final checklist

Both marts are complete only when every item below is true:

- `uv run make match-minute-inputs-validate` passes;
- `uv run make polygon-settlement-seed-validate` passes;
- `uv run make match-minute-live-smoke` exits zero;
- `uv run make polygon-settlement-live-smoke` exits zero;
- the match-minute warehouse exists at
  `.cache/match_minute_live_smoke.duckdb`;
- the Polygon warehouse exists at
  `.cache/polygon_settlement/benchmarks/v4/live_smoke.duckdb`;
- the match-minute mart satisfies the 104-match/248-market unique-grain
  contract; and
- the Polygon mart satisfies the
  39,120-row/104-match/248-proposition unique-grain contract.

The two marts intentionally live in separate source-specific warehouses. The
commands do not upload either warehouse or mart.

Check local Git state:

```bash
git status --short
```

The populated schedule and Polygon manifest should appear only as local
modifications. The real attestation is ignored. Never run `git add -A` in this
operator checkout.

## Alternative: rebuild completed raw warehouses

Use this route only when network ingestion already succeeded and you have
SSD-backed copies of both completed raw DuckDB warehouses.

Steps 1–4 and 6–8 above still apply: the rebuild requires the complete schedule
overlay plus the reviewed Polygon manifest and matching attestation.

Place both warehouse copies below `ODDSFOX_STORAGE_ROOT`, then run:

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

Use copies when the original raw snapshots must remain immutable.

## Troubleshooting

| Failure | What to check |
| --- | --- |
| `supply a complete operator-local 104-match schedule` | The schedule must contain exactly 104 records and the integer IDs 1–104 with no duplicate or missing ID. |
| Candidate directory already exists | Use a new unused plain SemVer. Candidate evidence is immutable and is never overwritten. |
| RPC rejects `finalized` | Use a Polygon mainnet provider that implements the finalized block tag. |
| Historical `eth_getLogs` or `eth_call` fails | Use an archive-capable endpoint and confirm the provider permits the required historical ranges and request volume. |
| `FAILED` appears in the candidate directory | Do not install any candidate output. Correct the reported source/RPC/evidence failure and generate a new version. |
| Polygon seed validation reports a hash mismatch | The manifest and attestation came from different candidate runs. Reinstall one reviewed matching pair. |
| Gamma/CLOB inventory or history is incomplete | Retry only if the failure is transient; otherwise use a previously completed operator raw warehouse. |
| A dbt publication/readiness assertion fails | Inspect the named quality relation. Do not bypass the gate or manually publish the candidate table. |
| A warehouse path is rejected | Keep it below the SSD-backed `ODDSFOX_STORAGE_ROOT` and make sure the file already exists for `local-marts-rebuild`. |

Public availability, technical interoperability, or an unauthenticated
endpoint does not itself grant permission to access, retain, or redistribute
data. See the
[authoritative licence scope](https://github.com/hypertrial/oddsfox-pipeline/blob/main/THIRD_PARTY_NOTICES.md).

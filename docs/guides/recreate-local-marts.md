# Recreate the WC2026 minute marts locally

The repository distributes software and header-only input shells. It still
retains the complete ingestion, storage, orchestration, and dbt paths needed to
create both WC2026 minute marts from operator-supplied inputs:

- `polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds`
- `polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds`

Production rows remain local. Do not add them to Git, a source archive, a
Python package, a documentation build, or a container build context.

## Put all runtime storage on an SSD

Clone the repository on the SSD. Before the first `uv` command, point temporary
files and caches below that checkout:

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

uv sync --extra dev
uv run make runtime-dirs
```

The Makefile exports the same compatible paths to every child process. Its
default warehouses, Dagster state, dbt target/log directories, bytecode,
caches, and temporary files remain below the SSD-backed checkout. Polygon and
local mart rebuild targets also use SSD-local DuckDB extension directories.
`ODDSFOX_STORAGE_ROOT` is the boundary enforced by the offline mart rebuild
command.

On macOS, Colima otherwise keeps its VM below the home directory and Lima may
create DNS-forwarding sockets below `/tmp`. Keep the VM, Docker client state,
socket, and transient files on the SSD and use explicit DNS resolvers:

```bash
export ODDSFOX_PERSISTENT_RUNTIME_ROOT="/Volumes/Your SSD/oddsfox-runtime"
export COLIMA_HOME="$ODDSFOX_PERSISTENT_RUNTIME_ROOT/colima"
export DOCKER_CONFIG="$ODDSFOX_RUNTIME_ROOT/docker"
export DOCKER_HOST="unix://$COLIMA_HOME/default/docker.sock"

mkdir -p "$COLIMA_HOME" "$DOCKER_CONFIG"
printf '{"cliPluginsExtraDirs":["%s/lib/docker/cli-plugins"]}\n' \
  "$(brew --prefix)" > "$DOCKER_CONFIG/config.json"
colima start --dns 1.1.1.1 --dns 8.8.8.8
docker buildx version
```

Set `ODDSFOX_PERSISTENT_RUNTIME_ROOT` to a real directory on the SSD, outside
the checkout. Do not put `COLIMA_HOME` below `.cache/`:
`make clean-local-artifacts` intentionally removes that transient tree.
The `cliPluginsExtraDirs` entry keeps a Homebrew-installed Buildx visible after
moving `DOCKER_CONFIG`; adjust that directory when Docker plugins are installed
elsewhere.
Use the same exported environment for `uv run make release-gate`. The
`colima status` command must report the socket below `COLIMA_HOME`, and no
`lima-psl-*` directory should appear below `/tmp`.

## Supply the local inputs

Use a dedicated SSD-backed clone or worktree for production inputs. Populate
the existing paths without staging them:

| Mart | Required local input |
| --- | --- |
| Match minute | `dbt/seeds/wc2026_schedule_matches.csv` with exactly match IDs 1–104. |
| Polygon settlement | `dbt/seeds/polymarket_wc2026_polygon_settlement_markets.csv` with 248 reviewed propositions. |
| Polygon settlement | `config/polygon-settlement-resolution-attestation.yml` matching the manifest. |

The tracked CSV versions contain only their headers. The real attestation path
is ignored. The distribution policy checks the Git index rather than these
local overlays, while package and container checks still require a clean
distribution worktree.

## Create the real marts from their public-source flows

The match-minute job obtains the immutable results revision, OpenFootball
knockout fixtures, Polymarket market inventory, token histories, and fetch
audit before running the selected dbt graph:

```bash
uv run make match-minute-live-smoke
```

It writes `.cache/match_minute_live_smoke.duckdb` and fails unless all 104
matches, 248 propositions, and 496 tokens are mapped with no blocking issue.

The Polygon job requires local `POLYGON_RPC_URL` and
`POLYGON_RPC_PROVIDER_LABEL` values. It validates the 248-row manifest and its
attestation, scans finalized Polygon logs, publishes the raw snapshot, and
runs the isolated dbt graph:

```bash
POLYGON_SETTLEMENT_LIVE_SMOKE_RESET=true \
  uv run make polygon-settlement-live-smoke
```

It writes
`.cache/polygon_settlement/benchmarks/v4/live_smoke.duckdb` and fails unless
the mart has exactly 39,120 rows.

## Rebuild both marts from completed raw warehouses

Use SSD-backed copies of completed raw warehouses when the network ingestion
has already succeeded. This command runs both current dbt graphs with
`--full-refresh` and then verifies grain, inventory, publication readiness, and
the absence of blocking issues:

```bash
uv run make local-marts-rebuild \
  MATCH_MINUTE_REBUILD_DUCKDB_PATH="$PWD/.cache/operator-marts/match.duckdb" \
  POLYGON_SETTLEMENT_REBUILD_DUCKDB_PATH="$PWD/.cache/operator-marts/polygon.duckdb"
```

Both warehouse paths must already exist below `ODDSFOX_STORAGE_ROOT`. Use
copies when the original raw snapshots must remain immutable.

Expected verification:

- match minute: more than zero rows, 104 matches, 248 markets, unique
  `(odds_minute_epoch, market_id)` grain, and no blocking issue;
- Polygon settlement: 39,120 rows, 104 matches, 248 propositions, unique
  `(proposition_id, settlement_minute_epoch)` grain, and publication ready.

After the local run, restore the six tracked header shells before building a
package or container. Keep the real inputs and warehouses in ignored
SSD-backed operator storage.

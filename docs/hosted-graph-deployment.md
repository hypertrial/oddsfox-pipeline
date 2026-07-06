# Hosted WC2026 Graph Deployment

This deployment keeps durable runtime data on one host SSD directory. It uses
bind mounts, not Docker named volumes, because named-volume storage is managed by
Docker and is not guaranteed to live on the requested SSD path.

## SSD Layout

Set `ODDSFOX_DATA_DIR` to the host SSD root. The local default is:

```bash
export ODDSFOX_DATA_DIR="/Volumes/Mac SSD/hypertrial_trilemma/hypertrial/OddsFox/.runtime"
```

The deployment writes durable data here:

```text
$ODDSFOX_DATA_DIR/warehouse/oddsfox.duckdb
$ODDSFOX_DATA_DIR/artifacts/releases/<UTC_BUILD_ID>/
$ODDSFOX_DATA_DIR/artifacts/current -> releases/<UTC_BUILD_ID>
$ODDSFOX_DATA_DIR/exports/
$ODDSFOX_DATA_DIR/replay/
$ODDSFOX_DATA_DIR/dagster-home/
$ODDSFOX_DATA_DIR/dlt/
$ODDSFOX_DATA_DIR/logs/
```

`oddsfox-live` reads `/artifacts/current` and reloads when the artifact builder
repoints that symlink.

## First Deploy

From the OddsFox workspace root:

```bash
cd oddsfox-pipeline
cp deploy/hosted-graph/.env.example deploy/hosted-graph/.env
set -a
. deploy/hosted-graph/.env
set +a
mkdir -p "$ODDSFOX_DATA_DIR"/{warehouse,artifacts/releases,exports,replay,dagster-home,dlt,logs}
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml build live dash artifact-builder
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml up -d live dash
```

For production, run a refresh before starting or immediately after starting
`live`/`dash`:

```bash
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml --profile refresh run --rm artifact-builder
```

That runs the Dagster WC2026 pipeline, dbt build, graph export, graph build,
validation, and atomic publish into `$ODDSFOX_DATA_DIR/artifacts`.

## Manual Refresh

```bash
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml --profile refresh run --rm artifact-builder
```

Useful local fixture run:

```bash
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml --profile refresh run --rm artifact-builder \
  --skip-refresh \
  --skip-dbt \
  --input-parquet /exports/wc2026_graph_hourly.parquet \
  --allow-stale-current
```

Put fixture parquet files under `$ODDSFOX_DATA_DIR/exports` so the container can
read them at `/exports`.

## Scheduled Refresh

Run the hourly loop explicitly:

```bash
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml --profile refresh run -d artifact-builder --interval-seconds 3600
```

Alternatively, run the manual refresh command from host cron/systemd with the
same `.env` file.

## Rollback

List releases:

```bash
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml --profile refresh run --rm --entrypoint sh artifact-builder -lc 'ls -1 /artifacts/releases'
```

Repoint `current` to a prior release:

```bash
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml --profile refresh run --rm --entrypoint sh artifact-builder -lc \
  'ln -sfn releases/<UTC_BUILD_ID> /artifacts/current'
```

`oddsfox-live` reloads on the next `-artifact-reload-interval` tick.

## Real-Data Validation

Run one bounded current refresh into the SSD-backed warehouse:

```bash
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml --profile refresh run --rm artifact-builder
```

Then confirm the expected files exist:

```bash
ls -l "$ODDSFOX_DATA_DIR/warehouse/oddsfox.duckdb"
ls -l "$ODDSFOX_DATA_DIR/artifacts/current"/{build_manifest.json,graph_snapshot.json,knockout_artifacts.json}
```

## Health Checks

```bash
curl -fsS http://127.0.0.1:8787/api/v0/health
curl -fsS http://127.0.0.1:8787/api/v0/knockout/snapshot
curl -fsS http://127.0.0.1:8787/api/v0/graph/snapshot
```

The dashboard is served on `http://127.0.0.1:4173` by the compose example. Set
`VITE_ODDSFOX_LIVE_URL` in `deploy/hosted-graph/.env` before building `dash` for
a non-local backend URL.

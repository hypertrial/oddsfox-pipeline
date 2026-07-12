# Deploy the hosted stack

Use this guide to get from an empty local runtime directory to a visible
WC2026 dashboard. It assumes the OddsFox workspace has `oddsfox-pipeline`,
`oddsfox-graph`, `oddsfox-live`, and `oddsfox-dash` checked out as sibling
directories, with `oddsfox-pipeline` as the command root.

## Prerequisites

- Docker with Compose.
- Network access to the configured source APIs.
- A host SSD directory for durable runtime data.
- `deploy/hosted-graph/.env` copied from `.env.example`.

## Configure Runtime Storage

Set `ODDSFOX_DATA_DIR` to a durable host directory:

```bash
cd oddsfox-pipeline
cp deploy/hosted-graph/.env.example deploy/hosted-graph/.env
set -a
. deploy/hosted-graph/.env
set +a
mkdir -p "$ODDSFOX_DATA_DIR"/{warehouse,artifacts/releases,exports,replay,dagster-home,dlt,logs}
```

The deployment uses this layout:

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

## Build And Start Live Services

Set `VITE_ODDSFOX_LIVE_URL` in `deploy/hosted-graph/.env` before building the
dashboard. For local compose, use `http://localhost:8787`.

```bash
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml build live dash artifact-builder
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml up -d live dash
```

`oddsfox-live` reads `$ODDSFOX_DATA_DIR/artifacts/current` and reloads when the
artifact builder repoints that symlink.

## First Refresh

Run one refresh before or immediately after starting `live` and `dash`:

```bash
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml --profile refresh run --rm artifact-builder
```

That command runs the Dagster WC2026 pipeline, dbt build, graph export,
`oddsfox-graph` build, validation, and atomic publish into
`$ODDSFOX_DATA_DIR/artifacts`.

## Verify Artifacts And Services

Confirm the warehouse and current artifacts exist:

```bash
ls -l "$ODDSFOX_DATA_DIR/warehouse/oddsfox.duckdb"
ls -l "$ODDSFOX_DATA_DIR/artifacts/current"/{build_manifest.json,graph_snapshot.json,knockout_artifacts.json}
```

Check the live API:

```bash
curl -fsS http://127.0.0.1:8787/api/v0/health
curl -fsS http://127.0.0.1:8787/api/v0/graph/snapshot
curl -fsS http://127.0.0.1:8787/api/v0/knockout/snapshot
```

Open the dashboard at `http://127.0.0.1:4173`.

## Manual Refresh

Run the refresh command again whenever you need a fresh snapshot:

```bash
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml --profile refresh run --rm artifact-builder
```

For a local fixture parquet under `$ODDSFOX_DATA_DIR/exports`, skip source
refresh and dbt:

```bash
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml --profile refresh run --rm artifact-builder \
  --skip-refresh \
  --skip-dbt \
  --input-parquet /exports/wc2026_graph_hourly.parquet \
  --allow-stale-current
```

## Scheduled Refresh

Run the hourly refresh loop explicitly:

```bash
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml --profile refresh run -d artifact-builder --interval-seconds 3600
```

Alternatively, run the manual refresh command from host cron or systemd with the
same `.env` file.

## Rollback

List artifact releases:

```bash
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml --profile refresh run --rm --entrypoint sh artifact-builder -lc 'ls -1 /artifacts/releases'
```

Repoint `current` to a prior release:

```bash
docker compose --env-file deploy/hosted-graph/.env -f deploy/hosted-graph/docker-compose.yml --profile refresh run --rm --entrypoint sh artifact-builder -lc \
  'ln -sfn releases/<UTC_BUILD_ID> /artifacts/current'
```

`oddsfox-live` reloads on the next artifact polling tick, which defaults to
`60s`.

## If Something Looks Wrong

Use [Troubleshooting](troubleshooting.md#cross-repo-runtime-symptoms) for
dashboard, artifact, live API, and SSE symptoms.

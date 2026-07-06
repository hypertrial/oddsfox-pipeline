# Hosted WC2026 Graph Deployment

This deployment keeps all runtime state on one local artifact volume. No object
storage is required.

## Artifact Layout

```text
/artifacts/releases/<UTC_BUILD_ID>/build_manifest.json
/artifacts/releases/<UTC_BUILD_ID>/graph_snapshot.json
/artifacts/releases/<UTC_BUILD_ID>/knockout_artifacts.json
/artifacts/current -> releases/<UTC_BUILD_ID>
```

`oddsfox-live` reads `/artifacts/current` and reloads when the artifact builder
repoints that symlink.

## First Deploy

From the OddsFox workspace root:

```bash
docker compose build live dash artifact-builder
docker compose run --rm artifact-builder --skip-refresh --skip-dbt --input-parquet /path/to/wc2026_graph_hourly.parquet --allow-stale-current
docker compose up -d live dash
```

For a production refresh, omit the skip flags so the builder runs the Dagster
WC2026 full pipeline, dbt build, graph export, graph build, validation, and
atomic publish.

## Manual Refresh

```bash
docker compose run --rm artifact-builder
```

Useful local fixture run:

```bash
docker compose run --rm artifact-builder \
  --skip-refresh \
  --skip-dbt \
  --input-parquet /fixtures/wc2026_graph_hourly.parquet \
  --allow-stale-current
```

## Scheduled Refresh

The compose file keeps `artifact-builder` behind the `refresh` profile. Start
the hourly loop explicitly:

```bash
docker compose --profile refresh run -d artifact-builder --interval-seconds 3600
```

Alternatively, run the manual refresh command from host cron/systemd and keep the
same `oddsfox-artifacts` Docker volume.

## Rollback

List releases:

```bash
docker compose run --rm --entrypoint sh artifact-builder -lc 'ls -1 /artifacts/releases'
```

Repoint `current` to a prior release:

```bash
docker compose run --rm --entrypoint sh artifact-builder -lc \
  'ln -sfn releases/<UTC_BUILD_ID> /artifacts/current'
```

`oddsfox-live` reloads on the next `-artifact-reload-interval` tick.

## Health Checks

```bash
curl -fsS http://127.0.0.1:8787/api/v0/health
curl -fsS http://127.0.0.1:8787/api/v0/knockout/snapshot
curl -fsS http://127.0.0.1:8787/api/v0/graph/snapshot
```

The dashboard is served on `http://127.0.0.1:4173` by the compose example. Set
`VITE_ODDSFOX_LIVE_URL` before building `dash` for a non-local backend URL.

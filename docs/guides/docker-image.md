# Docker Image

The signed software container is an advanced release artifact, not the default
install path. Prefer the source [Quickstart](../getting-started/index.md) for
local development and first runs.

## Image

Published as `ghcr.io/hypertrial/oddsfox-pipeline`. Release manifests include
`linux/amd64` and `linux/arm64` images, SBOMs, provenance, and GitHub OIDC
signatures.

Pull a published tag when you intentionally want the container artifact:

```bash
docker pull ghcr.io/hypertrial/oddsfox-pipeline:latest
```

Prefer a release tag or digest from the GitHub Packages / release notes over
`:latest` for reproducible operator environments.

The image runs as a non-root user and defaults to a Dagster gRPC worker:

```text
dagster api grpc -h 0.0.0.0 -p 4000 -m oddsfox_pipeline.orchestration.definitions
```

Runtime paths inside the image include `/runtime/warehouse/warehouse.duckdb`
(`DUCKDB_PATH`) and `/runtime/dagster` (`DAGSTER_HOME`).

## Operator Notes

- The image contains application code and packaged dbt/config surfaces. It does
  not bundle production datasets or operator seed rows.
- Mount or otherwise supply operator-controlled warehouse, `.env`/config, and
  any populated seed overlays; a fresh container does not create a populated
  analytics warehouse by itself.
- Keep schedules disabled until you have validated the same jobs you would run
  from source.
- MIT labelling on the image covers the Hypertrial-owned application; third
  parties retain their own licences. See
  [THIRD_PARTY_NOTICES.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/THIRD_PARTY_NOTICES.md).

## Related Pages

- [Operators](../audiences/operators.md)
- [Configuration](../reference/configuration.md)
- [Orchestration](../reference/orchestration.md)

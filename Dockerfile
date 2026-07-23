FROM python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    ODDSFOX_PIPELINE_ROOT=/opt/oddsfox-pipeline \
    DUCKDB_PATH=/runtime/warehouse/warehouse.duckdb \
    DAGSTER_HOME=/runtime/dagster

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 oddsfox \
    && useradd --system --uid 10001 --gid oddsfox --home /opt/oddsfox-pipeline oddsfox

WORKDIR /opt/oddsfox-pipeline
COPY pyproject.toml README.md LICENSE THIRD_PARTY_NOTICES.md ./
COPY src ./src
COPY dbt ./dbt
COPY config ./config
COPY workspace.yaml dagster_instance.yaml ./
RUN python -m pip install --no-cache-dir . \
    && install -d -o oddsfox -g oddsfox \
      /runtime/raw /runtime/warehouse /runtime/dagster

ARG VCS_REF=unknown
LABEL org.opencontainers.image.title="OddsFox Pipeline" \
      org.opencontainers.image.description="MIT-licensed local-first analytics pipeline software; no production datasets" \
      org.opencontainers.image.source="https://github.com/hypertrial/oddsfox-pipeline" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.licenses="MIT"

USER 10001:10001
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "oddsfox_pipeline.orchestration.definitions"]

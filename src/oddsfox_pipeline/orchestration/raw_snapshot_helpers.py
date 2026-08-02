"""Shared raw-layer snapshot and dlt pipeline cache helpers."""

from __future__ import annotations

import os
from typing import Any, Callable

import dlt
from dagster import MetadataValue

from oddsfox_pipeline.storage.duckdb.observability import (
    delta_raw_layer,
    snapshot_raw_layer,
)


def _raw_snapshot_metadata(
    pre: dict[str, Any],
    post: dict[str, Any],
    delta: dict[str, Any],
    *,
    run_summary: dict[str, Any] | None = None,
) -> dict[str, MetadataValue]:
    metadata = {
        "duckdb_raw_pre": MetadataValue.json(pre),
        "duckdb_raw_post": MetadataValue.json(post),
        "duckdb_raw_delta": MetadataValue.json(delta),
    }
    if run_summary is not None:
        metadata["run_summary"] = MetadataValue.json(run_summary)
    return metadata


def _run_with_raw_snapshot(
    raw_snapshot_level: str,
    run_fn: Callable[[dict[str, Any]], dict[str, Any]],
    *,
    snapshot_raw_layer_fn: Callable[..., dict[str, Any]] = snapshot_raw_layer,
    delta_raw_layer_fn: Callable[
        [dict[str, Any], dict[str, Any]], dict[str, Any]
    ] = delta_raw_layer,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, MetadataValue],
]:
    pre = snapshot_raw_layer_fn(level=raw_snapshot_level)
    run_summary = run_fn(pre)
    post = snapshot_raw_layer_fn(level=raw_snapshot_level)
    delta = delta_raw_layer_fn(pre, post)
    return (
        run_summary,
        pre,
        post,
        delta,
        _raw_snapshot_metadata(
            pre,
            post,
            delta,
            run_summary=run_summary,
        ),
    )


_DLT_PIPELINE_BY_PATH: dict[str, dlt.Pipeline] = {}


def _dlt_pipeline_name(dataset_name: str) -> str:
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if worker:
        return f"{dataset_name}_{worker}_landing"
    return f"{dataset_name}_landing"


def get_cached_dlt_pipeline(
    *,
    dataset_name: str,
    active_duckdb_path_fn: Callable[[], Any],
    dlt_module: Any = dlt,
    pipeline_cache: dict[str, dlt.Pipeline] | None = None,
) -> dlt.Pipeline:
    cache = _DLT_PIPELINE_BY_PATH if pipeline_cache is None else pipeline_cache
    db_path = str(active_duckdb_path_fn())
    cache_key = f"{db_path}:{dataset_name}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    pipe = dlt_module.pipeline(
        pipeline_name=_dlt_pipeline_name(dataset_name),
        destination=dlt_module.destinations.duckdb(credentials=db_path),
        dataset_name=dataset_name,
    )
    cache[cache_key] = pipe
    return pipe


__all__ = [
    "_DLT_PIPELINE_BY_PATH",
    "_dlt_pipeline_name",
    "_raw_snapshot_metadata",
    "_run_with_raw_snapshot",
    "get_cached_dlt_pipeline",
]

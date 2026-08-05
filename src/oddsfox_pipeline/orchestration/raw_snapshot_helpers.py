"""Shared raw-layer snapshot helpers."""

from __future__ import annotations

from typing import Any, Callable

from dagster import MetadataValue

from oddsfox_pipeline.storage.duckdb.observability import (
    delta_raw_layer,
    snapshot_raw_layer,
)


def _snapshot_refreshed_scope_name(snapshot_metrics: dict[str, Any]) -> str | None:
    scope_name = snapshot_metrics.get("scope_name")
    return str(scope_name) if scope_name else None


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


__all__ = [
    "_raw_snapshot_metadata",
    "_run_with_raw_snapshot",
    "_snapshot_refreshed_scope_name",
]

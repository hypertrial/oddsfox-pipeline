from __future__ import annotations

import logging
from typing import Any, Callable

from oddsfox_pipeline.storage.duckdb.metadata import save_sync_run_metrics

logger = logging.getLogger(__name__)


def build_failure_metrics(
    exc: BaseException,
    *,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": "failed",
        "error_type": exc.__class__.__name__,
        "error_message": str(exc),
    }
    summary = getattr(exc, "summary", None)
    if isinstance(summary, dict):
        failure_status = summary.get("status")
        if failure_status is not None:
            payload["failure_status"] = failure_status
        payload.update(
            {key: value for key, value in summary.items() if key != "status"}
        )
    if extra:
        payload.update(extra)
    payload["status"] = "failed"
    return payload


def save_asset_failure_metrics(
    task: str,
    exc: BaseException,
    *,
    scope_name: str | None = None,
    source: str = "polymarket",
    extra: dict[str, Any] | None = None,
    save_fn: Callable[..., None] = save_sync_run_metrics,
) -> None:
    try:
        save_fn(
            task,
            build_failure_metrics(exc, extra=extra),
            scope_name=scope_name,
            source=source,
        )
    except Exception as persist_exc:
        logger.warning(
            "Could not persist failure metrics for %s: %s",
            task,
            persist_exc,
        )


__all__ = ["build_failure_metrics", "save_asset_failure_metrics"]

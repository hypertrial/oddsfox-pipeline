from __future__ import annotations

from typing import Any


def _snapshot_refreshed_scope_name(snapshot_metrics: dict[str, Any]) -> str | None:
    scope_name = snapshot_metrics.get("scope_name")
    return str(scope_name) if scope_name else None


__all__ = ["_snapshot_refreshed_scope_name"]

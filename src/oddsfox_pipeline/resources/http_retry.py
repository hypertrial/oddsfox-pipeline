"""Shared transient-HTTP-status classification and Retry-After parsing."""

from __future__ import annotations

from typing import Any, Mapping

TRANSIENT_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


def is_transient_status(status: int) -> bool:
    return status in TRANSIENT_HTTP_STATUSES or status == 0


def retry_after_seconds(resp: Any, *, cap: float = 120.0) -> float | None:
    headers: Mapping[str, str] = getattr(resp, "headers", {}) or {}
    raw = headers.get("Retry-After")
    if raw is None or not str(raw).strip():
        return None
    try:
        sec = float(str(raw).strip())
        if sec < 0:
            return None
        return min(sec, cap)
    except ValueError:
        return None


def exponential_backoff_seconds(attempt: int, *, cap: float = 30.0) -> float:
    """Shared attempt backoff for snapshot-style HTTP retry loops."""
    return min(2.0 ** max(1, int(attempt)), cap)


__all__ = [
    "TRANSIENT_HTTP_STATUSES",
    "exponential_backoff_seconds",
    "is_transient_status",
    "retry_after_seconds",
]

"""Shared helpers for market-scope registry persistence."""

from __future__ import annotations

from datetime import datetime, timezone


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_scope(scope_name: str) -> str:
    normalized = str(scope_name or "").strip().lower()
    if not normalized:
        raise ValueError("scope_name must not be empty")
    return normalized


__all__ = ["_normalize_scope", "_utc_now"]

"""Active Polymarket warehouse scope for storage-layer table routing."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

from oddsfox_pipeline.naming import SCOPE_WC2026

_active_polymarket_scope: ContextVar[str] = ContextVar(
    "active_polymarket_scope",
    default=SCOPE_WC2026,
)


def get_active_polymarket_scope() -> str:
    return _active_polymarket_scope.get()


@contextmanager
def active_polymarket_scope(scope_name: str) -> Iterator[None]:
    token = _active_polymarket_scope.set(scope_name.strip().lower())
    try:
        yield
    finally:
        _active_polymarket_scope.reset(token)


__all__ = ["active_polymarket_scope", "get_active_polymarket_scope"]

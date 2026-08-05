"""Polymarket Gamma identity helpers."""

from __future__ import annotations


def is_numeric_polymarket_id(value: str | None) -> bool:
    """True when ``value`` is a numeric Polymarket event or market ID."""
    return bool(str(value or "").strip().isdigit())

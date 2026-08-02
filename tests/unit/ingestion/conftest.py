"""Shared fixtures for Polymarket backfill unit tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from tests.unit.ingestion.backfill_test_support import (
    bf_events_fallback,
    bf_gamma,
    bf_metadata,
)


@pytest.fixture
def no_sleep_tqdm(monkeypatch):
    monkeypatch.setattr(
        bf_metadata,
        "tqdm",
        lambda *a, **k: MagicMock(__enter__=lambda s: s, __exit__=lambda *x: None),
    )
    monkeypatch.setattr(bf_gamma.time, "sleep", lambda s: None)
    monkeypatch.setattr(bf_events_fallback.time, "sleep", lambda s: None)

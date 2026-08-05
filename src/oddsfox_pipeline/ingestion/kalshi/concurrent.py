"""Bounded concurrent Kalshi fetch helpers."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, TypeVar

T = TypeVar("T")
R = TypeVar("R")

DEFAULT_KALSHI_FETCH_WORKERS = 8
logger = logging.getLogger(__name__)


def bounded_worker_count(
    item_count: int,
    *,
    max_workers: int = DEFAULT_KALSHI_FETCH_WORKERS,
) -> int:
    if item_count <= 1:
        return 1
    return min(max(1, max_workers), item_count)


def _safe_worker(
    worker: Callable[[T], R],
    item: T,
    *,
    on_error: Callable[[T, Exception], None] | None = None,
) -> R | None:
    try:
        return worker(item)
    except Exception as exc:
        logger.exception("Kalshi bounded worker failed for item=%r", item)
        if on_error is not None:
            on_error(item, exc)
        return None


def map_bounded(
    items: list[T],
    worker: Callable[[T], R],
    *,
    max_workers: int = DEFAULT_KALSHI_FETCH_WORKERS,
    on_error: Callable[[T, Exception], None] | None = None,
) -> list[R]:
    if not items:
        return []
    workers = bounded_worker_count(len(items), max_workers=max_workers)
    if workers == 1:
        return [
            result
            for item in items
            if (result := _safe_worker(worker, item, on_error=on_error)) is not None
        ]
    results: list[R | None] = [None] * len(items)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_safe_worker, worker, item, on_error=on_error): index
            for index, item in enumerate(items)
        }
        for future in as_completed(futures):
            results[futures[future]] = future.result()
    return [result for result in results if result is not None]


__all__ = ["map_bounded"]

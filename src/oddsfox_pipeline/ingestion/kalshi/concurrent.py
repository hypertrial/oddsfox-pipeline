"""Bounded concurrent Kalshi fetch helpers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, TypeVar

T = TypeVar("T")
R = TypeVar("R")

DEFAULT_KALSHI_FETCH_WORKERS = 8


def bounded_worker_count(
    item_count: int,
    *,
    max_workers: int = DEFAULT_KALSHI_FETCH_WORKERS,
) -> int:
    if item_count <= 1:
        return 1
    return min(max(1, max_workers), item_count)


def map_bounded(
    items: list[T],
    worker: Callable[[T], R],
    *,
    max_workers: int = DEFAULT_KALSHI_FETCH_WORKERS,
) -> list[R]:
    if not items:
        return []
    workers = bounded_worker_count(len(items), max_workers=max_workers)
    if workers == 1:
        return [worker(item) for item in items]
    results: list[R | None] = [None] * len(items)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(worker, item): index for index, item in enumerate(items)
        }
        for future in as_completed(futures):
            results[futures[future]] = future.result()
    return [result for result in results if result is not None]


__all__ = [
    "DEFAULT_KALSHI_FETCH_WORKERS",
    "bounded_worker_count",
    "map_bounded",
]

from __future__ import annotations

import socket
from typing import Callable, TypeVar

from dagster import RetryRequested

from oddsfox_pipeline.resources.http_retry import is_transient_status

_TRANSIENT_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    socket.timeout,
    BrokenPipeError,
)

T = TypeVar("T")


def is_transient_pipeline_error(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_EXCEPTIONS):
        return True
    status = getattr(exc, "status_code", None)
    if status is not None and is_transient_status(int(status)):
        return True
    response = getattr(exc, "response", None)
    if response is not None:
        resp_status = getattr(response, "status_code", None)
        if resp_status is not None and is_transient_status(int(resp_status)):
            return True
    retryable = getattr(exc, "retryable", None)
    return retryable is True


def raise_retry_if_transient(
    exc: BaseException,
    *,
    max_retries: int = 2,
) -> None:
    if is_transient_pipeline_error(exc):
        raise RetryRequested(max_retries=max_retries) from exc


def run_with_transient_retry(
    fn: Callable[[], T],
    *,
    max_retries: int = 2,
) -> T:
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except RetryRequested:
            raise
        except Exception as exc:
            last_error = exc
            if not is_transient_pipeline_error(exc) or attempt >= max_retries:
                raise
    assert last_error is not None
    raise last_error


__all__ = [
    "is_transient_pipeline_error",
    "raise_retry_if_transient",
    "run_with_transient_retry",
]

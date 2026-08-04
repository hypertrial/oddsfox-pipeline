from __future__ import annotations

import socket

from dagster import RetryRequested

from oddsfox_pipeline.resources.http_retry import is_transient_status

_TRANSIENT_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    socket.timeout,
    BrokenPipeError,
)


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


__all__ = [
    "is_transient_pipeline_error",
    "raise_retry_if_transient",
]

from __future__ import annotations

import socket

import requests
from dagster import RetryRequested

from oddsfox_pipeline.resources.http_retry import is_transient_status

# Builtin network types plus requests' own hierarchy. Gamma/CLOB wrap those as
# GammaRequestError/ClobRequestError with the original on __cause__, so
# classification must check both the outer and the cause.
_TRANSIENT_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    socket.timeout,
    BrokenPipeError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


def _matches_transient_type(exc: BaseException | None) -> bool:
    return exc is not None and isinstance(exc, _TRANSIENT_EXCEPTIONS)


def is_transient_pipeline_error(exc: BaseException) -> bool:
    if _matches_transient_type(exc) or _matches_transient_type(exc.__cause__):
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

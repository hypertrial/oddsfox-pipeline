from __future__ import annotations

import socket

import pytest
import requests

pytest.importorskip("dagster")
from dagster import RetryRequested

from oddsfox_pipeline.ingestion.polymarket.errors import (
    ClobRequestError,
    GammaRequestError,
    _wrap_request_error,
)
from oddsfox_pipeline.orchestration.transient_retry import (
    is_transient_pipeline_error,
    raise_retry_if_transient,
)


class _StatusError(Exception):
    def __init__(self, status_code: int) -> None:
        super().__init__(f"status={status_code}")
        self.status_code = status_code


def test_is_transient_pipeline_error_classifies_network_and_http() -> None:
    assert is_transient_pipeline_error(ConnectionError("reset"))
    assert is_transient_pipeline_error(TimeoutError())
    assert is_transient_pipeline_error(socket.timeout())
    assert is_transient_pipeline_error(_StatusError(503))
    assert not is_transient_pipeline_error(_StatusError(404))
    assert not is_transient_pipeline_error(ValueError("bad config"))
    assert not is_transient_pipeline_error(FileNotFoundError("missing warehouse"))
    assert not is_transient_pipeline_error(PermissionError("denied"))


def test_is_transient_pipeline_error_classifies_requests_timeouts() -> None:
    assert is_transient_pipeline_error(requests.exceptions.ConnectionError("reset"))
    assert is_transient_pipeline_error(requests.exceptions.Timeout("timed out"))
    assert is_transient_pipeline_error(
        requests.exceptions.ReadTimeout("read timed out")
    )
    assert is_transient_pipeline_error(
        requests.exceptions.ConnectTimeout("connect timed out")
    )


def test_is_transient_pipeline_error_classifies_wrapped_gamma_read_timeout() -> None:
    # Mirrors the live failure: urllib3 ReadTimeoutError -> requests ConnectionError
    # -> GammaRequestError via gamma_get/_wrap_request_error (no response object).
    cause = requests.exceptions.ConnectionError(
        "HTTPSConnectionPool(host='gamma-api.polymarket.com', port=443): "
        "Read timed out."
    )
    wrapped = _wrap_request_error(cause, GammaRequestError)
    assert isinstance(wrapped, GammaRequestError)
    assert wrapped.__cause__ is cause
    assert is_transient_pipeline_error(wrapped)
    with pytest.raises(RetryRequested):
        raise_retry_if_transient(wrapped)


def test_is_transient_pipeline_error_classifies_wrapped_clob_timeout() -> None:
    cause = requests.exceptions.ReadTimeout("Read timed out.")
    wrapped = _wrap_request_error(cause, ClobRequestError)
    assert isinstance(wrapped, ClobRequestError)
    assert is_transient_pipeline_error(wrapped)


def test_is_transient_pipeline_error_classifies_wrapped_chunked_encoding() -> None:
    cause = requests.exceptions.ChunkedEncodingError(
        "Connection broken: IncompleteRead(0 bytes read)"
    )
    wrapped = _wrap_request_error(cause, GammaRequestError)
    assert isinstance(wrapped, GammaRequestError)
    assert wrapped.__cause__ is cause
    assert is_transient_pipeline_error(wrapped)
    with pytest.raises(RetryRequested):
        raise_retry_if_transient(wrapped)


def test_is_transient_pipeline_error_rejects_non_transient_wrapped_request() -> None:
    # A Gamma wrap of a non-network requests error must not become transient
    # just because it is a RequestException subclass.
    cause = requests.exceptions.InvalidURL("bad url")
    wrapped = _wrap_request_error(cause, GammaRequestError)
    assert not is_transient_pipeline_error(wrapped)


def test_raise_retry_if_transient_wraps_transient_errors() -> None:
    with pytest.raises(RetryRequested):
        raise_retry_if_transient(ConnectionError("reset"))

    raise_retry_if_transient(ValueError("bad"))

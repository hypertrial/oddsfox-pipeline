from __future__ import annotations

import socket

import pytest

pytest.importorskip("dagster")
from dagster import RetryRequested

from oddsfox_pipeline.orchestration.transient_retry import (
    is_transient_pipeline_error,
    raise_retry_if_transient,
    run_with_transient_retry,
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


def test_raise_retry_if_transient_wraps_transient_errors() -> None:
    with pytest.raises(RetryRequested):
        raise_retry_if_transient(ConnectionError("reset"))

    raise_retry_if_transient(ValueError("bad"))


def test_run_with_transient_retry_retries_then_succeeds() -> None:
    attempts = {"count": 0}

    def _flaky() -> str:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise ConnectionError("reset")
        return "ok"

    assert run_with_transient_retry(_flaky, max_retries=1) == "ok"
    assert attempts["count"] == 2

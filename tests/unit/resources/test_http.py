from unittest.mock import MagicMock, patch

import pytest
import requests

from oddsfox_pipeline.resources.http import APIClient, RateLimiter
from oddsfox_pipeline.resources.http_retry import (
    TRANSIENT_HTTP_STATUSES,
    exponential_backoff_seconds,
    is_transient_status,
    retry_after_seconds,
)


def test_is_transient_status_covers_shared_set():
    for status in (408, 429, 500, 502, 503, 504):
        assert is_transient_status(status)
    assert is_transient_status(0)
    assert not is_transient_status(404)
    assert TRANSIENT_HTTP_STATUSES == frozenset({408, 429, 500, 502, 503, 504})


def test_retry_after_and_backoff_helpers():
    assert retry_after_seconds(MagicMock(headers={})) is None
    assert retry_after_seconds(MagicMock(headers={"Retry-After": " "})) is None
    assert retry_after_seconds(MagicMock(headers={"Retry-After": "-1"})) is None
    assert retry_after_seconds(MagicMock(headers={"Retry-After": "bad"})) is None
    assert retry_after_seconds(MagicMock(headers={"Retry-After": "2.5"})) == 2.5
    assert retry_after_seconds(MagicMock(headers={"Retry-After": "999"}), cap=10) == 10

    assert exponential_backoff_seconds(0) == 2.0
    assert exponential_backoff_seconds(4, cap=8.0) == 8.0


def test_rate_limiter_negative_rate_raises():
    with pytest.raises(ValueError, match="rate must be positive"):
        RateLimiter(0)


def test_rate_limiter_wait_consumes_token():
    rl = RateLimiter(1000.0)
    rl.wait()
    assert rl.tokens < rl.capacity


def test_rate_limiter_wait_sleeps_when_depleted(monkeypatch):
    rl = RateLimiter(10.0)
    rl.tokens = 0.0
    # Align with patched monotonic so elapsed is 0; otherwise last_check from __init__
    # makes elapsed huge and the bucket refills without ever calling sleep.
    rl.last_check = 1000.0
    sleeps = []

    def capture_sleep(s):
        sleeps.append(s)
        rl.tokens = rl.capacity  # unblock after one sleep

    monkeypatch.setattr("oddsfox_pipeline.resources.http.time.sleep", capture_sleep)
    monkeypatch.setattr(
        "oddsfox_pipeline.resources.http.time.monotonic", lambda: 1000.0
    )
    rl.wait()
    assert sleeps


def test_rate_limiter_set_rate_negative_raises():
    rl = RateLimiter(10.0)
    with pytest.raises(ValueError, match="new_rate must be positive"):
        rl.set_rate(0)


def test_rate_limiter_set_rate_preserves_ratio():
    rl = RateLimiter(10.0)
    rl.tokens = 5.0
    rl.set_rate(20.0)
    assert rl.get_rate() == 20.0


def test_api_client_get_with_relative_endpoint_and_params():
    session = MagicMock()
    session.get.return_value.json.return_value = {"ok": True}
    session.get.return_value.raise_for_status = MagicMock()
    client = APIClient(base_url="https://api.example.com")
    client.session = session
    out = client.get("/x", params={"b": "2", "a": "1"})
    assert out == {"ok": True}
    session.get.assert_called_once()
    url = session.get.call_args[0][0]
    assert "http" in url


def test_api_client_get_absolute_endpoint():
    session = MagicMock()
    session.get.return_value.json.return_value = []
    session.get.return_value.raise_for_status = MagicMock()
    client = APIClient(base_url="https://api.example.com")
    client.session = session
    client.get("https://other.example/full", params=None)
    session.get.assert_called_once_with(
        "https://other.example/full",
        params=None,
        headers={},
        timeout=client.request_timeout,
    )


def test_api_client_get_uses_default_timeout():
    session = MagicMock()
    session.get.return_value.json.return_value = {"ok": True}
    session.get.return_value.raise_for_status = MagicMock()
    client = APIClient(
        base_url="https://api.example.com",
        request_timeout=(2.0, 7.0),
    )
    client.session = session
    client.get("/timeout")
    assert session.get.call_args.kwargs["timeout"] == (2.0, 7.0)


def test_api_client_get_explicit_timeout_overrides_default():
    session = MagicMock()
    session.get.return_value.json.return_value = {"ok": True}
    session.get.return_value.raise_for_status = MagicMock()
    client = APIClient(
        base_url="https://api.example.com",
        request_timeout=(2.0, 7.0),
    )
    client.session = session
    client.get("/timeout", timeout=(1.0, 3.0))
    assert session.get.call_args.kwargs["timeout"] == (1.0, 3.0)


def test_api_client_rate_limiter_wait():
    rl = RateLimiter(10000.0)
    client = APIClient(base_url="https://x.com", rate_limiter=rl)
    with patch.object(rl, "wait", wraps=rl.wait) as w:
        client._wait_for_rate_limit()
        w.assert_called_once()


def test_api_client_delay_without_limiter():
    client = APIClient(base_url="https://x.com", requests_per_second=10.0)
    with patch("oddsfox_pipeline.resources.http.time.sleep") as sl:
        client.last_request_time = 0.0
        times = iter([0.0, 0.05, 0.15, 0.2, 0.25, 0.3])

        def next_time():
            return next(times)

        with patch("oddsfox_pipeline.resources.http.time.time", side_effect=next_time):
            client._wait_for_rate_limit()
            client._wait_for_rate_limit()
        assert sl.called


def test_api_client_delay_skips_sleep_when_caught_up():
    """Second wait: elapsed already exceeds delay — no sleep branch."""
    client = APIClient(base_url="https://x.com", requests_per_second=10.0)
    with patch("oddsfox_pipeline.resources.http.time.sleep") as sl:
        with patch("oddsfox_pipeline.resources.http.time.time", return_value=100.0):
            client.last_request_time = 99.0
            client._wait_for_rate_limit()
        sl.assert_not_called()


def test_api_client_http_error_propagates():
    client = APIClient(base_url="https://x.com")
    err = requests.HTTPError()
    err.response = MagicMock(status_code=500)
    client.session.get = MagicMock(side_effect=err)
    with pytest.raises(requests.HTTPError):
        client.get("/fail")


def test_rate_limiter_wait_skips_sleep_when_sleep_time_zero(monkeypatch):
    """When (1-tokens)/rate == 0, skip time.sleep and loop until tokens refill (80->69)."""
    rl = RateLimiter(1.0)
    rl.rate = float("inf")
    rl.capacity = float("inf")
    rl.tokens = 0.5
    rl.last_check = 100.0
    times = iter([100.0, 101.0])

    def mono():
        return next(times)

    monkeypatch.setattr("oddsfox_pipeline.resources.http.time.monotonic", mono)
    sleeps = []
    monkeypatch.setattr(
        "oddsfox_pipeline.resources.http.time.sleep", lambda s: sleeps.append(s)
    )
    rl.wait()
    assert all(s <= 0.0 for s in sleeps)


def test_rate_limiter_wait_skips_sleep_when_rounds_to_zero(monkeypatch):
    """Cover branch where sleep_time is 0 so the inner sleep is skipped (loop continues)."""
    rl = RateLimiter(1e300)
    rl.tokens = 1.0 - 1e-320
    rl.last_check = 0.0
    sleeps = []
    monkeypatch.setattr(
        "oddsfox_pipeline.resources.http.time.sleep", lambda s: sleeps.append(s)
    )
    # Enough monotonic samples so wait() never exhausts the iterator mid-loop.
    base = [0.0, 0.0, 1.0, 2.0, 3.0]
    it = iter(base)

    def mono():
        try:
            return next(it)
        except StopIteration:
            return base[-1] + 1.0

    monkeypatch.setattr("oddsfox_pipeline.resources.http.time.monotonic", mono)
    rl.wait()
    assert rl.tokens < rl.capacity


def test_rate_limiter_wait_zero_sleep_loops_without_sleep(monkeypatch):
    rl = RateLimiter(1.0)
    rl.rate = float("inf")
    rl.capacity = float("inf")
    rl.tokens = 0.0
    rl.last_check = 0.0
    times = iter([0.0, 1.0])
    sleeps = []
    monkeypatch.setattr(
        "oddsfox_pipeline.resources.http.time.monotonic", lambda: next(times)
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.resources.http.time.sleep", lambda value: sleeps.append(value)
    )
    rl.wait()
    assert all(s <= 0.0 for s in sleeps)

import logging
import time
from threading import Lock
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from oddsfox_pipeline.config.settings import HTTP_REQUEST_TIMEOUT
from oddsfox_pipeline.resources.http_retry import TRANSIENT_HTTP_STATUSES

logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, rate: float):
        if rate <= 0:
            raise ValueError("rate must be positive")
        self.rate = rate
        self.capacity = rate
        self.tokens = rate
        self.lock = Lock()
        self.last_check = time.monotonic()

    def wait(self):
        while True:
            sleep_time = 0.0
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last_check
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_check = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                sleep_time = (1 - self.tokens) / self.rate
            time.sleep(max(0.0, sleep_time))

    def set_rate(self, new_rate: float):
        if new_rate <= 0:
            raise ValueError("new_rate must be positive")
        with self.lock:
            ratio = (self.tokens / self.capacity) if self.capacity > 0 else 0.0
            self.rate = float(new_rate)
            self.capacity = float(new_rate)
            self.tokens = min(self.capacity, max(0.0, ratio * self.capacity))

    def get_rate(self) -> float:
        with self.lock:
            return float(self.rate)


class APIClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        retries: int = 3,
        backoff_factor: float = 1.0,
        requests_per_second: Optional[float] = None,
        rate_limiter: Optional[RateLimiter] = None,
        request_timeout: Optional[float | tuple[float, float]] = HTTP_REQUEST_TIMEOUT,
    ):
        self.base_url = base_url.rstrip("/") if base_url else ""
        self.session = requests.Session()
        self.rate_limit_lock = Lock()
        self.rate_limiter = rate_limiter
        self.request_timeout = request_timeout
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            other=retries,
            backoff_factor=backoff_factor,
            status_forcelist=tuple(sorted(TRANSIENT_HTTP_STATUSES)),
            allowed_methods=["GET", "POST", "PUT", "DELETE"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.delay = 1.0 / requests_per_second if requests_per_second else 0
        self.last_request_time = 0.0

    def _wait_for_rate_limit(self):
        if self.rate_limiter:
            self.rate_limiter.wait()
            return
        if self.delay > 0:
            with self.rate_limit_lock:
                elapsed = time.time() - self.last_request_time
                wait_time = self.delay - elapsed
                if wait_time > 0:
                    time.sleep(wait_time)
                self.last_request_time = time.time()

    def get(
        self,
        endpoint: str,
        params: dict | None = None,
        **kwargs,
    ):
        self._wait_for_rate_limit()
        url = (
            f"{self.base_url}{endpoint}"
            if self.base_url and not endpoint.startswith("http")
            else endpoint
        )
        headers = kwargs.pop("headers", {})
        if "timeout" not in kwargs and self.request_timeout is not None:
            kwargs["timeout"] = self.request_timeout
        response = self.session.get(url, params=params, headers=headers, **kwargs)
        response.raise_for_status()
        return response.json()

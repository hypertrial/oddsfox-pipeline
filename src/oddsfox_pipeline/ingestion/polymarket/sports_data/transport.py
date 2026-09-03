"""Read-only public transports with the existing rate limiter and retry policy."""

from __future__ import annotations

import json
import math
import time
from decimal import Decimal
from urllib.parse import urlparse

import requests

from oddsfox_pipeline.config.acquisition_ownership import require_acquisition_url
from oddsfox_pipeline.resources.http import APIClient, RateLimiter
from oddsfox_pipeline.resources.http_retry import (
    is_transient_status,
    retry_after_seconds,
)
from oddsfox_pipeline.resources.outbound_url import validate_outbound_https_url


def public_url(source: str, url: str) -> str:
    parsed = urlparse(url)
    if (
        parsed.username
        or parsed.password
        or parsed.fragment
        or parsed.port not in (None, 443)
    ):
        raise ValueError(
            "public acquisition requires a standard credential-free origin"
        )
    return validate_outbound_https_url(require_acquisition_url(source, url))


class PublicHTTP:
    def __init__(self, source: str, base: str, rate: float = 5):
        if not math.isfinite(rate) or rate < 1:
            raise ValueError("shared token-bucket transport requires rate >= 1")
        self.source, self.base = source, public_url(source, base)
        self.client = APIClient(
            base_url=base, source_id=source, retries=0, rate_limiter=RateLimiter(rate)
        )
        self.client.session.trust_env = (
            False  # No implicit netrc credentials or proxies.
        )
        self.received_at = None

    def request(self, method: str, url: str, **kwargs):
        if method not in ("GET", "HEAD"):
            raise ValueError("read-only acquisition")
        url = public_url(self.source, url)
        for attempt in range(4):
            self.client._wait_for_rate_limit()
            try:
                response = self.client.session.request(
                    method,
                    url,
                    timeout=(10, 45),
                    allow_redirects=False,
                    stream=True,
                    **kwargs,
                )
            except requests.RequestException:
                if attempt == 3:
                    raise
                time.sleep(min(10, 2**attempt))
                continue
            if 300 <= response.status_code < 400:
                response.close()
                raise ValueError("unexpected public-source redirect")
            if is_transient_status(response.status_code) and attempt < 3:
                delay = retry_after_seconds(response)
                response.close()
                time.sleep(min(30, delay if delay is not None else 2**attempt))
                continue
            return response
        raise RuntimeError("unreachable retry state")

    def get(self, endpoint: str, params=None):
        from oddsfox_pipeline.ingestion.polymarket.sports_data.storage import now

        with self.request("GET", self.base + endpoint, params=params) as response:
            response.raise_for_status()
            chunks, size = [], 0
            for chunk in response.iter_content(1024**2):
                size += len(chunk)
                if size > 64 * 1024**2:
                    raise ValueError("metadata response exceeds 64 MiB")
                chunks.append(chunk)
            self.received_at = now()
            return json.loads(
                b"".join(chunks),
                parse_float=Decimal,
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
            )

    def close(self):
        self.client.session.close()

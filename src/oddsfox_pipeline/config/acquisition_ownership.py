"""Deny-by-default registry for Pipeline runtime acquisition."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final
from urllib.parse import urlparse


@dataclass(frozen=True, slots=True)
class AcquisitionSource:
    source_id: str
    owner: str
    purpose: str
    allowed_hosts: frozenset[str]
    api_class: str


ACQUISITION_SOURCES: Final = {
    source.source_id: source
    for source in (
        AcquisitionSource(
            "polymarket",
            "oddsfox-pipeline",
            "public prediction-market catalog, prices, and books",
            frozenset(
                {
                    "gamma-api.polymarket.com",
                    "clob.polymarket.com",
                    "data-api.polymarket.com",
                }
            ),
            "prediction_market_api",
        ),
        AcquisitionSource(
            "pmxt",
            "oddsfox-pipeline",
            "prediction-market historical order books and trades",
            frozenset({"api.pmxt.dev"}),
            "prediction_market_api",
        ),
        AcquisitionSource(
            "kalshi",
            "oddsfox-pipeline",
            "public prediction-market events, markets, and candlesticks",
            frozenset(
                {
                    "api.elections.kalshi.com",
                    "api.kalshi.com",
                    "external-api.kalshi.com",
                }
            ),
            "prediction_market_api",
        ),
        AcquisitionSource(
            "polygon",
            "oddsfox-pipeline",
            "operator-selected Polygon JSON-RPC settlement evidence",
            frozenset(),
            "prediction_market_chain",
        ),
    )
}


def require_acquisition_url(source_id: str, url: str) -> str:
    """Reject unregistered sources and unexpected fixed-provider hosts."""
    try:
        source = ACQUISITION_SOURCES[source_id]
    except KeyError as exc:
        raise ValueError(
            f"Pipeline does not own acquisition source {source_id!r}"
        ) from exc
    host = (urlparse(url).hostname or "").casefold()
    if source.allowed_hosts and host not in source.allowed_hosts:
        raise ValueError(f"host {host!r} is not registered for {source_id!r}")
    return url


__all__ = ["ACQUISITION_SOURCES", "AcquisitionSource", "require_acquisition_url"]

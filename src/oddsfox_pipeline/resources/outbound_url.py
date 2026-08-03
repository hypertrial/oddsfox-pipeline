"""Validate outbound HTTP(S) URLs before ingestion fetches."""

from __future__ import annotations

import ipaddress
import posixpath
import socket
from functools import lru_cache
from urllib.parse import urljoin, urlparse


class OutboundUrlError(ValueError):
    """Raised when an outbound URL fails security validation."""


def _origin_key(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        raise OutboundUrlError(f"URL missing host: {url!r}")
    host_key = host.lower()
    port = parsed.port
    if port is not None and port not in (443, 80):
        return f"{host_key}:{port}"
    return host_key


def _host_from_parsed(parsed) -> str:
    host = parsed.hostname
    if not host:
        raise OutboundUrlError(f"URL missing host: {parsed.geturl()!r}")
    return host


def _is_non_public_ip(addr: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return not addr.is_global or addr.is_multicast


def _reject_non_public_ip_literal(host: str) -> None:
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return
    if _is_non_public_ip(addr):
        raise OutboundUrlError(f"URL host {host!r} resolves to a non-public address")


@lru_cache(maxsize=256)
def _resolve_host_must_be_public(host: str) -> None:
    _reject_non_public_ip_literal(host)
    if host.endswith("."):
        host = host[:-1]
    try:
        infos = socket.getaddrinfo(
            host,
            None,
            type=socket.SOCK_STREAM,
            proto=socket.IPPROTO_TCP,
        )
    except socket.gaierror as exc:
        raise OutboundUrlError(f"URL host {host!r} could not be resolved") from exc
    if not infos:
        raise OutboundUrlError(f"URL host {host!r} could not be resolved")
    for info in infos:
        sockaddr = info[4]
        if not sockaddr:
            continue
        ip_str = sockaddr[0]
        try:
            addr = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if _is_non_public_ip(addr):
            raise OutboundUrlError(
                f"URL host {host!r} resolves to non-public address {ip_str!r}"
            )


def clear_outbound_url_host_cache() -> None:
    """Clear cached public DNS validation (tests only)."""
    _resolve_host_must_be_public.cache_clear()


def validate_outbound_https_url(url: str) -> str:
    """Require HTTPS and a publicly routable target (literal or resolved)."""
    raw = url.strip()
    if not raw:
        raise OutboundUrlError("URL must be non-empty")
    parsed = urlparse(raw)
    if parsed.scheme.lower() != "https":
        raise OutboundUrlError(f"URL must use https scheme, got {parsed.scheme!r}")
    if not parsed.netloc:
        raise OutboundUrlError(f"URL missing authority: {raw!r}")
    host = _host_from_parsed(parsed)
    _resolve_host_must_be_public(host)
    return raw


def assert_same_origin(url: str, base_url: str) -> str:
    """Return url when its origin matches base_url."""
    validated = validate_outbound_https_url(url)
    base_origin = _origin_key(validate_outbound_https_url(base_url))
    if _origin_key(validated) != base_origin:
        raise OutboundUrlError(
            f"URL origin {_origin_key(validated)!r} does not match "
            f"base origin {base_origin!r}"
        )
    return validated


def join_under_base(base_url: str, href: str) -> str:
    """Join href under base_url; reject foreign absolute or protocol-relative URLs."""
    base = validate_outbound_https_url(base_url)
    base_origin = _origin_key(base)
    link = href.strip()
    if not link:
        raise OutboundUrlError("href must be non-empty")
    if link.startswith("//"):
        raise OutboundUrlError("protocol-relative href is not allowed")
    parsed_href = urlparse(link)
    if parsed_href.scheme:
        if parsed_href.scheme.lower() != "https":
            raise OutboundUrlError(
                f"absolute href must use https scheme, got {parsed_href.scheme!r}"
            )
        return assert_same_origin(link, base)
    joined = urljoin(base + "/", link.removeprefix("/"))
    base_path = posixpath.normpath(urlparse(base).path or "/")
    joined_path = posixpath.normpath(urlparse(joined).path or "/")
    if (
        not joined_path.startswith(base_path.rstrip("/") + "/")
        and joined_path != base_path
    ):
        raise OutboundUrlError(
            f"URL path {joined_path!r} escapes base path {base_path!r}"
        )
    # This defensive branch is unreachable after foreign links are rejected.
    # Its message therefore has no observable behavior for accepted inputs.
    # pragma: no mutate start
    if _origin_key(joined) != base_origin:  # pragma: no cover
        raise OutboundUrlError(
            f"URL origin {_origin_key(joined)!r} does not match "
            f"base origin {base_origin!r}"
        )
    # pragma: no mutate end
    return joined

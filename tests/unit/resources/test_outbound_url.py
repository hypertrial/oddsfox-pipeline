import socket
from unittest.mock import patch
from urllib.parse import urlparse

import pytest
from hypothesis import given
from hypothesis import strategies as st

from oddsfox_pipeline.resources.outbound_url import (
    OutboundUrlError,
    _host_from_parsed,
    _origin_key,
    _reject_non_public_ip_literal,
    _resolve_host_must_be_public,
    assert_same_origin,
    clear_outbound_url_host_cache,
    join_under_base,
    validate_outbound_https_url,
)


@pytest.fixture(autouse=True)
def _clear_dns_cache():
    clear_outbound_url_host_cache()
    yield
    clear_outbound_url_host_cache()


def _mock_public_dns(monkeypatch, public_host: str = "example.com") -> None:
    public_hosts = {public_host, "other.example"}

    def fake_getaddrinfo(host, *args, **kwargs):
        if host in public_hosts:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)


def test_validate_outbound_https_url_accepts_public_https(monkeypatch):
    _mock_public_dns(monkeypatch)

    assert validate_outbound_https_url("https://example.com/path") == (
        "https://example.com/path"
    )


@given(st.from_regex(r"[a-z][a-z0-9-]{0,16}", fullmatch=True))
def test_validate_outbound_https_url_property_accepts_public_https(label):
    host = f"{label}.example.com"

    def fake_getaddrinfo(resolved_host, *args, **kwargs):
        assert resolved_host == host
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]

    clear_outbound_url_host_cache()
    with patch("socket.getaddrinfo", fake_getaddrinfo):
        assert validate_outbound_https_url(f"https://{host}/data.csv") == (
            f"https://{host}/data.csv"
        )
    clear_outbound_url_host_cache()


@given(st.sampled_from(("127.0.0.1", "10.0.0.1", "169.254.1.1", "0.0.0.0")))
def test_validate_outbound_https_url_property_rejects_non_public_ip_literals(ip):
    with pytest.raises(OutboundUrlError):
        validate_outbound_https_url(f"https://{ip}/data.csv")


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/path",
        "file:///tmp/data.csv",
        "https://127.0.0.1/data.csv",
        "https://10.0.0.1/data.csv",
        "https://",
        "  ",
    ],
)
def test_validate_outbound_https_url_rejects_unsafe_targets(monkeypatch, url):
    _mock_public_dns(monkeypatch)

    with pytest.raises(OutboundUrlError):
        validate_outbound_https_url(url)


def test_assert_same_origin_accepts_matching_origin(monkeypatch):
    _mock_public_dns(monkeypatch)

    assert (
        assert_same_origin(
            "https://example.com/a.csv",
            "https://example.com/root",
        )
        == "https://example.com/a.csv"
    )


def test_origin_key_includes_non_default_port_and_requires_host():
    assert _origin_key("https://example.com:8443/x") == "example.com:8443"
    with pytest.raises(OutboundUrlError, match="missing host"):
        _origin_key("https:///missing")

    with pytest.raises(OutboundUrlError, match="missing host"):
        _host_from_parsed(urlparse("https:///missing"))

    _reject_non_public_ip_literal("93.184.216.34")


def test_assert_same_origin_rejects_other_origin(monkeypatch):
    _mock_public_dns(monkeypatch)

    with pytest.raises(OutboundUrlError, match="does not match"):
        assert_same_origin("https://other.example/x", "https://example.com/root")


def test_join_under_base_accepts_relative_path(monkeypatch):
    _mock_public_dns(monkeypatch)

    assert join_under_base("https://example.com/root", "/data/file.csv") == (
        "https://example.com/root/data/file.csv"
    )


def test_join_under_base_accepts_same_origin_absolute_href(monkeypatch):
    _mock_public_dns(monkeypatch)

    assert (
        join_under_base(
            "https://example.com/root",
            "https://example.com/data/file.csv",
        )
        == "https://example.com/data/file.csv"
    )


def test_join_under_base_rejects_empty_href(monkeypatch):
    _mock_public_dns(monkeypatch)

    with pytest.raises(OutboundUrlError, match="href must be non-empty"):
        join_under_base("https://example.com", " ")


def test_join_under_base_rejects_protocol_relative(monkeypatch):
    _mock_public_dns(monkeypatch)

    with pytest.raises(OutboundUrlError, match="protocol-relative"):
        join_under_base("https://example.com", "//evil.example/file.csv")


def test_join_under_base_rejects_non_https_absolute_href(monkeypatch):
    _mock_public_dns(monkeypatch)

    with pytest.raises(OutboundUrlError, match="absolute href"):
        join_under_base("https://example.com", "http://example.com/file.csv")


def test_resolve_host_public_dns_edge_branches(monkeypatch):
    def fake_getaddrinfo(host, *args, **kwargs):
        if host == "empty.example":
            return []
        if host == "mixed.example":
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ()),
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("not-an-ip", 443)),
                (
                    socket.AF_INET,
                    socket.SOCK_STREAM,
                    6,
                    "",
                    ("93.184.216.34", 443),
                ),
            ]
        raise socket.gaierror("no host")

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    _resolve_host_must_be_public("mixed.example.")
    with pytest.raises(OutboundUrlError, match="could not be resolved"):
        _resolve_host_must_be_public("empty.example")
    with pytest.raises(OutboundUrlError, match="could not be resolved"):
        _resolve_host_must_be_public("missing.example")


def test_resolve_host_rejects_non_public_dns(monkeypatch):
    def fake_getaddrinfo(host, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    with pytest.raises(OutboundUrlError, match="non-public address"):
        validate_outbound_https_url("https://private.example/data.csv")

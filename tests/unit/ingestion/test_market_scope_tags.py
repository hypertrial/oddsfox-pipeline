"""Unit tests for WC 2026 Gamma tag discovery."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests

from oddsfox_pipeline.ingestion.polymarket.errors import GammaRequestError
from oddsfox_pipeline.ingestion.polymarket.market_scope_tags import (
    discover_market_scope_tag_slugs,
    fetch_gamma_sports,
    fetch_gamma_tag_by_slug,
    fetch_gamma_tags,
    tag_matches_keywords,
)


def test_tag_matches_keywords_world_cup():
    assert tag_matches_keywords(
        {"label": "FIFA World Cup", "slug": "fifa-world-cup"},
        ("fifa", "world cup"),
    )
    assert not tag_matches_keywords(
        {"label": "Premier League", "slug": "epl"},
        ("fifa", "world cup"),
    )


def test_fetch_gamma_tag_by_slug_returns_payload():
    client = MagicMock()
    client.get.return_value = {
        "id": "102232",
        "slug": "fifa-world-cup",
        "label": "FIFA World Cup",
    }
    tag = fetch_gamma_tag_by_slug(client, "fifa-world-cup")
    assert tag is not None
    assert tag["id"] == "102232"


def test_tag_matches_keywords_market_scope_specific_terms():
    assert tag_matches_keywords(
        {"label": "World Cup Qualifiers", "slug": "world-cup-qualifiers"},
        ("world-cup-qualifiers",),
    )
    assert tag_matches_keywords(
        {"label": "WC 2026", "slug": "wc-2026"},
        ("wc-2026",),
    )
    assert not tag_matches_keywords(
        {"label": "Premier League", "slug": "epl"},
        ("world-cup-qualifiers", "wc-2026"),
    )


def test_discover_market_scope_tag_slugs_unions_seed_and_list():
    client = MagicMock()

    def _get(endpoint, **kwargs):
        if endpoint == "/tags/slug/fifa-world-cup":
            return {"id": "102232", "slug": "fifa-world-cup", "label": "FIFA World Cup"}
        if endpoint == "/tags":
            return [
                {"id": "519", "slug": "world-cup", "label": "world cup"},
                {"id": "999", "slug": "epl", "label": "Premier League"},
            ]
        if endpoint == "/sports":
            return [{"id": 1, "sport": "wc", "tags": "519,999"}]
        return []

    client.get.side_effect = _get
    result = discover_market_scope_tag_slugs(
        client,
        seed_slugs=["fifa-world-cup"],
        keywords=("world cup", "fifa"),
    )
    assert "fifa-world-cup" in result.tag_slugs
    assert "world-cup" in result.tag_slugs
    assert "epl" not in result.tag_slugs
    assert result.sources["world-cup"] == ("sports", "tags_list")


def test_tag_matches_keywords_rejects_empty_blob():
    assert not tag_matches_keywords({}, ("fifa",))


def test_fetch_gamma_tag_by_slug_missing_returns_none():
    client = MagicMock()
    client.get.return_value = {}
    assert fetch_gamma_tag_by_slug(client, "fifa-world-cup") is None


def test_fetch_gamma_tag_by_slug_invalid_slug_returns_none():
    assert fetch_gamma_tag_by_slug(MagicMock(), "bad slug!") is None


def test_fetch_gamma_tag_by_slug_404_returns_none():
    client = MagicMock()
    response = MagicMock(status_code=404)
    client.get.side_effect = GammaRequestError(response=response)
    assert fetch_gamma_tag_by_slug(client, "missing-tag") is None


def test_fetch_gamma_tag_by_slug_reraises_non_404_errors():
    client = MagicMock()
    response = MagicMock(status_code=500)
    client.get.side_effect = GammaRequestError(response=response)
    with pytest.raises(GammaRequestError):
        fetch_gamma_tag_by_slug(client, "fifa-world-cup")

    client.get.side_effect = requests.RequestException("network down")
    with pytest.raises(requests.RequestException):
        fetch_gamma_tag_by_slug(client, "fifa-world-cup")


def test_fetch_gamma_tag_by_slug_reraises_direct_request_exception(monkeypatch):
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.market_scope_tags.gamma_get",
        lambda *_a, **_k: (_ for _ in ()).throw(
            requests.RequestException("network down")
        ),
    )

    with pytest.raises(requests.RequestException):
        fetch_gamma_tag_by_slug(MagicMock(), "fifa-world-cup")


def test_fetch_gamma_tags_and_sports():
    client = MagicMock()

    def _get(endpoint, **kwargs):
        if endpoint == "/tags":
            return [{"id": "1", "slug": "fifa-world-cup", "label": "FIFA"}]
        if endpoint == "/sports":
            return [{"tags": "1,2"}, {"tags": ["3"]}]
        return []

    client.get.side_effect = _get
    tags = fetch_gamma_tags(client, limit=50)
    sports = fetch_gamma_sports(client)
    assert tags[0]["slug"] == "fifa-world-cup"
    assert len(sports) == 2


def test_discover_tolerates_api_failures():
    client = MagicMock()
    client.get.side_effect = requests.RequestException("network down")
    result = discover_market_scope_tag_slugs(client, seed_slugs=["fifa-world-cup"])
    assert result.tag_slugs == ("fifa-world-cup",)


def test_discover_skips_invalid_seed_slug():
    client = MagicMock()
    client.get.return_value = []
    result = discover_market_scope_tag_slugs(client, seed_slugs=["bad slug!"])
    assert result.tag_slugs == ()


def test_discover_handles_empty_seed_id_invalid_slugs_and_sports_lists():
    client = MagicMock()

    def _get(endpoint, **kwargs):
        if endpoint == "/tags/slug/seed-tag":
            return {"id": "", "slug": "seed-tag", "label": "FIFA"}
        if endpoint == "/tags":
            return [
                {"id": "1", "slug": "world-cup", "label": "World Cup"},
                {"slug": "fifa-tag", "label": "FIFA"},
                {"id": "2", "slug": "bad slug!", "label": "World Cup"},
                {"id": "3", "slug": "bad sports slug!", "label": "World Cup"},
            ]
        if endpoint == "/sports":
            return [{"tags": ["1", "3", "missing"]}, {"tags": None}]
        return []

    client.get.side_effect = _get
    result = discover_market_scope_tag_slugs(
        client,
        seed_slugs=["seed-tag"],
        keywords=("world cup", "fifa"),
    )

    assert "seed-tag" in result.tag_slugs
    assert "fifa-tag" in result.tag_slugs
    assert "world-cup" in result.tag_slugs
    assert "bad sports slug!" not in result.tag_slugs


def test_discover_skips_invalid_slug_returned_from_seed_lookup():
    client = MagicMock()

    def _get(endpoint, **kwargs):
        if endpoint == "/tags/slug/seed-tag":
            return {"id": "1", "slug": "bad slug!", "label": "FIFA"}
        return []

    client.get.side_effect = _get
    result = discover_market_scope_tag_slugs(
        client,
        seed_slugs=["seed-tag"],
        keywords=("fifa",),
    )

    assert result.tag_slugs == ("seed-tag",)


def test_discover_keeps_seed_when_patched_seed_lookup_has_empty_id(monkeypatch):
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.market_scope_tags.fetch_gamma_tag_by_slug",
        lambda *_a, **_k: {"id": "", "slug": "seed-tag"},
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.market_scope_tags.fetch_gamma_tags",
        lambda *_a, **_k: [],
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.polymarket.market_scope_tags.fetch_gamma_sports",
        lambda *_a, **_k: [],
    )

    result = discover_market_scope_tag_slugs(
        MagicMock(),
        seed_slugs=["seed-tag"],
        keywords=("fifa",),
    )

    assert result.tag_slugs == ("seed-tag",)
    assert result.tag_ids == ()

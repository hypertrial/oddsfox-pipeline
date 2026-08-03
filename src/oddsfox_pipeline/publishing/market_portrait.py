"""Deterministic private bundle builder for World Cup market portraits.

The public boundary deliberately accepts football facts rather than importing
the private collector.  It performs read-only queries against a completed PMXT
scan and publishes a content-addressed directory.
"""

from __future__ import annotations

from oddsfox_pipeline.publishing import market_portrait_export as _export
from oddsfox_pipeline.publishing import market_portrait_story as _story

BUNDLE_CONTRACT_VERSION = _story.BUNDLE_CONTRACT_VERSION
FootballEvent = _story.FootballEvent
MatchFacts = _story.MatchFacts
RenderProfile = _story.RenderProfile
build_story = _story.build_story
build_market_portrait_bundle = _export.build_market_portrait_bundle

_decimal = _story._decimal
_landscape_roles = _story._landscape_roles
_validate_story = _story._validate_story
_fetch_rows = _export._fetch_rows

__all__ = [
    "BUNDLE_CONTRACT_VERSION",
    "FootballEvent",
    "MatchFacts",
    "RenderProfile",
    "build_market_portrait_bundle",
    "build_story",
]

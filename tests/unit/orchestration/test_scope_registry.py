from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from hypothesis import given
from hypothesis import strategies as st

from oddsfox_pipeline.ingestion.kalshi.series_scope.config import (
    default_market_scopes_seed_path as kalshi_scope_seed_path,
)
from oddsfox_pipeline.ingestion.polymarket.market_scope.config import (
    default_market_scopes_seed_path as polymarket_scope_seed_path,
)
from oddsfox_pipeline.naming import SOURCE_KALSHI, SOURCE_POLYMARKET
from oddsfox_pipeline.orchestration.definitions import defs
from oddsfox_pipeline.orchestration.scope_registry import (
    KALSHI_WC2026_SCOPE,
    POLYMARKET_US_MIDTERMS_2026_SCOPE,
    POLYMARKET_WC2026_SCOPE,
    SCOPE_STEPS,
    SHIPPED_SCOPE_SPECS,
    ScopeSpec,
    get_scope_spec,
    iter_scope_specs,
    scope_dbt_config,
    scope_spec_index,
)

_KNOWN_SCOPE_ALIASES = {alias for spec in SHIPPED_SCOPE_SPECS for alias in spec.aliases}


def test_scope_registry_lookup_accepts_keys_and_namespace_aliases():
    assert get_scope_spec("polymarket:wc2026") is POLYMARKET_WC2026_SCOPE
    assert get_scope_spec(" polymarket_wc2026 ") is POLYMARKET_WC2026_SCOPE
    assert get_scope_spec("polymarket_us_midterms_2026") is (
        POLYMARKET_US_MIDTERMS_2026_SCOPE
    )
    assert get_scope_spec("kalshi:wc2026") is KALSHI_WC2026_SCOPE
    assert iter_scope_specs() == SHIPPED_SCOPE_SPECS
    assert iter_scope_specs(source=SOURCE_POLYMARKET) == (
        POLYMARKET_WC2026_SCOPE,
        POLYMARKET_US_MIDTERMS_2026_SCOPE,
    )
    assert iter_scope_specs(source=SOURCE_KALSHI) == (KALSHI_WC2026_SCOPE,)


@given(st.sampled_from(SHIPPED_SCOPE_SPECS), st.booleans())
def test_scope_registry_lookup_property_accepts_all_aliases(spec, use_namespace):
    alias = spec.namespace if use_namespace else spec.key

    assert get_scope_spec(f" {alias} ") is spec


@given(
    st.from_regex(r"unknown_[a-z0-9_]{1,32}", fullmatch=True).filter(
        lambda ref: ref not in _KNOWN_SCOPE_ALIASES
    )
)
def test_scope_registry_lookup_property_rejects_unknown_aliases(ref):
    with pytest.raises(ValueError, match="Unknown scope"):
        get_scope_spec(ref)


def test_scope_registry_rejects_unknown_and_duplicate_aliases():
    with pytest.raises(ValueError, match="Unknown scope"):
        get_scope_spec("polymarket:missing")

    duplicate = ScopeSpec(
        source=POLYMARKET_WC2026_SCOPE.source,
        scope=POLYMARKET_WC2026_SCOPE.scope,
        label="Duplicate",
        registry_job_name="duplicate_registry",
        odds_job_name="duplicate_odds",
        dbt_job_name="duplicate_dbt",
        full_job_name="duplicate_full",
        dbt_select="tag:duplicate",
    )
    with pytest.raises(ValueError, match="Duplicate scope alias"):
        scope_spec_index([POLYMARKET_WC2026_SCOPE, duplicate])


def test_scope_specs_define_fixed_steps_jobs_and_dbt_config():
    for spec in SHIPPED_SCOPE_SPECS:
        assert spec.key in spec.aliases
        assert spec.namespace in spec.aliases
        assert spec.supported_steps == SCOPE_STEPS
        assert scope_dbt_config(spec.key) == {
            "dbt_select": spec.dbt_select,
            "dbt_exclude": spec.dbt_exclude,
        }
        for step in SCOPE_STEPS:
            assert defs.resolve_job_def(spec.job_for_step(step)).name


def test_scope_specs_are_backed_by_source_seed_entries():
    seeds = {
        "polymarket.market_scopes": yaml.safe_load(
            polymarket_scope_seed_path().read_text(encoding="utf-8")
        ),
        "kalshi.market_scopes": yaml.safe_load(
            kalshi_scope_seed_path().read_text(encoding="utf-8")
        ),
    }

    for spec in SHIPPED_SCOPE_SPECS:
        assert spec.source_seed is not None
        assert spec.scope in seeds[spec.source_seed]["scopes"]


def test_scope_specs_are_documented():
    repo_root = Path(__file__).resolve().parents[3]
    docs = "\n".join(
        [
            (repo_root / "docs" / "quickstart.md").read_text(encoding="utf-8"),
            (repo_root / "docs" / "operations.md").read_text(encoding="utf-8"),
            (repo_root / "docs" / "scripts.md").read_text(encoding="utf-8"),
        ]
    )

    for spec in SHIPPED_SCOPE_SPECS:
        assert spec.key in docs
        assert spec.namespace in docs

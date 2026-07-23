"""Static registry of shipped source/scope orchestration surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal

from oddsfox_pipeline.naming import (
    KALSHI_WC2026,
    POLYMARKET_US_MIDTERMS_2026,
    POLYMARKET_WC2026,
    SCOPE_US_MIDTERMS_2026,
    SCOPE_WC2026,
    SOURCE_KALSHI,
    SOURCE_POLYMARKET,
    flat_name,
)

ScopeStep = Literal["registry", "odds", "dbt", "full"]
SCOPE_STEPS: tuple[ScopeStep, ...] = ("registry", "odds", "dbt", "full")


@dataclass(frozen=True)
class ScopeSpec:
    source: str
    scope: str
    label: str
    registry_job_name: str
    odds_job_name: str
    dbt_job_name: str
    full_job_name: str
    dbt_select: str
    dbt_exclude: str | None = None
    source_seed: str | None = None
    includes_international_results: bool = False

    @property
    def key(self) -> str:
        return f"{self.source}:{self.scope}"

    @property
    def namespace(self) -> str:
        return flat_name(self.source, self.scope)

    @property
    def aliases(self) -> tuple[str, str]:
        return (self.key, self.namespace)

    @property
    def supported_steps(self) -> tuple[ScopeStep, ...]:
        return SCOPE_STEPS

    def job_for_step(self, step: ScopeStep) -> str:
        return {
            "registry": self.registry_job_name,
            "odds": self.odds_job_name,
            "dbt": self.dbt_job_name,
            "full": self.full_job_name,
        }[step]


POLYMARKET_WC2026_SCOPE = ScopeSpec(
    source=SOURCE_POLYMARKET,
    scope=SCOPE_WC2026,
    label="Polymarket WC2026",
    registry_job_name="polymarket_wc2026_market_registry_refresh",
    odds_job_name="polymarket_wc2026_hourly_odds_ingest",
    dbt_job_name="polymarket_wc2026_dbt_build",
    full_job_name="polymarket_wc2026_full_pipeline",
    dbt_select="+tag:polymarket,tag:wc2026",
    dbt_exclude=(
        "tag:cross_domain wc2026_fixtures wc2026_schedule_matches "
        "wc2026_team_canonical_aliases tag:polygon_settlement"
    ),
    source_seed="polymarket.market_scopes",
    includes_international_results=True,
)
POLYMARKET_US_MIDTERMS_2026_SCOPE = ScopeSpec(
    source=SOURCE_POLYMARKET,
    scope=SCOPE_US_MIDTERMS_2026,
    label="Polymarket US midterms 2026",
    registry_job_name="polymarket_us_midterms_2026_market_registry_refresh",
    odds_job_name="polymarket_us_midterms_2026_hourly_odds_ingest",
    dbt_job_name="polymarket_us_midterms_2026_dbt_build",
    full_job_name="polymarket_us_midterms_2026_full_pipeline",
    dbt_select="tag:us_midterms_2026",
    source_seed="polymarket.market_scopes",
)
KALSHI_WC2026_SCOPE = ScopeSpec(
    source=SOURCE_KALSHI,
    scope=SCOPE_WC2026,
    label="Kalshi WC2026",
    registry_job_name="kalshi_wc2026_market_registry_refresh",
    odds_job_name="kalshi_wc2026_hourly_odds_ingest",
    dbt_job_name="kalshi_wc2026_dbt_build",
    full_job_name="kalshi_wc2026_full_pipeline",
    dbt_select="+tag:kalshi",
    dbt_exclude="tag:cross_domain tag:polymarket",
    source_seed="kalshi.market_scopes",
    includes_international_results=True,
)

SHIPPED_SCOPE_SPECS: tuple[ScopeSpec, ...] = (
    POLYMARKET_WC2026_SCOPE,
    POLYMARKET_US_MIDTERMS_2026_SCOPE,
    KALSHI_WC2026_SCOPE,
)


def scope_spec_index(specs: Iterable[ScopeSpec]) -> dict[str, ScopeSpec]:
    index: dict[str, ScopeSpec] = {}
    for spec in specs:
        for alias in spec.aliases:
            previous = index.get(alias)
            if previous is not None:
                raise ValueError(
                    f"Duplicate scope alias {alias!r}: {previous.key} and {spec.key}"
                )
            index[alias] = spec
    return index


_SCOPE_INDEX = scope_spec_index(SHIPPED_SCOPE_SPECS)


def iter_scope_specs(*, source: str | None = None) -> tuple[ScopeSpec, ...]:
    if source is None:
        return SHIPPED_SCOPE_SPECS
    return tuple(spec for spec in SHIPPED_SCOPE_SPECS if spec.source == source)


def get_scope_spec(ref: str) -> ScopeSpec:
    ref = ref.strip()
    try:
        return _SCOPE_INDEX[ref]
    except KeyError as exc:
        known = ", ".join(spec.key for spec in SHIPPED_SCOPE_SPECS)
        raise ValueError(f"Unknown scope {ref!r}; expected one of: {known}") from exc


def scope_dbt_config(ref: str) -> dict[str, str | None]:
    spec = get_scope_spec(ref)
    return {"dbt_select": spec.dbt_select, "dbt_exclude": spec.dbt_exclude}


__all__ = [
    "KALSHI_WC2026",
    "KALSHI_WC2026_SCOPE",
    "POLYMARKET_US_MIDTERMS_2026",
    "POLYMARKET_US_MIDTERMS_2026_SCOPE",
    "POLYMARKET_WC2026",
    "POLYMARKET_WC2026_SCOPE",
    "SCOPE_STEPS",
    "SHIPPED_SCOPE_SPECS",
    "ScopeSpec",
    "ScopeStep",
    "get_scope_spec",
    "iter_scope_specs",
    "scope_dbt_config",
    "scope_spec_index",
]

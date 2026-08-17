# Integration

This guide is for downstream tools that read OddsFox Pipeline outputs. It does
not cover private strategy internals or order execution. Repository roles and
the execution boundary live in [System overview](system-overview.md).

## Allowed Inputs

| Consume | Notes |
| --- | --- |
| Public `*_marts` relations | Supported query API per pipeline (`polymarket_wc2026_marts`, `polymarket_soccer_marts`, `kalshi_wc2026_marts`, `international_results_wc2026_marts`; isolated advanced marts only from dedicated backfills). Start with [Data contracts](../reference/data-contracts.md) and the [Data dictionary](../reference/data-dictionary.md). |
| `*_observability` | Optional trust and freshness checks before analysis. |
| Strategy / raw.v1 consumers only | Private canonical snapshots and the strategy clean-data set (`wc2026.v1`); see [Strategy contracts](../reference/strategy-contracts.md). |

Vocabulary: [Terminology](../reference/terminology.md).

## Do Not Treat As APIs

- `*_raw`, `*_ops`, staging, and intermediate schemas
- Operator-local Polygon audit bundles or technical exports as substitutes for
  the strategy contract `wc2026.v1`
- Dagster UI state or local script side effects

## Versioning Expectations

OddsFox Pipeline is `v0.2.x`. Public marts and Dagster asset keys may break
between releases. Breaking changes belong in
[CHANGELOG.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/CHANGELOG.md)
and [Data contracts](../reference/data-contracts.md). Do not assume long-term
semver stability for warehouse layouts.

## Analyst Shortcut

If you only need SQL against an existing warehouse, use the
[Analysts](../audiences/analysts.md) hub instead of this page.

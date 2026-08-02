# Integration

This guide is for downstream tools that read OddsFox Pipeline outputs. It does
not cover private strategy internals or order execution.

## Allowed Inputs

| Consume | Notes |
| --- | --- |
| Public `*_marts` relations | Supported query API; start with [Data contracts](../reference/data-contracts.md) and the [Data dictionary](../reference/data-dictionary.md). |
| Cross-platform knockout match mart | `wc2026_marts.wc2026_knockout_match_hourly_odds` and related public WC2026 analytics metadata. |
| WC2026 logical-v1 bundle | Seven-file static logical input for `oddsfox-graph`; produce with `scripts/export_polymarket_wc2026_logical_bundle.py` and validate `manifest.json` (see the [logical-atlas runbook](../guides/build-wc2026-logical-atlas.md)). |
| `*_observability` | Optional trust and freshness checks before analysis. |
| Strategy / raw.v1 consumers only | Private canonical snapshots and the strategy clean-data set (`wc2026.v1`); see [Strategy contracts](../reference/strategy-contracts.md). |

Vocabulary: [Terminology](../reference/terminology.md).

## Do Not Treat As APIs

- `*_raw`, `*_ops`, staging, and intermediate schemas
- Operator-local Polygon audit bundles or technical exports as substitutes for
  the strategy contract `wc2026.v1`
- Dagster UI state or local script side effects

## Versioning Expectations

OddsFox Pipeline is `v0.1.x`. Public marts and Dagster asset keys may break
between releases. Breaking changes belong in
[CHANGELOG.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/CHANGELOG.md)
and [Data contracts](../reference/data-contracts.md). Do not assume long-term
semver stability for warehouse layouts.

## Execution Boundary

Pipeline and logical-atlas outputs are not execution inputs unless a separate
control plane converts them into an admitted explicit intent for
`oddsfox-execution`. This repository never contains strategy or execution code.

See [System overview](system-overview.md) for repository roles.

## Analyst Shortcut

If you only need SQL against an existing warehouse, use the
[Analysts](../audiences/analysts.md) hub instead of this page.

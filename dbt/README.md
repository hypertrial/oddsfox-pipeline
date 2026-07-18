# OddsFox Pipeline dbt Project

This dbt project models Polymarket and Kalshi WC2026 data, FIFA-numbered
knockout fixtures, FIFA World Cup fixture/results data, and US midterms odds in
DuckDB.

See the operator docs for warehouse details:

- [Warehouse](../docs/reference/warehouse.md)
- [Orchestration](../docs/reference/orchestration.md)

Modeled layers:

- `polymarket_wc2026_staging`
- `polymarket_wc2026_intermediate`
- `polymarket_wc2026_marts`
- `polymarket_wc2026_observability`
- `international_results_wc2026_staging`
- `international_results_wc2026_intermediate`
- `international_results_wc2026_marts`
- `international_results_wc2026_observability`
- `openfootball_wc2026_staging`
- `wc2026_intermediate`
- `wc2026_marts`
- `wc2026_observability`

Run locally:

```bash
dbt parse --project-dir dbt --profiles-dir dbt/profiles
dbt build --full-refresh --project-dir dbt --profiles-dir dbt/profiles
```

WC2026 scoping is encoded in the model graph and
`polymarket_wc2026_ops.market_scope_registry`; real-team validation comes from
`international_results_wc2026_team_status`. There is no dbt scope-selection var.

Public knockout marts:

- `polymarket_wc2026_knockout_market_tokens`
- `polymarket_wc2026_knockout_token_hourly_odds`
- `polymarket_wc2026_knockout_markets`
- `international_results_wc2026_matches`
- `international_results_wc2026_team_status`
- `wc2026_knockout_match_hourly_odds`

The stable strategy surface is contract version `wc2026.v1` in
`wc2026_marts.contract_metadata`. Its concise relation aliases include
`fixtures`, `results`, `team_identities`, point-in-time player/team/club/travel
features, venue/token identity, current and historical price/liquidity,
international match history, and source provenance. Strategy readiness is
published in
`wc2026_observability.wc2026_strategy_input_readiness`.

Private canonical sources are optional for public builds. The project creates
schema-correct empty raw tables and publishes explicit availability/blocking
rows; it does not ship private collectors, payloads, URLs, or fixtures. Raw
snapshot rows and provenance remain append-only, while strategy-facing marts
use only the latest ledger-declared complete snapshot for each private source.
Readiness requires that latest payload to contain rows.

If a local DuckDB file still has deleted broad marts or older relation types,
reset the local warehouse or drop the affected dbt schemas before rebuilding.

`polymarket_wc2026_knockout_token_hourly_odds` is a public view over the private
incremental `int_polymarket_wc2026_token_hourly_odds` hourly price fact.

`wc2026_knockout_match_hourly_odds` uses permanent incremental platform facts
and raw hourly closes for team advancement. It excludes FIFA match 103 and does
not forward-fill or renormalize missing platform-side hours.

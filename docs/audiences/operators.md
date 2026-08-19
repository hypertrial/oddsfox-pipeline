# Operators

Use this hub to run, validate, and maintain a local OddsFox Pipeline warehouse.
Schedules stay disabled until manual jobs and dbt builds are healthy.

## Path

1. **First run** — [Quickstart](../getting-started/index.md) (Polymarket or Kalshi
   WC2026 tabs).
2. **Other scopes** — [Choose a scope](../getting-started/choose-a-scope.md) and
   [Run a scope](../guides/run-a-scope.md).
3. **Day-two** — [Day-two operations](../guides/day-two-operations.md).
4. **Recover** — [Validate and recover](../guides/validate-and-recover.md) and
   [Troubleshooting](../guides/troubleshooting.md).

## Credentials And Inputs

| Scope or pipeline | Network / credentials | Operator-local inputs |
| --- | --- | --- |
| `polymarket:wc2026` | Public Gamma/CLOB; CLOB auth optional unless a live job requires it | `.env` only for the ordinary full run |
| `polymarket:soccer` | Public Gamma/CLOB | `.env` only; the daily schedule is stopped by default |
| `kalshi:wc2026` | Public trade API; no API credentials | `.env` only |
| FIFA / international results | Public CSV feeds pulled by WC2026 jobs | `.env` only |
| Match-minute odds (mature, isolated) | Live APIs or completed raw warehouse | Populated schedule overlay (tracked shell) |
| Match order book (mature, isolated) | Live APIs or completed raw warehouse; PMXT API key | Reviewed target manifest for match 95 |
| Market portrait (mature, isolated) | Completed order-book + trades scan; PMXT API key | Reviewed `TARGET_MANIFEST` for one approved match |
| Polygon settlement (advanced) | Finalized-capable Polygon JSON-RPC | Reviewed 248-row manifest + resolution attestation (tracked seed is a header-only shell) |
| Soccer pre-match Elo (manual) | Pinned CC0 result snapshots; optional benchmark acquisition | Exact target Parquet, reviewed team identity map, optional ClubElo/EloRatings snapshot |

Never commit `.env`, operator seed rows, reviewed attestations, DuckDB files, or
exports. See [Operator responsibilities](../concepts/operator-responsibilities.md),
[Scope and non-goals](../concepts/scope-and-non-goals.md), and
[dbt/seeds/README.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/dbt/seeds/README.md).

For pre-match Elo, first acquire and inspect the tracked source catalog. The
inspection must report zero unparsed scored lines before release construction.
Keep normalized rows and aliases below ignored `artifacts/`; do not promote
fuzzy candidates without review. The release command refuses a dirty Git tree,
an already-used version, a different target SHA, or incomplete event
accounting.

```bash
make pre-match-elo-acquire
make pre-match-elo-inspect

make pre-match-elo-identity-prepare \
  PRE_MATCH_ELO_TARGET_PARQUET=/absolute/path/to/polymarket_soccer_match_result_minute_odds_modeling.parquet
# Inspect alias_review.csv and target_dispositions.csv before recording review.
make pre-match-elo-identity-review
make pre-match-elo-identity-compile
make pre-match-elo-identity-audit \
  PRE_MATCH_ELO_TARGET_PARQUET=/absolute/path/to/polymarket_soccer_match_result_minute_odds_modeling.parquet

make pre-match-elo-release \
  PRE_MATCH_ELO_TARGET_PARQUET=/absolute/path/to/polymarket_soccer_match_result_minute_odds_modeling.parquet
```

Preparation creates deterministic source-local identities and at most five
evidence-ranked candidates per target label. Review accepts exact or
fixture-supported aliases, creates pool-safe target-only identities when no
history is proven, and keeps normalized labels with conflicting contexts
ambiguous. Compilation fails on stale, unreviewed, contradictory, or cross-pool
decisions. Audit calculates the release in temporary storage and retains event,
coverage, unresolved, and mixed-component reports without claiming publication.

For an optional benchmark file, normalize columns to `system`, `team_id`,
`rating`, `as_of_date`, `snapshot_id`, `mapping_method`, and `is_pre_match`, then
set `PRE_MATCH_ELO_BENCHMARK_PATH`. ClubElo observations must predate the match.
Only reconstructed EloRatings pre-match rows may use the match date itself.

## Confirm Success

After a first Polymarket WC2026 full run you should have `oddsfox.duckdb` with
`polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds`. After a Kalshi
full run, confirm the stage and group-winner marts plus shared FIFA fixtures.
Those local checks verify technical shape;
they are not Hypertrial certification of data rights or fitness for trading.
See [Operator responsibilities](../concepts/operator-responsibilities.md).
Query with [Query the warehouse](../guides/query-the-warehouse.md) or hand off
to an [analyst](analysts.md).

## Advanced

These are optional. They are not part of the default quickstart. Start at
[Advanced pipelines](../guides/advanced-pipelines.md) for the decision tree and
maturity tiers.

| Topic | Page |
| --- | --- |
| Isolated WC2026 paths (minute, order book, portrait, Polygon) | [Advanced pipelines](../guides/advanced-pipelines.md) |
| Enable hourly schedules | [Enable schedules](../guides/enable-schedules.md) |
| Shared rebuild setup (minute + Polygon) | [Recreate local marts](../guides/recreate-local-marts.md) |
| Golden mart Parquet export | [Scripts](../reference/scripts.md) (`export_polymarket_wc2026_market_hourly_odds.py`) |
| Registry hygiene cleanup | [Scripts](../reference/scripts.md) / [Troubleshooting](../guides/troubleshooting.md#tests-writing-to-production-warehouse) (`cleanup-polymarket-wc2026-registry-hygiene`) |
| Configuration reference | [Configuration](../reference/configuration.md) |

## See also

- [Quickstart](../getting-started/index.md)
- [Advanced pipelines](../guides/advanced-pipelines.md)
- [Day-two operations](../guides/day-two-operations.md)
- [Troubleshooting](../guides/troubleshooting.md)
- [Operator responsibilities](../concepts/operator-responsibilities.md)

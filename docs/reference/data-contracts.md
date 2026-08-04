# Data Contracts

This page is the formal analytics **contract** for warehouse marts that
notebooks, scripts, and open-source integrators should rely on: grains, scope
rules, and guarantees. A **contract** is a named guarantee about a relation set,
bundle, or collector format; see [Terminology](terminology.md#guarantee).
OddsFox Pipeline is a prediction-market pipeline; the current documented marts
are WC2026 Polymarket market hourly odds, Kalshi WC2026 stage and group-winner
odds, plus WC2026 FIFA fixtures/results used by shared validation and isolated
match pipelines. Model-level column docs and tests live in the dbt project.

!!! note "Reference ladder"

    Chooser → dictionary → documented contracts → warehouse reference; do not treat
    staging/raw as APIs. Start with
    [Query the warehouse](../guides/query-the-warehouse.md), then the
    [Data dictionary](data-dictionary.md). For private `oddsfox.raw.v1` snapshots
    and the strategy clean-data relation set, see
    [Strategy contracts](strategy-contracts.md).

## Documented Marts

“Public” on this page historically meant a supported warehouse query contract.
Prefer **mart** or **documented mart**. It does not mean that every relation is
sanitized or intended for external distribution; the Polygon settlement mart
has a separate allowlisted exporter.

Schema: `polymarket_wc2026_marts`

| Relation | Grain | Pipeline | Contract |
| --- | --- | --- | --- |
| `polymarket_wc2026_market_hourly_odds` | One row per `(market_id, odds_hour_epoch)` | Polymarket WC2026 | Golden WC2026 hourly odds mart. Every market under a sticky event-volume-eligible WC2026 event (reported lifetime volume at or above the pipeline policy floor, currently $100,000 USD) with Yes-outcome CLOB prices in `[0, 1]`, full lifetime hourly OHLC history, and comprehensive market and enclosing-event metadata. |
| `polymarket_wc2026_match_minute_odds` | One row per `(odds_minute_utc, market_id)` | Match-minute odds | Dense in-game minute OHLC for 216 group moneyline markets and 32 knockout advance/win markets across FIFA match IDs 1–104. |
| `polymarket_wc2026_match_order_book` | One row per `(fifa_match_id, market_id, clob_token_id, snapshot_timestamp_ms, snapshot_sha256, book_side, level_rank)` | Match order book; market portrait | Every bid and ask level from every PMXT historical L2 snapshot in the reviewed Argentina–Egypt match-95 market window. |
| `polymarket_wc2026_polygon_settlement_minute_odds` | One row per `(proposition_id, settlement_minute_utc)` | Polygon settlement history | Finalized Polygon V2 settlement-time OHLC/VWAP over fixed half-open scheduled windows; exactly 39,120 dense rows. |

`polymarket_wc2026_match_order_book_states` and `polymarket_wc2026_match_trades`
are additional `polymarket_wc2026_marts` tables built only by the market-portrait
pipeline as bundle inputs; they are not independently documented contracts. See
[Market portrait](market-portrait.md).

The match-minute contract contains 248 markets and 496 source tokens. Group
rows preserve each binary market's literal Yes and No tokens for `home_win`,
`draw`, or `away_win`; a group No price is the proposition's logical
complement, not necessarily an opponent win. Knockout rows are oriented to the
official fixture: Yes is the home-team outcome token and No is the away-team
outcome token. Match 103 means winning the third-place match. Match 104 means
winning the final and becoming champion.

FIFA match numbers and kickoff context come from the audited schedule and
OpenFootball fixtures, while team names and home/away orientation are reconciled
to one 104-row `international_results_wc2026_matches` snapshot fetched from the
latest immutable Git revision affecting `results.csv`. Every public row carries
the matched results ID, revision, exact-payload SHA-256, and load time. Missing,
mixed, malformed, duplicate, or unmatched provenance blocks publication.

Minute spines include the minute containing Gamma `startTime` through the
minute containing the primary match event's `finishedTimestamp`. Observations
are first filtered to the exact timestamp interval. Yes and No open, high, low,
close, average, point count, and first/last source times are raw probabilities
in `[0, 1]`. Missing minute observations remain null; the mart does not
forward-fill, normalize, convert to decimal odds, or calculate `1 - price`.
`elapsed_window_minute` is the uncapped zero-based difference from the truncated
Gamma start bucket. It remains contiguous through weather delays, halftime,
extra time, and penalties, so it is a wall-clock analysis axis rather than the
official football match clock. UTC timestamps remain authoritative.
`minute_status` distinguishes complete rows from incomplete start, finish, both,
or interior buckets. The inclusive final-whistle bucket can legitimately be null
when Gamma emitted no observation in that partial minute; it is measured but is
not itself a quality-warning row.

Close-pair, cadence, and timing diagnostics never alter prices. Warnings use
fixed strict-greater-than thresholds: close-pair deviation `0.05`, observation
gap or first/last boundary offset 120 seconds, scheduled-to-actual kickoff shift
60 minutes, group window 150 minutes, and knockout window 210 minutes. A token
with one distinct in-game price and every incomplete interior minute are also
warnings. These source anomalies remain publishable. Structural inventory,
mapping, timing, provenance, fetch-audit, token-history, price/OHLC, or spine
failures block publication. Spine validation requires each market's elapsed axis
to start at zero, remain nonnegative and contiguous, match every UTC bucket
offset, and end at the truncated Gamma finish offset.

The supported publication path is
`polymarket_wc2026_match_minute_odds_backfill`. It rejects empty or partial live
inventories before fetching and the dbt publication gate preserves the prior
public table unless all 104 games, 248 markets, 496 tokens, timing windows, and
per-token in-game histories validate. It also refreshes and validates the latest
104 international-results rows before publication. Each attempted fetch run
keeps 496 append-only token audit rows; a successful run publishes one exact raw
snapshot, while failed runs preserve the prior raw and public tables. The job has
no schedule.

### PMXT historical match order book

`polymarket_wc2026_marts.polymarket_wc2026_match_order_book` initially covers
only FIFA match 95, Argentina–Egypt, market `2793969`, from
`2026-07-04T10:34:02Z` through `2026-07-07T18:18:44Z`, inclusive. The market is
`soccer_team_to_advance`; both pinned outcome-token streams are retained
independently. Rows are not synchronized, paired, forward-filled, sampled onto
a fixed cadence, or converted into complementary prices.

“Full order book” means every bid and ask level from every complete historical
L2 snapshot returned by PMXT across demonstrably unsaturated ranges. It does
not mean individual order events or a claim of fixed snapshot cadence. Empty
snapshot books remain in private raw/audit coverage and intentionally emit no
public level rows.

The public grain is
`fifa_match_id + market_id + clob_token_id + snapshot_timestamp_ms +
snapshot_sha256 + book_side + level_rank`. `snapshot_sha256` distinguishes
different books at the same token millisecond. Bids rank from highest to lowest
price; asks rank from lowest to highest. Exact `DECIMAL(38,18)` price and size
fields feed `level_notional`, `cumulative_size`, and `cumulative_notional`,
calculated independently per side.

Every row also exposes match/event/market/condition/outcome/token identity,
UTC and epoch-millisecond snapshot time, optional `order_count`, snapshot best
bid and ask, spread, midpoint, last-trade price, negative-risk flag, published
scan ID, manifest hash, source label, and ingestion time. Missing best sides
produce null spread/midpoint; no price is synthesized.

Publication requires one published manifest-consistent scan, both expected
tokens, complete terminal window trees, nonempty snapshot inventories, exact
OpenFootball match-95 team/stage identity, valid JSON/numerics, exact raw-to-
exploded level counts, unique grain/prices, and consistent ranks/cumulative
depth. Empty books, crossed books, and gaps over six hours are warnings only.
The only supported writer is the unscheduled
`polymarket_wc2026_match_order_book_backfill`.

### Polygon settlement minute odds

`polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds` is a
parallel historical contract at one row per `(proposition_id,
settlement_minute_utc)`. It does not replace or alias the Gamma/CLOB match-minute
mart.

The inventory is fixed and dense:

- 216 group propositions × the half-open scheduled window
  `[kickoff, kickoff + 150 minutes)` = 32,400 rows;
- 32 knockout propositions × `[kickoff, kickoff + 210 minutes)` = 6,720 rows;
- 248 propositions, 496 oriented tokens, 104 FIFA match IDs, and exactly 39,120
  mart rows.

Runtime identity and semantics come exclusively from a complete operator-local
Polygon market seed. Group propositions are `home_win`, `draw`, and `away_win`;
matches 73–102 are `home_advances`, match 103 is `home_win_third_place`, and
match 104 is `home_wins_final`. The seed pins the source revision and hashes,
on-chain question/condition locators, ancillary-data hash, verified token
orientation, exchange, manifest version, and review time. The backfill does not
call Gamma, CLOB, the Polymarket UI, OpenFootball, international-results, or
private match-event collectors.

The matching resolution attestation is also operator-local. The authoring tool
writes candidate evidence below ignored `artifacts/`; operators review it and
supply the final attestation at
`config/polygon-settlement-resolution-attestation.yml`. The repository tracks
only a placeholder example.

The independent fixture vocabulary is not a CLOB-mart join key. Cross-pipeline
analysis must join on `condition_id` and the oriented token IDs, then use
`yes_represents`/`no_represents` for meaning. Do not join on raw team strings or
`(fifa_match_id, proposition_type)`, because independently sourced fixture
orientation and aliases can differ.

Correcting a local market seed requires regenerating and reviewing its
supporting evidence before building a new immutable local audit/export SemVer.
An existing local artifact version is never amended in place.

For each Yes and No side the mart exposes chain-ordered open/high/low/close,
VWAP (`sum(gross_collateral) / sum(shares)`), normalized and derived economic-leg
counts, share and gross-collateral volume, first/last settlement timestamp, and
an observed flag. `minute_status` is `both_observed`, `yes_only`, `no_only`, or
`no_fills`. Empty minutes retain null prices and timestamps with zero counts and
volumes. Prices are never forward-filled, interpolated, pair-normalized, or
inferred as complements.

These times are finalized Polygon event-block timestamps. They are not order
matching times, quotes, order-book snapshots, or CLOB price history. A
normalized economic leg is not necessarily one unique user trade. Complementary
MINT/MERGE legs are included, explicitly flagged, and counted separately. Fees
are neither subtracted nor published. For mixed MINT/MERGE settlement, V2 emits
the active order's requested maker-asset fill before refunding any unused active
collateral (BUY) or shares (SELL). The normalizer reconciles the received asset
exactly, requires passive legs to consume no more than the active maker asset,
and excludes that non-trade refund surplus from fill rows and public outputs.

Mart materialization fails closed for seed/inventory errors, a missing or stale
published scan, target ranges that do not represent both fixed V2 exchanges,
incomplete/overlapping finalized coverage, an empty canonical scan, invalid or
duplicate normalized fills, invalid price/volume/OHLC, a broken 150/210-minute
axis, or any row count other than 39,120. Whole propositions or token sides
without fills, sparse minutes, derived-fill prevalence, Yes/No pair deviations,
and missing/disagreeing secondary RPC verification are technical warnings only.

The mart is an internal audit surface, not the allowlisted technical export. In addition to
the fields described above it contains these eight audit-only columns:
`settlement_minute_epoch`, `condition_id`, `yes_token_id`, `no_token_id`,
`market_structure`, `exchange_address`, `manifest_sha256`, and
`manifest_version`. A direct mart export bypasses the technical allowlist.

#### Complete column contract

Types below are the materialized DuckDB types. “Required” describes the
publication contract rather than a physical DuckDB `NOT NULL` constraint.
Prices are USDC.e collateral per outcome share and are validated in `[0, 1]`.
All timestamps are UTC without a stored timezone suffix.

Identity, schedule, and provenance:

| Column | Type | Contract |
| --- | --- | --- |
| `proposition_id` | `VARCHAR` | Required stable authored identifier; one of 248 propositions. |
| `fifa_match_id` | `INTEGER` | Required FIFA schedule identifier in `1..104`. |
| `stage` | `VARCHAR` | Required: `group_stage`, `round_of_32`, `round_of_16`, `quarterfinal`, `semifinal`, `third_place`, or `final`. |
| `group_name` | `VARCHAR` | OpenFootball group label for group-stage matches; null for knockout matches. |
| `home_team` | `VARCHAR` | Required independently sourced fixture home/display team; not a cross-pipeline join key. |
| `away_team` | `VARCHAR` | Required independently sourced fixture away/display team; not a cross-pipeline join key. |
| `proposition_type` | `VARCHAR` | Required: `home_win`, `draw`, `away_win`, `home_advances`, `home_win_third_place`, or `home_wins_final`. |
| `yes_represents` | `VARCHAR` | Required authored meaning of the oriented Yes token. |
| `no_represents` | `VARCHAR` | Required authored meaning of the oriented No token. |
| `scheduled_kickoff_at_utc` | `TIMESTAMP` | Required minute-aligned scheduled kickoff from the pinned fixture source. |
| `analysis_window_start_at_utc` | `TIMESTAMP` | Required inclusive window start; equal to scheduled kickoff. |
| `analysis_window_end_at_utc` | `TIMESTAMP` | Required exclusive window end; start plus 150 minutes for group propositions or 210 minutes for knockout propositions. |
| `settlement_minute_utc` | `TIMESTAMP` | Required UTC minute bucket in `[analysis_window_start_at_utc, analysis_window_end_at_utc)`. |
| `settlement_minute_epoch` | `BIGINT` | Required Unix seconds for `settlement_minute_utc`; always minute-aligned. |
| `elapsed_window_minute` | `BIGINT` | Required zero-based scheduled-window index: `0..149` for group propositions or `0..209` for knockout propositions. |
| `condition_id` | `VARCHAR` | Required canonical 32-byte Polygon condition ID; use with oriented token IDs for cross-pipeline reconciliation. |
| `yes_token_id` | `VARCHAR` | Required decimal ConditionalTokens position ID oriented to `yes_represents`. |
| `no_token_id` | `VARCHAR` | Required decimal ConditionalTokens position ID oriented to `no_represents`. |
| `market_structure` | `VARCHAR` | Required `neg_risk` for the 216 group propositions or `standard` for the 32 knockout propositions. |
| `exchange_address` | `VARCHAR` | Required lower-case Polygon V2 exchange address: neg-risk `0xe2222d279d744050d28e00520010520000310f59` or standard `0xe111180000d2663c0091e4f400237545b87b996b`. |
| `manifest_sha256` | `VARCHAR` | Required SHA-256 of the complete reviewed 248-row market manifest used by the published scan. |
| `manifest_version` | `VARCHAR` | Required semantic version of that reviewed manifest. |

Yes-side minute aggregates:

| Column | Type | Contract |
| --- | --- | --- |
| `yes_open` | `DECIMAL(38,18)` | First Yes normalized leg in chain order; null when `yes_observed = false`. |
| `yes_high` | `DECIMAL(38,18)` | Maximum Yes normalized-leg price; null when unobserved. |
| `yes_low` | `DECIMAL(38,18)` | Minimum Yes normalized-leg price; null when unobserved. |
| `yes_close` | `DECIMAL(38,18)` | Last Yes normalized leg in chain order; null when unobserved. |
| `yes_vwap` | `DECIMAL(38,18)` | `sum(gross_collateral) / sum(shares)`, rounded deterministically half-even to 18 decimal places; null when unobserved. |
| `yes_normalized_fill_count` | `BIGINT` | Count of normalized Yes economic legs, including derived counterparts; zero when unobserved. |
| `yes_derived_fill_count` | `BIGINT` | Subset of normalized Yes legs derived as MINT/MERGE counterparts; between zero and `yes_normalized_fill_count`. |
| `yes_share_volume` | `DECIMAL(38,6)` | Sum of normalized Yes outcome shares; zero when unobserved. |
| `yes_gross_collateral_volume` | `DECIMAL(38,6)` | Sum of Yes gross USDC.e collateral before fees; zero when unobserved. |
| `yes_first_settlement_at_utc` | `TIMESTAMP` | Earliest finalized event-block timestamp contributing to the minute; null when unobserved. |
| `yes_last_settlement_at_utc` | `TIMESTAMP` | Latest finalized event-block timestamp contributing to the minute; null when unobserved. |
| `yes_observed` | `BOOLEAN` | True when at least one normalized Yes leg exists in the minute. |

No-side minute aggregates:

| Column | Type | Contract |
| --- | --- | --- |
| `no_open` | `DECIMAL(38,18)` | First No normalized leg in chain order; null when `no_observed = false`. |
| `no_high` | `DECIMAL(38,18)` | Maximum No normalized-leg price; null when unobserved. |
| `no_low` | `DECIMAL(38,18)` | Minimum No normalized-leg price; null when unobserved. |
| `no_close` | `DECIMAL(38,18)` | Last No normalized leg in chain order; null when unobserved. |
| `no_vwap` | `DECIMAL(38,18)` | `sum(gross_collateral) / sum(shares)`, rounded deterministically half-even to 18 decimal places; null when unobserved. |
| `no_normalized_fill_count` | `BIGINT` | Count of normalized No economic legs, including derived counterparts; zero when unobserved. |
| `no_derived_fill_count` | `BIGINT` | Subset of normalized No legs derived as MINT/MERGE counterparts; between zero and `no_normalized_fill_count`. |
| `no_share_volume` | `DECIMAL(38,6)` | Sum of normalized No outcome shares; zero when unobserved. |
| `no_gross_collateral_volume` | `DECIMAL(38,6)` | Sum of No gross USDC.e collateral before fees; zero when unobserved. |
| `no_first_settlement_at_utc` | `TIMESTAMP` | Earliest finalized event-block timestamp contributing to the minute; null when unobserved. |
| `no_last_settlement_at_utc` | `TIMESTAMP` | Latest finalized event-block timestamp contributing to the minute; null when unobserved. |
| `no_observed` | `BOOLEAN` | True when at least one normalized No leg exists in the minute. |

Minute completeness:

| Column | Type | Contract |
| --- | --- | --- |
| `minute_complete` | `BOOLEAN` | Required; exactly `yes_observed AND no_observed`. It describes two-sided settlement activity, not finality or football-time completeness. |
| `minute_status` | `VARCHAR` | Required mapping: both sides → `both_observed`, Yes only → `yes_only`, No only → `no_only`, neither → `no_fills`. |

OHLC chain order is `(block_number, transaction_index, passive_log_index,
normalized_leg_ordinal)`, not event timestamp alone. `first` and `last`
settlement timestamps are the minimum and maximum contributing finalized
event-block timestamps. Derived counts are already included in normalized fill
counts and volumes; they must not be added a second time.

### Internal audit release and operator-local technical export

`polymarket_wc2026_polygon_settlement_release` reads an already valid mart and
writes a new immutable SemVer audit directory below
`artifacts/polygon_settlement/audit/releases/`. Existing versions are never
overwritten and there is no mutable `latest` alias. The audit release contains:

- `wc2026_polygon_settlement_minute_odds.csv`
- `wc2026_polygon_settlement_markets.csv`
- `schema.json`
- `README.md`
- `SOURCES.csv`
- `PROVENANCE.json`
- `QUALITY_REPORT.json`
- `CHANGELOG.md`
- `DO_NOT_PUBLISH.md`
- `CHECKSUMS.sha256`

The market sidecar, full provenance, and issue-level quality report deliberately
retain identifiers and locators needed for internal verification. The audit
directory is internal and is excluded from repository distributions.

The standalone
`export_polymarket_wc2026_polygon_settlement_minute_odds.py` command consumes
only a checksum-valid immutable audit directory. It never queries the warehouse
or calls a network service. It copies the primary CSV byte-for-byte, validates the
literal 41-column allowlist and 39,120-row contract, scans for forbidden
identifiers and unsafe text, and writes a new immutable directory below
`artifacts/polygon_settlement/exports/releases/`.

The operator-local technical export is titled **WC2026 Polygon Settlement Minute
Aggregates** and contains exactly:

- `wc2026_polygon_settlement_minute_odds.csv`
- `schema.json`
- `README.md`
- `SOURCES.csv`
- `MANIFEST.json`
- `QUALITY_SUMMARY.json`
- `QUALITY_SUMMARY.md`
- `CHANGELOG.md`
- `CHECKSUMS.sha256`

`schema.json` covers only the exported CSV and fixes column order, nullability,
units, RFC3339 UTC timestamps, `DECIMAL(38,18)` probability fields,
`DECIMAL(38,6)` volume fields, integers, booleans, enums, and the
proposition-minute grain. The analyzer disables DuckDB type inference so exact
decimal thresholds, including pair deviation `0.05`, are not changed by binary
floating-point rounding.

Its manifest and quality reports contain only redacted aggregate inventory,
lineage, verification, coverage, derived-fill, exact-decimal pair-deviation, and
single-leg/linkability metrics. The quality reports contain no proposition IDs,
per-row timestamps, token IDs, exchange/provider addresses,
transaction/log/block locators, or issue-level warning rows.

The exported CSV repeats only dataset version and stable proposition semantics.
It omits the eight audit-only mart fields plus wallets, transaction/log/block
IDs, provider fields, raw amounts, order hashes, signatures, raw event payloads,
Gamma/CLOB fields, source question prose, and pair diagnostics. This is
de-identified data, not anonymous data: a sparse aggregate over a public ledger
can still be reverse-linked to source transactions by time, amount, and price.

The software creates no upload operation or remote destination. Operators
control the local artifact and remain responsible for their inputs and outputs.

Schema: `international_results_wc2026_marts`

| Relation | Grain | Pipeline | Contract |
| --- | --- | --- | --- |
| `international_results_wc2026_matches` | One row per `match_id` | Shared (Kalshi WC2026, match-minute odds) | Clean WC2026 FIFA World Cup fixture/result rows from `martj42/international_results`, including stage, status, score, inferred knockout advancer metadata, and immutable source revision/hash provenance. |
| `international_results_wc2026_team_status` | One row per `team_name` | Shared (Kalshi WC2026) | Canonical 48-team WC2026 roster and current tournament status derived from fixture/result rows. |

Schema: `kalshi_wc2026_marts`

| Relation | Grain | Pipeline | Contract |
| --- | --- | --- | --- |
| `kalshi_wc2026_stage_markets` | One row per `market_ticker` | Kalshi WC2026 | Latest stage-of-elimination market snapshot with team/stage classification, progression-side pricing, and current-price status. |
| `kalshi_wc2026_stage_market_hourly_odds` | One row per `(market_ticker, odds_hour_epoch)` | Kalshi WC2026 | Trailing contract-window hourly OHLC odds for stage markets joined to classified metadata. |
| `kalshi_wc2026_group_winner_markets` | One row per `market_ticker` | Kalshi WC2026 | Latest group-winner market snapshot with team classification and current-price status. |
| `kalshi_wc2026_group_winner_market_hourly_odds` | One row per `(market_ticker, odds_hour_epoch)` | Kalshi WC2026 | Trailing contract-window hourly OHLC odds for group-winner markets. |

## Health And Observability

- Use `polymarket_wc2026_observability.polymarket_wc2026_ingestion_run_observability` for run-level ingestion
  telemetry, market-discovery provenance, request counts, and sync metrics.
- Use `kalshi_wc2026_observability.kalshi_wc2026_ingestion_run_observability` for Kalshi run-level ingestion telemetry.
- Use `kalshi_wc2026_observability.kalshi_wc2026_stage_coverage` to inspect classified market coverage and hourly completeness against the pipeline policy window.
- Use `kalshi_wc2026_observability.kalshi_wc2026_data_quality` for Kalshi source-state anomalies, sparse coverage, and stale or missing live odds findings.

## Current Scope Rules

- Kalshi WC2026 marts expose stage-of-elimination and group-winner markets
  from the fixed `wc2026` registry across the packaged Kalshi series tickers.
  Shared Kalshi thresholds live in `dbt/seeds/kalshi_wc2026_pipeline_policy.csv`.
- Polymarket WC2026 exposes one documented odds mart:
  `polymarket_wc2026_market_hourly_odds`. It includes every market under a
  sticky event-volume-eligible WC2026 event from `polymarket_wc2026_ops.market_scope_registry`.
- The current event admission floor is `event_min_lifetime_volume_usd = 100000`
  in `dbt/seeds/polymarket_wc2026_pipeline_policy.csv`. Eligibility is sticky:
  once an event crosses the floor it remains admitted even if later snapshots
  report lower lifetime volume.
- Shared Polymarket WC2026 thresholds live in that seed; dbt models/tests read it
  and Python parity tests assert the Dagster defaults match it.
- Prices are raw Yes-outcome CLOB probabilities in `[0, 1]` from
  `int_polymarket_wc2026_primary_market_token`. They are not normalized to team
  progression, and the mart does not classify knockout stage or canonical team.
- Grain is one row per `(market_id, odds_hour_epoch)` with full lifetime hourly
  history from the private incremental `int_polymarket_wc2026_token_hourly_odds`
  fact. Market and enclosing-event metadata come from
  `int_polymarket_wc2026_markets` and `int_polymarket_wc2026_event_latest`.
- WC2026 match/result rows come from `martj42/international_results` at the
  immutable Git revision pinned during ingest (`source_revision`, `source_url` on
  each row), filtered to `tournament = 'FIFA World Cup'` and `match_date` between
  `2026-06-11` and `2026-07-19`. The golden hourly mart does not depend on those
  marts; refresh them with `international_results_wc2026_match_results_ingest`
  or the Kalshi/match-minute pipelines when needed.
- `international_results_wc2026_data_quality` emits a warning when the latest
  fixture/result source load is older than the pipeline policy freshness window.
- Use `polymarket_wc2026_market_scope_registry_refresh`, `polymarket_wc2026_hourly_odds_ingest`,
  `polymarket_wc2026_dbt_build`, and `polymarket_wc2026_full_pipeline` for WC2026
  Dagster operations. `polymarket_wc2026_dbt_build` and
  `polymarket_wc2026_full_pipeline` select `+polymarket_wc2026_market_hourly_odds`
  only.
- Use `kalshi_wc2026_market_scope_registry_refresh`, `kalshi_wc2026_hourly_odds_ingest`,
  and `kalshi_wc2026_full_pipeline` for Kalshi WC2026 Dagster operations.
  `kalshi_wc2026_full_pipeline` also runs `international_results_wc2026_match_results_ingest`
  and a scoped dbt build (`+tag:kalshi`, including `international_results` parents).
  `international_results_wc2026_match_results_ingest` refreshes only the FIFA
  World Cup fixture/result source.
- `scripts/export_polymarket_wc2026_market_hourly_odds.py` is the supported
  offline export for the golden mart.
- Raw hourly collection is a separate temporal-foundation branch. An existing
  `(clobTokenId, timestamp)` point is not overwritten on replay.
- `scripts/prune_odds_history.py` permanently exempts observations from
  2026-06-11 00:00:00 through 2026-10-18 23:59:59 UTC, inclusive, covering the
  tournament acceptance window. Outside that interval its default retention is
  365 days; preserve a backup before shortening it.
- `int_polymarket_wc2026_markets` is the canonical registry-scoped market
  dimension for the golden mart. It admits only markets whose enclosing event
  is volume-eligible in the scope registry.

## dbt Checks

`uv run make dbt-build` runs model builds plus generic and singular data tests for:

- Source and staging grain.
- Price sanity and OHLC bounds.
- WC2026 market scope (`accepted_values` on `scope_name`).
- Golden mart grain, Yes-outcome token selection, and event lifetime volume floor
  from the WC2026 pipeline policy seed.
- FIFA World Cup result scope, stage counts, 48-team roster shape, tied knockout
  advancer inference/DQ surfacing, and stale fixture/result source loads.
- Observability run health (warn-level: latest run error-token regression and history coverage floor).
- Kalshi WC2026 grain, OHLC order, progression-side selection, real-team scope,
  and data-quality checks from `kalshi_wc2026_pipeline_policy.csv`.

Warn-level observability tests fail softly in `dbt build` output; treat warnings
as operator signals on real warehouses, not hard release blockers when the
disposable fixture is healthy.

## Breaking change: source-first namespace reset

Mart, asset, job, script, and schema names now use the source-first
`polymarket_wc2026` namespace. Dagster asset keys are hierarchical under
`polymarket/wc2026/...`; jobs, op config keys, scripts, dbt relations, and
DuckDB schemas use flat `polymarket_wc2026_*` names.

There are no compatibility views, env aliases, or migration shims in v0.1.x.
Delete old local warehouse files (`rm oddsfox.duckdb*`) and rerun quickstart
after upgrading from older layouts.

The golden hourly mart reads a private incremental hourly fact. If an existing
local DuckDB warehouse still has deleted knockout/catalog marts or old relation
types, reset the warehouse or drop the affected dbt schemas before rebuilding.

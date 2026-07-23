# Data Dictionary

This page is the analyst-facing dictionary for public marts. For formal grains,
scope rules, and data-contract tests, see [Data Contracts](data-contracts.md).

## Core Semantics

- `wc2026_marts.wc2026_knockout_match_hourly_odds` compares exact match-level
  team-advance prices across Polymarket and Kalshi. Extra time and penalties
  count; the final means winning the World Cup.
- Match-mart prices are raw hourly closes. Null means no observation for that
  side/provider/hour; do not forward-fill or renormalize.
- Public Polymarket WC2026 knockout prices are normalized to team progression.
- Winner and reach markets use the Yes token; elimination-framed markets may use
  the No token.
- `price_represents = 'progression'` means price columns represent the normalized
  progression outcome in `progression_outcome_label`.
- `current_price_status` separates `fresh_live`, `stale_live`, `missing_live`,
  `historical_closed`, `historical_resolved`, and `inactive`.
- For current analysis, prefer `is_actionable_live_market` where available.
- Midterms Balance of Power combinations are independent binary markets; do not
  force probabilities across combinations to sum to 1.0.
- Kalshi stage marts expose progression prices separately from raw Yes prices.
  Use `progression_*_price` for team-progression analysis.
- Polygon settlement prices use finalized event-block time and normalized
  economic legs. They are not quotes, order-book snapshots, unique-user trade
  counts, order-match timestamps, or CLOB observations.

## Cross-platform WC2026 Mart

### `wc2026_marts.wc2026_knockout_match_hourly_odds`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Compare raw match-advance probabilities for the official home and away teams across both providers. |
| Grain | One row per published `fifa_match_id`, `odds_hour_epoch`. |
| Identifiers | FIFA match numbers 73–102 and 104; match 103 is excluded. Provider identifiers are retained separately. |
| Time columns | `odds_hour_utc`, `odds_hour_epoch`, `kickoff_at_utc`; `is_pre_kickoff` distinguishes pregame hours. |
| Price columns | Four nullable home/away advance closes, one pair per provider. |
| Recommended filters | Use `both_sources_complete` for direct comparisons or provider-specific completeness flags for single-source analysis. |
| Common mistakes | Treating prices as regulation moneylines, using provider home/away order, filling nulls, or normalizing pair sums. |

## Polymarket WC2026 Marts

### `polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds`

| Property | Value |
| --- | --- |
| Grain | One row per `(proposition_id, settlement_minute_utc)` |
| Coverage | FIFA match IDs 1–104; 216 group propositions × 150 minutes plus 32 knockout propositions × 210 minutes = 39,120 rows |
| Intended use | Historical settlement-flow studies over fixed scheduled match windows |
| Timing | Finalized Polygon event-block timestamp bucket inside `[kickoff, window_end)` |
| Prices | Oriented Yes/No OHLC and share-weighted VWAP, ordered by block, transaction, passive log, and normalized-leg ordinal |
| Activity | Per side: normalized/derived economic-leg counts, share/collateral volume, first/last settlement timestamp, and observed flag |
| Null policy | Dense empty minutes keep null price/timestamps and zero counts/volumes; no forward-fill, interpolation, pair normalization, or inferred complement |
| Status | `both_observed`, `yes_only`, `no_only`, or `no_fills`; `minute_complete` means both oriented sides were observed |
| Semantics | Use authored `proposition_type`, `yes_represents`, and `no_represents`; no match results are included |
| Identity | Stable proposition and FIFA fixture fields; on-chain evidence identifiers remain in the seed/market sidecar, not the release's main CSV |
| Common joins | Compare with the Gamma/CLOB mart on `condition_id` plus oriented Yes/No token IDs, then read authored Yes/No semantics |
| Common mistakes | Joining flows on raw team strings or `(fifa_match_id, proposition_type)`; independent aliases and home/away order can differ |

`elapsed_window_minute` is zero-based and bounded to `0..149` for group
propositions or `0..209` for knockout propositions. It is a scheduled analysis
axis, not official football match time. A normalized fill count measures
economic legs; MINT/MERGE can add an explicitly derived counterpart, so it
should not be interpreted as unique users or transactions. Empty propositions
and sparse token sides are retained and reported as warnings.

#### Data sources and lineage

The mart has two externally sourced inputs: a reviewed static market manifest
for fixture/proposition/token meaning and finalized Polygon execution data for
settlement activity. Authoring-only evidence is embedded in the committed
manifest; it is not fetched again during a backfill.

| Source | Use in the mart | Pin, contract, and license/terms |
| --- | --- | --- |
| [Committed 248-row market manifest](https://github.com/hypertrial/oddsfox-pipeline/blob/main/dbt/seeds/polymarket_wc2026_polygon_settlement_markets.csv) | Runtime source of proposition IDs and meanings, match/stage/team identity, scheduled windows, condition and oriented token IDs, market structure, exchange, evidence locators, manifest hash, and version. It is the only runtime fixture or semantic source. | Reviewed repository seed. Each row records its OpenFootball revision/path/line hash, initialization transaction/log locators, ancillary-data SHA-256, and token-verification block/hash. |
| [OpenFootball `cup.txt`](https://github.com/openfootball/worldcup/blob/bd46a148289f9930da66c140d4d7d2325e95d387/2026--usa/cup.txt) and [`cup_finals.txt`](https://github.com/openfootball/worldcup/blob/bd46a148289f9930da66c140d4d7d2325e95d387/2026--usa/cup_finals.txt) | Authoring-only source of group/knockout fixture identity, display order, group label, and scheduled kickoff. Source lines and line hashes are copied into the manifest; file prose is not copied into the mart. | Revision `bd46a148289f9930da66c140d4d7d2325e95d387`; SHA-256 `4f52c563a5d470702fedf5078fd379c8f5ddfb2192d23b6f88ce84e997c30028` and `03631f10fff8a3a9c485d866c98fb099f8d2612e97a034c64c28c7d189dd5949`. [CC0 notice](https://github.com/openfootball/worldcup/blob/bd46a148289f9930da66c140d4d7d2325e95d387/LICENSE.md), SHA-256 `36ffd9dc085d529a7e60e1276d73ae5a030b020313e6c5408593a6ae2af39673`. |
| [FIFA World Cup 26 Match Schedule](https://digitalhub.fifa.com/asset/4b5d4417-3343-4732-9cdf-14b6662af407/FWC26-Match-Schedule_English.pdf) | Authoring/review-only source of official numeric match IDs. It does not supply mart kickoff times, expressive content, or a runtime dependency. | `FWC26 Match Schedule_v31_16072026_EN`; SHA-256 `165fb909253b746e6173a4443bdc3e5d786530f0684af6e85c1fd21fff252811`. The PDF is not redistributed. |
| [Polygon PoS mainnet](https://docs.polygon.technology/pos) | Primary runtime source of finalized exchange logs, transaction receipts, block numbers/hashes, transaction ordering, and event-block timestamps. `OrdersMatched` logs discover candidate transactions; receipts supply the complete `OrderFilled`/`OrdersMatched` segments; headers supply strict window timestamps and boundary hashes. | Chain ID `137`; the primary provider must support the [`finalized` block tag](https://docs.polygon.technology/pos/concepts/finality/finality). Exact finalized head and covered block-range hashes are recorded per scan and release. |
| [Standard V2 exchange `0xe111…996b`](https://polygonscan.com/address/0xe111180000d2663c0091e4f400237545b87b996b) and [neg-risk V2 exchange `0xe222…0f59`](https://polygonscan.com/address/0xe2222d279d744050d28e00520010520000310f59) | Runtime settlement-event contracts. The manifest assigns each proposition to exactly one exchange: standard for knockout propositions and neg-risk for group propositions. | `OrderFilled` and `OrdersMatched` layouts are pinned to [Polymarket CTF Exchange V2 revision `ccc0596…`](https://github.com/Polymarket/ctf-exchange-v2/tree/ccc0596074f4dfd62c944fbca4de252893b82b4b) (BUSL-1.1). No upstream source code is redistributed. |
| [ConditionalTokens `0x4d97…045c`](https://polygonscan.com/address/0x4d97dcd97ec945f40cf65f87097ace5ea0476045) | Authoring-only source of standard `ConditionPreparation`/resolution evidence and standard Yes/No position derivation. Condition and token identifiers, not event prose, are retained in the manifest and mart. | Minimal interface pinned to [Gnosis ConditionalTokens revision `eeefca6…`](https://github.com/gnosis/conditional-tokens-contracts/tree/eeefca66eb46c800a9aaab88db2064a99026fde5) (LGPL-3.0). |
| [UMA CTF Adapter revision `8b76cc9…`](https://github.com/Polymarket/uma-ctf-adapter/tree/8b76cc9e0d46c6f7450a0adb0ddc0f5b0568c9cc) | Authoring-only source/interface for `QuestionInitialized`, creator-scoped ancillary updates, question/condition linkage, and resolution verification. Standard adapter addresses are discovered from Polygon events rather than hardcoded. | Repository license/terms apply. Only required event layouts and view selectors are implemented; source code and oracle prose are not redistributed. |
| [NegRisk Adapter `0xd91e…5296`](https://polygonscan.com/address/0xd91e80cf2e7be2e162c6513ced06f1dd0da35296) | Authoring-only source of `MarketPrepared`/`QuestionPrepared` evidence and neg-risk position IDs. Its operator and UMA adapter are discovered and verified through the event/deployment chain. | Minimal interface pinned to [NegRisk CTF Adapter revision `f78b35b…`](https://github.com/Polymarket/neg-risk-ctf-adapter/tree/f78b35b0863b4308a431ca307d06f49b2ea65e78); repository license/terms apply. |
| [Polygon USDC.e `0x2791…4174`](https://polygonscan.com/address/0x2791bca1f2de4661ed88a30c99a7a9449aa84174) | Authoring-only collateral address used with CTF collection/index sets to derive and verify standard position IDs. Runtime integer collateral amounts are normalized from six decimals into mart volume/price fields. | Fixed Polygon bridged-USDC contract address; no token metadata or off-chain price feed is used. |
| [Configured Polygon JSON-RPC provider](https://docs.polygon.technology/pos/reference/rpc-endpoints) | Transport for the Polygon facts above, not a separate semantic or pricing source. It returns finalized heads, logs, receipts, and headers; provider errors never become empty ranges. | Provider-specific. `polygon_settlement_scan_runs` records the non-secret label and sanitized origin. A release records the actual provider, terms URL/evidence state, finalized head, and range hashes in `SOURCES.csv` and `PROVENANCE.json`; credentials and full endpoints are never persisted. |

The internal transformation lineage is:

```text
committed manifest
  → stg_polymarket_wc2026_polygon_settlement_markets
  → int_polymarket_wc2026_polygon_settlement_market_universe

finalized Polygon V2 logs/receipts/headers
  → polymarket_wc2026_raw.polygon_settlement_fills
  → stg_polymarket_wc2026_polygon_settlement_fills
  → int_polymarket_wc2026_polygon_settlement_token_minute_odds

manifest universe + token-minute aggregates
  → int_polymarket_wc2026_polygon_settlement_minute_odds_candidate
  → int_polymarket_wc2026_polygon_settlement_publication_gate
  → polymarket_wc2026_polygon_settlement_minute_odds
```

The publication gate also reads
`polymarket_wc2026_ops.polygon_settlement_scan_runs` and
`polymarket_wc2026_ops.polygon_settlement_scan_chunks` to prove that the raw
snapshot matches the manifest and has gap-free, exchange-specific finalized
coverage. Those tables are internal audit evidence, not additional external
data sources.

This flow does **not** use the Polymarket Gamma API, CLOB API or price history,
the Polymarket website/UI, the repository's existing FIFA schedule seed,
international-results, FotMob, match results, or runtime OpenFootball requests.
It does not infer prices from complements or use an external currency/odds
feed.

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
| `home_team` | `VARCHAR` | Required independently sourced fixture home/display team; not a cross-flow join key. |
| `away_team` | `VARCHAR` | Required independently sourced fixture away/display team; not a cross-flow join key. |
| `proposition_type` | `VARCHAR` | Required: `home_win`, `draw`, `away_win`, `home_advances`, `home_win_third_place`, or `home_wins_final`. |
| `yes_represents` | `VARCHAR` | Required authored meaning of the oriented Yes token. |
| `no_represents` | `VARCHAR` | Required authored meaning of the oriented No token. |
| `scheduled_kickoff_at_utc` | `TIMESTAMP` | Required minute-aligned scheduled kickoff from the pinned fixture source. |
| `analysis_window_start_at_utc` | `TIMESTAMP` | Required inclusive window start; equal to scheduled kickoff. |
| `analysis_window_end_at_utc` | `TIMESTAMP` | Required exclusive window end; start plus 150 minutes for group propositions or 210 minutes for knockout propositions. |
| `settlement_minute_utc` | `TIMESTAMP` | Required UTC minute bucket in `[analysis_window_start_at_utc, analysis_window_end_at_utc)`. |
| `settlement_minute_epoch` | `BIGINT` | Required Unix seconds for `settlement_minute_utc`; always minute-aligned. |
| `elapsed_window_minute` | `BIGINT` | Required zero-based scheduled-window index: `0..149` for group propositions or `0..209` for knockout propositions. |
| `condition_id` | `VARCHAR` | Required canonical 32-byte Polygon condition ID; use with oriented token IDs for cross-flow reconciliation. |
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

### `polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds`

| Property | Value |
| --- | --- |
| Grain | One row per `(odds_minute_utc, market_id)` |
| Coverage | FIFA match IDs 1–104; 216 group moneylines and 32 knockout advance/win markets |
| Intended use | In-game event studies, backtests, and minute-level market analysis |
| Timing | Primary Gamma event `startTime` through `finishedTimestamp`, boundary minutes inclusive |
| Prices | Raw Yes/No minute OHLC, average, point counts, and first/last observation times |
| Null policy | Dense rows are retained; missing token minutes stay null and are never carried forward |
| Semantics | Group Yes/No is literal; knockout Yes/No is official home/away team orientation |
| Match identity | FIFA numeric ID from the schedule; team names and home/away orientation from the uniquely matched latest international-results row |
| Timing diagnostics | Scheduled kickoff, actual start/finish, start delta, window length, boundary flags, `minute_status`, and uncapped zero-based `elapsed_window_minute` wall-clock offset |
| Pair diagnostics | Nullable raw close sum/deviation and a strict `> 0.05` anomaly flag; prices are never normalized |
| Provenance | Selected and primary timing events plus matched results ID, immutable revision, payload SHA-256, and load time |

Use `proposition_type`, `yes_represents`, and `no_represents` instead of
inferring meaning from token order. For match 103 the proposition is the
official home team winning third place; for match 104 it is winning the final.
Expected partial terminal-minute nulls remain visible as
`finish_boundary_incomplete`; use `interior_incomplete` to isolate gaps inside a
game rather than treating every boundary null as equivalent. Use
`elapsed_window_minute` for within-game alignment, but do not interpret it as
stoppage-adjusted football time: delays, halftime, extra time, and penalties all
remain on the axis.

### `polymarket_wc2026_marts.polymarket_wc2026_knockout_markets`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Current snapshot for WC2026 Polymarket progression-side knockout prices. |
| Grain | One row per `clob_token_id`. |
| Identifiers | `market_id`, `clob_token_id`, `condition_id`, `canonical_team_name`, `stage_key`. |
| Time columns | `current_price_hour_utc`, `current_price_hour_epoch`, `current_price_age_hours`, match-status dates. |
| Price columns | `current_price`; semantics are `price_represents = 'progression'`. |
| Recommended filters | Use `is_actionable_live_market` for current live analysis. Use `current_price_status` for freshness buckets. |
| Common joins | Join `canonical_team_name` to `international_results_wc2026_marts.international_results_wc2026_team_status.team_name`. |
| Common mistakes | Treating all rows as live; ignoring `current_price_status`; inferring progression from source question text. |

### `polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Trailing hourly OHLC time series for progression-side WC2026 knockout odds. |
| Grain | One row per `clob_token_id`, `odds_hour_epoch`. |
| Identifiers | `market_id`, `clob_token_id`, `canonical_team_name`, `stage_key`, `progression_outcome_label`. |
| Time columns | `odds_hour_utc`, `odds_hour_epoch`, `first_observed_at`, `last_observed_at`. |
| Price columns | `open_price`, `high_price`, `low_price`, `close_price`, `avg_price`. |
| Recommended filters | Use `price_represents = 'progression'`; filter by `canonical_team_name`, `stage_key`, and `market_status` for focused analyses. |
| Common joins | Join to `polymarket_wc2026_knockout_markets` on `clob_token_id` for latest price status. |
| Common mistakes | Comparing source Yes/No labels directly without checking progression semantics. |

### `polymarket_wc2026_marts.polymarket_wc2026_knockout_market_tokens`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Token universe and classification for public WC2026 Polymarket progression marts. |
| Grain | One row per `clob_token_id`. |
| Identifiers | `market_id`, `clob_token_id`, `outcome_index`, `canonical_team_name`, `stage_key`. |
| Time columns | Match/team status dates such as `next_match_date` and `latest_completed_match_date`. |
| Price columns | None; this is a classified token dimension. |
| Recommended filters | Use for token membership and classification audits, not as a price fact. |
| Common joins | Join to hourly odds on `clob_token_id`; join to team status on `canonical_team_name`. |
| Common mistakes | Expecting current prices here; use `polymarket_wc2026_knockout_markets` instead. |

### `polymarket_wc2026_marts.polymarket_wc2026_graph_token_hourly_odds`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Graph/export analysis that needs both Yes and No tokens for each real-team knockout market. |
| Grain | One row per `market_id`, `clob_token_id`, `odds_hour_epoch`. |
| Identifiers | `market_id`, `clob_token_id`, `opposite_clob_token_id`, `canonical_team_name`, `stage_key`. |
| Time columns | `odds_hour_utc`, `odds_hour_epoch`, `first_observed_at`, `last_observed_at`. |
| Price columns | `open_price`, `high_price`, `low_price`, `close_price`, `avg_price`. |
| Recommended filters | Use `is_progression_token` when you only want the normalized progression side. |
| Common joins | Join progression-only analysis back to `polymarket_wc2026_knockout_token_hourly_odds` on `clob_token_id` and `odds_hour_epoch`. |
| Common mistakes | Using both tokens as independent progression probabilities; one is the opposite token. |

## International Results WC2026 Marts

### `international_results_wc2026_marts.international_results_wc2026_matches`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Clean FIFA World Cup 2026 fixture and result rows. |
| Grain | One row per `match_id`. |
| Identifiers | `match_id`, `home_team`, `away_team`, `stage_key`. |
| Time columns | `match_date`, `source_loaded_at`. |
| Provenance | `source_url`, `source_revision`, and `source_payload_sha256` identify the exact immutable CSV payload. |
| Price columns | None. |
| Recommended filters | Use `match_status = 'completed'` for results; include scheduled rows for future fixtures. |
| Common joins | Join team names to `international_results_wc2026_team_status.team_name`. |
| Common mistakes | Treating tied knockout matches as unresolved without checking `advancing_team` and `advancer_inference_status`, or ignoring mixed source revisions when combining snapshots. |

### `international_results_wc2026_marts.international_results_wc2026_team_status`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Canonical WC2026 team roster and current tournament status. |
| Grain | One row per `team_name`. |
| Identifiers | `team_name`. |
| Time columns | `next_match_date`, `eliminated_match_date`, `latest_completed_match_date`. |
| Price columns | None. |
| Recommended filters | Use `is_still_alive` for active-team analysis; use `tournament_status` for active, eliminated, champion buckets. |
| Common joins | Join Polymarket/Kalshi `canonical_team_name` to `team_name`. |
| Common mistakes | Joining on source team text instead of canonical team names. |

## Kalshi WC2026 Marts

### `kalshi_wc2026_marts.kalshi_wc2026_stage_markets`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Current Kalshi stage-of-elimination market snapshots normalized to progression semantics. |
| Grain | One row per `market_ticker`. |
| Identifiers | `market_ticker`, `event_ticker`, `canonical_team_name`, `stage_key`. |
| Time columns | `current_price_hour_utc`, `current_price_hour_epoch`, `current_price_age_hours`, `scraped_at`. |
| Price columns | `progression_price`; use `price_represents = 'progression'`. |
| Recommended filters | Use `is_actionable_live_market` for current live analysis; inspect `current_price_status`. |
| Common joins | Join to team status on `canonical_team_name`. |
| Common mistakes | Using `last_price` as the normalized progression price. |

### `kalshi_wc2026_marts.kalshi_wc2026_stage_market_hourly_odds`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Hourly Kalshi stage-of-elimination OHLC odds. |
| Grain | One row per `market_ticker`, `odds_hour_epoch`. |
| Identifiers | `market_ticker`, `event_ticker`, `canonical_team_name`, `stage_key`. |
| Time columns | `odds_hour_utc`, `odds_hour_epoch`, `latest_refreshed_at`. |
| Price columns | Raw Yes prices: `yes_open_price`, `yes_high_price`, `yes_low_price`, `yes_close_price`, `yes_avg_price`; progression prices: `progression_open_price`, `progression_high_price`, `progression_low_price`, `progression_close_price`, `progression_avg_price`. |
| Recommended filters | Use `progression_*_price` for team progression; filter by `stage_key` and `canonical_team_name`. |
| Common joins | Join to `kalshi_wc2026_stage_markets` on `market_ticker` for latest status. |
| Common mistakes | Mixing raw Yes prices with progression prices in one analysis. |

### `kalshi_wc2026_marts.kalshi_wc2026_group_winner_markets`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Current Kalshi group-winner market snapshots. |
| Grain | One row per `market_ticker`. |
| Identifiers | `market_ticker`, `event_ticker`, `canonical_team_name`, `group_letter`. |
| Time columns | `current_price_hour_utc`, `current_price_hour_epoch`, `current_price_age_hours`, `scraped_at`. |
| Price columns | `group_winner_price`. |
| Recommended filters | Use `is_actionable_live_market` for current live analysis; inspect `current_price_status`. |
| Common joins | Join to team status on `canonical_team_name`. |
| Common mistakes | Treating group-winner prices as stage progression prices. |

### `kalshi_wc2026_marts.kalshi_wc2026_group_winner_market_hourly_odds`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Hourly Kalshi group-winner OHLC odds. |
| Grain | One row per `market_ticker`, `odds_hour_epoch`. |
| Identifiers | `market_ticker`, `event_ticker`, `canonical_team_name`, `group_letter`. |
| Time columns | `odds_hour_utc`, `odds_hour_epoch`, `latest_refreshed_at`. |
| Price columns | `open_price`, `high_price`, `low_price`, `close_price`, `avg_price`. |
| Recommended filters | Filter by `group_letter`, `canonical_team_name`, or `market_ticker`. |
| Common joins | Join to `kalshi_wc2026_group_winner_markets` on `market_ticker` for latest status. |
| Common mistakes | Comparing group-winner prices to stage progression prices without labeling the market type. |

## Polymarket US Midterms 2026 Mart

### `polymarket_us_midterms_2026_marts.polymarket_us_midterms_2026_market_token_hourly_odds`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Hourly OHLC odds for admitted US midterms Polymarket tokens. |
| Grain | One row per `clob_token_id`, `odds_hour_epoch`. |
| Identifiers | `market_id`, `clob_token_id`, `outcome_index`, `outcome_label`, `event_slug`. |
| Time columns | `odds_hour_utc`, `odds_hour_epoch`, `first_observed_at`, `last_observed_at`. |
| Price columns | `open_price`, `high_price`, `low_price`, `close_price`, `avg_price`. |
| Recommended filters | Filter by `event_slug`, `question`, and `outcome_label`; use `is_active` and `is_closed` when separating live and historical rows. |
| Common joins | Join on `market_id` or `clob_token_id` in downstream notebooks. |
| Common mistakes | Summing Balance of Power combination probabilities as if they were mutually exclusive. |

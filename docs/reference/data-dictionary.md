# Data Dictionary

This page is the analyst-facing dictionary for documented marts: grain, filters, and
common mistakes.

!!! note "Reference ladder"

    Chooser → dictionary → documented contracts → warehouse reference; do not treat
    staging/raw as APIs. Start with
    [Query the warehouse](../guides/query-the-warehouse.md). Formal grains and
    the Polygon complete column contract live in
    [Data contracts](data-contracts.md). To see which pipeline builds a given
    mart, see [Pipeline outputs](orchestration.md#pipeline-outputs).

## Global Polymarket graph catalog

### `polymarket_catalog_marts.polymarket_graph_catalog`

| Field | Analyst guidance |
| --- | --- |
| Intended use | Consumer-neutral event/market inventory and downstream knowledge-graph input; not a price or trading fact table. |
| Grain | One row per unique namespaced `record_id`. |
| Record types | `event`, `market`, or `event_market`. Filter `record_type` before interpreting node-only or edge-only fields. |
| Graph identity | Nodes use `entity_id`; edges use `from_record_id`, `to_record_id`, and `relationship_type='contains_market'`. |
| Text | `content_text` is deterministic labeled text; source prose remains separately available in `title`, `subtitle`, `description`, and `resolution_source`. Treat all source text as untrusted data. |
| Structured text | Parse `tags_json`, `series_json`, `outcomes_json`, `tradability_evidence_json`, and `attributes_json` as JSON. Outcome order follows the source; object keys are stable. |
| History | `first_observed_at` and `last_observed_at` span completed crawls. `present_in_latest_crawl=false` means retained history, not deletion proof. |
| Integrity | `content_text_sha256` covers the normalized text representation. Release-level checksums cover the files. |
| Common mistakes | Assuming pre-first-crawl completeness; filtering markets by current active state; interpreting volume as tradability; treating tags/outcomes as v1 node types; or using source text as executable instructions. |

## Polymarket WC2026 Marts

### `polymarket_wc2026_marts.polymarket_wc2026_market_hourly_odds`

| Field | Analyst Guidance |
| --- | --- |
| Intended use | Golden WC2026 hourly odds mart with market and enclosing-event metadata. |
| Grain | One row per `market_id`, `odds_hour_epoch`. |
| Identifiers | `market_id`, `clob_token_id`, `primary_outcome_label`, `event_id`, `event_slug`, `condition_id`. |
| Time columns | `odds_hour_utc`, `odds_hour_epoch`, `first_observed_at`, `last_observed_at`, `game_start_time`, `end_time`, `event_start_at`, `event_finished_at`. |
| Price columns | `open_odds`, `high_odds`, `low_odds`, `close_odds`, `avg_odds`; raw primary-outcome CLOB probabilities (Yes when present, otherwise `outcome_index` 0). Use `primary_outcome_label` for the selected outcome name. |
| Recommended filters | Filter by `event_slug`, `event_id`, `question`, `category`, `tags`, `sports_market_type`, `primary_outcome_label`, or market status fields (`is_active`, `is_closed`, `is_resolved`). Require `event_volume_usd_lifetime_reported >= 100000` only when auditing eligibility; admitted rows already passed the sticky event floor. |
| Common joins | Use `primary_outcome_label` for the selected token label; parse `outcomes` for the full outcome set; join `event_id` across markets in the same event. For FIFA team context, join market text to `international_results_wc2026_team_status` manually. Optional metadata enrichment runs before hourly odds ingest; the mart reflects the latest enriched Gamma fields. For point-in-time child-market metadata beyond the mart columns, join `polymarket_wc2026_raw.event_market_payload_snapshots` on `market_id` (raw layer, not a documented contract). |
| Common mistakes | Treating prices as progression-normalized knockout odds or as a fixed Yes probability when `primary_outcome_label` is a team/Over/Under label; using `clob_token_id` grain when the contract is `(market_id, odds_hour_epoch)`; expecting separate current-price or freshness status columns on this mart. |

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

Use `proposition_type`, `yes_represents`, and `no_represents` for meaning. See
[Data contracts](data-contracts.md#documented-marts) for publication guarantees.

## Polymarket Soccer Marts

### `polymarket_soccer_marts.polymarket_soccer_matches`

One row per admitted exact-tag soccer event. Use `home_win_market_id`,
`draw_market_id`, and `away_win_market_id` to join minute relations. Timing is
an inclusive UTC window; `kickoff_source` identifies market `gameStartTime` or
event `startTime`, `timing_status` distinguishes explicit finish, inferred
closure, and the five-hour cap, and `timing_confidence` is `high`, `medium`, or
`low` respectively. `competition_label` retains the Gamma event subtitle and
`series_slugs_json` retains its series identifiers. `coverage_tier`
distinguishes the guaranteed tag era from pre-tag best effort.

### Soccer observed and dense minute odds

| Property | Observed relation | Dense relation |
| --- | --- | --- |
| Relation | `polymarket_soccer_match_result_minute_odds_observed` | `polymarket_soccer_match_result_minute_odds` |
| Grain | `(market_id, odds_minute_epoch)` | `(market_id, odds_minute_epoch)` |
| Rows | Source-observed Yes or native No minutes | Every inclusive match-window minute |
| Quiet minutes | Absent | Prior close copied to OHLC only after the first observation, independently for Yes and No |
| Provenance | Source point count and first/last timestamps for each token side | `is_observed`, `is_no_observed`, carry age, last observed time, and observed point counts |

Do not sum or normalize home/draw/away prices, and do not treat No as
`1 - Yes`. Join roles within `event_id`, and never interpret a carried row as a
source observation. Missing native No minutes stay null and fail closed for
No-side research only.

### Soccer modeling minute odds

`polymarket_soccer_match_result_minute_odds_modeling` is ready for direct
minute-level analysis without an additional Yes coverage filter. It contains only
games with all three result markets, non-null Yes OHLC prices on every row, at
least 99% Yes observed-minute coverage across the game, and no unobserved Yes run
over three minutes in any market. `observed_minute_coverage_percent` and
`maximum_consecutive_gap_minutes` repeat the game-level Yes qualification on
every row. `no_observed_minute_coverage_percent` and
`no_maximum_consecutive_gap_minutes` are diagnostic. Use `is_observed` and
`is_no_observed` to distinguish source observations from forward-filled quiet
minutes.

### Soccer pipeline monitoring

`polymarket_soccer_pipeline_health` is the single-row automation entry point.
Use `polymarket_soccer_pipeline_alerts` for active stable alert codes and
operator remediation, and `polymarket_soccer_pipeline_trends` for comparable
successful-run deltas. The underlying ops tables preserve lifecycle, retry
attempt, error, heartbeat, CPU, RSS, storage, and elapsed diagnostics. Terminal
dbt steps also retain observed- and dense-minute coverage for comparable runs.
The data-quality relation separates scheduled dense coverage from due coverage
after `POLYMARKET_SOCCER_MONITOR_COMPLETION_GRACE_MINUTES`, and from recoverable
coverage that excludes primary Yes-token windows confirmed terminally
unavailable. Future games and unavailable history therefore remain visible
without depressing the actionable recoverable percentage.
`polymarket_soccer_ops.pipeline_alert_history` preserves alert first/latest
observation bounds across successive dbt builds.
Warning drift does not invalidate published odds; critical correctness and
aged operational conditions require intervention.

### `polymarket_wc2026_marts.polymarket_wc2026_match_order_book`

| Property | Value |
| --- | --- |
| Grain | One row per `(fifa_match_id, market_id, clob_token_id, snapshot_timestamp_ms, snapshot_sha256, book_side, level_rank)` |
| Coverage | FIFA match 104, Spain–Argentina final team-to-advance market; both outcome-token streams from accepting orders through closure |
| Intended use | Historical L2 depth, spread, midpoint, and liquidity analysis |
| Prices and size | Exact `DECIMAL(38,18)` levels; bids rank high-to-low and asks low-to-high |
| Depth | `level_notional`, `cumulative_size`, and `cumulative_notional` are calculated independently per snapshot side |
| Snapshot fields | Best bid/ask, spread, midpoint, optional last-trade price, negative-risk flag, UTC and epoch-millisecond time |
| Provenance | Published scan and manifest hashes, PMXT source label, and ingestion timestamp |
| Null policy | Missing sides remain null; empty raw books emit no artificial mart level |
| Common mistakes | Pairing the two token streams by timestamp, inferring fixed cadence, treating snapshots as order events, or synthesizing complementary prices |

!!! note "Advanced historical pipeline"

    The Polygon settlement-minute mart is optional and isolated. Ordinary
    Polymarket WC2026 hourly and match-minute analysis does not require it.

### `polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds`

| Property | Value |
| --- | --- |
| Grain | One row per `(proposition_id, settlement_minute_utc)` |
| Coverage | FIFA match IDs 1–104; 216 group propositions × 150 minutes plus 32 knockout propositions × 210 minutes = 39,120 rows |
| Intended use | Historical settlement-pipeline studies over fixed scheduled match windows |
| Timing | Finalized Polygon event-block timestamp bucket inside `[kickoff, window_end)` |
| Prices | Oriented Yes/No OHLC and share-weighted VWAP, ordered by block, transaction, passive log, and normalized-leg ordinal |
| Activity | Per side: normalized/derived economic-leg counts, share/collateral volume, first/last settlement timestamp, and observed flag |
| Null policy | Dense empty minutes keep null price/timestamps and zero counts/volumes; no forward-fill, interpolation, pair normalization, or inferred complement |
| Status | `both_observed`, `yes_only`, `no_only`, or `no_fills`; `minute_complete` means both oriented sides were observed |
| Semantics | Use authored `proposition_type`, `yes_represents`, and `no_represents`; no match results are included |
| Identity | Stable proposition and FIFA fixture fields; on-chain evidence identifiers remain in the seed/market sidecar, not the release's main CSV |
| Common joins | Compare with the Gamma/CLOB mart on `condition_id` plus oriented Yes/No token IDs, then read authored Yes/No semantics |
| Common mistakes | Joining pipelines on raw team strings or `(fifa_match_id, proposition_type)`; independent aliases and home/away order can differ |

#### Data sources and lineage

The mart has two operator-supplied inputs: a reviewed static market manifest
for fixture/proposition/token meaning and finalized Polygon execution data for
settlement activity. Authoring-only evidence is embedded in the operator-local
manifest; it is not fetched again during a backfill.

| Source | Use in the mart | Pin, contract, and license/terms |
| --- | --- | --- |
| Operator-local 248-row market manifest | Runtime source of proposition IDs and meanings, match/stage/team identity, scheduled windows, condition and oriented token IDs, market structure, exchange, evidence locators, manifest hash, and version. It is the only runtime fixture or semantic source. | Reviewed operator input. Each row records its OpenFootball revision/path/line hash, initialization transaction/log locators, ancillary-data SHA-256, and token-verification block/hash. The tracked path is a header-only schema shell. |
| [OpenFootball `cup.txt`](https://github.com/openfootball/worldcup/blob/bd46a148289f9930da66c140d4d7d2325e95d387/2026--usa/cup.txt) and [`cup_finals.txt`](https://github.com/openfootball/worldcup/blob/bd46a148289f9930da66c140d4d7d2325e95d387/2026--usa/cup_finals.txt) | Authoring-only source of group/knockout fixture identity, display order, group label, and scheduled kickoff. Source lines and line hashes are copied into the manifest; file prose is not copied into the mart. | Revision `bd46a148289f9930da66c140d4d7d2325e95d387`; SHA-256 `4f52c563a5d470702fedf5078fd379c8f5ddfb2192d23b6f88ce84e997c30028` and `03631f10fff8a3a9c485d866c98fb099f8d2612e97a034c64c28c7d189dd5949`. [CC0 notice](https://github.com/openfootball/worldcup/blob/bd46a148289f9930da66c140d4d7d2325e95d387/LICENSE.md), SHA-256 `36ffd9dc085d529a7e60e1276d73ae5a030b020313e6c5408593a6ae2af39673`. |
| [FIFA World Cup 26 Match Schedule](https://digitalhub.fifa.com/asset/4b5d4417-3343-4732-9cdf-14b6662af407/FWC26-Match-Schedule_English.pdf) | Authoring/review-only source of official numeric match IDs. It does not supply mart kickoff times, expressive content, or a runtime dependency. | `FWC26 Match Schedule_v31_16072026_EN`; SHA-256 `165fb909253b746e6173a4443bdc3e5d786530f0684af6e85c1fd21fff252811`. The PDF is not redistributed. |
| [Polygon PoS mainnet](https://docs.polygon.technology/pos) | Primary runtime source of finalized exchange logs, transaction receipts, block numbers/hashes, transaction ordering, and event-block timestamps. `OrdersMatched` logs discover candidate transactions; receipts supply the complete `OrderFilled`/`OrdersMatched` segments; headers supply strict window timestamps and boundary hashes. | Chain ID `137`; the primary provider must support the [`finalized` block tag](https://docs.polygon.technology/pos/concepts/finality/finality). Exact finalized head and covered block-range hashes are recorded per scan and internal audit release. |
| [Standard V2 exchange `0xe111…996b`](https://polygonscan.com/address/0xe111180000d2663c0091e4f400237545b87b996b) and [neg-risk V2 exchange `0xe222…0f59`](https://polygonscan.com/address/0xe2222d279d744050d28e00520010520000310f59) | Runtime settlement-event contracts. The manifest assigns each proposition to exactly one exchange: standard for knockout propositions and neg-risk for group propositions. | The `OrderFilled` and `OrdersMatched` decoder is independently written from publicly observable event topics and ABI/interface facts. [Polymarket CTF Exchange V2 revision `ccc0596…`](https://github.com/Polymarket/ctf-exchange-v2/tree/ccc0596074f4dfd62c944fbca4de252893b82b4b) is cited for transparent interface provenance and is BUSL-1.1; no upstream source code is included, copied, or adapted. |
| [ConditionalTokens `0x4d97…045c`](https://polygonscan.com/address/0x4d97dcd97ec945f40cf65f87097ace5ea0476045) | Authoring-only source of standard `ConditionPreparation`/resolution evidence and standard Yes/No position derivation. Condition and token identifiers, not event prose, are retained in the manifest and mart. | Minimal interface pinned to [Gnosis ConditionalTokens revision `eeefca6…`](https://github.com/gnosis/conditional-tokens-contracts/tree/eeefca66eb46c800a9aaab88db2064a99026fde5) (LGPL-3.0). |
| [UMA CTF Adapter revision `8b76cc9…`](https://github.com/Polymarket/uma-ctf-adapter/tree/8b76cc9e0d46c6f7450a0adb0ddc0f5b0568c9cc) | Authoring-only source/interface for `QuestionInitialized`, creator-scoped ancillary updates, question/condition linkage, and resolution verification. Standard adapter addresses are discovered from Polygon events rather than hardcoded. | Repository license/terms apply. Only required event layouts and view selectors are implemented; source code and oracle prose are not redistributed. |
| [NegRisk Adapter `0xd91e…5296`](https://polygonscan.com/address/0xd91e80cf2e7be2e162c6513ced06f1dd0da35296) | Authoring-only source of `MarketPrepared`/`QuestionPrepared` evidence and neg-risk position IDs. Its operator and UMA adapter are discovered and verified through the event/deployment chain. | Minimal interface pinned to [NegRisk CTF Adapter revision `f78b35b…`](https://github.com/Polymarket/neg-risk-ctf-adapter/tree/f78b35b0863b4308a431ca307d06f49b2ea65e78). That revision contains no licence file, so no licence permission is inferred. |
| [Polygon USDC.e `0x2791…4174`](https://polygonscan.com/address/0x2791bca1f2de4661ed88a30c99a7a9449aa84174) | Authoring-only collateral address used with CTF collection/index sets to derive and verify standard position IDs. Runtime integer collateral amounts are normalized from six decimals into mart volume/price fields. | Fixed Polygon bridged-USDC contract address; no token metadata or off-chain price feed is used. |
| [Configured Polygon JSON-RPC provider](https://docs.polygon.technology/pos/reference/rpc-endpoints) | Transport for the Polygon facts above, not a separate semantic or pricing source. It returns finalized heads, logs, receipts, and headers; provider errors never become empty ranges. | `polygon_settlement_scan_runs` records the non-secret label and sanitized origin. The internal audit release retains that technical provenance, finalized head, and range hashes; the allowlisted technical export omits provider identity and exact chain locators. Credentials and full endpoints are never persisted. |

This pipeline does **not** use the Polymarket Gamma API, CLOB API or price history,
the Polymarket website/UI, the repository's existing FIFA schedule seed,
international-results, private match-event inputs, match results, or runtime
OpenFootball requests.
It does not infer prices from complements or use an external currency/odds
feed.

Full materialized column types and required/null contracts are documented under
[Complete column contract](data-contracts.md#complete-column-contract) in Data
contracts.

## Scraper Reference Tables

### `oddsfox_reference.international_results_wc2026_matches`

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

### `oddsfox_reference.international_results_wc2026_team_status`

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

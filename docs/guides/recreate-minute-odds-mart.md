# Recreate the unified minute-odds mart

Build `polymarket_wc2026_marts.polymarket_wc2026_market_minute_odds` from a clean
clone or preserved raw warehouse. Complete
[shared setup](recreate-local-marts.md#shared-setup-every-route) first.

This pipeline is the minute-grain counterpart of the hourly golden mart. It
reuses the match-minute path for in-game markets and adds a futures-minute path
for every other registry-eligible WC2026 market over the tournament span
(`[2026-06-11, 2026-07-19]`, capped by each market's close/resolution time).

## Create and validate the 104-match schedule overlay

Follow the same schedule overlay steps as
[Recreate the match-minute mart](recreate-match-minute-mart.md#create-and-validate-the-104-match-schedule-overlay):

```bash
uv run make match-minute-inputs-validate
```

Do not continue until the command prints:

```text
104 operator-local schedule rows
```

## Create the unified minute-odds mart

Default job config refreshes the shared Polymarket event catalog and registry
before minute fetch. After a successful catalog land, set
`POLYMARKET_WC2026_MINUTE_ODDS_REFRESH_CATALOG=false` in `.env` and restart
Dagster to skip rediscovery on odds/dbt reruns (see
[Configuration](../reference/configuration.md#unified-minute-odds)).

```bash
uv run make minute-odds-backfill
```

The job obtains:

- market inventory from the public
  [Polymarket Gamma API](https://docs.polymarket.com/api-reference/introduction);
- minute token history from the public
  [Polymarket CLOB API](https://docs.polymarket.com/market-data/overview);
- CC0 knockout fixture identity from
  [OpenFootball](https://github.com/openfootball/worldcup); and
- CC0 result validation from
  [`martj42/international_results`](https://github.com/martj42/international_results).

It runs both raw legs:

1. Match-minute odds for the curated 104-game / 248-market inventory.
2. Futures-minute odds for registry-eligible markets that are not
   `moneyline` / `soccer_team_to_advance`.

Both legs share the hourly odds fetch stack: CLOB batch POST (≤20 tokens),
24-hour window pre-chunking, workers/RPS 40 with auto-tune up to 90, and a
vectorized columnar Arrow stage build (`take` / `repeat` broadcast with
dictionary-encoded token/market ids; no per-row dict or Python list
materialization for broadcast columns) for the raw replace.
Discovery includes open markets so
in-tournament futures are eligible; match selection still requires closed game
markets for the 104/248/496 inventory.

Then dbt builds `+polymarket_wc2026_market_minute_odds_data_quality`
(tagged `minute_odds`), producing the mart and its observability row:

```text
polymarket_wc2026_marts.polymarket_wc2026_market_minute_odds
polymarket_wc2026_observability.polymarket_wc2026_market_minute_odds_data_quality
```

Inspect health with:

```sql
select *
from polymarket_wc2026_observability.polymarket_wc2026_market_minute_odds_data_quality;
```

`blocking_issue_keys` must be null before treating the mart as publication-ready.

Historical API availability is not guaranteed. If Gamma or CLOB no longer
returns the complete interval, use an operator's previously completed raw
warehouse through the
[completed-warehouse route](recreate-local-marts.md#alternative-rebuild-completed-raw-warehouses).

## Troubleshooting

| Failure | What to check |
| --- | --- |
| `supply a complete operator-local 104-match schedule` | Same as match-minute: exactly 104 IDs `1..104`. |
| `No registry-eligible WC2026 futures markets` | Registry refresh must admit non-match markets under the volume eligibility gate. |
| Futures fetch empty/error | Empty in-window CLOB history is audited and skipped on publish; hard `error`/`cancelled` fail the run. Retry transient failures or rebuild from a completed raw warehouse. |
| `blocking_issue_keys` not null | Inspect the observability row and match-minute / futures audit tables. |

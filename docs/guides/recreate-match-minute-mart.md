# Recreate the match-minute mart

Build `polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds` from a clean
clone or a preserved raw warehouse. Complete
[shared setup](recreate-local-marts.md#shared-setup-every-route) first.

You need Git, [`uv`](https://docs.astral.sh/uv/getting-started/installation/),
an SSD-backed working directory, and network access to FIFA, GitHub, Polymarket
Gamma, and Polymarket CLOB for the source-fetch route.

## Create and validate the 104-match schedule overlay

Open the
[official FIFA schedule](https://www.fifa.com/en/tournaments/mens/worldcup/canadamexicousa2026/articles/match-schedule-fixtures-results-teams-stadiums)
and populate:

```text
dbt/seeds/wc2026_schedule_matches.csv
```

Keep its existing header unchanged. Supply exactly one row for each
`match_id` from 1 through 104. The repository validates this file but does not
download or redistribute a populated copy.

Use this exact header:

```text
match_id,stage,group_label,matchday,match_date,kickoff_time_et,venue,home_slot,away_slot,home_team,away_team,status,source
```

Fill the columns as follows:

| Column | Required format |
| --- | --- |
| `match_id` | Official integer match number, exactly 1–104 with no gaps or duplicates. |
| `stage` | One of `Group Stage`, `Round of 32`, `Round of 16`, `Quarter-final`, `Semi-final`, `Third-place`, or `Final`. |
| `group_label` | Group letter for group matches; empty for knockout matches. |
| `matchday` | Integer matchday from the reviewed schedule. |
| `match_date` | `YYYY-MM-DD`. |
| `kickoff_time_et` | Eastern Time using `HH:MM AM` or `HH:MM PM`, for example `03:00 PM`. |
| `venue` | Schedule venue label. |
| `home_slot`, `away_slot` | Published schedule slots; retain them even after teams are known. |
| `home_team`, `away_team` | Resolved team names used by the source markets. |
| `status` | Operator-maintained schedule status. |
| `source` | Source URL or revision used to author the row. |

The safest editing workflow is to copy the header shell to an untracked
working file, populate and review it in a spreadsheet, export it as UTF-8 CSV,
then replace the local shell at the same path. Do not add, remove, or reorder
columns.

Validate it:

```bash
uv run make match-minute-inputs-validate
```

Do not continue until the command prints:

```text
104 operator-local schedule rows
```

## Create the match-minute mart

```bash
uv run make match-minute-live-smoke
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

The job fails closed unless it maps 104 matches, 248 markets, and 496 tokens
with no blocking issue. On success, the first real mart is in:

```text
.cache/match_minute_live_smoke.duckdb
```

The relation is:

```text
polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds
```

Expected contract:

- 30,936 rows for the reviewed completed-WC2026 source state;
- 104 distinct matches;
- 248 distinct markets; and
- unique `(odds_minute_epoch, market_id)` grain.

The Make target prints and asserts the inventory and quality result. Do not
continue if it exits nonzero.

Historical API availability is not guaranteed. If Gamma or CLOB no longer
returns the complete interval, use an operator's previously completed raw
warehouse through the
[completed-warehouse route](recreate-local-marts.md#alternative-rebuild-completed-raw-warehouses).
The repository does not host one.

## Troubleshooting

| Failure | What to check |
| --- | --- |
| `supply a complete operator-local 104-match schedule` | The schedule must contain exactly 104 records and the integer IDs 1–104 with no duplicate or missing ID. |
| Gamma/CLOB inventory or history is incomplete | Retry only if the failure is transient; otherwise use a previously completed operator raw warehouse. |
| A dbt publication/readiness assertion fails | Inspect the named quality relation. Do not bypass the gate or manually publish the candidate table. |
| A warehouse path is rejected | Keep it below the SSD-backed `ODDSFOX_STORAGE_ROOT` and make sure the file already exists for `local-marts-rebuild`. |

See also [Recreate Polygon settlement mart](recreate-polygon-settlement-mart.md)
and the [index](recreate-local-marts.md).

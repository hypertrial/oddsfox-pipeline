# Recreate the match-minute mart

Build `polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds` from a clean
clone or preserved raw warehouse. Complete
[shared setup](recreate-local-marts.md#shared-setup-every-route) first.

## Load and validate the 104-match Scraper reference

Obtain an immutable `oddsfox.reference.v1` bundle from OddsFox Scraper, then
load it transactionally into the target warehouse:

```bash
uv run make reference-bundle-load \
  REFERENCE_BUNDLE_DIR=/absolute/path/to/bundle \
  DUCKDB_NAME=/absolute/path/to/oddsfox.duckdb
```

Validate its fixture inventory:

```bash
uv run make match-minute-inputs-validate
```

Do not continue until the command prints:

```text
104 Scraper reference fixture rows
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
- fixture identity and result validation from the previously activated,
  checksummed Scraper `oddsfox.reference.v1` bundle.

The job fails closed unless it maps 104 matches, 248 markets, and 496 tokens
with no blocking issue. On success, the first real mart is in:

```text
.cache/match_minute_live_smoke.duckdb
```

The relation is:

```text
polymarket_wc2026_marts.polymarket_wc2026_match_minute_odds
```

The Make target prints and asserts the inventory and quality result. It uses a
disposable DuckDB file and a disposable
`.cache/runtime/smoke/match-minute-live` runtime root for Parquet snapshots.
Do not continue if it exits nonzero.

Historical API availability is not guaranteed. If Gamma or CLOB no longer
returns the complete interval, use an operator's previously completed raw
warehouse through the
[completed-warehouse route](recreate-local-marts.md#alternative-rebuild-completed-raw-warehouses).

## Troubleshooting

| Failure | What to check |
| --- | --- |
| `supply a complete operator-local 104-match schedule` | The schedule must contain exactly 104 records and the integer IDs 1–104 with no duplicate or missing ID. |
| Gamma/CLOB inventory or history is incomplete | Retry only if the failure is transient; otherwise use a previously completed operator raw warehouse. |

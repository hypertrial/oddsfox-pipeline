# CLI reference

See `oddsfox --help` for full flags.

## Analyst workflow (recommended)

One command builds a DuckDB catalog of all markets with full CLOB price history:

```bash
oddsfox backfill --fidelity 60
oddsfox backfill --since 2023-01-01 --fidelity 1 --rps 5 --concurrency 4
oddsfox backfill --source kalshi --fidelity 60 --limit 25
```

Defaults (overridable via flags or `[backfill]` in `oddsfox.toml`):

- `--all` markets (active + closed + resolved)
- `--interval max` for full history (ignored when `--since`/`--until` are set)
- `--fidelity 60` (minutes between price points)
- `--rps 5` and `--concurrency 4`

Resume a partial backfill by re-running the same command (skips tokens already written). Use `--overwrite` to refetch.

Query the catalog:

```bash
oddsfox sql "SELECT m.question, p.ts, p.price FROM bronze_prices p JOIN bronze_outcomes o ON p.token_id = o.token_id JOIN bronze_markets m ON o.market_id = m.market_id LIMIT 10"
oddsfox serve --port 8787
```

## Core workflow

```bash
oddsfox init --out ~/.oddsfox
oddsfox sync markets --all
oddsfox sync prices --all --interval max --fidelity 60 --rps 5 --concurrency 4
oddsfox snapshot books --active --top-volume 100
oddsfox compute all --since 2024-01-01
oddsfox duckdb --out ~/.oddsfox --db ~/.oddsfox/catalog.duckdb
oddsfox serve --port 8787
```

## Kalshi workflow

Configure read-only API credentials in `oddsfox.toml` (or the lake copy at `{out}/oddsfox.toml`):

```toml
[kalshi]
key_id = "your-key-id"
private_key_path = "/path/to/kalshi-private-key.pem"
```

Some public endpoints work without keys; authenticated requests use RSA-PSS signing for market-data reads only.

```bash
oddsfox sync markets --source kalshi --status open --limit 100
oddsfox sync prices --source kalshi --market KXEXAMPLE-26 --series KXEXAMPLE --period 60
oddsfox sync trades --source kalshi --market KXEXAMPLE-26 --since 2026-01-01
oddsfox snapshot books --source kalshi --market KXEXAMPLE-26 --depth 20
oddsfox sql "SELECT market_id, question FROM bronze_markets WHERE source LIKE 'kalshi%' LIMIT 10"
```

## Explore

```bash
oddsfox search "election"
oddsfox market <market_id>
oddsfox event <event_id>
oddsfox resolved --since 2024-01-01
oddsfox top --by volume_24h
```

## Maintenance

```bash
oddsfox stats
oddsfox head
oddsfox head --limit 30 --export-dir ./heads
```

`head` prints the first 30 rows of every registered bronze and gold table to stdout and writes one CSV per table. Empty tables get a header-only CSV. Default export directory: `{lake}/_exports/heads/`.

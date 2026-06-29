# Changelog

## Unreleased

### Breaking

- Removed ignored `--db` flags from `quickstart` and `serve`. Use `oddsfox duckdb` / `oddsfox sql --db` for catalog paths; `serve` reads Parquet directly.

## 0.1.0 — 2026-06-27

Initial release:

- Medallion lake (bronze/silver/gold) with DuckDB views
- Gamma market sync, CLOB price/book sync, WebSocket watch
- Liquidity, accuracy, and calibration metrics
- CLI, local HTTP API, minimal web UI
- Read-only — no trading

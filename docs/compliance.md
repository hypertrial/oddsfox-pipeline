# Compliance

oddsfox is **code-only** FOSS. It does not ship Polymarket or Kalshi data.

Users fetch data directly from Polymarket and Kalshi APIs under their own access rights. Local caches are created on the user's machine.
Kalshi API keys, when configured, are used only for read-only market-data endpoints.

## Out of scope

- Historical dumps or hosted mirrors
- Trade execution or auto-betting
- Portfolio, balances, fills, orders, or account data
- Geo-bypass tooling

## Research caveats

Public order-book feeds may disagree with on-chain trade direction. Quote-lifecycle attribution is structurally unavailable from public chain data alone. Quality flags in `_metadata/data_quality.parquet` document these limits.

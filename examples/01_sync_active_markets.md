# Sync one day of active markets

```bash
oddsfox init --out ./lake
oddsfox sync markets --active --out ./lake --limit 500
oddsfox duckdb --out ./lake --db ./lake/catalog.duckdb
oddsfox sql "SELECT question, volume_24h FROM bronze_markets ORDER BY volume_24h DESC NULLS LAST" --limit 10 --out ./lake
```

Query in DuckDB:

```sql
SELECT question, volume_24h FROM bronze_markets ORDER BY volume_24h DESC LIMIT 10;
```

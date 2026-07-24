# Day-Two Operations

Use this guide after a successful first scope run. Cadence below is guidance,
not a required operations policy.

## Suggested Cadence

| Activity | When to consider it |
| --- | --- |
| Inspect `*_observability` freshness and data-quality relations | Before trusting prices for analysis or after any failed job |
| Manual hourly odds ingest for a scope | When you want newer prices without enabling schedules |
| [Enable schedules](enable-schedules.md) | Only after manual jobs and dbt builds are healthy |
| [Validate and recover](validate-and-recover.md) | After failures, schema conflicts, or suspicious gaps |
| Prune or compact via [Scripts](../reference/scripts.md) | When the local warehouse grows large or odds history needs trimming |
| Full warehouse reset (`rm oddsfox.duckdb*`) | After layout upgrades or unrecoverable corruption |

Example manual odds refresh after the first full run:

```bash
uv run python scripts/run_scope.py polymarket:wc2026 --step odds
uv run python scripts/run_scope.py polymarket:wc2026 --step dbt
```

## Freshness And Trust

1. Prefer read-only inspection:
   `uv run python scripts/profile_warehouse.py`
2. Check matching `*_data_quality` and `*_sync_run_observability` relations.
3. For live analysis, prefer `is_actionable_live_market` and inspect
   `current_price_status` in the public marts.

## Lock Hygiene

Only one read-write connection should hold the DuckDB file. Stop Dagster and
other writers before repairs. Use
`uv run python scripts/profile_warehouse.py --snapshot-copy` when you must
inspect while another process is active. See
[Troubleshooting](troubleshooting.md#duckdb-lock-errors).

## Schedules

Keep schedule flags false in `.env` until you intentionally enable them.
Polygon settlement backfill and audit-release jobs are unscheduled and have no
schedule-enable flags.

## Advanced Paths

- [Recreate local marts](recreate-local-marts.md) for WC2026 minute marts
- [Docker image](docker-image.md) for the signed GHCR artifact
- [Operators](../audiences/operators.md) hub for the full operator map

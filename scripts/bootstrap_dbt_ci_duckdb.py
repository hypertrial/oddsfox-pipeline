#!/usr/bin/env python3
"""Bootstrap a disposable DuckDB database for CI dbt targets.

Creates Polymarket and Kalshi test raw/ops tables, seeds one ingestion-run
event per platform, and seeds OpenFootball schedule fixtures. Source-freshness
seeding stays in ``seed_dbt_source_freshness.py``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

ensure_src_on_path()

import oddsfox_pipeline.storage.duckdb.connection as connection  # noqa: E402
from oddsfox_pipeline.storage.duckdb.schemas.kalshi import (  # noqa: E402
    create_all_kalshi_test_raw_tables,
    seed_test_kalshi_ingestion_run_event,
)
from oddsfox_pipeline.storage.duckdb.schemas.openfootball import (  # noqa: E402
    seed_test_openfootball_schedule_fixtures,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (  # noqa: E402
    create_all_scope_test_markets_tables,
    seed_test_ingestion_run_event,
)


def bootstrap_dbt_ci_duckdb() -> Path:
    """Reset connection state, init schemas, and seed CI smoke rows."""
    connection.reset_duckdb_connection_state()
    connection.init_duck_db()
    conn = connection.get_persistent_connection()
    try:
        create_all_scope_test_markets_tables(conn)
        seed_test_ingestion_run_event(conn)
        create_all_kalshi_test_raw_tables(conn)
        seed_test_kalshi_ingestion_run_event(conn)
        seed_test_openfootball_schedule_fixtures(conn)
    finally:
        conn.close()
    return connection.active_duckdb_path()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    path = bootstrap_dbt_ci_duckdb()
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

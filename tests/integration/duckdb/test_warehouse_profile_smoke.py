from __future__ import annotations

import duckdb

from oddsfox.storage.duckdb.profile.discovery import discover_relations
from oddsfox.storage.duckdb.profile.models import ProfileConfig, StatsLevel
from oddsfox.storage.duckdb.profile.report import (
    build_warehouse_profile_report,
    render_markdown_report,
)


def test_warehouse_profile_smoke(tmp_path):
    db_path = tmp_path / "profile.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("CREATE SCHEMA polymarket_raw")
        conn.execute(
            """
            CREATE TABLE polymarket_raw.profile_probe (
                id TEXT,
                price DOUBLE,
                active BOOLEAN
            )
            """
        )
        conn.execute(
            "INSERT INTO polymarket_raw.profile_probe VALUES ('m1', 0.5, TRUE)"
        )

        relations = discover_relations(
            conn,
            schema_whitelist={"polymarket_raw"},
            include_views=False,
        )
        report = build_warehouse_profile_report(
            conn,
            ProfileConfig(
                duckdb_path=db_path,
                schemas={"polymarket_raw"},
                include_views=False,
                stats_level=StatsLevel.full,
            ),
        )

    assert [r.table_name for r in relations] == ["profile_probe"]
    assert report.relations[0].row_count == 1
    assert "profile_probe" in render_markdown_report(report)
    assert "profile_probe" in report.as_json()

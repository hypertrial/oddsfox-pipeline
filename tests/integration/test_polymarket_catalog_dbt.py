from __future__ import annotations

import json

import duckdb
from tests.integration.conftest import dbt_subprocess_env, write_dbt_profile
from tests.integration.dbt_cli import run_dbt

from oddsfox_pipeline.ingestion.polymarket.catalog import normalize_catalog_pages
from oddsfox_pipeline.publishing import polymarket_catalog as publisher
from oddsfox_pipeline.storage.duckdb.polymarket_catalog import (
    activate_catalog_crawl,
    save_catalog_page,
    start_catalog_crawl,
)


def _activate(conn, crawl_id: str, event, markets) -> None:
    observed_at = start_catalog_crawl(conn, crawl_id)
    payloads = {
        "events_open": {"events": [event]},
        "events_closed": {"events": []},
        "markets_open": {"markets": markets},
        "markets_closed": {"markets": []},
    }
    pages = []
    for pass_name, payload in payloads.items():
        save_catalog_page(
            conn,
            crawl_id=crawl_id,
            pass_name=pass_name,
            page_number=0,
            payload=payload,
            next_cursor=None,
            is_complete=True,
        )
        pages.append(
            {
                "pass_name": pass_name,
                "payload_json": json.dumps(payload),
            }
        )
    events, market_rows, edges = normalize_catalog_pages(
        pages, crawl_id=crawl_id, observed_at=observed_at
    )
    activate_catalog_crawl(
        conn,
        crawl_id=crawl_id,
        event_rows=events,
        market_rows=market_rows,
        edge_rows=edges,
    )


def test_catalog_mart_is_cumulative_unique_and_graph_complete(
    tmp_path, monkeypatch, dbt_profiles_dir, dbt_target_dir
):
    db_path = tmp_path / "catalog.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    with duckdb.connect(str(db_path)) as conn:
        _activate(
            conn,
            "crawl-1",
            {
                "id": "1",
                "title": "Original event",
                "markets": [
                    {
                        "id": "2",
                        "question": "Original market?",
                        "clobTokenIds": ["yes", "no"],
                    }
                ],
            },
            [],
        )
        _activate(
            conn,
            "crawl-2",
            {
                "id": "1",
                "title": "Updated event",
                "markets": [{"id": "2", "question": "Updated market?"}],
            },
            [
                {"id": "2", "question": "Updated market?"},
                {"id": "3", "question": "Orphan market?", "enableOrderBook": True},
            ],
        )
        _activate(
            conn,
            "crawl-3",
            {
                "id": "4",
                "title": "Temporary event",
                "markets": [
                    {
                        "id": "5",
                        "question": "Temporary market?",
                        "enableOrderBook": True,
                    }
                ],
            },
            [],
        )
        _activate(
            conn,
            "crawl-4",
            {
                "id": "1",
                "title": "Reappeared event",
                "markets": [{"id": "2", "question": "Reappeared market?"}],
            },
            [],
        )

    write_dbt_profile(dbt_profiles_dir, db_path, threads=1)
    env = dbt_subprocess_env(
        db_path=db_path,
        profiles_dir=dbt_profiles_dir,
        target_dir=dbt_target_dir,
        dbt_threads=1,
    )
    run_dbt(
        ["build", "--select", "+polymarket_graph_catalog"],
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            select record_type, record_id, title, is_tradable,
                present_in_latest_crawl
            from polymarket_catalog_marts.polymarket_graph_catalog
            order by record_id
            """
        ).fetchall()
        monkeypatch.setattr(
            publisher, "current_generator_commit", lambda _root: "a" * 40
        )
        release = publisher.build_polymarket_catalog_release(
            conn,
            tmp_path / "releases",
            dataset_version="1.0.0",
        )
    assert rows == [
        ("event", "event:1", "Reappeared event", None, True),
        ("event", "event:4", "Temporary event", None, False),
        ("event_market", "event_market:1:2", None, None, True),
        ("event_market", "event_market:4:5", None, None, False),
        ("market", "market:2", "Reappeared market?", True, True),
        ("market", "market:3", "Orphan market?", True, False),
        ("market", "market:5", "Temporary market?", True, False),
    ]
    assert release["rows"] == len(rows)
    assert (
        publisher.validate_polymarket_catalog_release(tmp_path / "releases" / "1.0.0")[
            "crawl_id"
        ]
        == "crawl-4"
    )

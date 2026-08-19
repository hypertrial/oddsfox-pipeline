from __future__ import annotations

import json

import duckdb
import pyarrow.parquet as pq
import pytest

from oddsfox_pipeline.publishing import polymarket_catalog as subject


def _warehouse() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        create schema polymarket_catalog_marts;
        create schema polymarket_catalog_ops;
        create table polymarket_catalog_marts.polymarket_graph_catalog as
        select 'event'::varchar record_type, 'event:1'::varchar record_id,
            'Event'::varchar content_text, null::boolean is_tradable,
            null::varchar from_record_id, null::varchar to_record_id,
            true::boolean present_in_latest_crawl
        union all
        select 'market', 'market:2', 'Market', true, null, null, true
        union all
        select 'event_market', 'event_market:1:2', 'Edge', null,
            'event:1', 'market:2', true;
        create table polymarket_catalog_ops.crawl_runs (
            crawl_id varchar, observed_at timestamp, completed_at timestamp,
            status varchar, summary_json varchar
        );
        insert into polymarket_catalog_ops.crawl_runs values (
            'crawl', timestamp '2026-01-01', timestamp '2026-01-02', 'complete',
            '{"passes":{"events_open":{"pages":1,"source_rows":1,"complete":true},"events_closed":{"pages":1,"source_rows":0,"complete":true},"markets_open":{"pages":1,"source_rows":1,"complete":true},"markets_closed":{"pages":1,"source_rows":0,"complete":true}}}'
        );
        """
    )
    column_types = {
        "contract_version": "varchar",
        "entity_id": "varchar",
        "relationship_type": "varchar",
        "title": "varchar",
        "subtitle": "varchar",
        "description": "varchar",
        "resolution_source": "varchar",
        "slug": "varchar",
        "canonical_url": "varchar",
        "tags_json": "varchar",
        "series_json": "varchar",
        "outcomes_json": "varchar",
        "tradability_evidence_json": "varchar",
        "attributes_json": "varchar",
        "is_active": "boolean",
        "is_closed": "boolean",
        "is_archived": "boolean",
        "is_resolved": "boolean",
        "source_created_at": "timestamp",
        "source_updated_at": "timestamp",
        "start_at": "timestamp",
        "end_at": "timestamp",
        "closed_at": "timestamp",
        "first_observed_at": "timestamp",
        "last_observed_at": "timestamp",
        "last_observed_crawl_id": "varchar",
        "latest_catalog_crawl_id": "varchar",
        "content_text_sha256": "varchar",
    }
    for name, data_type in column_types.items():
        conn.execute(
            f"alter table polymarket_catalog_marts.polymarket_graph_catalog add column {name} {data_type}"
        )
    conn.execute(
        """
        update polymarket_catalog_marts.polymarket_graph_catalog set
            contract_version = 'oddsfox.polymarket.graph-catalog.v1',
            entity_id = case record_type when 'event' then '1' when 'market' then '2' end,
            relationship_type = case record_type when 'event_market' then 'contains_market' end,
            tags_json = '[]', series_json = '[]', outcomes_json = '[]',
            tradability_evidence_json = case record_type when 'market' then '["clob_token_ids"]' else '[]' end,
            attributes_json = '{}',
            first_observed_at = timestamp '2026-01-01',
            last_observed_at = timestamp '2026-01-01',
            last_observed_crawl_id = 'crawl', latest_catalog_crawl_id = 'crawl',
            content_text_sha256 = sha256(content_text);
        create schema polymarket_catalog_raw;
        create table polymarket_catalog_raw.market_snapshots (
            market_id varchar, is_tradable boolean
        );
        insert into polymarket_catalog_raw.market_snapshots values ('2', true);
        create table polymarket_catalog_raw.event_market_snapshots (
            event_id varchar, market_id varchar
        );
        insert into polymarket_catalog_raw.event_market_snapshots values ('1', '2');
        """
    )
    return conn


def test_release_is_immutable_checksumned_and_deterministic(tmp_path, monkeypatch):
    monkeypatch.setattr(subject, "current_generator_commit", lambda _root: "a" * 40)
    first = subject.build_polymarket_catalog_release(
        _warehouse(), tmp_path / "one", dataset_version="1.0.0"
    )
    second = subject.build_polymarket_catalog_release(
        _warehouse(), tmp_path / "two", dataset_version="1.0.0"
    )
    first_dir = tmp_path / "one" / "1.0.0"
    second_dir = tmp_path / "two" / "1.0.0"
    assert (
        pq.ParquetFile(first_dir / "polymarket_graph_catalog.parquet").metadata.num_rows
        == 3
    )
    assert first["rows"] == second["rows"] == 3
    for name in (
        "polymarket_graph_catalog.parquet",
        "manifest.json",
        "schema.json",
        "quality_report.json",
        "checksums.sha256",
    ):
        assert (first_dir / name).read_bytes() == (second_dir / name).read_bytes()
    manifest = json.loads((first_dir / "manifest.json").read_text())
    assert manifest["crawl_id"] == "crawl"
    assert manifest["source"]["passes"]["markets_open"] == {
        "closed": False,
        "complete": True,
        "endpoint": "/markets/keyset",
        "pages": 1,
        "record_type": "markets",
        "source_rows": 1,
    }
    with pytest.raises(FileExistsError):
        subject.build_polymarket_catalog_release(
            _warehouse(), tmp_path / "one", dataset_version="1.0.0"
        )
    assert subject.validate_polymarket_catalog_release(first_dir)["crawl_id"] == "crawl"

    (first_dir / "quality_report.json").write_text("{}\n")
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        subject.validate_polymarket_catalog_release(first_dir)


def test_release_refuses_an_existing_broken_symlink(tmp_path, monkeypatch):
    monkeypatch.setattr(subject, "current_generator_commit", lambda _root: "a" * 40)
    release_root = tmp_path / "releases"
    release_root.mkdir()
    (release_root / "1.0.0").symlink_to(tmp_path / "missing")
    with pytest.raises(FileExistsError):
        subject.build_polymarket_catalog_release(
            _warehouse(), release_root, dataset_version="1.0.0"
        )


def test_release_failure_removes_temporary_output_atomically(tmp_path, monkeypatch):
    monkeypatch.setattr(subject, "current_generator_commit", lambda _root: "a" * 40)
    monkeypatch.setattr(
        subject,
        "validate_polymarket_catalog_release",
        lambda _path: (_ for _ in ()).throw(RuntimeError("injected failure")),
    )
    with pytest.raises(RuntimeError, match="injected failure"):
        subject.build_polymarket_catalog_release(
            _warehouse(), tmp_path / "releases", dataset_version="1.0.0"
        )
    assert list((tmp_path / "releases").iterdir()) == []


def test_release_rejects_dangling_edges(tmp_path, monkeypatch):
    monkeypatch.setattr(subject, "current_generator_commit", lambda _root: "a" * 40)
    conn = _warehouse()
    conn.execute(
        "delete from polymarket_catalog_marts.polymarket_graph_catalog where record_id = 'market:2'"
    )
    with pytest.raises(RuntimeError, match="failed validation"):
        subject.build_polymarket_catalog_release(
            conn, tmp_path, dataset_version="1.0.0"
        )


def test_release_rejects_schema_and_text_hash_drift(tmp_path, monkeypatch):
    monkeypatch.setattr(subject, "current_generator_commit", lambda _root: "a" * 40)
    conn = _warehouse()
    conn.execute(
        "alter table polymarket_catalog_marts.polymarket_graph_catalog add column surprise varchar"
    )
    with pytest.raises(RuntimeError, match="schema drift"):
        subject.build_polymarket_catalog_release(
            conn, tmp_path / "schema", dataset_version="1.0.0"
        )

    conn = _warehouse()
    conn.execute(
        "update polymarket_catalog_marts.polymarket_graph_catalog set content_text = 'changed' where record_id = 'market:2'"
    )
    with pytest.raises(RuntimeError, match="failed validation"):
        subject.build_polymarket_catalog_release(
            conn, tmp_path / "hash", dataset_version="1.0.0"
        )

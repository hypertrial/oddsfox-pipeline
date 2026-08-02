"""Branch coverage for terminology-cutover modules that miss release-gate 100%."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest
import requests

from oddsfox_pipeline.ingestion.polymarket import event_catalog as catalog
from oddsfox_pipeline.ingestion.polymarket import gamma_events
from oddsfox_pipeline.ingestion.polymarket import polygon_settlement as polygon
from oddsfox_pipeline.ingestion.polymarket.reviewed_membership import (
    REVIEWED_MEMBERSHIP_COLUMNS,
    load_reviewed_membership_csv,
    replace_reviewed_membership,
)
from oddsfox_pipeline.orchestration import assets_polymarket as assets_mod
from oddsfox_pipeline.orchestration import config as orch_config
from oddsfox_pipeline.storage.duckdb.dlt_batch import merge_event_catalog_batch
from oddsfox_pipeline.storage.duckdb.schemas import dbt_schemas
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import _add_column_if_missing


def test_dbt_catalog_source_slug_and_shorten_paths() -> None:
    assert (
        dbt_schemas.resolve_source_slug({"name": "stg_polymarket_catalog_markets"})
        == dbt_schemas.DBT_SOURCE_POLYMARKET_CATALOG
    )
    assert (
        dbt_schemas.shorten_model_name(
            "stg_polymarket_catalog_markets",
            dbt_schemas.DBT_SOURCE_POLYMARKET_CATALOG,
        )
        == "markets"
    )
    assert (
        dbt_schemas.shorten_model_name(
            "custom_catalog_model",
            dbt_schemas.DBT_SOURCE_POLYMARKET_CATALOG,
        )
        == "custom_catalog_model"
    )


def test_add_column_if_missing_alters_absent_column() -> None:
    with duckdb.connect(":memory:") as conn:
        conn.execute("create table sample(id integer)")
        _add_column_if_missing(conn, "sample", "label", "label text")
        _add_column_if_missing(conn, "sample", "label", "label text")
        columns = {
            str(description[0]).casefold()
            for description in conn.execute("select * from sample limit 0").description
        }
    assert "label" in columns


def test_merge_event_catalog_batch_fail_closed_inputs(duck) -> None:
    observed_at = "2026-08-02T00:00:00"
    event = {
        "event_id": "event-1",
        "event_slug": "event-1",
        "event_title": "World Cup",
        "event_volume_usd_lifetime_reported": 1.0,
        "tags_json": "[]",
        "series_slugs_json": "[]",
        "candidate_sources_json": "[]",
        "source_market_count": 0,
        "observed_at": observed_at,
        "source_endpoint": "/events/keyset",
    }
    with duck.get_connection() as conn:
        with pytest.raises(ValueError, match="event_rows must not be empty"):
            merge_event_catalog_batch(
                event_rows=[],
                tag_rows=[],
                event_market_rows=[],
                market_rows=[],
                conn=conn,
            )
        with pytest.raises(ValueError, match="tag_rows must share"):
            merge_event_catalog_batch(
                event_rows=[event],
                tag_rows=[
                    {
                        "event_id": "event-1",
                        "tag_key": "t",
                        "tag_id": "t",
                        "tag_slug": "t",
                        "tag_label": "t",
                        "observed_at": "2026-08-03T00:00:00",
                    }
                ],
                event_market_rows=[],
                market_rows=[],
                conn=conn,
            )
        with pytest.raises(ValueError, match="non-empty id"):
            merge_event_catalog_batch(
                event_rows=[event],
                tag_rows=[],
                event_market_rows=[],
                market_rows=[{"id": ""}],
                conn=conn,
            )


def test_event_catalog_helper_edge_branches() -> None:
    observed = datetime(2026, 8, 2)
    assert catalog._tag_rows({"id": None}, observed) == []
    assert (
        catalog._tag_rows(
            {
                "id": "1",
                "tags": ["x", {"id": None, "slug": None}, {"id": "t", "slug": "T"}],
            },
            observed,
        )[0]["tag_slug"]
        == "t"
    )
    assert catalog._event_market_rows({"id": None}, observed) == ([], [])
    bridge, markets = catalog._event_market_rows(
        {
            "id": "1",
            "slug": "e1",
            "markets": [
                "skip",
                {"id": None},
                {
                    "id": "m1",
                    "events": ["skip", {"id": None}, {"id": "2"}, {"id": "2"}],
                },
            ],
        },
        observed,
    )
    assert {row["event_id"] for row in bridge} == {"1", "2"}
    assert markets[0]["id"] == "m1"

    merged = catalog._merge_market_payload(
        {"id": "m1", "question": None, "events": [{"id": "1"}, "x"]},
        {"id": "m1", "question": "Q", "events": [{"id": "2"}, {"id": None}, "y"]},
    )
    assert merged["question"] == "Q"
    assert [item["id"] for item in merged["events"]] == ["1", "2"]

    assert catalog._referenced_event_ids(
        [{"markets": ["x", {"id": "m", "events": ["y", {"id": "9"}]}]}]
    ) == {"9"}
    inventory, children, memberships = catalog._partition_inventory(
        {
            "1": {
                "markets": [
                    "skip",
                    {"id": None},
                    {"id": "m1", "events": ["skip", {"id": "2"}]},
                ]
            }
        }
    )
    assert children == 1
    assert memberships == 2
    assert inventory[0][0] == "1"
    assert catalog._merge_event_payloads([{"id": None}, {"id": "1"}])["1"]["id"] == "1"


def test_fixture_series_lookup_and_missing_related(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(catalog, "gamma_get", lambda *_a, **_k: [])
    with pytest.raises(RuntimeError, match="exactly one soccer-fifwc"):
        catalog._fixture_series_id(object())

    monkeypatch.setattr(
        catalog,
        "gamma_get",
        lambda *_a, **_k: [{"id": "series-1", "slug": "soccer-fifwc"}],
    )
    monkeypatch.setattr(
        catalog,
        "iter_gamma_events_keyset",
        lambda *_a, **_k: iter(
            [
                (
                    [
                        {
                            "id": "1",
                            "slug": "fifwc-event-1",
                            "title": "2026 FIFA World Cup",
                            "volume": "120000",
                            "tags": [{"id": "tag-1", "slug": catalog.WC2026_EVENT_TAG}],
                            "series": [{"id": "series-1", "slug": "soccer-fifwc"}],
                            "markets": [
                                {
                                    "id": "m1",
                                    "events": [{"id": "missing", "slug": "gone"}],
                                }
                            ],
                        }
                    ],
                    gamma_events.EventsPageMeta(pages_done=1, truncated=False),
                )
            ]
        ),
    )
    monkeypatch.setattr(catalog, "fetch_gamma_event_by_id", lambda *_a, **_k: None)
    with pytest.raises(RuntimeError, match="references missing Gamma event"):
        catalog.collect_wc2026_event_catalog(client=object())


def test_gamma_event_fetch_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    assert gamma_events.fetch_gamma_event_by_id(object(), "  ") is None
    assert (
        gamma_events._events_params(
            limit=10,
            keyset_closed=None,
            keyset_tag_slug=None,
            keyset_series_id="series-1",
            keyset_related_tags=False,
            keyset_volume_min=None,
        )["series_id"]
        == "series-1"
    )

    class GammaRequestError(Exception):
        def __init__(self, status: int):
            self.response = MagicMock(status_code=status)

    monkeypatch.setattr(gamma_events, "GammaRequestError", GammaRequestError)

    def missing(_client, path):
        raise GammaRequestError(404)

    monkeypatch.setattr(gamma_events, "gamma_get", missing)
    assert gamma_events.fetch_gamma_event_by_id(object(), "99") is None

    def other_error(_client, path):
        raise GammaRequestError(500)

    monkeypatch.setattr(gamma_events, "gamma_get", other_error)
    with pytest.raises(GammaRequestError):
        gamma_events.fetch_gamma_event_by_id(object(), "99")

    def network(_client, path):
        raise requests.RequestException("down")

    monkeypatch.setattr(gamma_events, "gamma_get", network)
    with pytest.raises(requests.RequestException):
        gamma_events.fetch_gamma_event_by_id(object(), "99")

    monkeypatch.setattr(gamma_events, "gamma_get", lambda *_a, **_k: {"id": "99"})
    assert gamma_events.fetch_gamma_event_by_id(object(), "99")["id"] == "99"
    monkeypatch.setattr(gamma_events, "gamma_get", lambda *_a, **_k: {})
    assert gamma_events.fetch_gamma_event_by_id(object(), "99") is None

    def fake_get(_client, path, params=None):
        return []

    monkeypatch.setattr(gamma_events, "gamma_get", fake_get)
    progress = MagicMock()
    list(
        gamma_events.iter_gamma_events_keyset(
            object(),
            max_pages=1,
            keyset_series_id="series-1",
            progress_callback=progress,
            progress_task="task",
        )
    )
    assert progress.call_args.args[1]["keyset_series_id"] == "series-1"


def test_reviewed_membership_remaining_fail_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    missing = tmp_path / "absent.csv"
    with pytest.raises(ValueError, match="does not exist"):
        load_reviewed_membership_csv(missing)

    bad_bytes = tmp_path / "bad.csv"
    bad_bytes.write_bytes(b"\xff\xfe")
    with pytest.raises(ValueError, match="UTF-8"):
        load_reviewed_membership_csv(bad_bytes)

    header = tmp_path / "header.csv"
    header.write_text("wrong,header\n", encoding="utf-8")
    with pytest.raises(ValueError, match="header"):
        load_reviewed_membership_csv(header)

    source = tmp_path / "dup.csv"
    row = (
        "30615,included,sporting,tournament_wide,basis,Reason,reviewer,"
        "2026-08-02T00:00:00Z"
    )
    source.write_text(
        ",".join(REVIEWED_MEMBERSHIP_COLUMNS) + "\n" + row + "\n" + row + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate event_id"):
        load_reviewed_membership_csv(source)

    status = tmp_path / "status.csv"
    status.write_text(
        ",".join(REVIEWED_MEMBERSHIP_COLUMNS)
        + "\n30615,maybe,sporting,tournament_wide,basis,Reason,reviewer,"
        "2026-08-02T00:00:00Z\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="invalid membership_status"):
        load_reviewed_membership_csv(status)

    klass = tmp_path / "class.csv"
    klass.write_text(
        ",".join(REVIEWED_MEMBERSHIP_COLUMNS)
        + "\n30615,excluded,not_a_class,tournament_wide,basis,Reason,reviewer,"
        "2026-08-02T00:00:00Z\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="invalid membership_class"):
        load_reviewed_membership_csv(klass)

    naive = tmp_path / "naive.csv"
    naive.write_text(
        ",".join(REVIEWED_MEMBERSHIP_COLUMNS)
        + "\n30615,excluded,sporting,tournament_wide,basis,Reason,reviewer,"
        "2026-08-02T00:00:00\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="lacks timezone"):
        load_reviewed_membership_csv(naive)

    with duckdb.connect(":memory:") as conn:
        conn.execute("create schema polymarket_wc2026_raw")
        conn.execute("create schema polymarket_wc2026_ops")
        good = tmp_path / "good.csv"
        good.write_text(
            ",".join(REVIEWED_MEMBERSHIP_COLUMNS) + "\n" + row + "\n",
            encoding="utf-8",
        )
        replace_reviewed_membership(good, conn)

        def boom(self, *_args, **_kwargs):
            raise RuntimeError("forced insert failure")

        monkeypatch.setattr(duckdb.DuckDBPyConnection, "executemany", boom)
        with pytest.raises(RuntimeError, match="forced insert failure"):
            replace_reviewed_membership(good, conn)
        assert (
            conn.execute(
                "select count(*) from polymarket_wc2026_raw.reviewed_event_membership"
            ).fetchone()[0]
            == 1
        )


def test_logical_atlas_config_and_empty_bundle(monkeypatch) -> None:
    with pytest.raises(ValueError, match="output_dir must not be blank"):
        orch_config.LogicalAtlasBundleConfig(output_dir="   ")
    with pytest.raises(ValueError, match="reviewed_membership_path must not be blank"):
        orch_config.ReviewedMembershipConfig(reviewed_membership_path="")

    result = MagicMock()
    result.fetchone.return_value = (0,)
    conn = MagicMock()
    conn.execute.return_value = result

    @contextmanager
    def connection():
        yield conn

    monkeypatch.setattr(assets_mod, "get_connection", connection)
    fn = assets_mod.polymarket_wc2026_release_logical_bundle.op.compute_fn.decorated_fn
    with pytest.raises(RuntimeError, match="cannot be empty"):
        fn(MagicMock(), orch_config.LogicalAtlasBundleConfig())

    result.fetchone.return_value = (1,)
    run = MagicMock()
    monkeypatch.setattr(assets_mod, "active_duckdb_path", lambda: "/tmp/atlas.db")
    monkeypatch.setattr(assets_mod.subprocess, "run", run)
    materialization = fn(
        MagicMock(),
        orch_config.LogicalAtlasBundleConfig(output_dir="/tmp/logical-out"),
    )
    command = run.call_args.args[0]
    assert "--output-dir" in command
    assert materialization.metadata["publication_mode"] == "exported"


def test_polygon_target_ranges_reject_empty_and_wrong_keys() -> None:
    with pytest.raises(RuntimeError, match="malformed"):
        polygon._parse_target_ranges("[]")
    with pytest.raises(RuntimeError, match="malformed"):
        polygon._parse_target_ranges(json.dumps([{"exchange_address": "x"}]))

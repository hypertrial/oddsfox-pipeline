"""Synthetic sports data only: native identities, exact depth and recovery invariants."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oddsfox_pipeline.ingestion.polymarket.sports_data.archive import (
    V1_SCHEMA,
    V2_SCHEMA,
    Listing,
    archive_message,
    download,
    exclusion_runs,
    ingest_file,
    inventory,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.books import (
    Book,
    build,
    historical_quote,
    levels,
    normalize_message,
    number,
    pair_quote,
    reset_rows,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.catalog import (
    active_catalog,
    catalog_targets,
    evidence,
    normalize,
    observed_as_of,
    refresh_catalog,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.recorder import (
    NoRedirectConnect,
    exception_category,
    shards,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.storage import (
    UPDATE_SCHEMA,
    BudgetStop,
    Limits,
    Segments,
    Store,
    json_bytes,
    sha256,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.verify import verify

AT = datetime(2026, 1, 1, tzinfo=timezone.utc)
CONDITION = "0x" + "a" * 64


@pytest.fixture
def store(tmp_path):
    return Store(tmp_path, Limits(reserve_bytes=0))


class Gamma:
    received_at = AT

    def __init__(self):
        self.market = {
            "id": "2",
            "conditionId": CONDITION,
            "clobTokenIds": '["11","12"]',
            "outcomes": '["Yes","No"]',
            "enableOrderBook": True,
            "closed": False,
            "sportsMarketType": "moneyline",
            "startDate": "2025-12-01T00:00:00Z",
            "gameStartTime": "2026-01-01T00:00:00Z",
            "description": "source rules",
        }
        self.fail = False

    def get(self, endpoint, params=None):
        if endpoint == "/sports":
            return [{"id": 1, "primaryTagId": 20, "series": "30", "sport": "synthetic"}]
        if endpoint == "/sports/market-types":
            return {"marketTypes": ["moneyline", "player_points", "first_period_total"]}
        if endpoint == "/tags/slug/sports":
            return {"id": 1}
        if self.fail:
            raise ValueError("interrupted source")
        key = "events" if "events" in endpoint else "markets"
        if params["closed"]:
            return {key: [], "next_cursor": None}
        row = (
            {"id": "1", "tags": [{"id": 1}], "markets": [self.market]}
            if key == "events"
            else self.market
        )
        return {key: [row], "next_cursor": None}


def message(kind="book", **kwargs):
    value = {
        "event_type": kind,
        "market": CONDITION,
        "asset_id": "11",
        "timestamp": "1767225600000",
        "bids": [{"price": ".4", "size": "3"}, {"price": ".3", "size": "5"}],
        "asks": [{"price": ".7", "size": "5"}, {"price": ".6", "size": "2"}],
        **kwargs,
    }
    return json_bytes(value)


def normalized(payload=None, sequence=1, received_at=AT):
    return normalize_message(
        payload or message(),
        source="fixture",
        stream="session/1",
        sequence=sequence,
        received_at=received_at,
        identity=f"fixture:{sequence}",
    )


def test_catalog_activation_and_failed_refresh(store):
    client = Gamma()
    first = refresh_catalog(store, client=client)
    assert first["counts"]["admitted_tokens"] == 2
    assert len(first["passes"]) == 4
    assert first["counts"]["orphan_markets"] == 0
    assert catalog_targets(store)["11"]["outcome_index"] == 0
    client.fail = True
    with pytest.raises(ValueError, match="interrupted"):
        refresh_catalog(store, client=client)
    assert active_catalog(store)["sha256"] == first["sha256"]
    assert verify(store)["status"] == "verified_committed_evidence"


def test_cancelled_catalog_materialization_never_activates(store):
    class CancelAfterFourPasses:
        calls = 0

        def is_set(self):
            self.calls += 1
            return self.calls > 4

    cancelled = CancelAfterFourPasses()
    with pytest.raises(InterruptedError, match="materialization cancelled"):
        refresh_catalog(store, client=Gamma(), cancelled=cancelled)
    assert not store.path("catalog-active.json").exists()


def test_verify_never_counts_uncommitted_session_summary(store):
    refresh_catalog(store, client=Gamma())
    store.atomic_json("sessions/orphan.json", {"status": "duration_complete"})
    result = verify(store)
    assert result["completed_recording_seconds"] == 0
    assert result["sessions"] == []
    assert result["uncommitted_session_summaries"] == ["sessions/orphan.json"]


def test_completed_acquisition_survives_materialization_failure(store, monkeypatch):
    from oddsfox_pipeline.ingestion.polymarket.sports_data import catalog

    def fail(*args, **kwargs):
        raise ValueError("synthetic materialization failure")

    monkeypatch.setattr(catalog, "materialize_catalog", fail)
    with pytest.raises(ValueError, match="synthetic materialization"):
        refresh_catalog(store, client=Gamma())
    captures = list(store.root.glob("catalog/*/acquisition.json"))
    assert len(captures) == 1
    checkpoint = json.loads(captures[0].read_bytes())
    assert checkpoint["complete"] and len(checkpoint["passes"]) == 4
    for item in checkpoint["observation_files"]:
        store.validate_file(item)
    assert not store.path("catalog-active.json").exists()


def test_targeted_refresh_preserves_full_and_open_refresh_ages(store):
    class TargetedGamma(Gamma):
        def get(self, endpoint, params=None):
            if endpoint == "/markets":
                return [self.market]
            return super().get(endpoint, params)

    client = TargetedGamma()
    with pytest.raises(ValueError, match="partial refresh"):
        refresh_catalog(store, client=client, target_conditions={CONDITION})
    first = refresh_catalog(store, client=client)
    result = refresh_catalog(
        store, client=client, open_only=True, target_conditions={CONDITION}
    )
    assert result["refresh_kind"] == "targeted"
    assert result["last_full_refresh_at"] == str(first["last_full_refresh_at"])
    assert result["last_open_refresh_at"] == str(first["last_open_refresh_at"])
    assert [p["name"] for p in result["passes"]] == [f"condition:{CONDITION}"]


def test_catalog_receipt_null_and_start_semantics(store):
    client = Gamma()
    client.market["groupItemTitle"] = "Source group label"
    client.market.update(
        active=True,
        archived=False,
        endDate="2026-01-01T03:00:00Z",
        marketType="normal",
        version="source-v1",
        umaResolutionStatuses=["proposed"],
    )
    refresh_catalog(store, client=client)
    before = observed_as_of(store, "2", "market", AT)
    row = normalize(before)
    fields = json.loads(row["normalized_json"])
    assert fields["source_start_date"] != fields["contest_start_at"]
    assert fields["missing_fields"]["listing_at"] == "absent"
    assert fields["subject"] is None
    assert fields["raw_group_item_title"] == "Source group label"
    assert fields["active"] is True and fields["archived"] is False
    assert fields["source_end_date"] == "2026-01-01T03:00:00Z"
    assert fields["source_market_type"] == "normal"
    assert fields["source_market_version"] == "source-v1"
    assert fields["resolution_statuses"] == ["proposed"]
    assert fields["contract_version"] is None
    assert fields["missing_fields"]["period"] == "absent"
    client.received_at = AT + timedelta(days=1)
    client.market["description"] = None
    refresh_catalog(store, client=client)
    assert (
        json.loads(observed_as_of(store, "2", "market", AT)["payload_json"])[
            "description"
        ]
        == "source rules"
    )
    assert (
        json.loads(
            observed_as_of(store, "2", "market", client.received_at)["payload_json"]
        )["description"]
        is None
    )


def test_catalog_conflicting_condition_quarantines_only_affected_market(store):
    client = Gamma()
    refresh_catalog(store, client=client)
    client.received_at = AT + timedelta(days=1)
    client.market["conditionId"] = "0x" + "b" * 64
    result = refresh_catalog(store, client=client)
    assert result["counts"]["identity_conflict_markets"] == 1
    assert catalog_targets(store) == {}


def test_archived_market_is_retained_but_not_admitted(store):
    client = Gamma()
    client.market["archived"] = True
    result = refresh_catalog(store, client=client)
    assert result["counts"]["market"] == 1
    assert result["counts"]["admitted_tokens"] == 0
    assert len(catalog_targets(store, admitted_only=False)) == 2


@pytest.mark.parametrize(
    "raw", ["player_points", "first_period_total", "esports_winner", "outright"]
)
def test_source_sports_market_types_are_retained(raw):
    assert evidence({"sportsMarketType": raw}, []) == ["source_field:sportsMarketType"]


def test_sports_series_and_primary_tag_evidence():
    taxonomy = [{"primaryTagId": 20, "series": "30"}]
    assert evidence({"series": [{"id": 30}]}, taxonomy) == ["sports_series:30"]
    assert evidence({"tags": [{"id": 20}]}, taxonomy) == ["sports_primary_tag:20"]
    assert evidence({"title": "sports"}, taxonomy) == []


@pytest.mark.parametrize(
    "value",
    [
        "NaN",
        "Infinity",
        "-1",
        "0.0000000000000000001",
        "100000000000000000000",
        0.1,
        True,
    ],
)
def test_exact_decimal_rejection(value):
    with pytest.raises(ValueError):
        number(value)


def test_unsorted_snapshot_replacement_zero_removal_and_partial_quote():
    book = Book()
    assert book.apply(normalized()[0])["status"] == "valid"
    assert list(book.asks) == [Decimal(".6"), Decimal(".7")]
    quote = book.quote("BUY", "4")
    assert quote["gross_amount"] == Decimal("2.6")
    assert quote["average_price"] == Decimal(".65")
    assert book.quote("BUY", "8")["complete"] is False
    update = normalized(
        message(
            "price_change",
            price_changes=[
                {"asset_id": "11", "side": "BUY", "price": ".4", "size": "0"}
            ],
        ),
        2,
    )[0]
    state = book.apply(update)
    assert Decimal(".4") not in book.bids
    assert state["displayed_removed"] == 3
    book.apply(normalized()[0])
    assert book.bids[Decimal(".4")] == 3


def test_uninitialized_disconnect_and_reset_do_not_bridge_depth():
    row = normalized(
        message(
            "price_change",
            price_changes=[
                {"asset_id": "11", "side": "BUY", "price": ".4", "size": "9"}
            ],
        )
    )[0]
    book = Book()
    assert book.apply(row)["status"] == "missing_initial_snapshot"
    book.apply(normalized()[0])
    reset = next(
        reset_rows(
            ["11"],
            source="fixture",
            stream="session/1",
            sequence=2,
            received_at=AT,
            reason="disconnect",
        )
    )
    book.apply(reset)
    assert book.apply(row)["status"] == "disconnect"
    assert book.quote("BUY", "1")["complete"] is False
    assert book.apply(normalized()[0])["midpoint_change"] is None


def test_duplicate_timestamp_is_not_duplicate_message():
    rows = normalized(
        message(
            "price_change",
            price_changes=[
                {"asset_id": "11", "side": "BUY", "price": ".4", "size": "9"},
                {"asset_id": "12", "side": "SELL", "price": ".5", "size": "3"},
            ],
        )
    )
    assert [r["ordinal"] for r in rows] == [0, 1]
    assert rows[0]["received_at"] == rows[1]["received_at"]


def test_empty_crossed_and_duplicate_levels():
    with pytest.raises(ValueError, match="duplicate"):
        levels([[".4", "1"], [".4", "2"]], bids=True)
    book = Book()
    book.apply(normalized(message(bids=[], asks=[]))[0])
    assert book.status == "empty"
    book.apply(normalized(message(bids=[[".8", "1"]], asks=[[".7", "1"]]))[0])
    assert book.status == "crossed"
    assert not book.quote("BUY", "1")["complete"]


def test_authoritative_one_sided_book_is_explicit_but_not_quoteable():
    book = Book()
    state = book.apply(normalized(message(bids=[], asks=[[".6", "2"]]))[0])
    assert state["status"] == "one_sided" and state["midpoint"] is None
    assert book.quote("BUY", "1")["quote_status"] == "unavailable"
    assert book.quote("SELL", "1")["quote_status"] == "unavailable"


def test_pair_quote_requires_provenance_and_known_mechanics():
    left, right = Book(), Book()
    left.apply(normalized()[0])
    right.apply(
        normalized(message(asset_id="12", asks=[[".3", "10"]], bids=[[".2", "10"]]))[0]
    )
    assert pair_quote(left, right, "1")["status"] == "unavailable"
    assert (
        pair_quote(left, right, "1", verified_collateral_unit="1")["status"]
        == "unavailable"
    )
    result = pair_quote(
        left,
        right,
        "1",
        verified_collateral_unit="1",
        verified_binary_tokens=["11", "12"],
    )
    assert result["combined_acquisition_cost"] == Decimal(".9")
    assert result["opposite_acquisition_cost"] == Decimal(".3")
    assert result["gross_merge_proceeds"] == Decimal("1")
    assert result["net_synthetic_exit_proceeds"] == Decimal(".7")


def test_initial_tick_size_and_changes_are_replayed():
    book = Book()
    assert book.apply(normalized(message(tick_size=".01"))[0])["tick_size"] == Decimal(
        ".01"
    )
    change = normalized(message("tick_size_change", new_tick_size=".001"), 2)[0]
    assert book.apply(change)["tick_size"] == Decimal(".001")
    assert book.apply(normalized(sequence=3)[0])["tick_size"] == Decimal(".001")
    book.apply({**normalized(sequence=4)[0], "kind": "reset", "reason": "disconnect"})
    assert book.apply(normalized(sequence=5)[0])["tick_size"] is None
    with pytest.raises(ValueError, match="positive"):
        normalized(message(tick_size="0"))


def test_backfill_requires_catalog_before_network_acquisition(store, monkeypatch):
    from oddsfox_pipeline.ingestion.polymarket.sports_data import archive

    def unexpected_inventory(*args, **kwargs):
        pytest.fail("unready backfill attempted network inventory")

    monkeypatch.setattr(archive, "inventory", unexpected_inventory)
    with pytest.raises(FileNotFoundError):
        archive.backfill(store, AT, AT + timedelta(hours=1))


def test_verify_reports_uncommitted_evidence_without_an_active_catalog(store):
    segment = Segments(store, "books/interrupted/updates", UPDATE_SCHEMA)
    segment.append(normalized()[0])
    files = segment.close()
    result = verify(store)
    assert result["status"] == "incomplete_no_active_catalog"
    assert result["catalog_manifest_sha256"] is None
    assert result["uncommitted_segments"] == [files[0]["path"]]
    assert result["completed_recording_seconds"] == 0


def test_resolution_invalidates_both_native_outcomes():
    rows = normalized(message("market_resolved", assets_ids=["11", "12"]), 2)
    assert [row["token_id"] for row in rows] == ["11", "12"]
    book = Book()
    book.apply(normalized()[0])
    assert book.apply(rows[0])["status"] == "market_resolved"
    assert book.quote("BUY", "1")["complete"] is False
    assert book.apply(normalized()[0])["status"] == "market_resolved"


def test_writer_exclusion_budget_and_manifest_corruption(store):
    with store.writer():
        with pytest.raises(RuntimeError, match="another writer"):
            with Store(store.root, store.limits).writer():
                pass
    with pytest.raises(BudgetStop):
        store.check(store.limits.retained_bytes + 1)
    writer = Segments(store, "test", UPDATE_SCHEMA)
    writer.append(normalized()[0])
    files = writer.close()
    assert store.validate_file(files[0]).exists()
    wrong = files[0] | {"sha256": "0" * 64}
    with pytest.raises(ValueError, match="checksum"):
        store.validate_file(wrong)
    with pytest.raises(ValueError, match="escapes"):
        store.path("../outside")


def test_verify_rejects_wrong_relation_schema(store):
    refresh_catalog(store, client=Gamma())
    writer = Segments(store, "fixture/exclusions", UPDATE_SCHEMA)
    writer.append(normalized()[0])
    files = writer.close()
    store.commit("record", "wrong-exclusion-schema", files, exclusions=files)
    with pytest.raises(ValueError, match="schema mismatch"):
        verify(store)


def test_byte_identical_build_and_quantity_quote(store):
    refresh_catalog(store, client=Gamma())
    writer = Segments(store, "fixture/updates", UPDATE_SCHEMA)
    writer.append(normalized()[0])
    files = writer.close()
    store.commit("record", "fixture", files, source="fixture", updates=files)
    first = build(store, AT, AT + timedelta(hours=1))
    # Remove only this test's derived manifest to exercise actual regeneration,
    # not merely the resumable cached-output path.
    store.path(first["path"]).unlink()
    second = build(store, AT, AT + timedelta(hours=1))
    assert first["sha256"] == second["sha256"]
    quote = historical_quote(store, "fixture", "11", "BUY", "2", AT)
    assert quote["gross_amount"] == Decimal("1.2")


def test_historical_quote_fails_closed_on_overlapping_independent_streams(store):
    refresh_catalog(store, client=Gamma())
    for suffix, price in (("a", ".6"), ("b", ".7")):
        row = normalized(message(asks=[{"price": price, "size": "3"}]))[0]
        row.update(
            stream=f"session/{suffix}",
            raw_identity=f"fixture:{suffix}",
        )
        writer = Segments(store, f"fixture/{suffix}/updates", UPDATE_SCHEMA)
        writer.append(row)
        files = writer.close()
        store.commit(
            "record",
            f"fixture-{suffix}",
            files,
            source="fixture",
            updates=files,
            observed_until={f"session/{suffix}": AT + timedelta(minutes=5)},
        )
    quote = historical_quote(store, "fixture", "11", "BUY", "1", AT)
    assert quote["status"] == "unavailable"
    assert quote["independent_streams"] == 2
    assert "overlap" in quote["reason"]


def test_archive_reingestion_is_deduplicated_by_source_identity(store):
    client = Gamma()
    refresh_catalog(store, client=client)
    row = {
        "timestamp_received": AT,
        "timestamp": AT,
        "market": CONDITION.encode(),
        "event_type": "book",
        "asset_id": "11",
        "bids": '[[".4","2"]]',
        "asks": '[[".6","3"]]',
    }
    path = store.path("repeat.parquet")
    pq.write_table(pa.Table.from_pylist([row], schema=V2_SCHEMA), path)
    item = {"version": "v2", "hour": AT}
    ingest_file(store, path, item, {})
    client.received_at += timedelta(days=1)
    refresh_catalog(store, client=client)
    ingest_file(store, path, item, {})
    assert historical_quote(store, "pmxt-v2", "11", "BUY", "1", AT)[
        "gross_amount"
    ] == Decimal(".6")
    rebuilt = build(store, AT, AT + timedelta(hours=1))
    assert rebuilt["counts"] == {"valid": 1}
    assert rebuilt["row_accounting"]["identical_reingestion_rows"] == 1


def test_shards_keep_native_pair_together_and_never_truncate():
    targets = {str(i): {"condition_id": str(i // 2)} for i in range(12)}
    result = shards(targets, 5)
    assert sorted(t for group in result for t in group) == sorted(targets)
    assert all(len(group) == 4 for group in result)
    error = ValueError("redirect")
    assert NoRedirectConnect.process_redirect(None, error) is error
    assert exception_category(asyncio.TimeoutError()) == "transport"


def test_archive_listing_and_v1_mapping():
    parser = Listing()
    parser.feed(
        "<script>Page 9 of 9</script><pre><span><span>1.</span>"
        '<a href="https://r2.pmxt.dev/polymarket_orderbook_2026-01-01T00.parquet">x</a>'
        "Thu, 1 Jan 2026<!-- --> 1.5 MB<!-- -->\n</span></pre>Page 1 of 2"
    )
    assert list(parser.files.values()) == [1500000]
    assert "Page 9" not in " ".join(parser.text)
    row = {
        "market_id": CONDITION,
        "timestamp_created_at": AT,
        "update_type": "price_change",
        "data": json.dumps(
            {
                "token_id": "11",
                "side": "YES",
                "change_side": "SELL",
                "change_price": ".6",
                "change_size": "0",
            }
        ),
    }
    result = normalized(json_bytes(archive_message(row, "v1")))[0]
    assert result["side"] == "SELL" and result["quantity"] == 0


def test_price_change_retains_message_level_source_hash():
    payload = message(
        "price_change",
        hash="source-hash",
        price_changes=[{"asset_id": "11", "side": "BUY", "price": ".4", "size": "2"}],
    )
    assert normalized(payload)[0]["source_hash"] == "source-hash"


def test_archive_exclusions_preserve_exact_contiguous_row_spans():
    assert exclusion_runs([False, True, True, False, True, False]) == [(1, 2), (4, 1)]
    assert exclusion_runs([]) == []


@pytest.mark.parametrize("version", ["v1", "v2"])
def test_archive_native_schema_and_deterministic_ingestion(store, version):
    refresh_catalog(store, client=Gamma())
    if version == "v1":
        row = {
            "timestamp_received": AT,
            "timestamp_created_at": AT,
            "market_id": CONDITION,
            "update_type": "book_snapshot",
            "data": json.dumps(
                {
                    "update_type": "book_snapshot",
                    "token_id": "11",
                    "bids": [[".4", "2"]],
                    "asks": [[".6", "3"]],
                }
            ),
        }
    else:
        row = {
            "timestamp_received": AT,
            "timestamp": AT,
            "market": CONDITION.encode(),
            "event_type": "book",
            "asset_id": "11",
            "bids": '[[".4","2"]]',
            "asks": '[[".6","3"]]',
        }
    path = store.path("archive-fixture.parquet")
    pq.write_table(
        pa.Table.from_pylist([row], schema=V1_SCHEMA if version == "v1" else V2_SCHEMA),
        path,
    )
    item = {"version": version, "hour": AT, "url": "https://example.invalid"}
    result = ingest_file(store, path, item, {})
    assert result["counts"]["normalized_rows"] == 1
    source = f"pmxt-{version}"
    assert historical_quote(
        store, source, "11", "BUY", "1", AT + timedelta(minutes=59)
    )["complete"]
    assert (
        historical_quote(store, source, "11", "BUY", "1", AT + timedelta(hours=1))[
            "status"
        ]
        == "unavailable"
    )
    assert ingest_file(store, path, item, {})["files"] == result["files"]
    with pytest.raises(ValueError, match="acquisition checksum"):
        ingest_file(store, path, item, {"sha256": "0" * 64})


def test_archive_schema_drift_fails_before_publication(store):
    refresh_catalog(store, client=Gamma())
    path = store.path("wrong.parquet")
    pq.write_table(pa.table({"wrong": [1]}), path)
    with pytest.raises(ValueError, match="unsupported"):
        ingest_file(store, path, {"version": "v1", "hour": AT}, {})
    assert list(store.manifests("archive")) == []


def test_unknown_archive_token_invalidates_known_condition_books(store):
    refresh_catalog(store, client=Gamma())
    rows = [
        {
            "timestamp_received": AT,
            "timestamp": AT,
            "market": CONDITION.encode(),
            "event_type": "book",
            "asset_id": "11",
            "bids": '[[".4","2"]]',
            "asks": '[[".6","3"]]',
        },
        {
            "timestamp_received": AT + timedelta(seconds=1),
            "timestamp": AT + timedelta(seconds=1),
            "market": CONDITION.encode(),
            "event_type": "book",
            "asset_id": "99",
            "bids": '[[".4","2"]]',
            "asks": '[[".6","3"]]',
        },
    ]
    path = store.path("unknown-token.parquet")
    pq.write_table(pa.Table.from_pylist(rows, schema=V2_SCHEMA), path)
    result = ingest_file(store, path, {"version": "v2", "hour": AT}, {})
    assert result["counts"]["unknown_or_conflicting_token"] == 1
    quote = historical_quote(
        store, "pmxt-v2", "11", "BUY", "1", AT + timedelta(seconds=1)
    )
    assert quote["status"] == "unknown_or_conflicting_archive_identity"


def test_valid_zero_row_archive_is_explicitly_empty(store):
    refresh_catalog(store, client=Gamma())
    path = store.path("empty.parquet")
    pq.write_table(pa.Table.from_pylist([], schema=V1_SCHEMA), path)
    result = ingest_file(store, path, {"version": "v1", "hour": AT}, {})
    assert result["empty_archive"]
    assert result["counts"]["source_rows"] == 0
    assert result["updates"] == []


def test_empty_archive_objects_count_toward_bounded_sample_limit(store, monkeypatch):
    from oddsfox_pipeline.ingestion.polymarket.sports_data import archive

    refresh_catalog(store, client=Gamma())
    monkeypatch.setattr(
        archive,
        "inventory",
        lambda *args: {
            "inventory_path": "fixture.json",
            "counts": {"listed": 2},
            "files": [
                {
                    "availability": "listed",
                    "url": f"https://r2.pmxt.dev/example-{i}.parquet",
                }
                for i in range(2)
            ],
        },
    )

    class HTTP:
        def __init__(self, *args):
            pass

        def close(self):
            pass

    monkeypatch.setattr(archive, "PublicHTTP", HTTP)
    monkeypatch.setattr(archive, "download", lambda *args: (None, {"status": "empty"}))
    result = archive.backfill(store, AT, AT + timedelta(hours=1), max_files=1)
    assert result["file_status_counts"] == {"empty": 1}
    assert result["pending_listed_files"] == 1


class HTTPResponse:
    def __init__(self, body=b"", status=200, headers=None):
        self.body, self.status_code, self.headers = body, status, headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(str(self.status_code))

    def iter_content(self, size):
        for index in range(0, len(self.body), size):
            yield self.body[index : index + size]

    def close(self):
        pass


def test_archive_inventory_paginates_both_versions_and_counts_missing_hours(
    store, monkeypatch
):
    from oddsfox_pipeline.ingestion.polymarket.sports_data import archive

    monkeypatch.setattr(archive, "public_url", lambda source, url: url)
    calls = []

    class HTTP:
        def request(self, method, url):
            calls.append(url)
            page = int(url.rsplit("=", 1)[1])
            host = "r2v2" if "/v2?" in url else "r2"
            body = (
                f'<pre><span><span>1.</span><a href="https://{host}.pmxt.dev/'
                f'polymarket_orderbook_2026-01-01T0{page - 1}.parquet">file</a>'
                f"<!-- --> 1.5 MB<!-- -->\n</span></pre>Page {page} of 2"
            ).encode()
            return HTTPResponse(body)

    result = inventory(store, AT, AT + timedelta(hours=3), http=HTTP())
    assert len(calls) == 4
    assert result["counts"] == {"listed": 4, "not_listed": 2}
    assert result["version_counts"] == {
        "v1": {"listed": 2, "not_listed": 1},
        "v2": {"listed": 2, "not_listed": 1},
    }
    assert result["schema_status_counts"] == {
        "declared_format_not_yet_file_validated": 6
    }
    assert result["estimated_listed_bytes"] == 6000000
    assert store.path(result["inventory_path"]).is_file()
    assert store.downloaded > 0
    assert verify(store)["inventories"][0]["counts"] == result["counts"]


def test_unrecognized_archive_listing_cannot_claim_empty_coverage(store, monkeypatch):
    from oddsfox_pipeline.ingestion.polymarket.sports_data import archive

    monkeypatch.setattr(archive.time, "sleep", lambda seconds: None)

    class HTTP:
        def request(self, *args):
            return HTTPResponse(b"<html>Temporarily unavailable</html>")

    with pytest.raises(ValueError, match="no recognized file rows"):
        inventory(store, AT, AT + timedelta(hours=1), http=HTTP())
    assert list(store.root.glob("inventories/*.json")) == []


def test_archive_placeholder_retry_is_bounded_and_counted(store, monkeypatch):
    from oddsfox_pipeline.ingestion.polymarket.sports_data import archive

    monkeypatch.setattr(archive.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(archive, "public_url", lambda source, url: url)

    class HTTP:
        calls = 0
        persistent = False

        def request(self, method, url):
            self.calls += 1
            if self.persistent or self.calls % 2:
                return HTTPResponse(b"<pre>coming soon</pre>")
            host = "r2v2" if "/v2?" in url else "r2"
            return HTTPResponse(
                (
                    f'<pre><span><a href="https://{host}.pmxt.dev/'
                    'polymarket_orderbook_2026-01-01T00.parquet">file</a> 1 MB'
                    "</span></pre>Page 1 of 1"
                ).encode()
            )

    client = HTTP()
    result = inventory(store, AT, AT + timedelta(hours=1), http=client)
    assert result["counts"] == {"listed": 2}
    assert len(result["listing_retries"]) == 2
    assert client.calls == 4
    client.calls, client.persistent = 0, True
    with pytest.raises(ValueError, match="no recognized file rows"):
        inventory(store, AT, AT + timedelta(hours=1), http=client)
    assert client.calls == 4


def test_archive_range_download_resume_and_changed_object(store, monkeypatch):
    from oddsfox_pipeline.ingestion.polymarket.sports_data import archive

    monkeypatch.setattr(archive, "public_url", lambda source, url: url)
    buffer = pa.BufferOutputStream()
    pq.write_table(pa.table({"x": [1, 2, 3]}), buffer)
    content = buffer.getvalue().to_pybytes()

    class HTTP:
        etag = '"original"'
        calls = 0

        def request(self, method, url, headers=None):
            if method == "HEAD":
                return HTTPResponse(
                    headers={"Content-Length": str(len(content)), "ETag": self.etag}
                )
            self.calls += 1
            start, end = map(int, headers["Range"][6:].split("-"))
            return HTTPResponse(
                content[start : end + 1],
                206,
                {
                    "ETag": self.etag,
                    "Content-Range": f"bytes {start}-{end}/{len(content)}",
                },
            )

    client = HTTP()
    item = {"version": "v1", "url": "https://r2.pmxt.dev/example.parquet"}
    store.limits.download_bytes = len(content) // 2
    with pytest.raises(BudgetStop):
        download(store, item, client)
    assert len(list(store.root.rglob("*.partial"))) == 1
    resumed = Store(store.root, Limits(reserve_bytes=0))
    path, metadata = download(resumed, item, client)
    assert path.read_bytes() == content
    assert metadata["sha256"] == sha256(path)
    assert resumed.downloaded + store.downloaded == len(content)
    assert download(resumed, item, client)[0] == path
    checkpoint_path = path.with_suffix(".json")
    ready = json.loads(checkpoint_path.read_bytes()) | {"status": "ready_to_activate"}
    resumed.atomic_json(str(checkpoint_path.relative_to(resumed.root)), ready)
    assert download(resumed, item, client)[1]["recovered_activation"]
    client.etag = '"changed"'
    with pytest.raises(ValueError, match="changed"):
        download(resumed, item, client)
    assert path.read_bytes() == content


def test_cached_archive_requires_revalidatable_upstream_identity(store, monkeypatch):
    from oddsfox_pipeline.ingestion.polymarket.sports_data import archive

    monkeypatch.setattr(archive, "public_url", lambda source, url: url)
    buffer = pa.BufferOutputStream()
    pq.write_table(pa.table({"x": [1]}), buffer)
    content = buffer.getvalue().to_pybytes()

    class HTTP:
        def request(self, method, url, headers=None):
            if method == "HEAD":
                return HTTPResponse(headers={"Content-Length": str(len(content))})
            start, end = map(int, headers["Range"][6:].split("-"))
            return HTTPResponse(
                content[start : end + 1],
                206,
                {"Content-Range": f"bytes {start}-{end}/{len(content)}"},
            )

    item = {"version": "v1", "url": "https://r2.pmxt.dev/example.parquet"}
    path = download(store, item, HTTP())[0]
    assert path.is_file()
    assert verify(store)["uncommitted_downloads"] == [str(path.relative_to(store.root))]
    with pytest.raises(ValueError, match="cannot be revalidated"):
        download(store, item, HTTP())


def test_download_crash_tail_is_revalidated_and_charged(store, monkeypatch):
    from oddsfox_pipeline.ingestion.polymarket.sports_data import archive

    monkeypatch.setattr(archive, "public_url", lambda source, url: url)
    buffer = pa.BufferOutputStream()
    pq.write_table(pa.table({"x": [1]}), buffer)
    content = buffer.getvalue().to_pybytes()

    class HTTP:
        def request(self, method, url, headers=None):
            if method == "HEAD":
                return HTTPResponse(
                    headers={"Content-Length": str(len(content)), "ETag": '"fixed"'}
                )
            start, end = map(int, headers["Range"][6:].split("-"))
            return HTTPResponse(
                content[start : end + 1],
                206,
                {
                    "ETag": '"fixed"',
                    "Content-Range": f"bytes {start}-{end}/{len(content)}",
                },
            )

    atomic = store.atomic_json

    def interrupted(relative, value):
        if value.get("verified_bytes", 0):
            raise OSError("simulated crash before checkpoint")
        return atomic(relative, value)

    monkeypatch.setattr(store, "atomic_json", interrupted)
    item = {"version": "v1", "url": "https://r2.pmxt.dev/example.parquet"}
    with pytest.raises(OSError, match="simulated"):
        download(store, item, HTTP())
    monkeypatch.setattr(store, "atomic_json", atomic)
    path, _ = download(store, item, HTTP())
    assert path.read_bytes() == content
    assert store.downloaded == 2 * len(content)


def test_truncated_retry_tail_is_immediately_counted_as_retained(store, monkeypatch):
    import requests

    from oddsfox_pipeline.ingestion.polymarket.sports_data import archive

    monkeypatch.setattr(archive, "public_url", lambda source, url: url)
    buffer = pa.BufferOutputStream()
    pq.write_table(pa.table({"x": [1]}), buffer)
    content = buffer.getvalue().to_pybytes()

    class Broken(HTTPResponse):
        def iter_content(self, size):
            yield self.body[: len(self.body) // 2]
            raise requests.ConnectionError("synthetic truncated range")

    class HTTP:
        def request(self, method, url, headers=None):
            if method == "HEAD":
                return HTTPResponse(
                    headers={"Content-Length": str(len(content)), "ETag": '"fixed"'}
                )
            start, end = map(int, headers["Range"][6:].split("-"))
            return Broken(
                content[start : end + 1],
                206,
                {
                    "ETag": '"fixed"',
                    "Content-Range": f"bytes {start}-{end}/{len(content)}",
                },
            )

    item = {"version": "v1", "url": "https://r2.pmxt.dev/example.parquet"}
    with pytest.raises(requests.ConnectionError, match="truncated"):
        download(store, item, HTTP())
    partial = next(store.root.rglob("*.partial"))
    assert partial.stat().st_size > 0
    assert store.usage() >= partial.stat().st_size


def test_free_archive_download_rejects_paid_host(store, monkeypatch):
    from oddsfox_pipeline.ingestion.polymarket.sports_data import archive

    monkeypatch.setattr(archive, "public_url", lambda source, url: url)
    with pytest.raises(ValueError, match="free version host"):
        download(store, {"version": "v1", "url": "https://api.pmxt.dev/file"}, None)


@pytest.mark.parametrize("value", [None, "unknown", "-1"])
def test_archive_download_requires_valid_nonnegative_size(store, monkeypatch, value):
    from oddsfox_pipeline.ingestion.polymarket.sports_data import archive

    monkeypatch.setattr(archive, "public_url", lambda source, url: url)

    class HTTP:
        def request(self, *args, **kwargs):
            headers = {} if value is None else {"Content-Length": value}
            return HTTPResponse(headers=headers)

    with pytest.raises(ValueError, match="Content-Length"):
        download(
            store,
            {"version": "v1", "url": "https://r2.pmxt.dev/example.parquet"},
            HTTP(),
        )


@pytest.mark.parametrize(
    "status,expected", [(404, "missing"), (403, "unavailable"), (200, "empty")]
)
def test_archive_missing_unavailable_and_empty_are_distinct(
    store, monkeypatch, status, expected
):
    from oddsfox_pipeline.ingestion.polymarket.sports_data import archive

    monkeypatch.setattr(archive, "public_url", lambda source, url: url)

    class HTTP:
        def request(self, *args, **kwargs):
            return HTTPResponse(status=status, headers={"Content-Length": "0"})

    path, result = download(
        store, {"version": "v1", "url": "https://r2.pmxt.dev/example.parquet"}, HTTP()
    )
    assert path is None and result["status"] == expected


def test_public_http_retries_but_rejects_redirects(monkeypatch):
    from oddsfox_pipeline.ingestion.polymarket.sports_data import transport

    monkeypatch.setattr(transport, "public_url", lambda source, url: url)
    with pytest.raises(ValueError, match="rate >= 1"):
        transport.PublicHTTP("pmxt", "https://r2.pmxt.dev", rate=0.5)
    monkeypatch.setattr(transport.time, "sleep", lambda value: None)
    client = transport.PublicHTTP("pmxt", "https://r2.pmxt.dev")
    responses = iter(
        [
            HTTPResponse(status=429, headers={"Retry-After": "0"}),
            HTTPResponse(status=200),
        ]
    )
    monkeypatch.setattr(
        client.client.session, "request", lambda *args, **kwargs: next(responses)
    )
    assert client.request("GET", "https://r2.pmxt.dev/file").status_code == 200
    monkeypatch.setattr(
        client.client.session,
        "request",
        lambda *args, **kwargs: HTTPResponse(status=302),
    )
    with pytest.raises(ValueError, match="redirect"):
        client.request("GET", "https://r2.pmxt.dev/file")
    with pytest.raises(ValueError, match="read-only"):
        client.request("POST", "https://r2.pmxt.dev/file")


def test_conflicting_archive_timestamp_invalidates_until_new_snapshot(store):
    refresh_catalog(store, client=Gamma())
    first = {
        "timestamp_received": AT,
        "timestamp": AT,
        "market": CONDITION.encode(),
        "event_type": "book",
        "asset_id": "11",
        "bids": '[[".4","2"]]',
        "asks": '[[".6","3"]]',
    }
    change = first | {
        "event_type": "price_change",
        "price": Decimal(".4"),
        "size": Decimal("5"),
        "side": "BUY",
        "bids": None,
        "asks": None,
    }
    last = first | {"timestamp_received": AT + timedelta(seconds=1)}
    path = store.path("tied.parquet")
    pq.write_table(pa.Table.from_pylist([first, change, last], schema=V2_SCHEMA), path)
    result = ingest_file(store, path, {"version": "v2", "hour": AT}, {})
    assert result["counts"]["uncertain_order_rows"] == 1
    assert (
        historical_quote(store, "pmxt-v2", "11", "BUY", "1", AT)["status"]
        == "ambiguous_archive_order"
    )
    assert historical_quote(
        store, "pmxt-v2", "11", "BUY", "1", AT + timedelta(seconds=1)
    )["complete"]


def test_archive_boundary_ties_cannot_be_healed_by_repeated_same_time_snapshot(store):
    refresh_catalog(store, client=Gamma())
    boundary = AT + timedelta(hours=1)
    base = {
        "timestamp_received": boundary,
        "timestamp": boundary,
        "market": CONDITION.encode(),
        "event_type": "book",
        "asset_id": "11",
        "bids": '[[".4","2"]]',
        "asks": '[[".6","3"]]',
    }
    changed = base | {"asks": '[[".7","3"]]'}
    for offset, rows in enumerate(
        (
            [base],
            [
                changed,
                changed,
                changed | {"timestamp_received": boundary + timedelta(seconds=1)},
            ],
        )
    ):
        path = store.path(f"boundary-{offset}.parquet")
        pq.write_table(pa.Table.from_pylist(rows, schema=V2_SCHEMA), path)
        ingest_file(
            store, path, {"version": "v2", "hour": AT + timedelta(hours=offset)}, {}
        )
    assert (
        historical_quote(store, "pmxt-v2", "11", "BUY", "1", boundary)["status"]
        == "ambiguous_archive_order"
    )
    assert historical_quote(
        store, "pmxt-v2", "11", "BUY", "1", boundary + timedelta(seconds=1)
    )["complete"]
    result = build(store, AT, boundary + timedelta(seconds=2))
    assert result["counts"]["ambiguous_archive_order"] == 2


def test_catalog_duplicate_cursor_and_duplicate_page_fail_closed(store):
    class Duplicate(Gamma):
        def get(self, endpoint, params=None):
            result = super().get(endpoint, params)
            if endpoint.endswith("keyset"):
                result["next_cursor"] = "same"
            return result

    with pytest.raises(RuntimeError, match="non-advancing"):
        refresh_catalog(store, client=Duplicate())
    assert not store.path("catalog-active.json").exists()


def test_removed_membership_does_not_carry_forward(store):
    client = Gamma()
    refresh_catalog(store, client=client)

    class Removed(Gamma):
        received_at = AT + timedelta(days=1)

        def get(self, endpoint, params=None):
            result = super().get(endpoint, params)
            if endpoint == "/events/keyset" and result["events"]:
                result["events"][0]["markets"] = []
            return result

    result = refresh_catalog(store, client=Removed())
    assert result["counts"]["orphan_markets"] == 1


def test_removed_inferred_sports_membership_remains_in_observed_history(store):
    class Inferred(Gamma):
        def __init__(self):
            super().__init__()
            self.removed = False

        def get(self, endpoint, params=None):
            result = super().get(endpoint, params)
            if endpoint == "/events/keyset" and result["events"]:
                result["events"][0]["tags"] = []
                if self.removed:
                    result["events"][0]["markets"] = []
            return result

    client = Inferred()
    assert refresh_catalog(store, client=client)["counts"]["event"] == 1
    client.removed = True
    client.market.pop("sportsMarketType")
    client.received_at = AT + timedelta(days=1)
    result = refresh_catalog(store, client=client)
    assert result["counts"]["event"] == 1
    assert result["counts"]["historical_sports_evidence_only"] >= 1


def test_removed_sports_classification_retains_history_but_stops_admission(store):
    class TagOnly(Gamma):
        def __init__(self):
            super().__init__()
            self.market.pop("sportsMarketType")
            self.remove_tag = False

        def get(self, endpoint, params=None):
            result = super().get(endpoint, params)
            if self.remove_tag and endpoint == "/events/keyset" and result["events"]:
                result["events"][0]["tags"] = []
            return result

    client = TagOnly()
    refresh_catalog(store, client=client)
    assert len(catalog_targets(store)) == 2
    client.remove_tag = True
    client.received_at = AT + timedelta(days=1)
    result = refresh_catalog(store, client=client)
    assert result["counts"]["market"] == 1
    assert result["counts"]["historical_sports_evidence_only"] == 2
    assert catalog_targets(store) == {}
    assert len(catalog_targets(store, admitted_only=False)) == 2


def test_archive_contiguous_hours_carry_but_missing_hour_resets(store):
    refresh_catalog(store, client=Gamma())
    base = {
        "timestamp_received": AT,
        "timestamp": AT,
        "market": CONDITION.encode(),
        "event_type": "book",
        "asset_id": "11",
        "bids": '[[".4","2"]]',
        "asks": '[[".6","3"]]',
    }
    for offset in (0, 1, 3):
        hour = AT + timedelta(hours=offset)
        row = base | {"timestamp_received": hour, "timestamp": hour}
        if offset:
            row.update(
                event_type="price_change",
                price=Decimal(".6"),
                size=Decimal("7"),
                side="SELL",
                bids=None,
                asks=None,
            )
        path = store.path(f"hour-{offset}.parquet")
        pq.write_table(pa.Table.from_pylist([row], schema=V2_SCHEMA), path)
        ingest_file(store, path, {"version": "v2", "hour": hour}, {})
    carried = historical_quote(
        store, "pmxt-v2", "11", "BUY", "5", AT + timedelta(hours=1)
    )
    assert carried["complete"] and carried["gross_amount"] == 3
    broken = historical_quote(
        store, "pmxt-v2", "11", "BUY", "1", AT + timedelta(hours=3)
    )
    assert broken["status"] == "missing_initial_snapshot"
    result = build(store, AT, AT + timedelta(hours=4))
    assert result["counts"] == {"valid": 2, "missing_initial_snapshot": 1}


def test_budget_reservations_are_released_after_failure(store):
    store.limits.retained_bytes = 4096
    with pytest.raises(RuntimeError):
        with store.reserve(2048):
            assert store.check()["remaining_retained_bytes"] == 2048
            with pytest.raises(BudgetStop):
                with store.reserve(2049):
                    pass
            raise RuntimeError("simulated interrupted writer")
    assert store.check()["remaining_retained_bytes"] == 4096


def test_managed_root_cannot_accidentally_enter_a_public_diff():
    root = Path(__file__).resolve().parents[3] / "unignored-sports-data"
    assert not root.exists()
    with pytest.raises(ValueError, match="gitignored"):
        Store(root)
    assert not root.exists()


def test_missing_condition_never_admits_historical_tokens(store):
    client = Gamma()
    client.market.pop("conditionId")
    refresh_catalog(store, client=client)
    assert catalog_targets(store, admitted_only=False) == {}


def test_unknown_message_and_malformed_condition_fail_closed():
    with pytest.raises(ValueError, match="unsupported"):
        normalized(message("future_book_protocol"))
    with pytest.raises(ValueError, match="condition"):
        normalized(message(market="untrusted source text"))


def test_no_sports_is_a_complete_catalog_not_a_failed_crawl(store):
    class NonSports(Gamma):
        def get(self, endpoint, params=None):
            result = super().get(endpoint, params)
            for event in result.get("events", []) if isinstance(result, dict) else []:
                event["tags"] = []
            return result

    client = NonSports()
    client.market.pop("sportsMarketType")
    result = refresh_catalog(store, client=client)
    assert result["complete"] and result["counts"]["admitted_tokens"] == 0

"""Offline all-sports command integration with synthetic source transports."""

import asyncio
import json
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from oddsfox_pipeline.ingestion.polymarket.sports_data import recorder
from oddsfox_pipeline.ingestion.polymarket.sports_data.books import build
from oddsfox_pipeline.ingestion.polymarket.sports_data.catalog import (
    catalog_targets,
    refresh_catalog,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.storage import (
    Limits,
    Store,
    json_bytes,
    now,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.verify import verify

pytestmark = pytest.mark.integration


class SportsSource:
    def get(self, endpoint, params=None):
        if endpoint == "/sports":
            return [{"id": 1, "sport": "example", "primaryTagId": 1}]
        if endpoint == "/sports/market-types":
            return {
                "marketTypes": [
                    "moneyline",
                    "player_points",
                    "first_period_total",
                    "esports_winner",
                ]
            }
        if endpoint == "/tags/slug/sports":
            return {"id": 1}
        markets = [
            {
                "id": str(i),
                "conditionId": "0x" + f"{i:064x}",
                "sportsMarketType": kind,
                "clobTokenIds": [str(i * 2), str(i * 2 + 1)],
                "outcomes": ["Yes", "No"],
                "enableOrderBook": True,
                "closed": False,
            }
            for i, kind in enumerate(
                ("moneyline", "player_points", "first_period_total", "esports_winner"),
                1,
            )
        ]
        key = "events" if "events" in endpoint else "markets"
        rows = (
            [{"id": "1", "tags": [{"id": 1}], "markets": markets}]
            if key == "events"
            else markets
        )
        return {key: [] if params["closed"] else rows, "next_cursor": None}


@pytest.mark.parametrize("cross_hour", [False, True])
def test_record_restart_replay_and_verify(tmp_path, monkeypatch, cross_hour):
    store = Store(tmp_path, Limits(reserve_bytes=0))
    refresh_catalog(store, client=SportsSource())
    monkeypatch.setattr(recorder, "validate_outbound_wss_url", lambda url: url)
    sent = []
    clock = [now().replace(minute=59, second=59, microsecond=0)]
    next_hour = clock[0] + timedelta(seconds=1)
    if cross_hour:
        monkeypatch.setattr(recorder, "now", lambda: clock[0])

    class Connection:
        def __init__(self, *args, **kwargs):
            self.tokens = []
            self.initial = True

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def send(self, payload):
            sent.append(payload)
            if payload != "PING":
                self.tokens = json.loads(payload)["assets_ids"]

        async def recv(self, decode=None):
            if self.initial:
                self.initial = False
                return json_bytes(
                    [
                        {
                            "event_type": "book",
                            "asset_id": token,
                            "market": "0x" + f"{int(token) // 2:064x}",
                            "timestamp": "1767225600000",
                            "bids": [{"price": ".4", "size": "10"}],
                            "asks": [{"price": ".6", "size": "10"}],
                        }
                        for token in self.tokens
                    ]
                )
            await asyncio.sleep(0.01)
            if cross_hour:
                clock[0] = next_hour
            return "PONG"

    started = clock[0] if cross_hour else now()
    with store.writer():
        first = asyncio.run(
            recorder.record(
                store,
                1,
                connector=Connection,
                shard_size=4,
                rotation_seconds=0.2,
                refresh_seconds=36000,
            )
        )
        second = asyncio.run(
            recorder.record(
                store,
                1,
                connector=Connection,
                shard_size=4,
                rotation_seconds=0.2,
                refresh_seconds=36000,
            )
        )
    assert first["session"] != second["session"]
    assert set(first["tokens"].values()) == {"recorded"}
    assert first["admitted"] == 8
    assert first["metrics"].get("overflow_dropped_messages", 0) == 0
    assert all(
        value == "PING" or json.loads(value)["type"] == "market" for value in sent
    )
    ended = clock[0] if cross_hour else now()
    report = build(store, started, ended + timedelta(seconds=1))
    assert report["counts"]["valid"] == 16
    assert verify(store)["admitted_tokens"] == 8
    if cross_hour:
        hours = set()
        for _, manifest in store.manifests("record"):
            for item in manifest["files"]:
                for row in pq.read_table(store.path(item["path"])).to_pylist():
                    hour = row["received_at"].strftime("%Y-%m-%d/%H")
                    assert f"/{hour}/" in item["path"]
                    hours.add(hour)
        assert len(hours) == 2


def test_cli_help_has_no_trading_surface():
    script = Path(__file__).resolve().parents[2] / "scripts/polymarket_sports_data.py"
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        text=True,
        capture_output=True,
        check=True,
    )
    assert "catalog-refresh" in result.stdout
    for forbidden in ("api-key", "private-key", "place-order", "sign-order"):
        assert forbidden not in result.stdout


def test_restart_does_not_postpone_overdue_full_reconciliation(tmp_path, monkeypatch):
    store = Store(tmp_path, Limits(reserve_bytes=0))
    catalog = refresh_catalog(store, client=SportsSource())
    monkeypatch.setattr(recorder, "validate_outbound_wss_url", lambda url: url)
    monkeypatch.setattr(
        recorder,
        "active_catalog",
        lambda store: (
            catalog
            | {
                "last_full_refresh_at": (now() - timedelta(days=2)).isoformat(),
                "last_open_refresh_at": now().isoformat(),
            }
        ),
    )
    calls = []

    def refresh(*args, **kwargs):
        calls.append(kwargs)
        raise ValueError("synthetic source outage")

    monkeypatch.setattr(recorder, "refresh_catalog", refresh)

    class Connection:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def send(self, value):
            pass

        async def recv(self, decode=None):
            await asyncio.sleep(0.05)
            return "PONG"

    asyncio.run(recorder.record(store, 1, connector=Connection))
    assert len(calls) == 1
    assert calls[0]["open_only"] is False
    assert calls[0]["target_conditions"] is None


def test_resolved_token_remains_terminal_in_session_summary(tmp_path, monkeypatch):
    store = Store(tmp_path, Limits(reserve_bytes=0))
    refresh_catalog(store, client=SportsSource())
    targets = catalog_targets(store)
    token = next(iter(targets))
    monkeypatch.setattr(recorder, "validate_outbound_wss_url", lambda url: url)

    class Connection:
        def __init__(self, *args, **kwargs):
            self.sent_resolution = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def send(self, payload):
            pass

        async def recv(self, decode=None):
            if not self.sent_resolution:
                self.sent_resolution = True
                return json_bytes(
                    {
                        "event_type": "market_resolved",
                        "market": targets[token]["condition_id"],
                        "assets_ids": [token],
                        "timestamp": "1767225600000",
                    }
                )
            await asyncio.sleep(0.01)
            return "PONG"

    result = asyncio.run(
        recorder.record(
            store,
            1,
            connector=Connection,
            refresh_seconds=36000,
            rotation_seconds=0.2,
        )
    )
    assert result["tokens"][token] == "unavailable"
    assert result["final_states"][token] == "market_resolved"


def test_transient_queue_pressure_applies_backpressure(tmp_path, monkeypatch):
    store = Store(tmp_path, Limits(reserve_bytes=0))
    refresh_catalog(store, client=SportsSource())
    targets = catalog_targets(store)
    monkeypatch.setattr(recorder, "validate_outbound_wss_url", lambda url: url)
    original_append = recorder.Segments.append

    def slow_append(segment, row):
        time.sleep(0.002)
        original_append(segment, row)

    monkeypatch.setattr(recorder.Segments, "append", slow_append)

    class Connection:
        def __init__(self, *args, **kwargs):
            self.tokens = []
            self.initial = True

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def send(self, payload):
            if payload != "PING":
                self.tokens = json.loads(payload)["assets_ids"]

        async def recv(self, decode=None):
            if self.initial:
                self.initial = False
                return json_bytes(
                    [
                        {
                            "event_type": "book",
                            "asset_id": token,
                            "market": targets[token]["condition_id"],
                            "timestamp": "1767225600000",
                            "bids": [{"price": ".4", "size": "1"}],
                            "asks": [{"price": ".6", "size": "1"}],
                        }
                        for token in self.tokens
                    ]
                )
            return "PONG"

    result = asyncio.run(
        recorder.record(
            store,
            1,
            connector=Connection,
            refresh_seconds=36000,
            rotation_seconds=5,
            shard_size=2,
            queue_messages=1,
            queue_bytes=64 * 1024,
            connection_start_stagger_seconds=0,
        )
    )
    assert result["metrics"]["queue_backpressure_events"] > 0
    assert result["metrics"].get("overflow_dropped_messages", 0) == 0
    assert result["metrics"]["peak_queue_bytes"] <= 64 * 1024
    assert result["metrics"]["max_queue_backpressure_seconds"] < 0.5
    assert set(result["tokens"].values()) == {"recorded"}


@pytest.mark.parametrize(
    "mode", ["overflow", "inactivity", "parse", "internal", "tls_resume"]
)
def test_recorder_loss_is_visible_and_invalidates(tmp_path, monkeypatch, mode):
    store = Store(tmp_path, Limits(reserve_bytes=0))
    refresh_catalog(store, client=SportsSource())
    monkeypatch.setattr(recorder, "validate_outbound_wss_url", lambda url: url)

    class Connection:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def send(self, value):
            pass

        async def recv(self, decode=None):
            if mode == "internal":
                raise AttributeError("synthetic internal receive failure")
            if mode == "tls_resume":
                raise AttributeError(
                    "'NoneType' object has no attribute 'resume_reading'"
                )
            if mode == "parse":
                return '{"event_type":"unrecognized_source_message"}'
            if mode == "inactivity":
                await asyncio.sleep(0.2)
                return "PONG"
            if mode == "overflow":
                return json.dumps(
                    {
                        "event_type": "last_trade_price",
                        "padding": "x" * (128 * 1024),
                    }
                )
            return "PONG"

    result = asyncio.run(
        recorder.record(
            store,
            1,
            connector=Connection,
            rotation_seconds=0.1,
            heartbeat_seconds=0.01,
            heartbeat_timeout=0.05,
            queue_bytes=64 * 1024 if mode == "overflow" else 32 * 1024**2,
        )
    )
    if mode == "overflow":
        assert result["metrics"]["overflow_dropped_messages"] > 0
        assert result["metrics"]["peak_queue_bytes"] <= 64 * 1024
    elif mode == "inactivity":
        assert result["metrics"]["TimeoutError"] > 0
    elif mode == "parse":
        assert result["metrics"]["ValueError"] > 0
        for _, manifest in store.manifests("record"):
            raw = [
                row
                for item in manifest["raw"]
                for row in pq.read_table(store.path(item["path"])).to_pylist()
                if row["kind"] == "message"
            ]
            updates = [
                row
                for item in manifest["updates"]
                for row in pq.read_table(store.path(item["path"])).to_pylist()
                if row["reason"] == "message_parsing_uncertainty"
            ]
            assert {row["received_at"] for row in updates} == {
                row["received_at"] for row in raw
            }
    elif mode == "internal":
        assert result["metrics"]["AttributeError"] > 0
        assert result["metrics"]["internal_exception_events"] > 0
    else:
        assert result["metrics"]["ConnectionError"] > 0
        assert result["metrics"]["transport_exception_events"] > 0
        assert result["metrics"].get("internal_exception_events", 0) == 0
    assert set(result["tokens"].values()) == {"failed"}
    expected_reason = {
        "overflow": "OverflowError",
        "inactivity": "TimeoutError",
        "parse": "ValueError",
        "internal": "AttributeError",
        "tls_resume": "ConnectionError",
    }[mode]
    assert set(result["token_failure_reasons"]) == set(result["tokens"])
    assert all(
        reasons == [expected_reason]
        for reasons in result["token_failure_reasons"].values()
    )
    expected_category = {
        "overflow": "overflow",
        "inactivity": "transport",
        "parse": "source_data_uncertainty",
        "internal": "internal",
        "tls_resume": "transport",
    }[mode]
    assert {event["category"] for event in result["exception_events"]} == {
        expected_category
    }
    assert result["status"] == (
        "completed_with_internal_errors" if mode == "internal" else "duration_complete"
    )
    states = build(store, now() - timedelta(minutes=1), now() + timedelta(seconds=1))
    assert "valid" not in states["counts"]


def test_catalog_readmission_resubscribes_without_retired_tokens(tmp_path, monkeypatch):
    store = Store(tmp_path, Limits(reserve_bytes=0))
    catalog = refresh_catalog(store, client=SportsSource())
    original = catalog_targets(store)
    stage = [0]
    subscribed = []
    monkeypatch.setattr(recorder, "validate_outbound_wss_url", lambda url: url)

    def refresh(*args, **kwargs):
        stage[0] += 1
        return catalog | {"counts": {"admitted_tokens": 6 if stage[0] == 1 else 8}}

    monkeypatch.setattr(recorder, "refresh_catalog", refresh)
    monkeypatch.setattr(
        recorder,
        "catalog_targets",
        lambda store: {
            token: row
            for token, row in original.items()
            if stage[0] != 1 or token not in {"2", "3"}
        },
    )

    class Connection:
        def __init__(self, *args, **kwargs):
            self.tokens = []
            self.initial = True

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def send(self, payload):
            if payload != "PING":
                item = json.loads(payload)
                if item.get("type") == "market":
                    self.tokens = item["assets_ids"]
                    subscribed.extend(self.tokens)

        async def recv(self, decode=None):
            if self.initial:
                self.initial = False
                return json_bytes(
                    [
                        {
                            "event_type": "book",
                            "asset_id": token,
                            "market": original[token]["condition_id"],
                            "timestamp": "1767225600000",
                            "bids": [{"price": ".4", "size": "1"}],
                            "asks": [{"price": ".6", "size": "1"}],
                        }
                        for token in self.tokens
                    ]
                )
            await asyncio.sleep(0.01)
            return "PONG"

    result = asyncio.run(
        recorder.record(
            store,
            2,
            connector=Connection,
            refresh_seconds=0.1,
            rotation_seconds=0.5,
        )
    )
    assert subscribed.count("2") == 2
    assert result["metrics"]["readmitted_tokens"] == 2
    assert set(result["tokens"].values()) == {"recorded"}

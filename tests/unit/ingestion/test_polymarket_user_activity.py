from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from oddsfox_pipeline.ingestion.polymarket import user_activity
from oddsfox_pipeline.ingestion.polymarket.user_activity import (
    SaturatedActivityWindowError,
    export_user_activity,
    fetch_window,
    normalize_activity,
    parse_utc,
    validate_wallet,
)

WALLET = "0x1111111111111111111111111111111111111111"
START = datetime(2025, 7, 1, tzinfo=timezone.utc)
END = START + timedelta(days=1)


class FakeClient:
    def __init__(self, pages: dict[tuple[int, int, int], list[dict[str, Any]]]):
        self.pages = pages
        self.calls: list[dict[str, Any]] = []

    def get(self, endpoint: str, params: dict[str, Any]) -> Any:
        assert endpoint == "/activity"
        self.calls.append(params)
        return self.pages.get((params["start"], params["end"], params["offset"]), [])


def _raw(timestamp: int, *, activity_type: str = "TRADE") -> dict[str, Any]:
    return {
        "proxyWallet": WALLET,
        "timestamp": timestamp,
        "conditionId": "condition-1",
        "type": activity_type,
        "size": "1.250000",
        "usdcSize": "0.500000",
        "transactionHash": "0xabc",
        "price": "0.4",
        "asset": "token-1",
        "side": "BUY",
        "outcomeIndex": 0,
        "title": "Generic market",
        "slug": "generic-market",
        "eventSlug": "generic-event",
        "outcome": "Yes",
        "isCombo": False,
    }


@pytest.mark.parametrize("wallet", ["0x123", "1" * 40, "0x" + "g" * 40])
def test_wallet_validation_rejects_invalid_syntax(wallet: str) -> None:
    with pytest.raises(ValueError):
        validate_wallet(wallet)


def test_time_validation_requires_ordered_whole_second_utc() -> None:
    assert parse_utc("2025-07-01T00:00:00Z") == START
    with pytest.raises(ValueError):
        parse_utc("2025-07-01T00:00:00")
    with pytest.raises(ValueError):
        parse_utc("2025-07-01T00:00:00.1Z")
    with pytest.raises(ValueError):
        export_user_activity(
            wallet=WALLET,
            start_utc=END,
            end_utc=START,
            output_dir=Path("unused"),
            client=FakeClient({}),
        )


def test_contract_fixture_normalizes_fixed_precision_values() -> None:
    fixture = Path("tests/fixtures/contracts/polymarket_user_activity_page.json")
    raw = json.loads(fixture.read_text())[0]
    normalized = normalize_activity(
        raw,
        wallet=WALLET,
        window_start_utc=START,
        api_offset=0,
        row_ordinal=0,
        acquired_at_utc=END,
    )
    assert normalized["size"] == Decimal("12.345678")
    assert normalized["usdc_size"] == Decimal("4.938271")
    assert normalized["price"] == Decimal("0.4")
    with pytest.raises(ValueError):
        normalize_activity(
            {**raw, "price": "NaN"},
            wallet=WALLET,
            window_start_utc=START,
            api_offset=0,
            row_ordinal=0,
            acquired_at_utc=END,
        )


def test_fetch_window_uses_ascending_pages_and_keeps_duplicate_timestamps() -> None:
    first_page = [_raw(int(START.timestamp()) + 1) for _ in range(500)]
    second_page = [_raw(int(START.timestamp()) + 1)]
    client = FakeClient(
        {
            (int(START.timestamp()), int(END.timestamp()) - 1, 0): first_page,
            (int(START.timestamp()), int(END.timestamp()) - 1, 500): second_page,
        }
    )
    rows = fetch_window(client, WALLET, START, END)
    assert len(rows) == 501
    assert [call["offset"] for call in client.calls] == [0, 500]
    assert all(call["sortDirection"] == "ASC" for call in client.calls)
    assert all(call["excludeDepositsWithdrawals"] == "false" for call in client.calls)
    assert rows[499][3] == 499
    assert rows[500][2:] == (500, 0)


def test_saturated_window_is_bisected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(user_activity, "MAX_OFFSET", 500)
    midpoint = START + timedelta(seconds=2)
    end = START + timedelta(seconds=4)

    class SaturatingClient:
        def get(self, endpoint: str, params: dict[str, Any]) -> Any:
            if (
                params["start"] == int(START.timestamp())
                and params["end"] == int(end.timestamp()) - 1
            ):
                return [_raw(params["start"])] * 500
            return [_raw(params["start"])]

    rows = fetch_window(SaturatingClient(), WALLET, START, end)
    assert len(rows) == 2
    assert {row[1] for row in rows} == {START, midpoint}


def test_one_second_saturation_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(user_activity, "MAX_OFFSET", 0)
    client = FakeClient(
        {
            (int(START.timestamp()), int(START.timestamp()), 0): [
                _raw(int(START.timestamp()))
            ]
            * 500
        }
    )
    with pytest.raises(SaturatedActivityWindowError):
        fetch_window(client, WALLET, START, START + timedelta(seconds=1))


def test_daily_export_resumes_only_after_checksum_validation(tmp_path: Path) -> None:
    acquired = END + timedelta(hours=1)
    client = FakeClient(
        {
            (
                int(START.timestamp()),
                int((START + timedelta(hours=1)).timestamp()) - 1,
                0,
            ): [_raw(int(START.timestamp()) + 1)]
        }
    )
    first = export_user_activity(
        wallet=WALLET,
        start_utc=START,
        end_utc=END,
        output_dir=tmp_path,
        client=client,
        acquired_at_utc=acquired,
    )
    assert first["rows"] == 1
    assert len(client.calls) == 24
    resumed = export_user_activity(
        wallet=WALLET,
        start_utc=START,
        end_utc=END,
        output_dir=tmp_path,
        client=FakeClient({}),
        acquired_at_utc=acquired,
    )
    assert resumed["rows"] == 1
    table = pq.read_table(tmp_path / "date=2025-07-01/activity.parquet")
    assert table.column("size")[0].as_py() == Decimal("1.250000000000000000")
    parquet_path = tmp_path / "date=2025-07-01/activity.parquet"
    parquet_path.write_bytes(parquet_path.read_bytes() + b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        export_user_activity(
            wallet=WALLET,
            start_utc=START,
            end_utc=END,
            output_dir=tmp_path,
            client=FakeClient({}),
            acquired_at_utc=acquired,
        )


def test_incompatible_output_is_refused_unless_replaced(tmp_path: Path) -> None:
    (tmp_path / "unrelated.txt").write_text("keep")
    with pytest.raises(ValueError, match="incompatible"):
        export_user_activity(
            wallet=WALLET,
            start_utc=START,
            end_utc=START + timedelta(hours=1),
            output_dir=tmp_path,
            client=FakeClient({}),
        )
    manifest = export_user_activity(
        wallet=WALLET,
        start_utc=START,
        end_utc=START + timedelta(hours=1),
        output_dir=tmp_path,
        replace=True,
        client=FakeClient({}),
        acquired_at_utc=END,
    )
    assert manifest["rows"] == 0
    assert (tmp_path / "unrelated.txt").read_text() == "keep"


def test_atomic_parquet_failure_leaves_no_partition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_write(*args: Any, **kwargs: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(user_activity.pq, "write_table", fail_write)
    with pytest.raises(OSError, match="disk full"):
        export_user_activity(
            wallet=WALLET,
            start_utc=START,
            end_utc=START + timedelta(hours=1),
            output_dir=tmp_path,
            client=FakeClient({}),
        )
    assert not (tmp_path / "date=2025-07-01/activity.parquet").exists()


def test_registered_client_owns_retry_configuration() -> None:
    client = user_activity.build_client()
    retries = client.session.get_adapter("https://").max_retries
    assert retries.total == 3
    assert retries.backoff_factor == 1.0

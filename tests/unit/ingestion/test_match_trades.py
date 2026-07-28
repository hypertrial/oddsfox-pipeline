from __future__ import annotations

from datetime import datetime, timezone

import pytest
import requests

from oddsfox_pipeline.ingestion.polymarket import match_trades as subject
from oddsfox_pipeline.ingestion.polymarket.match_order_book import (
    MatchOrderBookPaused,
    MatchOrderBookSyncError,
    load_order_book_manifest,
)


class PmxtClient:
    def __init__(self, responder):
        self.responder = responder
        self.calls = []

    def get(self, path, **kwargs):
        self.calls.append((path, kwargs))
        return self.responder(kwargs["params"], len(self.calls))


def _publish_book_run(connection, manifest, scan_id="scan"):
    now = datetime(2026, 7, 28)
    connection.execute(
        """
        INSERT INTO polymarket_wc2026_ops.match_order_book_scan_runs (
            scan_id, manifest_version, manifest_sha256, target_count,
            token_count, status, raw_published, started_at,
            last_checkpoint_at, finished_at
        )
        VALUES (?, ?, ?, ?, ?, 'published', true, ?, ?, ?)
        """,
        [
            scan_id,
            manifest.version,
            manifest.sha256,
            len(manifest.targets),
            sum(len(target.outcomes) for target in manifest.targets),
            now,
            now,
            now,
        ],
    )


def _millis(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def test_trade_sync_splits_saturated_range_deduplicates_and_publishes(duck):
    manifest = load_order_book_manifest()
    saturated_token = manifest.targets[0].outcomes[0].clob_token_id
    midpoint = (
        manifest.targets[0].window_start_ms + manifest.targets[0].window_end_ms
    ) // 2

    def respond(params, attempt):
        assert params["limit"] == 1_000
        assert params["start"].endswith("Z")
        assert params["end"].endswith("Z")
        if attempt == 1:
            return {"success": True, "data": [{}] * 1_000}
        timestamp = (
            midpoint
            if params["outcomeId"] == saturated_token
            else _millis(params["start"])
        )
        return {
            "success": True,
            "data": [
                {
                    "id": f"trade-{params['outcomeId']}",
                    "timestamp": timestamp,
                    "price": "0.6000",
                    "amount": "3.2500",
                    "outcomeId": params["outcomeId"],
                }
            ],
        }

    client = PmxtClient(respond)
    with duck.get_connection() as connection:
        _publish_book_run(connection, manifest)
        summary = subject.sync_match_trades(
            connection,
            api_key="pmxt-test",
            pmxt_client=client,
            sleep_fn=lambda _seconds: None,
        )
        rows = connection.execute(
            """
            SELECT clob_token_id, trade_id, event_sequence, price, amount
            FROM polymarket_wc2026_raw.match_trades
            ORDER BY clob_token_id, event_sequence
            """
        ).fetchall()
        split_count = connection.execute(
            """
            SELECT count(*)
            FROM polymarket_wc2026_ops.match_trade_scan_windows
            WHERE status='split'
            """
        ).fetchone()[0]

    assert summary["trade_count"] == 2
    assert summary["empty_landscape_warnings"] == []
    assert len(client.calls) == 4
    assert split_count == 1
    assert all(row[2:] == (0, "0.6", "3.25") for row in rows)


def test_total_zero_trade_coverage_blocks_publication(duck):
    manifest = load_order_book_manifest()
    client = PmxtClient(lambda _params, _attempt: {"success": True, "data": []})
    with duck.get_connection() as connection:
        _publish_book_run(connection, manifest)
        with pytest.raises(MatchOrderBookSyncError, match="PMXT trade scan failed"):
            subject.sync_match_trades(
                connection,
                api_key="pmxt-test",
                pmxt_client=client,
                sleep_fn=lambda _seconds: None,
            )
        status = connection.execute(
            """
            SELECT status
            FROM polymarket_wc2026_ops.match_trade_scan_runs
            """
        ).fetchone()[0]

    assert status == "failed"


def test_trade_normalization_rejects_out_of_window_timestamp():
    manifest = load_order_book_manifest()
    target = manifest.targets[0]
    outcome = target.outcomes[0]
    with pytest.raises(ValueError, match="outside the requested range"):
        subject._normalize(
            {
                "id": "trade",
                "timestamp": target.window_end_ms + 1,
                "price": "0.5",
                "amount": "1",
                "outcomeId": outcome.clob_token_id,
            },
            scan_id="scan",
            manifest_sha256=manifest.sha256,
            target=target,
            outcome=outcome,
            ordinal=0,
            window_start_ms=target.window_start_ms,
            window_end_ms=target.window_end_ms,
        )


@pytest.mark.parametrize(
    ("trade", "match"),
    [
        ("not-an-object", "must be an object"),
        ({"timestamp": 1, "price": "0.5", "amount": "1"}, "must not be blank"),
        (
            {"id": "trade", "timestamp": "1.5", "price": "0.5", "amount": "1"},
            "must be an integer",
        ),
        (
            {
                "id": "trade",
                "timestamp": 1,
                "price": "0.5",
                "amount": "1",
                "outcomeId": "changed",
            },
            "outcomeId changed",
        ),
    ],
)
def test_trade_normalization_rejects_malformed_provider_rows(trade, match):
    manifest = load_order_book_manifest()
    target = manifest.targets[0]
    outcome = target.outcomes[0]
    with pytest.raises(ValueError, match=match):
        subject._normalize(
            trade,
            scan_id="scan",
            manifest_sha256=manifest.sha256,
            target=target,
            outcome=outcome,
            ordinal=0,
            window_start_ms=0,
            window_end_ms=2,
        )


def test_trade_request_retries_transient_http_and_provider_errors():
    transient = requests.HTTPError()
    transient.response = requests.Response()
    transient.response.status_code = 503
    retryable = RuntimeError("temporary envelope")
    retryable.retryable = True
    responses = iter(
        [
            transient,
            retryable,
            {"success": True, "data": [{"id": "ok"}]},
        ]
    )
    sleeps = []

    def respond(_params, _attempt):
        value = next(responses)
        if isinstance(value, Exception):
            raise value
        return value

    rows = subject._request(
        PmxtClient(respond),
        api_key="key",
        token_id="token",
        start=0,
        end=1,
        retries=2,
        backoff=0,
        sleep_fn=sleeps.append,
    )

    assert rows == [{"id": "ok"}]
    assert len(sleeps) == 2


def test_trade_request_pauses_after_persistent_rate_limit():
    error = requests.HTTPError()
    error.response = requests.Response()
    error.response.status_code = 429
    client = PmxtClient(lambda _params, _attempt: (_ for _ in ()).throw(error))

    with pytest.raises(MatchOrderBookPaused, match="remained exhausted"):
        subject._request(
            client,
            api_key="key",
            token_id="token",
            start=0,
            end=1,
            retries=0,
            backoff=0,
            sleep_fn=lambda _seconds: None,
        )


@pytest.mark.parametrize(
    "error",
    [
        requests.HTTPError(response=requests.Response()),
        RuntimeError("permanent provider envelope"),
    ],
)
def test_trade_request_propagates_non_retryable_failures(error):
    if isinstance(error, requests.HTTPError):
        error.response.status_code = 400
    client = PmxtClient(lambda _params, _attempt: (_ for _ in ()).throw(error))

    with pytest.raises(type(error)):
        subject._request(
            client,
            api_key="key",
            token_id="token",
            start=0,
            end=1,
            retries=1,
            backoff=0,
            sleep_fn=lambda _seconds: None,
        )


def test_trade_sync_requires_published_book_run_and_api_key(duck, monkeypatch):
    manifest = load_order_book_manifest()
    with duck.get_connection() as connection:
        with pytest.raises(ValueError, match="order-book scan must publish"):
            subject.sync_match_trades(connection, api_key="key")
        _publish_book_run(connection, manifest)
        monkeypatch.setattr(subject, "PMXT_API_KEY", "")
        with pytest.raises(ValueError, match="PMXT_API_KEY"):
            subject.sync_match_trades(connection)


def test_trade_sync_pauses_at_shared_monthly_credit_budget(duck):
    manifest = load_order_book_manifest()
    month = datetime.now(timezone.utc).date().replace(day=1)
    with duck.get_connection() as connection:
        _publish_book_run(connection, manifest)
        connection.execute(
            """
            INSERT INTO polymarket_wc2026_ops.scrape_metadata VALUES (?, '1')
            """,
            [f"pmxt_api_attempts_{month}"],
        )
        with pytest.raises(MatchOrderBookPaused, match="monthly credit budget"):
            subject.sync_match_trades(
                connection,
                api_key="key",
                monthly_credit_budget=1,
                pmxt_client=PmxtClient(
                    lambda _params, _attempt: {"success": True, "data": []}
                ),
            )
        status = connection.execute(
            """
            SELECT status FROM polymarket_wc2026_ops.match_trade_scan_runs
            """
        ).fetchone()[0]

    assert status == "paused"


def test_trade_sync_is_noop_after_publication_and_warns_for_empty_role(duck):
    manifest = load_order_book_manifest()
    populated = manifest.targets[0].outcomes[0].clob_token_id

    def respond(params, _attempt):
        if params["outcomeId"] != populated:
            return {"success": True, "data": []}
        return {
            "success": True,
            "data": [
                {
                    "id": "trade",
                    "timestamp": _millis(params["start"]),
                    "price": "0.5",
                    "amount": "1",
                }
            ],
        }

    with duck.get_connection() as connection:
        _publish_book_run(connection, manifest)
        first = subject.sync_match_trades(
            connection,
            api_key="key",
            pmxt_client=PmxtClient(respond),
            sleep_fn=lambda _seconds: None,
        )
        second = subject.sync_match_trades(
            connection,
            api_key="key",
            pmxt_client=PmxtClient(respond),
            sleep_fn=lambda _seconds: None,
        )

    assert first["empty_landscape_warnings"] == ["away"]
    assert second == {"scan_id": "scan", "trade_count": 1, "noop": True}


def test_trade_sync_rejects_oversized_and_unsplittable_ranges(duck):
    manifest = load_order_book_manifest()
    with duck.get_connection() as connection:
        _publish_book_run(connection, manifest)
        with pytest.raises(MatchOrderBookSyncError):
            subject.sync_match_trades(
                connection,
                api_key="key",
                pmxt_client=PmxtClient(
                    lambda _params, _attempt: {
                        "success": True,
                        "data": [{}] * (subject.LIMIT + 1),
                    }
                ),
                sleep_fn=lambda _seconds: None,
            )


def test_trade_sync_rejects_contradictory_duplicate_trade_id(duck):
    manifest = load_order_book_manifest()
    target = manifest.targets[0]
    outcome = target.outcomes[0]
    now = datetime(2026, 7, 28)
    existing = subject._normalize(
        {
            "id": "duplicate",
            "timestamp": target.window_start_ms,
            "price": "0.4",
            "amount": "1",
        },
        scan_id="scan",
        manifest_sha256=manifest.sha256,
        target=target,
        outcome=outcome,
        ordinal=0,
        window_start_ms=target.window_start_ms,
        window_end_ms=target.window_end_ms,
    )
    with duck.get_connection() as connection:
        _publish_book_run(connection, manifest)
        connection.execute(
            """
            INSERT INTO polymarket_wc2026_ops.match_trade_scan_runs
            VALUES ('scan', ?, 'running', 0, NULL, ?, NULL, NULL, NULL)
            """,
            [manifest.sha256, now],
        )
        connection.execute(
            """
            INSERT INTO polymarket_wc2026_ops.match_trade_scan_windows
            VALUES ('scan', ?, ?, ?, ?, ?, ?, 0, 'pending',
                    0, 0, NULL, ?, NULL, NULL)
            """,
            [
                target.fifa_match_id,
                target.market_id,
                outcome.clob_token_id,
                outcome.role,
                target.window_start_ms,
                target.window_end_ms,
                now,
            ],
        )
        columns = ", ".join(existing)
        placeholders = ", ".join("?" for _ in existing)
        connection.execute(
            f"""
            INSERT INTO polymarket_wc2026_raw.match_trades ({columns})
            VALUES ({placeholders})
            """,
            list(existing.values()),
        )
        with pytest.raises(MatchOrderBookSyncError):
            subject.sync_match_trades(
                connection,
                api_key="key",
                pmxt_client=PmxtClient(
                    lambda _params, _attempt: {
                        "success": True,
                        "data": [
                            {
                                "id": "duplicate",
                                "timestamp": target.window_start_ms,
                                "price": "0.5",
                                "amount": "1",
                            }
                        ],
                    }
                ),
                sleep_fn=lambda _seconds: None,
            )


def test_trade_sync_rejects_unsplittable_range(duck):
    manifest = load_order_book_manifest()
    with duck.get_connection() as connection:
        _publish_book_run(connection, manifest)
        connection.execute(
            """
            INSERT INTO polymarket_wc2026_ops.match_trade_scan_runs
            VALUES ('scan', ?, 'running', 0, NULL, ?, NULL, NULL, NULL)
            """,
            [manifest.sha256, datetime(2026, 7, 28)],
        )
        target = manifest.targets[0]
        outcome = target.outcomes[0]
        now = datetime(2026, 7, 28)
        connection.execute(
            """
            INSERT INTO polymarket_wc2026_ops.match_trade_scan_windows
            VALUES ('scan', ?, ?, ?, ?, 10, 11, 0, 'pending',
                    0, 0, NULL, ?, NULL, NULL)
            """,
            [
                target.fifa_match_id,
                target.market_id,
                outcome.clob_token_id,
                outcome.role,
                now,
            ],
        )
        with pytest.raises(MatchOrderBookSyncError):
            subject.sync_match_trades(
                connection,
                api_key="key",
                pmxt_client=PmxtClient(
                    lambda _params, _attempt: {
                        "success": True,
                        "data": [{}] * subject.LIMIT,
                    }
                ),
                sleep_fn=lambda _seconds: None,
            )

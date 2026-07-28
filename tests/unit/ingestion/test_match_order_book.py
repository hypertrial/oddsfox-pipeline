from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path

import pytest
import requests
import yaml

from oddsfox_pipeline.ingestion.polymarket import match_order_book as subject
from oddsfox_pipeline.storage.duckdb.match_order_book import split_window


def _gamma_payload(manifest):
    target = manifest.targets[0]
    return {
        "id": target.market_id,
        "slug": target.market_slug,
        "sportsMarketType": target.market_type,
        "conditionId": target.condition_id,
        "outcomes": [outcome.label for outcome in target.outcomes],
        "clobTokenIds": [outcome.clob_token_id for outcome in target.outcomes],
        "closed": True,
        "acceptingOrdersTimestamp": target.accepting_orders_at.isoformat(),
        "closedTime": target.closed_at.isoformat(),
        "events": [{"id": target.event_id, "slug": target.event_slug}],
    }


class GammaClient:
    def __init__(self, payload):
        self.payload = payload
        self.paths = []

    def get(self, path):
        self.paths.append(path)
        return self.payload


class GammaMappingClient:
    def __init__(self, payloads):
        self.payloads = payloads

    def get(self, path):
        return self.payloads[path.rsplit("/", 1)[-1]]


class PmxtClient:
    def __init__(self, responder):
        self.responder = responder
        self.calls = []

    def post(self, path, **kwargs):
        self.calls.append((path, kwargs))
        raw_args = kwargs["json"]["args"]
        params = dict(raw_args[2])
        params["market_id"] = raw_args[0]
        params["outcome_id"] = params["outcome"]
        return self.responder(params, len(self.calls))


def _book(timestamp, *, bid="0.4", ask="0.6", suffix=""):
    size = f"10{suffix}" if suffix else "10"
    return {
        "timestamp": timestamp,
        "bids": [{"price": bid, "size": size, "orderCount": 2}],
        "asks": [{"price": ask, "size": "5"}],
        "isNegRisk": False,
        "lastTradePrice": "0.5",
        "sourceMetadata": {"must": "be discarded"},
    }


def _manifest_payload() -> dict:
    return yaml.safe_load(
        subject.default_order_book_targets_path().read_text(encoding="utf-8")
    )


def _group_manifest_payload() -> dict:
    source = _manifest_payload()["targets"][0]
    targets = []
    for offset, role in enumerate(("home_win", "draw", "away_win"), start=1):
        target = copy.deepcopy(source)
        target.update(
            {
                "fifa_match_id": 1,
                "stage": "group",
                "home_team": "Azure",
                "away_team": "Coral",
                "market_id": str(100 + offset),
                "market_slug": f"group-market-{offset}",
                "market_type": "moneyline",
                "condition_id": "0x" + str(offset) * 64,
                "outcomes": [
                    {
                        "label": "Yes",
                        "clob_token_id": str(1_000 + offset),
                        "role": role,
                    }
                ],
            }
        )
        targets.append(target)
    return {"version": 1, "targets": targets}


def _write_manifest(path: Path, payload: dict) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def test_manifest_pins_argentina_egypt_contract_and_hash_is_stable():
    first = subject.load_order_book_manifest()
    second = subject.load_order_book_manifest()
    target = first.targets[0]

    assert first.sha256 == second.sha256
    assert target.fifa_match_id == 95
    assert target.market_id == "2793969"
    assert target.event_id == "665733"
    assert target.market_type == "soccer_team_to_advance"
    assert [outcome.label for outcome in target.outcomes] == ["Argentina", "Egypt"]
    assert target.window_start_ms == 1_783_161_242_000
    assert target.window_end_ms == 1_783_448_324_000


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["targets"].append(
                copy.deepcopy(payload["targets"][0])
            ),
            "Duplicate clob_token_id",
        ),
        (
            lambda payload: payload["targets"][0].update(
                {"closed_at": "2026-07-04T10:34:02Z"}
            ),
            "must precede",
        ),
        (
            lambda payload: payload["targets"][0].update(
                {"market_type": "soccer_moneyline"}
            ),
            "soccer_team_to_advance",
        ),
    ],
)
def test_manifest_rejects_invalid_contracts(tmp_path, mutate, match):
    payload = _manifest_payload()
    mutate(payload)

    with pytest.raises(ValueError, match=match):
        subject.load_order_book_manifest(
            _write_manifest(tmp_path / "targets.yml", payload)
        )


def test_manifest_validates_declared_content_hash(tmp_path):
    payload = _manifest_payload()
    payload["content_sha256"] = "0" * 64

    with pytest.raises(ValueError, match="content_sha256 does not match"):
        subject.load_order_book_manifest(
            _write_manifest(tmp_path / "targets.yml", payload)
        )


def test_manifest_accepts_group_yes_books_and_infers_knockout_roles(tmp_path):
    group = subject.load_order_book_manifest(
        _write_manifest(tmp_path / "group.yml", _group_manifest_payload())
    )
    assert [
        outcome.role for target in group.targets for outcome in target.outcomes
    ] == ["home_win", "draw", "away_win"]

    knockout_payload = _manifest_payload()
    for outcome in knockout_payload["targets"][0]["outcomes"]:
        outcome.pop("role", None)
    knockout = subject.load_order_book_manifest(
        _write_manifest(tmp_path / "knockout.yml", knockout_payload)
    )
    assert [outcome.role for outcome in knockout.targets[0].outcomes] == [
        "home",
        "away",
    ]


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["targets"][0].update(
                {"market_type": "soccer_team_to_advance"}
            ),
            "group order-book targets must be moneyline",
        ),
        (
            lambda payload: payload["targets"][0]["outcomes"][0].update(
                {"role": "unknown"}
            ),
            "Invalid landscape role",
        ),
        (
            lambda payload: payload["targets"][0].update({"home_team": "Different"}),
            "exactly one match",
        ),
        (
            lambda payload: payload["targets"][0]["outcomes"][0].update(
                {"role": "draw"}
            ),
            "exactly home_win, draw, and away_win",
        ),
        (
            lambda payload: payload["targets"][0]["outcomes"][0].update(
                {"label": "No"}
            ),
            "literal Yes token",
        ),
    ],
)
def test_group_manifest_rejects_invalid_layout(tmp_path, mutate, match):
    payload = _group_manifest_payload()
    mutate(payload)
    with pytest.raises(ValueError, match=match):
        subject.load_order_book_manifest(
            _write_manifest(tmp_path / "group.yml", payload)
        )


def test_knockout_manifest_rejects_non_team_roles(tmp_path):
    payload = _manifest_payload()
    payload["targets"][0]["outcomes"][0]["role"] = "draw"

    with pytest.raises(ValueError, match="named home and away"):
        subject.load_order_book_manifest(
            _write_manifest(tmp_path / "knockout.yml", payload)
        )

    payload = _manifest_payload()
    payload["targets"][0]["outcomes"][0].pop("role", None)
    payload["targets"][0]["outcomes"][0]["label"] = "Neither team"
    with pytest.raises(ValueError, match="Invalid landscape role"):
        subject.load_order_book_manifest(
            _write_manifest(tmp_path / "knockout.yml", payload)
        )


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        (["not-a-mapping"], "manifest root"),
        ({"version": True, "targets": []}, "version"),
        ({"version": 1, "targets": {}}, "non-empty list"),
        ({"version": 1, "targets": ["not-a-mapping"]}, "must be a mapping"),
    ],
)
def test_manifest_rejects_invalid_container_shapes(tmp_path, payload, match):
    with pytest.raises(ValueError, match=match):
        subject.load_order_book_manifest(
            _write_manifest(tmp_path / "targets.yml", payload)
        )


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda target: target.update({"fifa_match_id": True}),
            "WC2026 match",
        ),
        (lambda target: target.update({"event_id": ""}), "must not be blank"),
        (lambda target: target.update({"event_id": "abc"}), "Invalid event_id"),
        (lambda target: target.update({"market_id": "0"}), "Invalid market_id"),
        (
            lambda target: target.update({"condition_id": "0x1"}),
            "Invalid condition_id",
        ),
        (
            lambda target: target.update({"event_slug": "Bad Slug"}),
            "lowercase slugs",
        ),
        (
            lambda target: target.update({"accepting_orders_at": ""}),
            "UTC timestamp",
        ),
        (
            lambda target: target.update({"accepting_orders_at": "not-a-timestamp"}),
            "ISO-8601",
        ),
        (
            lambda target: target.update(
                {"accepting_orders_at": "2026-07-04T10:34:02"}
            ),
            "include a timezone",
        ),
        (lambda target: target.update({"outcomes": []}), "exactly two outcomes"),
        (
            lambda target: target.update({"outcomes": ["Argentina", "Egypt"]}),
            "outcome must be a mapping",
        ),
        (
            lambda target: target["outcomes"][0].update({"clob_token_id": "x"}),
            "Invalid clob_token_id",
        ),
        (
            lambda target: target["outcomes"][1].update({"label": "ARGENTINA"}),
            "labels must be distinct",
        ),
    ],
)
def test_manifest_rejects_invalid_fields(tmp_path, mutate, match):
    payload = _manifest_payload()
    mutate(payload["targets"][0])

    with pytest.raises(ValueError, match=match):
        subject.load_order_book_manifest(
            _write_manifest(tmp_path / "targets.yml", payload)
        )


@pytest.mark.parametrize(
    ("duplicate_field", "match"),
    [
        ("fifa_match_id", "Duplicate fifa_match_id"),
        ("market_id", "Duplicate market_id"),
        ("condition_id", "Duplicate condition_id"),
    ],
)
def test_manifest_rejects_duplicate_target_identifiers(
    tmp_path, duplicate_field, match
):
    payload = _manifest_payload()
    first = payload["targets"][0]
    second = copy.deepcopy(first)
    second["fifa_match_id"] = 96
    second["market_id"] = "2793970"
    second["condition_id"] = "0x" + ("a" * 64)
    second["outcomes"][0]["clob_token_id"] += "1"
    second["outcomes"][1]["clob_token_id"] += "1"
    second[duplicate_field] = first[duplicate_field]
    payload["targets"].append(second)

    with pytest.raises(ValueError, match=match):
        subject.load_order_book_manifest(
            _write_manifest(tmp_path / "targets.yml", payload)
        )


def test_gamma_preflight_requires_pinned_token_order_and_boundaries():
    manifest = subject.load_order_book_manifest()
    payload = _gamma_payload(manifest)
    payload["clobTokenIds"] = list(reversed(payload["clobTokenIds"]))
    payload["closedTime"] = "2026-07-07T18:18:45Z"

    with pytest.raises(ValueError, match="clob_token_ids, closed_at"):
        subject.validate_gamma_targets(manifest, GammaClient(payload))


def test_gamma_preflight_accepts_json_lists_and_default_client(monkeypatch):
    manifest = subject.load_order_book_manifest()
    payload = _gamma_payload(manifest)
    payload["outcomes"] = '["Argentina", "Egypt"]'
    payload["clobTokenIds"] = (
        '["62322024443983575289896387710034399425619931224187000571202629586505505867789",'
        '"65153945878003754040337604701661751644439825992768932338975436339166807792069"]'
    )
    gamma = GammaClient(payload)
    monkeypatch.setattr(subject, "build_gamma_client", lambda: gamma)

    subject.validate_gamma_targets(manifest)

    assert gamma.paths == [f"/markets/slug/{manifest.targets[0].market_slug}"]


def test_gamma_preflight_accepts_short_utc_offset():
    manifest = subject.load_order_book_manifest()
    payload = _gamma_payload(manifest)
    payload["closedTime"] = "2026-07-07 18:18:44+00"

    subject.validate_gamma_targets(manifest, GammaClient(payload))


def test_group_gamma_preflight_checks_literal_yes_token(tmp_path):
    manifest = subject.load_order_book_manifest(
        _write_manifest(tmp_path / "group.yml", _group_manifest_payload())
    )
    payloads = {}
    for target in manifest.targets:
        payloads[target.market_slug] = {
            "id": target.market_id,
            "slug": target.market_slug,
            "sportsMarketType": target.market_type,
            "conditionId": target.condition_id,
            "outcomes": ["Yes", "No"],
            "clobTokenIds": [target.outcomes[0].clob_token_id, "9999"],
            "closed": True,
            "acceptingOrdersTimestamp": target.accepting_orders_at.isoformat(),
            "closedTime": target.closed_at.isoformat(),
            "events": [{"id": target.event_id, "slug": target.event_slug}],
        }
    payloads[manifest.targets[0].market_slug]["clobTokenIds"][0] = "changed"

    with pytest.raises(ValueError, match="clob_token_ids"):
        subject.validate_gamma_targets(manifest, GammaMappingClient(payloads))

    target = manifest.targets[0]
    payloads[target.market_slug]["clobTokenIds"][0] = target.outcomes[0].clob_token_id
    subject.validate_gamma_targets(manifest, GammaMappingClient(payloads))


@pytest.mark.parametrize(
    ("value", "match"),
    [
        ("not-json", "not valid JSON"),
        ({"not": "a list"}, "must be a list"),
    ],
)
def test_gamma_preflight_rejects_invalid_outcome_containers(value, match):
    manifest = subject.load_order_book_manifest()
    payload = _gamma_payload(manifest)
    payload["outcomes"] = value

    with pytest.raises(ValueError, match=match):
        subject.validate_gamma_targets(manifest, GammaClient(payload))


def test_gamma_preflight_rejects_missing_market_and_identity_fields():
    manifest = subject.load_order_book_manifest()
    with pytest.raises(ValueError, match="returned no market"):
        subject.validate_gamma_targets(manifest, GammaClient([]))

    payload = _gamma_payload(manifest)
    payload["events"] = {}
    payload["outcomes"] = ["Egypt", "Argentina"]
    payload["closed"] = False
    payload["acceptingOrdersTimestamp"] = "2026-07-04T10:34:03Z"
    with pytest.raises(
        ValueError,
        match="accepting_orders_at, closed, event_id, event_slug, outcomes",
    ):
        subject.validate_gamma_targets(manifest, GammaClient(payload))


def test_pmxt_client_uses_fixed_origin_and_explicit_rate():
    client = subject.build_pmxt_client(
        requests_per_minute=50,
        request_timeout=(2.0, 3.0),
    )

    assert client.base_url == "https://api.pmxt.dev"
    assert client.delay == pytest.approx(1.2)
    assert client.request_timeout == (2.0, 3.0)
    retry_policy = client.session.get_adapter("https://").max_retries
    assert retry_policy.total == 0
    assert not retry_policy.status_forcelist


def test_normalize_snapshot_preserves_exact_decimals_and_allowlisted_fields():
    manifest = subject.load_order_book_manifest()
    target = manifest.targets[0]
    outcome = target.outcomes[0]
    row = subject.normalize_pmxt_snapshot(
        {
            "timestamp": target.window_start_ms,
            "bids": [
                {"price": "0.5000", "size": "1.2300", "orderCount": 3},
                {"price": "0.49", "size": "2"},
                {"price": "0.000", "size": "1"},
            ],
            "asks": [{"price": "0.5100", "size": "4.000"}],
            "lastTradePrice": "0.5050",
            "isNegRisk": True,
            "sourceMetadata": {"secret": "discard"},
        },
        manifest=manifest,
        target=target,
        outcome=outcome,
        scan_id="scan",
        window_start_ms=target.window_start_ms,
        window_end_ms=target.window_end_ms,
    )

    assert row["last_trade_price"] == "0.505"
    assert row["bids_json"] == (
        '[{"order_count":3,"price":"0.5","size":"1.23"},'
        '{"order_count":null,"price":"0.49","size":"2"},'
        '{"order_count":null,"price":"0","size":"1"}]'
    )
    assert "sourceMetadata" not in str(row)
    assert len(row["snapshot_sha256"]) == 64


@pytest.mark.parametrize(
    ("book", "match"),
    [
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": [{"price": "0.2", "size": "1"}, {"price": "0.3", "size": "1"}],
                "asks": [],
            },
            "source-sorted",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": [{"price": "0.2", "size": "0"}],
                "asks": [],
            },
            "positive",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": [{"price": "NaN", "size": "1"}],
                "asks": [],
            },
            "finite",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": [
                    {"price": "0.2", "size": "1"},
                    {"price": "0.20", "size": "2"},
                ],
                "asks": [],
            },
            "duplicate prices",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": [{"price": "1.1", "size": "1"}],
                "asks": [],
            },
            "<= 1",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": [{"price": "not-a-decimal", "size": "1"}],
                "asks": [],
            },
            "must be a decimal",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": [{"price": "0.1234567890123456789", "size": "1"}],
                "asks": [],
            },
            r"DECIMAL\(38,18\)",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": [{"price": "0.2", "size": "123456789012345678901"}],
                "asks": [],
            },
            r"DECIMAL\(38,18\)",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": [{"price": "-0.1", "size": "1"}],
                "asks": [],
            },
            ">= 0",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": [{"price": "0.2", "size": "1", "orderCount": True}],
                "asks": [],
            },
            "nonnegative integer",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": [{"price": "0.2", "size": "1", "orderCount": -1}],
                "asks": [],
            },
            "nonnegative integer",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": ["not-an-object"],
                "asks": [],
            },
            "levels must be objects",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": {},
                "asks": [],
            },
            "bids must be a list",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": [],
                "asks": [
                    {"price": "0.4", "size": "1"},
                    {"price": "0.3", "size": "1"},
                ],
            },
            "source-sorted",
        ),
    ],
)
def test_normalize_snapshot_rejects_invalid_levels(book, match):
    manifest = subject.load_order_book_manifest()
    target = manifest.targets[0]

    with pytest.raises(ValueError, match=match):
        subject.normalize_pmxt_snapshot(
            book,
            manifest=manifest,
            target=target,
            outcome=target.outcomes[0],
            scan_id="scan",
            window_start_ms=target.window_start_ms,
            window_end_ms=target.window_end_ms,
        )


@pytest.mark.parametrize(
    ("book", "match"),
    [
        ("not-an-object", "must be an object"),
        (
            {"timestamp": "bad", "bids": [], "asks": []},
            "timestamp must be an integer",
        ),
        (
            {"timestamp": "1783161242000.5", "bids": [], "asks": []},
            "timestamp must be an integer",
        ),
        (
            {"timestamp": 1_783_161_241_999, "bids": [], "asks": []},
            "outside requested range",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": [],
                "asks": [],
                "isNegRisk": "false",
            },
            "isNegRisk",
        ),
        (
            {
                "timestamp": 1_783_161_242_000,
                "bids": [],
                "asks": [],
                "lastTradePrice": "2",
            },
            "<= 1",
        ),
    ],
)
def test_normalize_snapshot_rejects_invalid_snapshot_contract(book, match):
    manifest = subject.load_order_book_manifest()
    target = manifest.targets[0]
    with pytest.raises(ValueError, match=match):
        subject.normalize_pmxt_snapshot(
            book,
            manifest=manifest,
            target=target,
            outcome=target.outcomes[0],
            scan_id="scan",
            window_start_ms=target.window_start_ms,
            window_end_ms=target.window_end_ms,
        )


def test_normalize_snapshot_accepts_empty_book_and_naive_ingestion_time():
    manifest = subject.load_order_book_manifest()
    target = manifest.targets[0]
    ingested_at = datetime(2026, 7, 28, 1, 2, 3)
    row = subject.normalize_pmxt_snapshot(
        {"timestamp": target.window_end_ms, "bids": [], "asks": []},
        manifest=manifest,
        target=target,
        outcome=target.outcomes[0],
        scan_id="scan",
        window_start_ms=target.window_start_ms,
        window_end_ms=target.window_end_ms,
        ingested_at=ingested_at,
    )

    assert row["bids_json"] == "[]"
    assert row["asks_json"] == "[]"
    assert row["last_trade_price"] is None
    assert row["ingested_at"] == ingested_at


@pytest.mark.parametrize(
    ("payload", "match", "retryable"),
    [
        ([], "non-object", False),
        ({"success": False}, "request failed", False),
        (
            {"success": False, "error": {"retryable": True}},
            "request failed",
            True,
        ),
        ({"success": True, "data": {}}, "snapshot list", False),
    ],
)
def test_pmxt_envelope_rejects_invalid_contract(payload, match, retryable):
    with pytest.raises(subject._PmxtEnvelopeError, match=match) as caught:
        subject._pmxt_books(payload)

    assert caught.value.retryable is retryable


def test_window_target_must_still_match_manifest():
    manifest = subject.load_order_book_manifest()
    with pytest.raises(RuntimeError, match="no longer matches"):
        subject._window_target(
            manifest,
            {"market_id": "other", "clob_token_id": "other"},
        )


def test_sync_publishes_both_tokens_and_second_run_is_network_free(duck):
    manifest = subject.load_order_book_manifest()
    gamma = GammaClient(_gamma_payload(manifest))
    pmxt = PmxtClient(
        lambda args, _attempt: {
            "success": True,
            "data": [_book(args["since"], suffix=str(len(args["outcome_id"]) % 10))],
        }
    )
    landed = []

    def merge(rows, _conn):
        landed.extend(rows)
        subject.merge_match_order_book_snapshots(rows, _conn)

    with duck.get_connection() as conn:
        summary = subject.sync_match_order_book_history(
            conn,
            api_key="pmxt-test-key",
            gamma_client=gamma,
            pmxt_client=pmxt,
            merge_rows_fn=merge,
        )
        second = subject.sync_match_order_book_history(
            conn,
            api_key=None,
            gamma_client=GammaClient({}),
            pmxt_client=PmxtClient(lambda *_args: pytest.fail("network call")),
        )

    assert summary["status"] == "published"
    assert summary["snapshot_count"] == 2
    assert summary["token_count"] == 2
    assert second["noop"] is True
    assert len(pmxt.calls) == 2
    assert len(gamma.paths) == 1
    assert all(
        call[1]["headers"] == {"Authorization": "Bearer pmxt-test-key"}
        for call in pmxt.calls
    )
    assert [call[1]["json"]["args"][0] for call in pmxt.calls] == [
        manifest.targets[0].market_id,
        manifest.targets[0].market_id,
    ]
    assert [call[1]["json"]["args"][2]["outcome"] for call in pmxt.calls] == [
        outcome.clob_token_id for outcome in manifest.targets[0].outcomes
    ]
    assert len(landed) == 2


def test_saturated_windows_split_with_overlap_and_deduplicate(duck):
    manifest = subject.load_order_book_manifest()
    target = manifest.targets[0]
    root_bounds = (target.window_start_ms, target.window_end_ms)

    def respond(args, _attempt):
        bounds = (args["since"], args["until"])
        if bounds == root_bounds:
            return {"success": True, "data": [_book(args["since"])] * 1_000}
        timestamp = args["since"]
        return {"success": True, "data": [_book(timestamp)]}

    pmxt = PmxtClient(respond)
    progress = []
    with duck.get_connection() as conn:
        summary = subject.sync_match_order_book_history(
            conn,
            api_key="key",
            gamma_client=GammaClient(_gamma_payload(manifest)),
            pmxt_client=pmxt,
            progress_callback=lambda phase, metadata: progress.append(
                (phase, metadata)
            ),
        )
        windows = conn.execute(
            """
            SELECT clob_token_id, window_start_ms, window_end_ms, status
            FROM polymarket_wc2026_ops.match_order_book_scan_windows
            ORDER BY clob_token_id, window_start_ms, window_end_ms
            """
        ).fetchall()

    assert summary["snapshot_count"] == 4
    assert len(pmxt.calls) == 6
    assert sum(status == "split" for *_, status in windows) == 2
    assert {phase for phase, _ in progress} == {"split", "loaded"}
    for token_id in {row[0] for row in windows}:
        token_windows = [row for row in windows if row[0] == token_id]
        children = [row for row in token_windows if row[3] == "loaded"]
        assert children[0][2] == children[1][1]


def test_saturated_windows_do_not_require_progress_callback(duck):
    manifest = subject.load_order_book_manifest()
    target = manifest.targets[0]

    def respond(args, _attempt):
        if (args["since"], args["until"]) == (
            target.window_start_ms,
            target.window_end_ms,
        ):
            return {"success": True, "data": [_book(args["since"])] * 1_000}
        return {"success": True, "data": [_book(args["since"])]}

    with duck.get_connection() as conn:
        summary = subject.sync_match_order_book_history(
            conn,
            api_key="key",
            gamma_client=GammaClient(_gamma_payload(manifest)),
            pmxt_client=PmxtClient(respond),
        )

    assert summary["snapshot_count"] == 4


def test_saturated_window_is_validated_before_it_can_force_splitting(duck):
    manifest = subject.load_order_book_manifest()
    target = manifest.targets[0]

    def respond(args, _attempt):
        invalid = _book(target.window_end_ms + 1)
        return {"success": True, "data": [invalid] * 1_000}

    with duck.get_connection() as conn:
        with pytest.raises(
            subject.MatchOrderBookSyncError,
            match="PMXT order-book scan failed",
        ) as caught:
            subject.sync_match_order_book_history(
                conn,
                api_key="key",
                gamma_client=GammaClient(_gamma_payload(manifest)),
                pmxt_client=PmxtClient(respond),
            )
        window_statuses = conn.execute(
            """
            select distinct status
            from polymarket_wc2026_ops.match_order_book_scan_windows
            """
        ).fetchall()

    assert caught.value.summary["error_type"] == "ValueError"
    assert window_statuses == [("pending",)]


def test_credit_pause_checkpoints_without_publication(duck):
    manifest = subject.load_order_book_manifest()
    pmxt = PmxtClient(lambda *_args: pytest.fail("budget must block request"))

    with duck.get_connection() as conn:
        with pytest.raises(subject.MatchOrderBookPaused) as caught:
            subject.sync_match_order_book_history(
                conn,
                api_key="key",
                monthly_credit_budget=0,
                gamma_client=GammaClient(_gamma_payload(manifest)),
                pmxt_client=pmxt,
            )
        run = conn.execute(
            """
            SELECT status, raw_published
            FROM polymarket_wc2026_ops.match_order_book_scan_runs
            """
        ).fetchone()

    assert caught.value.summary["reason"] == "credit_budget"
    assert run == ("paused", False)


def test_missing_api_key_fails_without_network_or_secret_persistence(duck):
    with duck.get_connection() as conn:
        with pytest.raises(subject.MatchOrderBookSyncError) as caught:
            subject.sync_match_order_book_history(
                conn,
                api_key="",
                gamma_client=GammaClient({}),
                pmxt_client=PmxtClient(lambda *_args: pytest.fail("network call")),
            )
        persisted = conn.execute(
            """
            select status, error_type, error_message
            from polymarket_wc2026_ops.match_order_book_scan_runs
            """
        ).fetchone()

    assert caught.value.summary["remaining_window_count"] == 2
    assert persisted == (
        "failed",
        "ValueError",
        "ValueError: PMXT order-book scan failed",
    )


def test_irreducibly_saturated_one_millisecond_window_fails(duck):
    window = {
        "fifa_match_id": 95,
        "market_id": "2793969",
        "condition_id": "0x" + ("1" * 64),
        "outcome_label": "Argentina",
        "clob_token_id": "1",
        "window_start_ms": 100,
        "window_end_ms": 101,
        "depth": 20,
    }
    with duck.get_connection() as conn:
        with pytest.raises(RuntimeError, match="irreducible"):
            split_window(
                conn,
                scan_id="scan",
                lease_owner="owner",
                window=window,
            )


def test_authentication_failure_is_sanitized_and_not_retried(duck):
    manifest = subject.load_order_book_manifest()

    def unauthorized(_args, _attempt):
        response = requests.Response()
        response.status_code = 401
        error = requests.HTTPError("401 Client Error", response=response)
        raise error

    pmxt = PmxtClient(unauthorized)
    with duck.get_connection() as conn:
        with pytest.raises(subject.MatchOrderBookSyncError) as caught:
            subject.sync_match_order_book_history(
                conn,
                api_key="never-persist-this",
                gamma_client=GammaClient(_gamma_payload(manifest)),
                pmxt_client=pmxt,
                transient_retries=4,
            )
        persisted = conn.execute(
            """
            SELECT error_type, error_message
            FROM polymarket_wc2026_ops.match_order_book_scan_runs
            """
        ).fetchone()

    assert caught.value.summary["error_type"] == "HTTPError"
    assert len(pmxt.calls) == 1
    assert "never-persist-this" not in str(persisted)


def test_retry_after_is_honored_and_every_retry_is_budgeted(duck):
    manifest = subject.load_order_book_manifest()
    sleeps = []

    def rate_limited_once(args, attempt):
        if attempt == 1:
            response = requests.Response()
            response.status_code = 429
            response.headers["Retry-After"] = "7"
            raise requests.HTTPError("429 Client Error", response=response)
        return {"success": True, "data": [_book(args["since"])]}

    pmxt = PmxtClient(rate_limited_once)
    with duck.get_connection() as conn:
        summary = subject.sync_match_order_book_history(
            conn,
            api_key="key",
            gamma_client=GammaClient(_gamma_payload(manifest)),
            pmxt_client=pmxt,
            transient_retries=1,
            sleep_fn=sleeps.append,
        )
        usage = conn.execute(
            """
            select cast(value as bigint)
            from polymarket_wc2026_ops.scrape_metadata
            where key like 'pmxt_api_attempts_%'
            """
        ).fetchone()[0]

    assert summary["api_attempt_count"] == 3
    assert usage == 3
    assert sleeps == [7.0]


def test_timeout_retry_uses_bounded_backoff(duck):
    manifest = subject.load_order_book_manifest()
    sleeps = []

    def timeout_once(args, attempt):
        if attempt == 1:
            raise requests.Timeout("read timed out")
        return {"success": True, "data": [_book(args["since"])]}

    with duck.get_connection() as conn:
        summary = subject.sync_match_order_book_history(
            conn,
            api_key="key",
            gamma_client=GammaClient(_gamma_payload(manifest)),
            pmxt_client=PmxtClient(timeout_once),
            transient_retries=1,
            transient_backoff_seconds=0.1,
            sleep_fn=sleeps.append,
        )

    assert summary["status"] == "published"
    assert sleeps == [2.0]


def test_upstream_quota_exhaustion_pauses_after_bounded_retries(duck):
    manifest = subject.load_order_book_manifest()

    def exhausted(_args, _attempt):
        response = requests.Response()
        response.status_code = 429
        raise requests.HTTPError("429 Client Error", response=response)

    pmxt = PmxtClient(exhausted)
    with duck.get_connection() as conn:
        with pytest.raises(subject.MatchOrderBookPaused) as caught:
            subject.sync_match_order_book_history(
                conn,
                api_key="key",
                gamma_client=GammaClient(_gamma_payload(manifest)),
                pmxt_client=pmxt,
                transient_retries=1,
                sleep_fn=lambda _seconds: None,
            )

    assert caught.value.summary["reason"] == "upstream_429"
    assert len(pmxt.calls) == 2


def test_retryable_pmxt_envelope_discards_upstream_message(duck):
    manifest = subject.load_order_book_manifest()
    responses = iter(
        [
            {
                "success": False,
                "error": {
                    "retryable": True,
                    "message": "never-persist-this-sensitive-value",
                },
            },
            {
                "success": True,
                "data": [_book(manifest.targets[0].window_start_ms)],
            },
            {
                "success": True,
                "data": [_book(manifest.targets[0].window_start_ms)],
            },
        ]
    )
    pmxt = PmxtClient(lambda _args, _attempt: next(responses))
    with duck.get_connection() as conn:
        summary = subject.sync_match_order_book_history(
            conn,
            api_key="key",
            gamma_client=GammaClient(_gamma_payload(manifest)),
            pmxt_client=pmxt,
            transient_retries=1,
            sleep_fn=lambda _seconds: None,
        )
        persisted = conn.execute(
            """
            select error_message
            from polymarket_wc2026_ops.match_order_book_scan_runs
            """
        ).fetchone()[0]

    assert summary["status"] == "published"
    assert persisted is None


def test_nonretryable_pmxt_envelope_fails_immediately(duck):
    manifest = subject.load_order_book_manifest()
    pmxt = PmxtClient(
        lambda _args, _attempt: {
            "success": False,
            "error": {"retryable": False, "message": "discard-me"},
        }
    )
    with duck.get_connection() as conn:
        with pytest.raises(subject.MatchOrderBookSyncError) as caught:
            subject.sync_match_order_book_history(
                conn,
                api_key="key",
                gamma_client=GammaClient(_gamma_payload(manifest)),
                pmxt_client=pmxt,
                transient_retries=4,
            )

    assert caught.value.summary["error_type"] == "_PmxtEnvelopeError"
    assert len(pmxt.calls) == 1


def test_oversized_pmxt_page_fails_closed(duck):
    manifest = subject.load_order_book_manifest()
    with duck.get_connection() as conn:
        with pytest.raises(subject.MatchOrderBookSyncError) as caught:
            subject.sync_match_order_book_history(
                conn,
                api_key="key",
                gamma_client=GammaClient(_gamma_payload(manifest)),
                pmxt_client=PmxtClient(
                    lambda _args, _attempt: {
                        "success": True,
                        "data": [{}] * 1_001,
                    }
                ),
            )

    assert caught.value.summary["error_type"] == "ValueError"


def test_sync_builds_default_clients_when_not_injected(duck, monkeypatch):
    manifest = subject.load_order_book_manifest()
    gamma = GammaClient(_gamma_payload(manifest))
    pmxt = PmxtClient(
        lambda args, _attempt: {
            "success": True,
            "data": [_book(args["since"])],
        }
    )
    observed_rates = []
    monkeypatch.setattr(subject, "build_gamma_client", lambda: gamma)
    monkeypatch.setattr(
        subject,
        "build_pmxt_client",
        lambda *, requests_per_minute: (
            observed_rates.append(requests_per_minute) or pmxt
        ),
    )

    with duck.get_connection() as conn:
        summary = subject.sync_match_order_book_history(
            conn,
            api_key="key",
            requests_per_minute=37,
        )

    assert summary["status"] == "published"
    assert observed_rates == [37]


def test_same_millisecond_distinct_books_are_retained(duck):
    manifest = subject.load_order_book_manifest()

    def respond(args, _attempt):
        timestamp = args["since"]
        if args["outcome_id"] == manifest.targets[0].outcomes[0].clob_token_id:
            return {
                "success": True,
                "data": [
                    _book(timestamp, bid="0.4"),
                    _book(timestamp, bid="0.41"),
                    _book(timestamp, bid="0.4"),
                ],
            }
        return {"success": True, "data": [_book(timestamp)]}

    with duck.get_connection() as conn:
        summary = subject.sync_match_order_book_history(
            conn,
            api_key="key",
            gamma_client=GammaClient(_gamma_payload(manifest)),
            pmxt_client=PmxtClient(respond),
        )
        same_millisecond = conn.execute(
            """
            select count(*), count(distinct snapshot_sha256),
                   list(provider_sequence order by provider_sequence)
            from polymarket_wc2026_raw.match_order_book_snapshots
            where clob_token_id = ?
            """,
            [manifest.targets[0].outcomes[0].clob_token_id],
        ).fetchone()

    assert summary["snapshot_count"] == 3
    assert same_millisecond == (2, 2, [0, 1])


def test_crash_after_dlt_load_refetches_and_merges_without_duplicates(duck):
    manifest = subject.load_order_book_manifest()
    first_pmxt = PmxtClient(
        lambda args, _attempt: {
            "success": True,
            "data": [_book(args["since"])],
        }
    )
    crashed = False

    def merge_then_crash(rows, conn):
        nonlocal crashed
        subject.merge_match_order_book_snapshots(rows, conn)
        if not crashed:
            crashed = True
            raise RuntimeError("crash after dlt load")

    with duck.get_connection() as conn:
        with pytest.raises(subject.MatchOrderBookSyncError):
            subject.sync_match_order_book_history(
                conn,
                api_key="key",
                gamma_client=GammaClient(_gamma_payload(manifest)),
                pmxt_client=first_pmxt,
                merge_rows_fn=merge_then_crash,
            )
        summary = subject.sync_match_order_book_history(
            conn,
            api_key="key",
            gamma_client=GammaClient(_gamma_payload(manifest)),
            pmxt_client=PmxtClient(
                lambda args, _attempt: {
                    "success": True,
                    "data": [_book(args["since"])],
                }
            ),
        )
        raw_count = conn.execute(
            """
            select count(*)
            from polymarket_wc2026_raw.match_order_book_snapshots
            """
        ).fetchone()[0]

    assert summary["resumed"] is True
    assert summary["snapshot_count"] == 2
    assert raw_count == 2


def test_publication_rejects_raw_inventory_hash_mismatch(duck):
    manifest = subject.load_order_book_manifest()

    def respond(args, _attempt):
        return {
            "success": True,
            "data": [
                _book(args["since"], bid="0.4"),
                _book(args["since"] + 1, bid="0.41"),
            ],
        }

    def drop_one_snapshot(rows, conn):
        subject.merge_match_order_book_snapshots(rows[:1], conn)

    with duck.get_connection() as conn:
        with pytest.raises(subject.MatchOrderBookSyncError) as caught:
            subject.sync_match_order_book_history(
                conn,
                api_key="key",
                gamma_client=GammaClient(_gamma_payload(manifest)),
                pmxt_client=PmxtClient(respond),
                merge_rows_fn=drop_one_snapshot,
            )
        persisted = conn.execute(
            """
            select status, raw_published, snapshot_count
            from polymarket_wc2026_ops.match_order_book_scan_runs
            """
        ).fetchone()

    assert isinstance(caught.value.__cause__, RuntimeError)
    assert "does not match completed window hashes" in str(caught.value.__cause__)
    assert persisted == ("failed", False, 0)


def test_resume_skips_already_checkpointed_token_window(duck):
    manifest = subject.load_order_book_manifest()

    def fail_second(args, attempt):
        if attempt == 2:
            response = requests.Response()
            response.status_code = 500
            raise requests.HTTPError("500 Server Error", response=response)
        return {"success": True, "data": [_book(args["since"])]}

    first = PmxtClient(fail_second)
    second = PmxtClient(
        lambda args, _attempt: {
            "success": True,
            "data": [_book(args["since"])],
        }
    )
    with duck.get_connection() as conn:
        with pytest.raises(subject.MatchOrderBookSyncError):
            subject.sync_match_order_book_history(
                conn,
                api_key="key",
                gamma_client=GammaClient(_gamma_payload(manifest)),
                pmxt_client=first,
                transient_retries=0,
            )
        summary = subject.sync_match_order_book_history(
            conn,
            api_key="key",
            gamma_client=GammaClient(_gamma_payload(manifest)),
            pmxt_client=second,
        )

    assert summary["resumed"] is True
    assert len(first.calls) == 2
    assert len(second.calls) == 1


def test_empty_histories_never_publish(duck):
    manifest = subject.load_order_book_manifest()
    with duck.get_connection() as conn:
        with pytest.raises(subject.MatchOrderBookSyncError) as caught:
            subject.sync_match_order_book_history(
                conn,
                api_key="key",
                gamma_client=GammaClient(_gamma_payload(manifest)),
                pmxt_client=PmxtClient(
                    lambda _args, _attempt: {"success": True, "data": []}
                ),
            )

    assert caught.value.summary["error_type"] == "RuntimeError"

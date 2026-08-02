from __future__ import annotations

import json

import duckdb
import pytest

from oddsfox_pipeline.ingestion.polymarket.match_order_book import (
    load_order_book_manifest,
)
from oddsfox_pipeline.publishing import market_portrait_target as subject
from oddsfox_pipeline.publishing.market_portrait_target import (
    generate_target_manifest,
    write_target_manifest,
)


class GammaClient:
    def __init__(self, markets):
        self.markets = markets

    def get(self, path):
        return self.markets[path.rsplit("/", 1)[-1]]


def test_json_list_accepts_encoded_lists_and_rejects_other_values():
    assert subject._json_list(json.dumps(["Yes", 2])) == ["Yes", "2"]
    with pytest.raises(ValueError, match="must be lists"):
        subject._json_list({"Yes": "token"})


def _connection():
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE SCHEMA polymarket_wc2026_intermediate")
    connection.execute(
        """
        CREATE TABLE polymarket_wc2026_intermediate.int_polymarket_wc2026_match_working_set (
            fifa_match_id BIGINT, stage VARCHAR, home_team VARCHAR,
            away_team VARCHAR, market_id VARCHAR, proposition_type VARCHAR,
            yes_clob_token_id VARCHAR, no_clob_token_id VARCHAR,
            fixture_mapping_count BIGINT, primary_mapping_count BIGINT,
            selected_market_event_id VARCHAR, selected_market_event_slug VARCHAR
        )
        """
    )
    return connection


def _market(
    market_id,
    *,
    market_type,
    outcomes,
    tokens,
    event_id="100",
):
    return {
        "id": market_id,
        "slug": f"market-{market_id}",
        "sportsMarketType": market_type,
        "conditionId": "0x" + market_id.zfill(64),
        "outcomes": outcomes,
        "clobTokenIds": tokens,
        "closed": True,
        "acceptingOrdersTimestamp": "2026-06-01T00:00:00Z",
        "closedTime": "2026-06-02T00:00:00Z",
        "events": [{"id": event_id, "slug": f"event-{event_id}"}],
    }


def test_group_target_selects_only_three_literal_yes_books(tmp_path):
    connection = _connection()
    roles = ("home_win", "draw", "away_win")
    markets = {}
    for offset, role in enumerate(roles, start=1):
        market_id = str(100 + offset)
        yes_token = str(1_000 + offset)
        no_token = str(2_000 + offset)
        connection.execute(
            """
            INSERT INTO polymarket_wc2026_intermediate.int_polymarket_wc2026_match_working_set
            VALUES (1, 'group', 'Azure', 'Coral', ?, ?, ?, ?, 1, 1,
                    '100', 'event-100')
            """,
            [market_id, role, yes_token, no_token],
        )
        markets[market_id] = _market(
            market_id,
            market_type="moneyline",
            outcomes=["Yes", "No"],
            tokens=[yes_token, no_token],
        )

    payload = generate_target_manifest(
        connection, fifa_match_id=1, gamma_client=GammaClient(markets)
    )
    path = write_target_manifest(payload, tmp_path / "target.yml")
    loaded = load_order_book_manifest(path)

    assert [
        outcome.role for target in loaded.targets for outcome in target.outcomes
    ] == [
        "home_win",
        "draw",
        "away_win",
    ]
    assert all(
        outcome.label == "Yes"
        for target in loaded.targets
        for outcome in target.outcomes
    )


def test_knockout_target_maps_named_team_tokens():
    connection = _connection()
    connection.execute(
        """
        INSERT INTO polymarket_wc2026_intermediate.int_polymarket_wc2026_match_working_set
        VALUES (95, 'round_of_16', 'Azure', 'Coral', '200', 'home_advances',
                '3001', '3002', 1, 1, '100', 'event-100')
        """
    )
    payload = generate_target_manifest(
        connection,
        fifa_match_id=95,
        gamma_client=GammaClient(
            {
                "200": _market(
                    "200",
                    market_type="soccer_team_to_advance",
                    outcomes=["Azure", "Coral"],
                    tokens=["3001", "3002"],
                )
            }
        ),
    )

    assert [
        (outcome["label"], outcome["role"])
        for outcome in payload["targets"][0]["outcomes"]
    ] == [("Azure", "home"), ("Coral", "away")]


def test_target_generation_fails_before_gamma_when_working_set_is_ambiguous():
    connection = _connection()
    for market_id in ("200", "201"):
        connection.execute(
            """
            INSERT INTO polymarket_wc2026_intermediate.int_polymarket_wc2026_match_working_set
            VALUES (95, 'round_of_16', 'Azure', 'Coral', ?, 'home_advances',
                    '3001', '3002', 1, 1, '100', 'event-100')
            """,
            [market_id],
        )

    with pytest.raises(ValueError, match="exactly one"):
        generate_target_manifest(
            connection, fifa_match_id=95, gamma_client=GammaClient({})
        )


def _knockout_case():
    connection = _connection()
    connection.execute(
        """
        INSERT INTO polymarket_wc2026_intermediate.int_polymarket_wc2026_match_working_set
        VALUES (95, 'round_of_16', 'Azure', 'Coral', '200', 'home_advances',
                '3001', '3002', 1, 1, '100', 'event-100')
        """
    )
    return connection, _market(
        "200",
        market_type="soccer_team_to_advance",
        outcomes=["Azure", "Coral"],
        tokens=["3001", "3002"],
    )


def test_target_generation_requires_validated_working_set_rows():
    with pytest.raises(ValueError, match="absent from the validated working set"):
        generate_target_manifest(
            _connection(), fifa_match_id=1, gamma_client=GammaClient({})
        )


def test_group_target_rejects_missing_proposition_before_gamma():
    connection = _connection()
    connection.execute(
        """
        INSERT INTO polymarket_wc2026_intermediate.int_polymarket_wc2026_match_working_set
        VALUES (1, 'group', 'Azure', 'Coral', '101', 'home_win',
                '1001', '2001', 1, 1, '100', 'event-100')
        """
    )
    with pytest.raises(ValueError, match="exactly three propositions"):
        generate_target_manifest(
            connection, fifa_match_id=1, gamma_client=GammaClient({})
        )


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (lambda market: [], "identity check failed"),
        (lambda market: {**market, "id": "changed"}, "identity check failed"),
        (lambda market: {**market, "outcomes": {}}, "must be lists"),
        (
            lambda market: {**market, "clobTokenIds": ["3001"]},
            "inventory is inconsistent",
        ),
        (
            lambda market: {**market, "events": [{}, {}]},
            "one Gamma event",
        ),
        (
            lambda market: {**market, "clobTokenIds": ["changed", "3002"]},
            "team-token mapping changed",
        ),
        (lambda market: {**market, "closed": False}, "is not closed"),
    ],
)
def test_knockout_target_rejects_stale_gamma_contract(mutate, match):
    connection, market = _knockout_case()
    with pytest.raises(ValueError, match=match):
        generate_target_manifest(
            connection,
            fifa_match_id=95,
            gamma_client=GammaClient({"200": mutate(market)}),
        )


@pytest.mark.parametrize(
    ("event", "match"),
    [
        ({"id": "100", "slug": "event-100", "markets": []}, "does not contain"),
        (
            {"id": "changed", "slug": "event-100", "markets": [{"id": "200"}]},
            "event identity changed",
        ),
    ],
)
def test_knockout_target_rejects_changed_fallback_event(event, match):
    connection, market = _knockout_case()
    market["events"] = []
    with pytest.raises(ValueError, match=match):
        generate_target_manifest(
            connection,
            fifa_match_id=95,
            gamma_client=GammaClient({"200": market, "100": event}),
        )


def test_target_generation_uses_default_gamma_client(monkeypatch):
    connection, market = _knockout_case()
    gamma = GammaClient({"200": market})
    monkeypatch.setattr(subject, "build_client", lambda: gamma)

    payload = generate_target_manifest(connection, fifa_match_id=95)

    assert payload["targets"][0]["market_id"] == "200"


def test_target_generation_fetches_event_when_market_omits_embedding():
    connection, market = _knockout_case()
    market.pop("events")
    payload = generate_target_manifest(
        connection,
        fifa_match_id=95,
        gamma_client=GammaClient(
            {
                "200": market,
                "100": {
                    "id": "100",
                    "slug": "event-100",
                    "markets": [{"id": "200"}],
                },
            }
        ),
    )

    assert payload["targets"][0]["event_id"] == "100"


def test_group_target_rejects_inconsistent_identity_and_changed_yes_token():
    connection = _connection()
    markets = {}
    for offset, role in enumerate(("home_win", "draw", "away_win"), start=1):
        market_id = str(100 + offset)
        connection.execute(
            """
            INSERT INTO polymarket_wc2026_intermediate.int_polymarket_wc2026_match_working_set
            VALUES (1, 'group', 'Azure', ?, ?, ?, ?, ?, 1, 1,
                    '100', 'event-100')
            """,
            [
                "Different" if role == "draw" else "Coral",
                market_id,
                role,
                str(1_000 + offset),
                str(2_000 + offset),
            ],
        )
        markets[market_id] = _market(
            market_id,
            market_type="moneyline",
            outcomes=["Yes", "No"],
            tokens=[str(1_000 + offset), str(2_000 + offset)],
        )
    with pytest.raises(ValueError, match="inconsistent match identity"):
        generate_target_manifest(
            connection, fifa_match_id=1, gamma_client=GammaClient(markets)
        )

    connection.execute(
        """
        UPDATE polymarket_wc2026_intermediate.int_polymarket_wc2026_match_working_set
        SET away_team='Coral'
        """
    )
    markets["101"]["clobTokenIds"][0] = "changed"
    with pytest.raises(ValueError, match="literal Yes token changed"):
        generate_target_manifest(
            connection, fifa_match_id=1, gamma_client=GammaClient(markets)
        )

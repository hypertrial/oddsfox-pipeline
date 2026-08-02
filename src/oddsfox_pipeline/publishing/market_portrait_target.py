"""Resolve one validated WC2026 match into an operator-reviewable PMXT target."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb
import yaml

from oddsfox_pipeline.ingestion.polymarket.markets.fetch import build_client

WORKING_SET = "polymarket_wc2026_intermediate.int_polymarket_wc2026_match_working_set"


def _json_list(value: Any) -> list[str]:
    parsed = json.loads(value) if isinstance(value, str) else value
    if not isinstance(parsed, list):
        raise ValueError("Gamma outcomes and token IDs must be lists")
    return [str(item) for item in parsed]


def generate_target_manifest(
    connection: duckdb.DuckDBPyConnection,
    *,
    fifa_match_id: int,
    gamma_client: Any | None = None,
) -> dict[str, Any]:
    """Resolve an unambiguous match and refresh every identity from Gamma."""
    rows = connection.execute(
        f"""
        SELECT stage, home_team, away_team, market_id, proposition_type,
               yes_clob_token_id, no_clob_token_id,
               selected_market_event_id, selected_market_event_slug
        FROM {WORKING_SET}
        WHERE fifa_match_id=?
          AND fixture_mapping_count=1
          AND primary_mapping_count=1
        ORDER BY proposition_type, market_id
        """,
        [fifa_match_id],
    ).fetchall()
    if not rows:
        raise ValueError(
            f"match {fifa_match_id} is absent from the validated working set"
        )
    if fifa_match_id <= 72:
        by_type = {str(row[4]): row for row in rows}
        if set(by_type) != {"home_win", "draw", "away_win"} or len(rows) != 3:
            raise ValueError("group match does not resolve exactly three propositions")
        selected = [by_type[role] for role in ("home_win", "draw", "away_win")]
    else:
        if len(rows) != 1 or str(rows[0][4]) not in {
            "home_advances",
            "home_win_third_place",
            "home_wins_final",
        }:
            raise ValueError(
                "knockout match does not resolve exactly one advance/win market"
            )
        selected = rows
    identities = {(str(row[0]), str(row[1]), str(row[2])) for row in selected}
    if len(identities) != 1:
        raise ValueError("validated working set returned inconsistent match identity")
    gamma = gamma_client or build_client()
    targets = []
    for row in selected:
        market = gamma.get(f"/markets/{row[3]}")
        if not isinstance(market, dict) or str(market.get("id")) != str(row[3]):
            raise ValueError(f"Gamma identity check failed for market {row[3]}")
        outcomes = _json_list(market.get("outcomes"))
        tokens = _json_list(market.get("clobTokenIds"))
        if len(outcomes) != len(tokens):
            raise ValueError("Gamma outcome/token inventory is inconsistent")
        gamma_tokens = dict(zip(outcomes, tokens, strict=True))
        events = market.get("events")
        if events in (None, []):
            event = gamma.get(f"/events/{row[7]}")
            event_markets = event.get("markets") if isinstance(event, dict) else None
            if not isinstance(event_markets, list) or str(row[3]) not in {
                str(item.get("id")) for item in event_markets if isinstance(item, dict)
            }:
                raise ValueError(f"Gamma event does not contain market {row[3]}")
        elif isinstance(events, list) and len(events) == 1:
            event = events[0]
        else:
            raise ValueError(f"market {row[3]} does not have one Gamma event")
        if (
            not isinstance(event, dict)
            or str(event.get("id")) != str(row[7])
            or str(event.get("slug")) != str(row[8])
        ):
            raise ValueError(f"Gamma event identity changed for market {row[3]}")
        if fifa_match_id <= 72:
            if gamma_tokens.get("Yes") != str(row[5]):
                raise ValueError(f"group market {row[3]} literal Yes token changed")
            target_outcomes = [
                {
                    "label": "Yes",
                    "role": str(row[4]),
                    "clob_token_id": str(row[5]),
                }
            ]
        else:
            home_team, away_team = str(row[1]), str(row[2])
            if gamma_tokens.get(home_team) != str(row[5]) or gamma_tokens.get(
                away_team
            ) != str(row[6]):
                raise ValueError("knockout team-token mapping changed in Gamma")
            target_outcomes = [
                {
                    "label": home_team,
                    "role": "home",
                    "clob_token_id": str(row[5]),
                },
                {
                    "label": away_team,
                    "role": "away",
                    "clob_token_id": str(row[6]),
                },
            ]
        if market.get("closed") is not True:
            raise ValueError(f"market {row[3]} is not closed")
        targets.append(
            {
                "fifa_match_id": fifa_match_id,
                "stage": str(row[0]),
                "home_team": str(row[1]),
                "away_team": str(row[2]),
                "event_id": str(event.get("id") or ""),
                "event_slug": str(event.get("slug") or ""),
                "market_id": str(market["id"]),
                "market_slug": str(market.get("slug") or ""),
                "market_type": str(market.get("sportsMarketType") or ""),
                "condition_id": str(market.get("conditionId") or ""),
                "accepting_orders_at": str(
                    market.get("acceptingOrdersTimestamp") or ""
                ),
                "closed_at": str(market.get("closedTime") or ""),
                "outcomes": target_outcomes,
            }
        )
    payload: dict[str, Any] = {"version": 1, "targets": targets}
    payload["content_sha256"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return payload


def write_target_manifest(payload: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    candidate = output_path.with_suffix(output_path.suffix + ".tmp")
    candidate.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    candidate.replace(output_path)
    return output_path


__all__ = ["generate_target_manifest", "write_target_manifest"]

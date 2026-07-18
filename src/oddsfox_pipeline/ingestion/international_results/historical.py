"""Fetch deterministic 2006+ match, shootout, and goalscorer snapshots."""

from __future__ import annotations

import csv
import hashlib
from collections.abc import Callable, Mapping
from datetime import date, datetime, timezone
from io import StringIO

import requests

from oddsfox_pipeline.config.settings import HTTP_REQUEST_TIMEOUT
from oddsfox_pipeline.resources.outbound_url import validate_outbound_https_url
from oddsfox_pipeline.storage.duckdb.international_results import (
    replace_historical_international_results,
)

INTERNATIONAL_RESULTS_BASE_URL = (
    "https://raw.githubusercontent.com/martj42/international_results/refs/heads/master"
)
RESULTS_URL = f"{INTERNATIONAL_RESULTS_BASE_URL}/results.csv"
SHOOTOUTS_URL = f"{INTERNATIONAL_RESULTS_BASE_URL}/shootouts.csv"
GOALSCORERS_URL = f"{INTERNATIONAL_RESULTS_BASE_URL}/goalscorers.csv"
HISTORY_START_DATE = date(2006, 1, 1)

_RESULT_COLUMNS = (
    "date",
    "home_team",
    "away_team",
    "home_score",
    "away_score",
    "tournament",
    "city",
    "country",
    "neutral",
)
_SHOOTOUT_COLUMNS = (
    "date",
    "home_team",
    "away_team",
    "winner",
    "first_shooter",
)
_GOALSCORER_COLUMNS = (
    "date",
    "home_team",
    "away_team",
    "team",
    "scorer",
    "minute",
    "own_goal",
    "penalty",
)


class HistoricalResultsError(ValueError):
    """Raised when an upstream public CSV violates its versioned shape."""


def _hash_parts(*parts: object) -> str:
    return hashlib.sha256(
        "|".join("" if part is None else str(part) for part in parts).encode()
    ).hexdigest()


def _clean(value: object) -> str:
    return str(value or "").strip()


def _optional_text(value: object) -> str | None:
    text = _clean(value)
    return text or None


def _optional_int(value: object) -> int | None:
    text = _clean(value)
    if not text or text.upper() in {"NA", "N/A"}:
        return None
    return int(text)


def _bool(value: object) -> bool:
    return _clean(value).lower() in {"true", "t", "1", "yes"}


def _reader(text: str, expected: tuple[str, ...], source: str) -> csv.DictReader:
    reader = csv.DictReader(StringIO(text))
    actual = tuple(reader.fieldnames or ())
    if actual[: len(expected)] != expected:
        raise HistoricalResultsError(
            f"{source} schema changed: expected prefix {expected!r}, got {actual!r}"
        )
    return reader


def parse_historical_csvs(
    *,
    results_csv: str,
    shootouts_csv: str,
    goalscorers_csv: str,
    loaded_at: datetime | None = None,
    start_date: date = HISTORY_START_DATE,
) -> dict[str, object]:
    """Parse the three public files into referentially valid deterministic rows."""
    observed_at = loaded_at or datetime.now(timezone.utc)
    matches: list[dict[str, object]] = []
    match_ids: set[str] = set()
    match_lookup: dict[tuple[date, str, str], list[str]] = {}
    for source_row_number, raw in enumerate(
        _reader(results_csv, _RESULT_COLUMNS, "results.csv"),
        start=2,
    ):
        match_date = date.fromisoformat(_clean(raw["date"]))
        if match_date < start_date:
            continue
        home_team = _clean(raw["home_team"])
        away_team = _clean(raw["away_team"])
        tournament = _clean(raw["tournament"])
        city = _clean(raw["city"])
        country = _clean(raw["country"])
        match_id = _hash_parts(
            match_date,
            home_team,
            away_team,
            tournament,
            city,
            country,
            _bool(raw["neutral"]),
        )
        if match_id in match_ids:
            raise HistoricalResultsError(
                f"duplicate match ID at row {source_row_number}"
            )
        match_ids.add(match_id)
        matches.append(
            {
                "match_id": match_id,
                "match_date": match_date,
                "home_team": home_team,
                "away_team": away_team,
                "home_score": _optional_int(raw["home_score"]),
                "away_score": _optional_int(raw["away_score"]),
                "tournament": tournament,
                "city": city,
                "country": country,
                "is_neutral_site": _bool(raw["neutral"]),
                "source_url": RESULTS_URL,
                "source_row_number": source_row_number,
                "source_row_hash": _hash_parts(
                    *[raw.get(column) for column in _RESULT_COLUMNS]
                ),
                "source_loaded_at": observed_at,
            }
        )
        match_lookup.setdefault((match_date, home_team, away_team), []).append(match_id)

    shootouts: list[dict[str, object]] = []
    dropped_shootouts = 0
    for source_row_number, raw in enumerate(
        _reader(shootouts_csv, _SHOOTOUT_COLUMNS, "shootouts.csv"),
        start=2,
    ):
        match_date = date.fromisoformat(_clean(raw["date"]))
        if match_date < start_date:
            continue
        candidates = match_lookup.get(
            (match_date, _clean(raw["home_team"]), _clean(raw["away_team"])),
            [],
        )
        if not candidates:
            dropped_shootouts += 1
            continue
        shootouts.append(
            {
                "match_id": min(candidates),
                "shootout_winner": _optional_text(raw["winner"]),
                "shootout_first_shooter": _optional_text(raw["first_shooter"]),
                "source_url": SHOOTOUTS_URL,
                "source_row_number": source_row_number,
                "source_loaded_at": observed_at,
            }
        )

    goalscorers: list[dict[str, object]] = []
    dropped_goalscorers = 0
    for source_row_number, raw in enumerate(
        _reader(goalscorers_csv, _GOALSCORER_COLUMNS, "goalscorers.csv"),
        start=2,
    ):
        match_date = date.fromisoformat(_clean(raw["date"]))
        if match_date < start_date:
            continue
        candidates = match_lookup.get(
            (match_date, _clean(raw["home_team"]), _clean(raw["away_team"])),
            [],
        )
        if not candidates:
            dropped_goalscorers += 1
            continue
        match_id = min(candidates)
        goalscorers.append(
            {
                "goal_event_id": _hash_parts(
                    match_id,
                    source_row_number,
                    raw.get("team"),
                    raw.get("scorer"),
                    raw.get("minute"),
                    raw.get("own_goal"),
                    raw.get("penalty"),
                ),
                "match_id": match_id,
                "scoring_team": _optional_text(raw["team"]),
                "scorer": _optional_text(raw["scorer"]),
                "goal_minute": _optional_text(raw["minute"]),
                "is_own_goal": _bool(raw["own_goal"]),
                "is_penalty_goal": _bool(raw["penalty"]),
                "source_url": GOALSCORERS_URL,
                "source_row_number": source_row_number,
                "source_loaded_at": observed_at,
            }
        )

    return {
        "matches": matches,
        "shootouts": shootouts,
        "goalscorers": goalscorers,
        "dropped_shootouts_without_match": dropped_shootouts,
        "dropped_goalscorers_without_match": dropped_goalscorers,
    }


def fetch_csv(url: str) -> str:
    response = requests.get(
        validate_outbound_https_url(url),
        timeout=HTTP_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.text


def sync_historical_international_results(
    *,
    fetch: Callable[[str], str] = fetch_csv,
    urls: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Fetch, validate, and atomically publish the full public snapshot."""
    source_urls = dict(
        urls
        or {
            "results": RESULTS_URL,
            "shootouts": SHOOTOUTS_URL,
            "goalscorers": GOALSCORERS_URL,
        }
    )
    parsed = parse_historical_csvs(
        results_csv=fetch(source_urls["results"]),
        shootouts_csv=fetch(source_urls["shootouts"]),
        goalscorers_csv=fetch(source_urls["goalscorers"]),
    )
    persisted = replace_historical_international_results(
        matches=parsed["matches"],
        shootouts=parsed["shootouts"],
        goalscorers=parsed["goalscorers"],
    )
    return {
        **persisted,
        "dropped_shootouts_without_match": parsed["dropped_shootouts_without_match"],
        "dropped_goalscorers_without_match": parsed[
            "dropped_goalscorers_without_match"
        ],
    }


__all__ = [
    "GOALSCORERS_URL",
    "HISTORY_START_DATE",
    "HistoricalResultsError",
    "RESULTS_URL",
    "SHOOTOUTS_URL",
    "parse_historical_csvs",
    "sync_historical_international_results",
]

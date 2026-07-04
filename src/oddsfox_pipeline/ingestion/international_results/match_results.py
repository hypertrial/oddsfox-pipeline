"""Fetch and normalize WC2026 rows from martj42/international_results."""

from __future__ import annotations

import csv
import hashlib
from collections.abc import Callable
from datetime import date, datetime, timezone
from io import StringIO

import requests

from oddsfox_pipeline.config.settings import HTTP_REQUEST_TIMEOUT
from oddsfox_pipeline.resources.outbound_url import validate_outbound_https_url
from oddsfox_pipeline.storage.duckdb.international_results import (
    replace_wc2026_match_results,
)

INTERNATIONAL_RESULTS_CSV_URL = (
    "https://raw.githubusercontent.com/martj42/international_results/"
    "refs/heads/master/results.csv"
)
WC2026_TOURNAMENT = "FIFA World Cup"
WC2026_START_DATE = date(2026, 6, 11)
WC2026_END_DATE = date(2026, 7, 19)
_EXPECTED_COLUMNS = (
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


def _hash_parts(*parts: object) -> str:
    return hashlib.sha256(
        "|".join("" if part is None else str(part) for part in parts).encode("utf-8")
    ).hexdigest()


def _parse_score(value: str | None) -> int | None:
    text = (value or "").strip()
    if not text or text.upper() == "NA":
        return None
    return int(text)


def _parse_bool(value: str | None) -> bool:
    return (value or "").strip().upper() == "TRUE"


def _clean(value: str | None) -> str:
    return (value or "").strip()


def parse_wc2026_match_results_csv(
    csv_text: str,
    *,
    source_url: str = INTERNATIONAL_RESULTS_CSV_URL,
    loaded_at: datetime | None = None,
) -> list[dict[str, object]]:
    loaded_at = loaded_at or datetime.now(timezone.utc)
    reader = csv.DictReader(StringIO(csv_text))
    if tuple(reader.fieldnames or ()) != _EXPECTED_COLUMNS:
        raise ValueError("international_results CSV schema changed")

    rows: list[dict[str, object]] = []
    for source_row_number, raw in enumerate(reader, start=2):
        match_date = date.fromisoformat(_clean(raw["date"]))
        tournament = _clean(raw["tournament"])
        if (
            tournament != WC2026_TOURNAMENT
            or match_date < WC2026_START_DATE
            or match_date > WC2026_END_DATE
        ):
            continue
        home_team = _clean(raw["home_team"])
        away_team = _clean(raw["away_team"])
        home_score = _parse_score(raw["home_score"])
        away_score = _parse_score(raw["away_score"])
        city = _clean(raw["city"])
        country = _clean(raw["country"])
        rows.append(
            {
                "match_id": _hash_parts(
                    match_date, home_team, away_team, tournament, city, country
                ),
                "match_date": match_date,
                "home_team": home_team,
                "away_team": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "tournament": tournament,
                "city": city,
                "country": country,
                "neutral": _parse_bool(raw["neutral"]),
                "match_status": (
                    "completed"
                    if home_score is not None and away_score is not None
                    else "scheduled"
                ),
                "source_url": source_url,
                "source_row_number": source_row_number,
                "source_row_hash": _hash_parts(
                    *[raw[column] for column in _EXPECTED_COLUMNS]
                ),
                "source_loaded_at": loaded_at,
            }
        )
    return rows


def fetch_match_results_csv(url: str = INTERNATIONAL_RESULTS_CSV_URL) -> str:
    response = requests.get(
        validate_outbound_https_url(url),
        timeout=HTTP_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.text


def sync_wc2026_match_results(
    *,
    url: str = INTERNATIONAL_RESULTS_CSV_URL,
    fetch_csv: Callable[[str], str] = fetch_match_results_csv,
) -> dict[str, object]:
    loaded_at = datetime.now(timezone.utc)
    rows = parse_wc2026_match_results_csv(
        fetch_csv(url),
        source_url=url,
        loaded_at=loaded_at,
    )
    summary = replace_wc2026_match_results(rows)
    completed = sum(1 for row in rows if row["match_status"] == "completed")
    scheduled = len(rows) - completed
    return {
        **summary,
        "source_url": url,
        "loaded_at": loaded_at.isoformat(),
        "rows": len(rows),
        "completed_rows": completed,
        "scheduled_rows": scheduled,
    }


__all__ = [
    "INTERNATIONAL_RESULTS_CSV_URL",
    "WC2026_END_DATE",
    "WC2026_START_DATE",
    "WC2026_TOURNAMENT",
    "fetch_match_results_csv",
    "parse_wc2026_match_results_csv",
    "sync_wc2026_match_results",
]

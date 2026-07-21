"""Fetch and normalize WC2026 rows from martj42/international_results."""

from __future__ import annotations

import csv
import hashlib
import re
from collections.abc import Callable
from datetime import date, datetime, timezone
from io import StringIO

import requests

from oddsfox_pipeline.config.settings import HTTP_REQUEST_TIMEOUT
from oddsfox_pipeline.resources.outbound_url import validate_outbound_https_url
from oddsfox_pipeline.storage.duckdb.international_results import (
    replace_wc2026_match_results,
)

INTERNATIONAL_RESULTS_COMMITS_URL = (
    "https://api.github.com/repos/martj42/international_results/commits"
    "?path=results.csv&sha=master&per_page=1"
)
INTERNATIONAL_RESULTS_RAW_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/martj42/international_results/"
    "{revision}/results.csv"
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
_REVISION_RE = re.compile(r"[0-9a-f]{40}")


def _payload_bytes(csv_payload: str | bytes) -> bytes:
    return (
        csv_payload if isinstance(csv_payload, bytes) else csv_payload.encode("utf-8")
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
    csv_payload: str | bytes,
    *,
    source_revision: str,
    source_url: str | None = None,
    loaded_at: datetime | None = None,
) -> list[dict[str, object]]:
    source_revision = validate_source_revision(source_revision)
    source_url = source_url or build_match_results_url(source_revision)
    if isinstance(csv_payload, bytes):
        csv_text = csv_payload.decode("utf-8-sig")
    else:
        csv_text = csv_payload
    source_payload_sha256 = hashlib.sha256(_payload_bytes(csv_payload)).hexdigest()
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
                "source_revision": source_revision,
                "source_payload_sha256": source_payload_sha256,
                "source_loaded_at": loaded_at,
            }
        )
    return rows


def validate_source_revision(value: object) -> str:
    revision = str(value or "").strip().lower()
    if not _REVISION_RE.fullmatch(revision):
        raise ValueError("international_results revision must be a 40-character SHA")
    return revision


def build_match_results_url(revision: str) -> str:
    return INTERNATIONAL_RESULTS_RAW_URL_TEMPLATE.format(
        revision=validate_source_revision(revision)
    )


def resolve_latest_results_revision(
    url: str = INTERNATIONAL_RESULTS_COMMITS_URL,
) -> str:
    response = requests.get(
        validate_outbound_https_url(url),
        timeout=HTTP_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list) or not payload:
        raise ValueError("international_results commits response was empty")
    first = payload[0]
    if not isinstance(first, dict):
        raise ValueError("international_results commits response was malformed")
    return validate_source_revision(first.get("sha"))


def fetch_match_results_csv(url: str) -> bytes:
    response = requests.get(
        validate_outbound_https_url(url),
        timeout=HTTP_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.content


def sync_wc2026_match_results(
    *,
    resolve_revision: Callable[[], str] = resolve_latest_results_revision,
    fetch_csv: Callable[[str], str | bytes] = fetch_match_results_csv,
) -> dict[str, object]:
    revision = validate_source_revision(resolve_revision())
    url = build_match_results_url(revision)
    loaded_at = datetime.now(timezone.utc)
    csv_payload = fetch_csv(url)
    rows = parse_wc2026_match_results_csv(
        csv_payload,
        source_url=url,
        source_revision=revision,
        loaded_at=loaded_at,
    )
    summary = replace_wc2026_match_results(rows)
    completed = sum(1 for row in rows if row["match_status"] == "completed")
    scheduled = len(rows) - completed
    return {
        **summary,
        "source_url": url,
        "source_revision": revision,
        "source_payload_sha256": hashlib.sha256(
            _payload_bytes(csv_payload)
        ).hexdigest(),
        "loaded_at": loaded_at.isoformat(),
        "rows": len(rows),
        "completed_rows": completed,
        "scheduled_rows": scheduled,
    }


__all__ = [
    "INTERNATIONAL_RESULTS_COMMITS_URL",
    "INTERNATIONAL_RESULTS_RAW_URL_TEMPLATE",
    "WC2026_END_DATE",
    "WC2026_START_DATE",
    "WC2026_TOURNAMENT",
    "build_match_results_url",
    "fetch_match_results_csv",
    "parse_wc2026_match_results_csv",
    "resolve_latest_results_revision",
    "sync_wc2026_match_results",
    "validate_source_revision",
]

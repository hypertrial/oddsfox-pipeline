"""Fetch the machine-readable mirror of the official WC2026 knockout schedule."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

import requests

from oddsfox_pipeline.config.settings import HTTP_REQUEST_TIMEOUT
from oddsfox_pipeline.resources.outbound_url import validate_outbound_https_url
from oddsfox_pipeline.storage.duckdb.openfootball import replace_knockout_fixtures

OPENFOOTBALL_WC2026_KNOCKOUT_FIXTURES_URL = (
    "https://raw.githubusercontent.com/openfootball/worldcup/"
    "master/2026--usa/cup_finals.txt"
)

_STAGE_HEADERS = {
    "round of 32": ("round_of_32", 1),
    "round of 16": ("round_of_16", 2),
    "quarter-final": ("quarterfinal", 3),
    "semi-final": ("semifinal", 4),
    "match for third place": ("third_place", 0),
    "final": ("final", 5),
}
_EXPECTED_STAGE_BY_ID = {
    **{match_id: "round_of_32" for match_id in range(73, 89)},
    **{match_id: "round_of_16" for match_id in range(89, 97)},
    **{match_id: "quarterfinal" for match_id in range(97, 101)},
    **{match_id: "semifinal" for match_id in range(101, 103)},
    103: "third_place",
    104: "final",
}
_DATE_RE = re.compile(r"^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+([A-Z][a-z]{2})\s+(\d{1,2})")
_FIXTURE_RE = re.compile(
    r"^\s*\((\d{2,3})\)\s+(\d{1,2}):(\d{2})\s+UTC([+-]\d{1,2})\s+(.+)$"
)
_COMPLETED_MATCHUP_RE = re.compile(
    r"^(.*?)\s+(\d+)-(\d+)(?:\s+a\.e\.t\.)?\s+\([^)]*\)"
    r"(?:,\s+\d+-\d+\s+pen\.)?\s+(.+)$"
)


def _stage_header(line: str) -> tuple[str, int] | None:
    normalized = line.strip().lstrip("▪").strip().lower()
    return _STAGE_HEADERS.get(normalized)


def _parse_matchup(value: str) -> tuple[str, str, str]:
    scheduled = re.fullmatch(r"(.+?)\s+v\s+(.+)", value.strip())
    if scheduled:
        return scheduled.group(1).strip(), scheduled.group(2).strip(), "scheduled"
    completed = _COMPLETED_MATCHUP_RE.fullmatch(value.strip())
    if completed:
        return completed.group(1).strip(), completed.group(4).strip(), "completed"
    raise ValueError(f"Unsupported OpenFootball matchup: {value!r}")


def parse_knockout_fixtures(
    text: str,
    *,
    source_url: str = OPENFOOTBALL_WC2026_KNOCKOUT_FIXTURES_URL,
    loaded_at: datetime | None = None,
) -> list[dict[str, object]]:
    """Parse and validate the complete official-numbered knockout fixture set."""
    loaded_at = loaded_at or datetime.now(timezone.utc)
    current_stage: tuple[str, int] | None = None
    current_month_day: tuple[str, int] | None = None
    rows: list[dict[str, object]] = []

    for source_line_number, raw_line in enumerate(text.splitlines(), start=1):
        header = _stage_header(raw_line)
        if header is not None:
            current_stage = header
            continue
        date_match = _DATE_RE.match(raw_line.strip())
        if date_match:
            current_month_day = (date_match.group(1), int(date_match.group(2)))
            continue
        fixture_match = _FIXTURE_RE.match(raw_line)
        if fixture_match is None:
            continue
        if current_stage is None or current_month_day is None:
            raise ValueError("OpenFootball fixture appeared before its stage or date")

        match_id = int(fixture_match.group(1))
        hour = int(fixture_match.group(2))
        minute = int(fixture_match.group(3))
        utc_offset = int(fixture_match.group(4))
        remainder = fixture_match.group(5)
        without_comment = remainder.split("##", 1)[0].rstrip()
        try:
            matchup, venue = without_comment.rsplit("@", 1)
        except ValueError as exc:
            raise ValueError(f"OpenFootball fixture {match_id} has no venue") from exc
        home_team, away_team, match_status = _parse_matchup(matchup.strip())
        month, day = current_month_day
        local_naive = datetime.strptime(
            f"2026 {month} {day} {hour}:{minute}", "%Y %b %d %H:%M"
        )
        kickoff_at_utc = local_naive.replace(
            tzinfo=timezone(timedelta(hours=utc_offset))
        ).astimezone(timezone.utc)
        stage_key, stage_rank = current_stage
        rows.append(
            {
                "fifa_match_id": match_id,
                "stage_key": stage_key,
                "stage_rank": stage_rank,
                "kickoff_at_utc": kickoff_at_utc.replace(tzinfo=None),
                "home_team": home_team,
                "away_team": away_team,
                "venue": venue.strip(),
                "match_status": match_status,
                "source_url": source_url,
                "source_line_number": source_line_number,
                "source_line_hash": hashlib.sha256(
                    raw_line.encode("utf-8")
                ).hexdigest(),
                "source_loaded_at": loaded_at.replace(tzinfo=None),
            }
        )

    ids = [int(row["fifa_match_id"]) for row in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("OpenFootball knockout schedule contains duplicate match IDs")
    if set(ids) != set(_EXPECTED_STAGE_BY_ID):
        missing = sorted(set(_EXPECTED_STAGE_BY_ID) - set(ids))
        unexpected = sorted(set(ids) - set(_EXPECTED_STAGE_BY_ID))
        raise ValueError(
            f"OpenFootball knockout match IDs changed; missing={missing}, "
            f"unexpected={unexpected}"
        )
    for row in rows:
        match_id = int(row["fifa_match_id"])
        expected_stage = _EXPECTED_STAGE_BY_ID[match_id]
        if row["stage_key"] != expected_stage:
            raise ValueError(
                f"FIFA match {match_id} expected {expected_stage}, got "
                f"{row['stage_key']}"
            )
        if row["home_team"] == row["away_team"]:
            raise ValueError(f"FIFA match {match_id} has identical teams")
    return sorted(rows, key=lambda row: int(row["fifa_match_id"]))


def fetch_knockout_fixtures(
    url: str = OPENFOOTBALL_WC2026_KNOCKOUT_FIXTURES_URL,
) -> str:
    response = requests.get(
        validate_outbound_https_url(url),
        timeout=HTTP_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.text


def sync_knockout_fixtures(
    *,
    url: str = OPENFOOTBALL_WC2026_KNOCKOUT_FIXTURES_URL,
    fetch_text: Callable[[str], str] = fetch_knockout_fixtures,
) -> dict[str, object]:
    loaded_at = datetime.now(timezone.utc)
    rows = parse_knockout_fixtures(
        fetch_text(url),
        source_url=url,
        loaded_at=loaded_at,
    )
    summary = replace_knockout_fixtures(rows)
    return {
        **summary,
        "source_url": url,
        "loaded_at": loaded_at.isoformat(),
        "rows": len(rows),
        "scheduled_rows": sum(row["match_status"] == "scheduled" for row in rows),
        "completed_rows": sum(row["match_status"] == "completed" for row in rows),
    }


__all__ = [
    "OPENFOOTBALL_WC2026_KNOCKOUT_FIXTURES_URL",
    "fetch_knockout_fixtures",
    "parse_knockout_fixtures",
    "sync_knockout_fixtures",
]

"""Pinned source acquisition and strict historical-result normalization."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import tarfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Final
from urllib.parse import urlsplit

import requests
import yaml

from oddsfox_pipeline.resources.outbound_url import validate_outbound_https_url

_SHA256: Final = re.compile(r"^[0-9a-f]{64}$")
_REVISION: Final = re.compile(r"^[0-9a-f]{40}$")
_SCORED_LINE: Final = re.compile(r"\b\d+\s*-\s*\d+\b")
_TXT_MATCH: Final = re.compile(
    r"^\s*(?:(\d{4}-\d{2}-\d{2})\s+)?(.+?)\s{2,}"
    r"(\d+)\s*-\s*(\d+)(?:\s*\([^)]*\))?\s{2,}(.+?)\s*$"
)
_TXT_V_MATCH: Final = re.compile(
    r"^\s*(?:\d{1,2}:\d{2}\s+)?(.+?)\s+v\s+(.+?)\s{2,}"
    r"(\d+)\s*[-:]\s*(\d+)(?:\s.*)?$"
)
_TXT_QUALIFIED_MATCH: Final = re.compile(
    r"^\s*(?:\d{1,2}:\d{2}(?:\s+UTC[+-]\d+)?\s+)?"
    r"(.+?)\s{2,}((?:\d+\s*-\s*\d+).+?)\s{2,}(.+?)"
    r"(?:\s{2,}@\s.*)?$"
)
_MONTHS: Final = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


class SourceContractError(ValueError):
    """Raised when source metadata or result bytes violate their contract."""


@dataclass(frozen=True, slots=True)
class SourceSnapshot:
    snapshot_id: str
    source: str
    url: str
    revision: str
    sha256: str
    acquired_at: str
    license: str
    parser: str
    competition: str
    scope: str
    gender: str
    filename: str
    default_year: int | None = None
    default_end_year: int | None = None
    include_regex: str | None = None
    default_neutral: bool = False

    def __post_init__(self) -> None:
        if not self.snapshot_id or not self.source or not self.filename:
            raise SourceContractError("snapshot_id, source, and filename are required")
        parsed_url = urlsplit(self.url)
        if (
            parsed_url.scheme != "https"
            or not parsed_url.hostname
            or parsed_url.username
            or parsed_url.password
            or parsed_url.fragment
        ):
            raise SourceContractError("source URL must be an uncredentialed HTTPS URL")
        if not _REVISION.fullmatch(self.revision):
            raise SourceContractError(f"invalid revision for {self.snapshot_id}")
        if self.revision not in self.url:
            raise SourceContractError(
                f"snapshot URL must contain its pinned revision: {self.snapshot_id}"
            )
        if not _SHA256.fullmatch(self.sha256):
            raise SourceContractError(f"invalid SHA-256 for {self.snapshot_id}")
        try:
            acquired = (
                self.acquired_at
                if isinstance(self.acquired_at, datetime)
                else datetime.fromisoformat(self.acquired_at.replace("Z", "+00:00"))
            )
        except (AttributeError, ValueError) as exc:
            raise SourceContractError(
                f"invalid acquisition time for {self.snapshot_id}"
            ) from exc
        if acquired.tzinfo is None:
            raise SourceContractError("acquired_at must include a UTC offset")
        object.__setattr__(
            self,
            "acquired_at",
            acquired.isoformat().replace("+00:00", "Z"),
        )
        if self.license != "CC0-1.0":
            raise SourceContractError("only CC0-1.0 result sources are accepted")
        if self.parser not in {
            "openfootball_archive",
            "openfootball_json",
            "football_txt",
            "international_csv",
        }:
            raise SourceContractError(f"unsupported parser: {self.parser}")
        if self.scope not in {"club", "national"} or self.gender not in {
            "men",
            "women",
        }:
            raise SourceContractError("scope and gender must identify one rating pool")

    @property
    def rating_pool(self) -> str:
        return (
            f"{self.scope}_{self.gender}"
            if self.scope == "club"
            else f"national_{self.gender}"
        )


@dataclass(frozen=True, slots=True)
class RawResult:
    source_match_id: str
    match_date: date
    home_name: str
    away_name: str
    home_score: int
    away_score: int
    competition: str
    rating_pool: str
    neutral: bool
    friendly: bool
    source: str
    snapshot_id: str
    source_locator: str


@dataclass(frozen=True, slots=True)
class ParseIssue:
    source_locator: str
    reason: str
    text: str


@dataclass(frozen=True, slots=True)
class ParseResult:
    rows: tuple[RawResult, ...]
    issues: tuple[ParseIssue, ...]


def load_source_catalog(path: Path) -> tuple[SourceSnapshot, ...]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise SourceContractError(f"invalid source catalog: {path}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("snapshots"), list):
        raise SourceContractError("source catalog must contain a snapshots list")
    snapshots = tuple(SourceSnapshot(**row) for row in payload["snapshots"])
    ids = [row.snapshot_id for row in snapshots]
    if len(ids) != len(set(ids)):
        raise SourceContractError("snapshot_id must be unique")
    return snapshots


def acquire_snapshots(
    catalog: Sequence[SourceSnapshot],
    output_root: Path,
    *,
    fetch: Callable[[str], bytes] | None = None,
) -> tuple[Path, ...]:
    """Download exact source bytes only after revision and checksum validation."""

    def request(url: str) -> bytes:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        return response.content

    fetch_bytes = fetch or request
    paths: list[Path] = []
    output_root.mkdir(parents=True, exist_ok=True)
    for snapshot in catalog:
        validate_outbound_https_url(snapshot.url)
        payload = fetch_bytes(snapshot.url)
        digest = hashlib.sha256(payload).hexdigest()
        if digest != snapshot.sha256:
            raise SourceContractError(
                f"checksum mismatch for {snapshot.snapshot_id}: {digest}"
            )
        directory = output_root / snapshot.snapshot_id
        directory.mkdir(parents=True, exist_ok=True)
        destination = directory / snapshot.filename
        if destination.exists() and destination.read_bytes() != payload:
            raise SourceContractError(
                f"refusing to overwrite changed snapshot: {snapshot.snapshot_id}"
            )
        destination.write_bytes(payload)
        paths.append(destination)
    return tuple(paths)


def _source_match_id(
    snapshot: SourceSnapshot, locator: str, values: Iterable[object]
) -> str:
    payload = "\0".join(str(value) for value in values)
    digest = hashlib.sha256(
        f"{snapshot.snapshot_id}\0{locator}\0{payload}".encode()
    ).hexdigest()
    return f"{snapshot.source}:{digest}"


def _parse_iso_date(value: object, locator: str) -> date:
    if not isinstance(value, str):
        raise SourceContractError(f"match date is not text at {locator}")
    try:
        return date.fromisoformat(value[:10])
    except ValueError as exc:
        raise SourceContractError(
            f"invalid match date at {locator}: {value!r}"
        ) from exc


def _score_pair(value: object, locator: str) -> tuple[int, int] | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)) and len(value) == 2:
        pair = value
    elif isinstance(value, dict):
        for key in ("ft", "fullTime", "full_time", "final"):
            if key in value:
                return _score_pair(value[key], locator)
        if {"team1", "team2"} <= set(value):
            pair = (value["team1"], value["team2"])
        elif {"home", "away"} <= set(value):
            pair = (value["home"], value["away"])
        else:
            return None
    else:
        return None
    if any(
        isinstance(item, bool) or not isinstance(item, int) or item < 0 for item in pair
    ):
        raise SourceContractError(f"invalid full-time score at {locator}")
    return int(pair[0]), int(pair[1])


def _json_match_rows(
    value: object, context: str = "root"
) -> Iterable[tuple[str, Mapping[str, object], str]]:
    if isinstance(value, dict):
        competition = str(value.get("name") or value.get("competition") or context)
        matches = value.get("matches")
        if matches is not None:
            if not isinstance(matches, list):
                raise SourceContractError(f"matches must be a list at {context}")
            for index, match in enumerate(matches):
                if not isinstance(match, dict):
                    raise SourceContractError(
                        f"match must be an object at {context}.matches[{index}]"
                    )
                yield competition, match, f"{context}.matches[{index}]"
        for key, nested in value.items():
            if key != "matches" and isinstance(nested, (dict, list)):
                yield from _json_match_rows(nested, f"{context}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            yield from _json_match_rows(nested, f"{context}[{index}]")


def parse_openfootball_json(payload: bytes, snapshot: SourceSnapshot) -> ParseResult:
    try:
        value = json.loads(payload)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise SourceContractError(f"invalid JSON for {snapshot.snapshot_id}") from exc
    rows: list[RawResult] = []
    issues: list[ParseIssue] = []
    seen_match_container = False
    for competition, match, locator in _json_match_rows(value):
        seen_match_container = True
        home = match.get("team1", match.get("home"))
        away = match.get("team2", match.get("away"))
        score = _score_pair(match.get("score"), locator)
        if score is None and "score1" in match and "score2" in match:
            score = _score_pair((match["score1"], match["score2"]), locator)
        if score is None:
            continue
        if (
            not isinstance(home, str)
            or not home.strip()
            or not isinstance(away, str)
            or not away.strip()
        ):
            raise SourceContractError(f"scored match has invalid teams at {locator}")
        match_date = _parse_iso_date(match.get("date"), locator)
        native_id = match.get("id")
        source_id = (
            f"{snapshot.source}:{snapshot.snapshot_id}:{native_id}"
            if native_id is not None
            else _source_match_id(
                snapshot,
                locator,
                (match_date, home, away, *score),
            )
        )
        friendly = "friendly" in str(match.get("competition") or competition).casefold()
        rows.append(
            RawResult(
                source_match_id=source_id,
                match_date=match_date,
                home_name=home.strip(),
                away_name=away.strip(),
                home_score=score[0],
                away_score=score[1],
                competition=str(match.get("competition") or competition),
                rating_pool=snapshot.rating_pool,
                neutral=bool(match.get("neutral", snapshot.default_neutral)),
                friendly=friendly,
                source=snapshot.source,
                snapshot_id=snapshot.snapshot_id,
                source_locator=locator,
            )
        )
    if not seen_match_container:
        raise SourceContractError("OpenFootball JSON contains no matches collection")
    return ParseResult(tuple(rows), tuple(issues))


def _heading_date(
    line: str, default_year: int | None, default_end_year: int | None
) -> date | None:
    cleaned = line.strip().strip("[]")
    iso = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", cleaned)
    if iso:
        return date.fromisoformat(iso.group(1))
    numeric = re.fullmatch(r"\s*(\d{1,2})\.(\d{1,2})\.\s*", cleaned)
    if numeric and default_year:
        month = int(numeric.group(2))
        year = default_end_year if default_end_year and month <= 6 else default_year
        try:
            return date(year, month, int(numeric.group(1)))
        except ValueError:
            return None
    month_day = re.search(
        r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?\s*"
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*"
        r"[ /-](\d{1,2})(?:\s+(\d{4}))?\b",
        cleaned,
        re.IGNORECASE,
    )
    if month_day and default_year:
        month = _MONTHS[month_day.group(1)[:3].casefold()]
        inferred_year = (
            default_end_year
            if default_end_year is not None and month <= 6
            else default_year
        )
        try:
            return date(
                int(month_day.group(3)) if month_day.group(3) else inferred_year,
                month,
                int(month_day.group(2)),
            )
        except ValueError:
            return None
    return None


def parse_football_txt(payload: bytes, snapshot: SourceSnapshot) -> ParseResult:
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeError as exc:
        raise SourceContractError(f"invalid UTF-8 for {snapshot.snapshot_id}") from exc
    rows: list[RawResult] = []
    issues: list[ParseIssue] = []
    current_date: date | None = None
    in_penalty_detail = False
    for line_number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            in_penalty_detail = False
            continue
        if stripped.startswith("Penalties:"):
            in_penalty_detail = True
            continue
        if in_penalty_detail:
            continue
        v_match = _TXT_V_MATCH.match(line)
        match = _TXT_MATCH.match(line) if v_match is None else None
        qualified_match = (
            _TXT_QUALIFIED_MATCH.match(line)
            if match is None and v_match is None
            else None
        )
        heading = _heading_date(line, snapshot.default_year, snapshot.default_end_year)
        if heading and not match and not v_match and not qualified_match:
            current_date = heading
            continue
        if not match and not v_match and not qualified_match:
            if _SCORED_LINE.search(line) and not line.lstrip().startswith(("#", "//")):
                issues.append(
                    ParseIssue(
                        str(line_number), "unparsed_scored_match_line", line.rstrip()
                    )
                )
            continue
        if match:
            explicit_date, home, home_score, away_score, away = match.groups()
        elif v_match:
            home, away, home_score, away_score = v_match.groups()
            explicit_date = None
        else:
            home, score_text, away = qualified_match.groups()
            regulation = re.search(r"(\d+)\s*-\s*(\d+)\s+a\.e\.t\.", score_text)
            if regulation is None:
                scores = list(re.finditer(r"(\d+)\s*-\s*(\d+)", score_text))
                regulation = (
                    scores[1]
                    if len(scores) > 1 and "pen." in score_text[: scores[1].start()]
                    else scores[0]
                )
            home_score, away_score = regulation.groups()
            explicit_date = None
        match_date = (
            date.fromisoformat(explicit_date) if explicit_date else current_date
        )
        if match_date is None:
            issues.append(
                ParseIssue(str(line_number), "missing_match_date", line.rstrip())
            )
            continue
        locator = str(line_number)
        score = (int(home_score), int(away_score))
        rows.append(
            RawResult(
                source_match_id=_source_match_id(
                    snapshot, locator, (match_date, home, away, *score)
                ),
                match_date=match_date,
                home_name=home.strip(),
                away_name=away.strip(),
                home_score=score[0],
                away_score=score[1],
                competition=snapshot.competition,
                rating_pool=snapshot.rating_pool,
                neutral=snapshot.default_neutral,
                friendly="friendly" in snapshot.competition.casefold(),
                source=snapshot.source,
                snapshot_id=snapshot.snapshot_id,
                source_locator=locator,
            )
        )
    return ParseResult(tuple(rows), tuple(issues))


def parse_international_csv(payload: bytes, snapshot: SourceSnapshot) -> ParseResult:
    try:
        reader = csv.DictReader(io.StringIO(payload.decode("utf-8-sig")))
    except UnicodeError as exc:
        raise SourceContractError(f"invalid UTF-8 for {snapshot.snapshot_id}") from exc
    required = {
        "date",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
        "tournament",
        "neutral",
    }
    if reader.fieldnames is None or not required <= set(reader.fieldnames):
        raise SourceContractError("international-results CSV schema changed")
    rows: list[RawResult] = []
    for line_number, row in enumerate(reader, start=2):
        try:
            home_score = int(row["home_score"])
            away_score = int(row["away_score"])
            match_date = date.fromisoformat(row["date"])
        except (TypeError, ValueError) as exc:
            raise SourceContractError(
                f"invalid scored match at CSV line {line_number}"
            ) from exc
        if (
            home_score < 0
            or away_score < 0
            or not row["home_team"]
            or not row["away_team"]
        ):
            raise SourceContractError(f"invalid scored match at CSV line {line_number}")
        locator = str(line_number)
        tournament = row["tournament"]
        rows.append(
            RawResult(
                source_match_id=_source_match_id(
                    snapshot,
                    locator,
                    (
                        match_date,
                        row["home_team"],
                        row["away_team"],
                        home_score,
                        away_score,
                    ),
                ),
                match_date=match_date,
                home_name=row["home_team"].strip(),
                away_name=row["away_team"].strip(),
                home_score=home_score,
                away_score=away_score,
                competition=tournament,
                rating_pool=snapshot.rating_pool,
                neutral=str(row["neutral"]).strip().casefold() == "true",
                friendly="friendly" in tournament.casefold(),
                source=snapshot.source,
                snapshot_id=snapshot.snapshot_id,
                source_locator=locator,
            )
        )
    return ParseResult(tuple(rows), ())


def parse_openfootball_archive(payload: bytes, snapshot: SourceSnapshot) -> ParseResult:
    """Parse matching JSON and Football.TXT members without extracting an archive."""
    rows: list[RawResult] = []
    issues: list[ParseIssue] = []
    include = re.compile(snapshot.include_regex) if snapshot.include_regex else None
    try:
        archive = tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz")
    except (tarfile.TarError, OSError) as exc:
        raise SourceContractError(
            f"invalid archive for {snapshot.snapshot_id}"
        ) from exc
    with archive:
        members = sorted(
            (
                member
                for member in archive.getmembers()
                if member.isfile()
                and member.size <= 100 * 1024 * 1024
                and not member.name.rsplit("/", 1)[-1].startswith(".")
                and (member.name.endswith(".json") or member.name.endswith(".txt"))
                and (include is None or include.search(member.name))
            ),
            key=lambda member: member.name,
        )
        if not members:
            raise SourceContractError(
                f"archive include_regex matched no result files: {snapshot.snapshot_id}"
            )
        for member in members:
            handle = archive.extractfile(member)
            if handle is None:
                raise SourceContractError(
                    f"could not read archive member: {member.name}"
                )
            member_payload = handle.read()
            member_year = re.search(r"(?:^|/)(20\d{2})(?:[-_/]|$)", member.name)
            member_end_year = re.search(
                r"(?:^|/)(20\d{2})[-_](\d{2}|20\d{2})", member.name
            )
            nested = replace(
                snapshot,
                parser=(
                    "openfootball_json"
                    if member.name.endswith(".json")
                    else "football_txt"
                ),
                competition=member.name,
                default_year=(
                    int(member_year.group(1)) if member_year else snapshot.default_year
                ),
                default_end_year=(
                    (
                        int(member_end_year.group(2))
                        if len(member_end_year.group(2)) == 4
                        else int(
                            member_end_year.group(1)[:2] + member_end_year.group(2)
                        )
                    )
                    if member_end_year
                    else snapshot.default_end_year
                ),
                include_regex=None,
            )
            if member.name.endswith(".json"):
                try:
                    result = parse_openfootball_json(member_payload, nested)
                except SourceContractError as exc:
                    if (
                        "no matches collection" in str(exc)
                        and b'"score"' not in member_payload
                    ):
                        continue
                    raise
            else:
                result = parse_football_txt(member_payload, nested)
            member_hash = hashlib.sha1(member.name.encode()).hexdigest()[:12]
            rows.extend(
                replace(
                    row,
                    source_match_id=f"{row.source_match_id}:{member_hash}",
                    source_locator=f"{member.name}:{row.source_locator}",
                )
                for row in result.rows
            )
            issues.extend(
                ParseIssue(
                    f"{member.name}:{issue.source_locator}",
                    issue.reason,
                    issue.text,
                )
                for issue in result.issues
            )
    return ParseResult(tuple(rows), tuple(issues))


def parse_snapshot(payload: bytes, snapshot: SourceSnapshot) -> ParseResult:
    parser = {
        "openfootball_archive": parse_openfootball_archive,
        "openfootball_json": parse_openfootball_json,
        "football_txt": parse_football_txt,
        "international_csv": parse_international_csv,
    }[snapshot.parser]
    return parser(payload, snapshot)


def snapshot_manifest_rows(
    snapshots: Sequence[SourceSnapshot], parsed: Mapping[str, ParseResult]
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for snapshot in sorted(snapshots, key=lambda row: row.snapshot_id):
        result = parsed[snapshot.snapshot_id]
        dates = [row.match_date for row in result.rows]
        row = asdict(snapshot)
        row.update(
            {
                "row_count": len(result.rows),
                "unparsed_scored_line_count": len(result.issues),
                "minimum_match_date": min(dates).isoformat() if dates else None,
                "maximum_match_date": max(dates).isoformat() if dates else None,
            }
        )
        output.append(row)
    return output


__all__ = [
    "ParseIssue",
    "ParseResult",
    "RawResult",
    "SourceContractError",
    "SourceSnapshot",
    "acquire_snapshots",
    "load_source_catalog",
    "parse_football_txt",
    "parse_international_csv",
    "parse_openfootball_archive",
    "parse_openfootball_json",
    "parse_snapshot",
    "snapshot_manifest_rows",
]

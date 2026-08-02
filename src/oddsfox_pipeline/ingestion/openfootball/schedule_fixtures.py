"""Fetch and validate the complete, officially numbered WC2026 schedule."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import requests

from oddsfox_pipeline.config.settings import HTTP_REQUEST_TIMEOUT
from oddsfox_pipeline.resources.outbound_url import validate_outbound_https_url
from oddsfox_pipeline.storage.duckdb.openfootball import replace_schedule_fixtures

OPENFOOTBALL_REVISION = "bd46a148289f9930da66c140d4d7d2325e95d387"
OPENFOOTBALL_BASE = (
    f"https://raw.githubusercontent.com/openfootball/worldcup/{OPENFOOTBALL_REVISION}/"
)
OPENFOOTBALL_FILES = {
    "2026--usa/cup.txt": (
        "4f52c563a5d470702fedf5078fd379c8f5ddfb2192d23b6f88ce84e997c30028"
    ),
    "2026--usa/cup_finals.txt": (
        "03631f10fff8a3a9c485d866c98fb099f8d2612e97a034c64c28c7d189dd5949"
    ),
}
OPENFOOTBALL_WC2026_SCHEDULE_FIXTURES_URL = (
    OPENFOOTBALL_BASE + "2026--usa/cup_finals.txt"
)
FIFA_SCHEDULE_URL = (
    "https://digitalhub.fifa.com/asset/4b5d4417-3343-4732-9cdf-14b6662af407/"
    "FWC26-Match-Schedule_English.pdf"
)
FIFA_SCHEDULE_TITLE = "FWC26 Match Schedule_v31_16072026_EN"
FIFA_SCHEDULE_SHA256 = (
    "165fb909253b746e6173a4443bdc3e5d786530f0684af6e85c1fd21fff252811"
)

# Reviewed against the pinned FIFA schedule. OpenFootball's group-stage source
# is grouped by group, not ordered by official match number, so ordinal mapping
# is invalid. Each key is the SHA-256 of the exact date-to-fixture source slice.
_REVIEWED_GROUP_FIXTURE_HASHES_BY_MATCH_ID = (
    "59d5c1f0393f0d7a82172b059a59131e84eaf86eb4ed99fd74f36cf1276189bd",
    "ea2e2a61da99c76ab99497a7a0c0ce772cc17e0ac4871cb4b9e62ce5b33395ba",
    "5c52a03ad6142f6b90a1fe3d6677bef2ce5f90464710da074dc5db0d00082ec1",
    "34010c2f99636db613ac6b011a86308dfa55c94a4b38d44c83820ab1b363195a",
    "0bd92e8cf78e40529dd8c47bde8981c1a9b590a46e413353bf5ef7ba31030513",
    "d0256a00b98c134c4d26d632734c702268c47ab22e62e7161230e40887b9734b",
    "e1bfc951b2a12c6be2e5d056a31778a00d77dcbe391445707dd61a878748fcfb",
    "36ce2180d0ef6567ffb783e0ab7225e0cd10fb348dbfde9a232cf2c1642ffcb6",
    "501698ce918a5bf6a9590f428424317ee79068f2d8971a2b5a59ce7c4f5a0947",
    "69d1ca85d1ff5bfc0249d17b40ffe1fbe8a792ed8a9c80d1f95cc0184b430179",
    "6145d75ee6731c1d35d4748e10555921e7a8222b0df3b79991f2af2ff09d3de1",
    "d47afaa7465e12d1c57cbd51fdcf5222ad8658e9805b1475153565be480506a6",
    "b9ba008b3f4177eb1664cc76222c1a72e002cbb61a1607c0701ea454585a0966",
    "2c8bdcddd6b515c3dbf152aee6efc206f149d6871695f082cbc9704ab7a67170",
    "5d8ed746cbc922c3c70b1cc430c1cc8e70939a6a534268d74c70451ab0e41bfa",
    "38dba8b12efbd4132e0bf34d9ad5e0a84a1714f64baac90c10fda1f077f34af3",
    "509694fda93bbb51cc324803d8ccb21f4db9295d640be1f04faeae31225f37c0",
    "bfd64e62a6156c53152844615dd70d08df96e873b23afeb394d88257c1105472",
    "fdf1a473bdf3333b4d8c5118cfe23fbf09a367a7000acd875d863e93fa14bece",
    "04752c0a7c2ac188d12b220c38755666eb9f4f93c09f647c9e26177e297dc612",
    "b1a971090bb9999e41b9438a9e925f5e357fb75ce5fc1fca0ebbc4c6c94fbc3a",
    "8f3308ec8e467ad29b62c74b2f369df80a42cc3086dcdf206192320e820a157e",
    "9204bf4fe5230b0c10b0792dac0a5bc5a250b892c35f60a1e5bb3fa6ceddcd41",
    "cefd95eab2952ba94e0bf6d2f3594e5292117f2062656f8ce4e1e1ac17e793bd",
    "2c156ccd23b731a315516acda024326d57444a547b705389d2895c25959d6ad9",
    "979e9623f937af1a6fce8c283b72b8b6aaba150d5d753b9eec670e7118a5bb2d",
    "8463db70b42fbb5842186b9241afa2001a20d00e3159119cccf51720d0d7f222",
    "91e23caae024cf58ad482e5c98df00470a284f649a2c5da9bb82a911e3fd92b4",
    "f9c1df8429fe45d27e2c1153a1e9660c937775d96680ef5ff00359ff890cf573",
    "065ec8aad89f4456e7d9769cfc7a4fc3a0d06c826258ffaca7a57a717522787a",
    "a503f484355e0008de8d2ecf89b1ab67d89b615671a53193091553ca31339ad6",
    "40af69c338f909c835ef9b70f8b8b8892546639048209a9aafacf748f83aa8b3",
    "9c6207aba4fec6595561ba02a62cbec176acaabab9cca9f365a2dd1fa599a953",
    "829e6c0a96274d19d9135441bb1be661396d4c39cf15c86cf891b5bd4859041a",
    "2a441b5958e568e1f7bd50badc7d80ff4057d7650b1e74114c97d609baa7d903",
    "7d57631c11752ec3d9a34f31f3bf4998cf90c205ebc7c59f351ae0163c6f02b6",
    "9704971fc28c29c2458823a8daf4ae8a24c58f681d8d8c5ba07ab86cade00cef",
    "268ac33284343015fa6ad41aa8e959743c2e541840b62d435220d77bb599f766",
    "573230c4fff84b004ec3169f795e282f7cc5c7b8a9ccff2490736f0760626d67",
    "5c52bda6c2045674ce6f0a22790e36a7d7855d805e0b7951c680b6b01fe9fab8",
    "c6b0e5ab69302f876d86ca260e90dd5524b2fa1585244147029dccddd4f1948a",
    "772caa75b548480a41b5fc3e86db6ac39f56c1ee1dd089234d3d99a702e95f7b",
    "dd3d0acaa0efbeafb2742501696ac9f365f43a234f5b45097f214ca0ea79ffdb",
    "23798a7771c5fe2d3d28245c7c4d57d92001b89192db7f46186f4579ae728ba4",
    "fb876e3cd9e2d6a85c84267afa0cafb707bc7e3c09f74664357cedd09c2f1bd0",
    "91ff1a4f4e525f188ef555bc9b92459d7a03d67077f9ed7f0b75eae1da98ee51",
    "9464bd076b010964e67be13c4757b358543f00c073083ea0f8331e4e17edd79b",
    "6f126f48e6ad7c5b395e710d476d0717545c20538c2d7644ba66ab12e9067aa9",
    "612d93d8e27b988afd33054965d222e8f294a51f10a4e76b2c8888b1f9361400",
    "e09fbfcaa1fe30238784dc6cfb6ebc8d9cf8a258236f57e77b22f33516e52db8",
    "bba4c78f755a7a4af0e636c9d4676dca8353be57dbc0a9bca75b87bd886ac368",
    "e4253cb5406dd4f2ae50f307692b70dba88ac8abdbecdddea08b702dcc5d640e",
    "35f6c6c565d90bfebeb7e555500884e318757422fff22746a4188917529fa69f",
    "372227afd2d3ff274be9bbd84bcbee8dc3ffa4211a1019d0f600c7f35332338a",
    "0ce7001ad762d67fa8858c4ca7193eb7156d3b75ac89def5468c7c51538730db",
    "f92c732970b56b12e89c25c6d6c73ff22ae9c2e510a053c25cf1a6635c172b13",
    "d409264137651883019e5271652dffc930be98525aa2145bb8de4664f3e18df5",
    "1e955a6dc9dc6576bae6e933619409819a23fdd11ac0b1cb303cf331f5247f0e",
    "8cfb504fd92ed7964ec20040306932f139298790b397a65de74c6ab006085cdb",
    "1be11117a2fafa83116bfb104d8b7a7e45a9cfdef25e78a4714f4f30c0fb6b50",
    "0bd79dd87d1fbac5ceaf31bfe76273678485cbe3594a1ba512d4eaf160a3b866",
    "dc736688efdeadb3d1c1a67c15c89f4d8be51cfd9e765b6a0b4d157b9094371a",
    "edc027c187a455739ddec5993987840847b0e10d0be5f05db6337cb9841588d3",
    "12542e8e5368fedd3f4c20ef4e91d63a3c4f107c921642f553b513a8793c08d7",
    "94f47fe5e276b6212b09dc3eba8ec4dc27786054b9b97dc235eb50648c91017a",
    "ca1a314cfd17d4d126487ded1cf205d8a85cebe663f718352526e0d37afbacd8",
    "00c43c15c8b4af9bfc42e70291da214e60551c1931dd872b3ce7ee156effb94c",
    "0a7042ade145a63b3694142768248d94d2fcee3cf2fa8284cb6f601c65a45594",
    "c7899fa045002e396f718fe2223cbf92abeedd1c5555973b00aab9cb3f0fc2d6",
    "b9b15714014a2b61a0ca9d22181048cb409c47758003eab439d29332ec25c385",
    "28ddee94486d439dc81ce0fc972f23ec5eee917bbca054caadb21a479fffe336",
    "6594aa9633c6a58f5b19c50cb7b82c44ee85b7e0cdcbdb1b5aa75017f6cafb4f",
)
REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH = {
    line_hash: match_id
    for match_id, line_hash in enumerate(
        _REVIEWED_GROUP_FIXTURE_HASHES_BY_MATCH_ID, start=1
    )
}

_STAGES = {
    "Round of 32": ("round_of_32", 1),
    "Round of 16": ("round_of_16", 2),
    "Quarter-final": ("quarterfinal", 3),
    "Semi-final": ("semifinal", 4),
    "Match for third place": ("third_place", 0),
    "Final": ("final", 5),
}
_EXPECTED_STAGE_BY_ID = {
    **{match_id: "group_stage" for match_id in range(1, 73)},
    **{match_id: "round_of_32" for match_id in range(73, 89)},
    **{match_id: "round_of_16" for match_id in range(89, 97)},
    **{match_id: "quarterfinal" for match_id in range(97, 101)},
    **{match_id: "semifinal" for match_id in range(101, 103)},
    103: "third_place",
    104: "final",
}
_TEAM_ALIASES = {
    "south korea": ("south korea", "korea republic"),
    "czech republic": ("czech republic", "czechia"),
    "bosnia herzegovina": ("bosnia herzegovina", "bosnia and herzegovina"),
    "usa": ("usa", "united states"),
    "turkey": ("turkey", "turkiye"),
    "ivory coast": ("ivory coast", "cote d ivoire"),
    "iran": ("iran", "ir iran"),
    "cape verde": ("cape verde", "cabo verde"),
}


@dataclass(frozen=True)
class Fixture:
    fifa_match_id: int
    stage: str
    group_label: str | None
    home_team: str
    away_team: str
    kickoff_at_utc: datetime
    source_path: str
    source_lines: str
    source_line_hash: str
    venue: str = "unknown"
    match_status: str = "scheduled"


def _normalize(value: str) -> str:
    value = unicodedata.normalize("NFD", value)
    value = "".join(char for char in value if not unicodedata.combining(char))
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value.casefold()).split())


def _team_variants(team: str) -> tuple[str, ...]:
    normalized = _normalize(team)
    return _TEAM_ALIASES.get(normalized, (normalized,))


def _line_hash(lines: Sequence[str], start: int, end: int) -> str:
    payload = ("\n".join(lines[start - 1 : end]) + "\n").encode()
    return hashlib.sha256(payload).hexdigest()


def _parse_date(line: str) -> datetime | None:
    match = re.fullmatch(
        r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) (?:June|Jun|July|Jul) ([0-9]{1,2})",
        line.strip(),
    )
    if not match:
        return None
    month = 6 if "Jun" in line else 7
    return datetime(2026, month, int(match.group(1)))


def _parse_group_teams(lines: Sequence[str]) -> dict[str, tuple[str, ...]]:
    groups: dict[str, tuple[str, ...]] = {}
    for line in lines:
        match = re.match(r"Group ([A-L]) \| (.+)$", line.strip())
        if not match:
            continue
        teams = tuple(re.split(r"\s{2,}", match.group(2).strip()))
        if len(teams) != 4:
            raise ValueError(f"OpenFootball group {match.group(1)} is malformed")
        groups[match.group(1)] = teams
    if set(groups) != set("ABCDEFGHIJKL"):
        raise ValueError("OpenFootball must define exactly groups A-L")
    return groups


def _fixture_teams(line: str, teams: Iterable[str]) -> tuple[str, str]:
    normalized = _normalize(line)
    hits: list[tuple[int, str]] = []
    for team in teams:
        positions = [
            normalized.find(variant)
            for variant in _team_variants(team)
            if normalized.find(variant) >= 0
        ]
        if positions:
            hits.append((min(positions), team))
    hits.sort()
    if len(hits) != 2 or hits[0][0] == hits[1][0]:
        raise ValueError(f"Could not identify two OpenFootball teams in {line!r}")
    return hits[0][1], hits[1][1]


def _kickoff(date: datetime, line: str) -> datetime:
    match = re.search(r"([0-9]{1,2}):([0-9]{2}) UTC([+-][0-9]+)", line)
    if not match:
        raise ValueError(f"OpenFootball fixture lacks UTC offset: {line!r}")
    local = date.replace(hour=int(match.group(1)), minute=int(match.group(2)))
    local = local.replace(tzinfo=timezone(timedelta(hours=int(match.group(3)))))
    return local.astimezone(timezone.utc)


def _venue(line: str) -> str:
    without_comment = line.split("##", 1)[0].rstrip()
    if " @ " not in without_comment:
        raise ValueError(f"OpenFootball fixture lacks venue: {line!r}")
    venue = without_comment.rsplit(" @ ", 1)[1].strip()
    if not venue:
        raise ValueError(f"OpenFootball fixture lacks venue: {line!r}")
    return venue


def _match_status(line: str) -> str:
    return "completed" if re.search(r"\s\d+-\d+(?:\s|$)", line) else "scheduled"


def parse_openfootball_fixtures(
    group_text: str, finals_text: str
) -> tuple[Fixture, ...]:
    """Parse the two pinned sources and apply reviewed FIFA match numbering."""
    group_lines = group_text.splitlines()
    groups = _parse_group_teams(group_lines)
    group_fixtures: list[tuple[str, str, str, datetime, str, str, int, int]] = []
    current_group: str | None = None
    current_date: datetime | None = None
    date_line = 0
    for number, line in enumerate(group_lines, 1):
        group_match = re.fullmatch(r"▪ Group ([A-L])", line.strip())
        if group_match:
            current_group = group_match.group(1)
            continue
        parsed_date = _parse_date(line)
        if parsed_date:
            current_date, date_line = parsed_date, number
            continue
        if " UTC" not in line or " @ " not in line:
            continue
        if current_group is None or current_date is None:
            raise ValueError("OpenFootball group fixture lacks group/date context")
        home, away = _fixture_teams(line, groups[current_group])
        group_fixtures.append(
            (
                current_group,
                home,
                away,
                _kickoff(current_date, line),
                _venue(line),
                _match_status(line),
                date_line,
                number,
            )
        )
    if len(group_fixtures) != 72:
        raise ValueError(
            f"Expected 72 OpenFootball group fixtures; found {len(group_fixtures)}"
        )

    fixtures: list[Fixture] = []
    observed_group_hashes: set[str] = set()
    for group, home, away, kickoff, venue, status, start, end in group_fixtures:
        source_hash = _line_hash(group_lines, start, end)
        match_id = REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH.get(source_hash)
        if match_id is None:
            raise ValueError(
                "OpenFootball group fixture is absent from the reviewed FIFA "
                f"match-ID mapping: lines {start}-{end}"
            )
        if source_hash in observed_group_hashes:
            raise ValueError("OpenFootball group fixture evidence is duplicated")
        observed_group_hashes.add(source_hash)
        fixtures.append(
            Fixture(
                fifa_match_id=match_id,
                stage="group_stage",
                group_label=group,
                home_team=home,
                away_team=away,
                kickoff_at_utc=kickoff,
                venue=venue,
                match_status=status,
                source_path="2026--usa/cup.txt",
                source_lines=f"{start}-{end}",
                source_line_hash=source_hash,
            )
        )
    if observed_group_hashes != set(REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH):
        raise ValueError("Reviewed FIFA group match-ID mapping is incomplete")

    all_teams = tuple(team for values in groups.values() for team in values)
    final_lines = finals_text.splitlines()
    current_date = None
    date_line = 0
    stage: tuple[str, int] | None = None
    for number, line in enumerate(final_lines, 1):
        stage_match = re.fullmatch(r"▪ (.+?)\s*", line.strip())
        if stage_match and stage_match.group(1) in _STAGES:
            stage = _STAGES[stage_match.group(1)]
            continue
        parsed_date = _parse_date(line)
        if parsed_date:
            current_date, date_line = parsed_date, number
            continue
        match = re.search(r"\((7[3-9]|[89][0-9]|10[0-4])\)", line)
        if not match:
            continue
        if current_date is None or stage is None:
            raise ValueError("OpenFootball knockout fixture lacks stage/date context")
        match_id = int(match.group(1))
        home, away = _fixture_teams(line, all_teams)
        fixtures.append(
            Fixture(
                fifa_match_id=match_id,
                stage=stage[0],
                group_label=None,
                home_team=home,
                away_team=away,
                kickoff_at_utc=_kickoff(current_date, line),
                venue=_venue(line),
                match_status=_match_status(line),
                source_path="2026--usa/cup_finals.txt",
                source_lines=f"{date_line}-{number}",
                source_line_hash=_line_hash(final_lines, date_line, number),
            )
        )
    fixtures.sort(key=lambda fixture: fixture.fifa_match_id)
    if [fixture.fifa_match_id for fixture in fixtures] != list(range(1, 105)):
        raise ValueError("OpenFootball fixtures must map to FIFA match IDs 1..104")
    for fixture in fixtures:
        expected_stage = _EXPECTED_STAGE_BY_ID[fixture.fifa_match_id]
        if fixture.stage != expected_stage:
            raise ValueError(
                f"FIFA match {fixture.fifa_match_id} expected {expected_stage}, "
                f"got {fixture.stage}"
            )
        if fixture.home_team == fixture.away_team:
            raise ValueError(f"FIFA match {fixture.fifa_match_id} has identical teams")
    return tuple(fixtures)


def parse_schedule_fixtures(
    group_text: str,
    finals_text: str,
    *,
    loaded_at: datetime | None = None,
) -> list[dict[str, object]]:
    """Convert the reviewed complete fixture set to raw-warehouse rows."""
    loaded_at = (loaded_at or datetime.now(timezone.utc)).replace(tzinfo=None)
    rows = []
    for fixture in parse_openfootball_fixtures(group_text, finals_text):
        rows.append(
            {
                "fifa_match_id": fixture.fifa_match_id,
                "stage_key": fixture.stage,
                "stage_rank": _STAGES.get(
                    next(
                        (
                            label
                            for label, value in _STAGES.items()
                            if value[0] == fixture.stage
                        ),
                        "",
                    ),
                    ("group_stage", 0),
                )[1],
                "group_label": fixture.group_label,
                "kickoff_at_utc": fixture.kickoff_at_utc.replace(tzinfo=None),
                "home_team": fixture.home_team,
                "away_team": fixture.away_team,
                "venue": fixture.venue,
                "match_status": fixture.match_status,
                "source_url": OPENFOOTBALL_BASE + fixture.source_path,
                "source_line_number": int(fixture.source_lines.rsplit("-", 1)[1]),
                "source_line_hash": fixture.source_line_hash,
                "source_loaded_at": loaded_at,
            }
        )
    return rows


def fetch_schedule_fixtures(url: str) -> str:
    response = requests.get(
        validate_outbound_https_url(url),
        timeout=HTTP_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.text


def sync_schedule_fixtures(
    *,
    fetch_text: Callable[[str], str] = fetch_schedule_fixtures,
) -> dict[str, object]:
    loaded_at = datetime.now(timezone.utc)
    sources: dict[str, str] = {}
    for path, expected_sha256 in OPENFOOTBALL_FILES.items():
        text = fetch_text(OPENFOOTBALL_BASE + path)
        actual_sha256 = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if actual_sha256 != expected_sha256:
            raise ValueError(f"Pinned OpenFootball hash mismatch for {path}")
        sources[path] = text
    rows = parse_schedule_fixtures(
        sources["2026--usa/cup.txt"],
        sources["2026--usa/cup_finals.txt"],
        loaded_at=loaded_at,
    )
    summary = replace_schedule_fixtures(rows)
    return {
        **summary,
        "source_url": OPENFOOTBALL_BASE,
        "loaded_at": loaded_at.isoformat(),
        "rows": len(rows),
        "scheduled_rows": sum(row["match_status"] == "scheduled" for row in rows),
        "completed_rows": sum(row["match_status"] == "completed" for row in rows),
        "openfootball_revision": OPENFOOTBALL_REVISION,
        "openfootball_files": OPENFOOTBALL_FILES,
        "fifa_schedule_sha256": FIFA_SCHEDULE_SHA256,
    }


__all__ = [
    "FIFA_SCHEDULE_SHA256",
    "FIFA_SCHEDULE_TITLE",
    "FIFA_SCHEDULE_URL",
    "Fixture",
    "OPENFOOTBALL_BASE",
    "OPENFOOTBALL_FILES",
    "OPENFOOTBALL_REVISION",
    "OPENFOOTBALL_WC2026_SCHEDULE_FIXTURES_URL",
    "REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH",
    "fetch_schedule_fixtures",
    "parse_schedule_fixtures",
    "parse_openfootball_fixtures",
    "sync_schedule_fixtures",
]

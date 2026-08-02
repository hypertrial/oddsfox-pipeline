"""Load and enforce config/terminology_policy.toml (test-support only)."""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib  # type: ignore[no-redef]


REPO_ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = REPO_ROOT / "config" / "terminology_policy.toml"
TERMINOLOGY_DOC = REPO_ROOT / "docs" / "reference" / "terminology.md"


@dataclass(frozen=True)
class RetiredRule:
    rule_id: str
    pattern: re.Pattern[str]
    prefer: str
    kind: str  # "identifier" | "prose"
    owner_allow: tuple[str, ...] = ()


@dataclass(frozen=True)
class Policy:
    meta: dict[str, Any]
    scan_roots: tuple[str, ...]
    scan_suffixes: frozenset[str]
    exclude_prefixes: tuple[str, ...]
    exclude_parts: frozenset[str]
    frozen_literals: tuple[str, ...]
    core_terms: dict[str, tuple[str, ...]]
    local_owners: dict[str, str]
    extension_jobs: frozenset[str]
    critical_asset_keys: frozenset[tuple[str, ...]]
    identifier_rules: tuple[RetiredRule, ...]
    prose_rules: tuple[RetiredRule, ...]

    @property
    def all_core_terms(self) -> tuple[str, ...]:
        terms: list[str] = []
        for group in self.core_terms.values():
            terms.extend(group)
        return tuple(terms)

    @property
    def path_exception_count(self) -> int:
        return len(self.exclude_prefixes)


@dataclass(frozen=True)
class Violation:
    path: str
    line_number: int
    rule_id: str
    prefer: str
    line: str

    def format(self) -> str:
        return (
            f"{self.path}:{self.line_number}: [{self.rule_id}] "
            f"prefer {self.prefer!r}: {self.line.strip()}"
        )


def load_policy(path: Path = POLICY_PATH) -> Policy:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    known_top = {
        "meta",
        "scan",
        "frozen",
        "core_terms",
        "local_owners",
        "structural",
        "retired",
    }
    unknown = set(data) - known_top
    if unknown:
        raise ValueError(
            f"Unknown top-level keys in terminology policy: {sorted(unknown)}"
        )

    meta = dict(data["meta"])
    scan = data["scan"]
    frozen = data.get("frozen", {})
    core_raw = data["core_terms"]
    structural = data.get("structural", {})
    retired = data.get("retired", {})

    # tomllib yields {"execution": {"terms": [...]}, ...}
    core_terms = {
        group: tuple(block["terms"])
        for group, block in core_raw.items()
        if isinstance(block, dict) and "terms" in block
    }

    flat_terms = [term for group in core_terms.values() for term in group]
    if len(flat_terms) != int(meta["core_term_count"]):
        raise ValueError(
            f"core_term_count={meta['core_term_count']} but inventory has {len(flat_terms)}"
        )
    if len(flat_terms) != len(set(flat_terms)):
        raise ValueError("Duplicate core terms in terminology policy")

    identifier_rules = tuple(
        _compile_rule(item, kind="identifier") for item in retired.get("identifier", [])
    )
    prose_rules = tuple(
        _compile_rule(item, kind="prose") for item in retired.get("prose", [])
    )
    if len(identifier_rules) > int(meta["max_retired_identifier_rules"]):
        raise ValueError("Too many identifier retirement rules")
    if len(prose_rules) > int(meta["max_retired_prose_rules"]):
        raise ValueError("Too many prose retirement rules")

    exclude_prefixes = tuple(scan["exclude_prefixes"])
    if len(exclude_prefixes) > int(meta["max_path_exceptions"]):
        raise ValueError("Path exception budget exceeded")

    critical = frozenset(
        tuple(key) for key in structural.get("critical_asset_keys", [])
    )
    return Policy(
        meta=meta,
        scan_roots=tuple(scan["roots"]),
        scan_suffixes=frozenset(scan["suffixes"]),
        exclude_prefixes=exclude_prefixes,
        exclude_parts=frozenset(scan.get("exclude_parts", [])),
        frozen_literals=tuple(frozen.get("literals", [])),
        core_terms=core_terms,
        local_owners=dict(data.get("local_owners", {})),
        extension_jobs=frozenset(structural.get("extension_jobs", [])),
        critical_asset_keys=critical,
        identifier_rules=identifier_rules,
        prose_rules=prose_rules,
    )


def _compile_rule(item: dict[str, Any], *, kind: str) -> RetiredRule:
    return RetiredRule(
        rule_id=str(item["id"]),
        pattern=re.compile(str(item["pattern"])),
        prefer=str(item["prefer"]),
        kind=kind,
        owner_allow=tuple(item.get("owner_allow", [])),
    )


def iter_scan_paths(policy: Policy, repo_root: Path = REPO_ROOT) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError("git ls-files failed; cannot enforce terminology policy")
    paths: list[Path] = []
    for line in result.stdout.splitlines():
        rel = line.strip()
        if not rel:
            continue
        if any(
            rel == prefix or rel.startswith(prefix)
            for prefix in policy.exclude_prefixes
        ):
            continue
        if any(part in policy.exclude_parts for part in Path(rel).parts):
            continue
        if not any(
            rel == prefix or rel.startswith(prefix) for prefix in policy.scan_roots
        ):
            continue
        path = repo_root / rel
        if (
            path.suffix
            and path.suffix not in policy.scan_suffixes
            and path.name != "Makefile"
        ):
            continue
        if not path.is_file():
            continue
        try:
            with path.open("rb") as handle:
                if b"\x00" in handle.read(8192):
                    continue
        except OSError:
            continue
        paths.append(path)
    return paths


def _mask_frozen(text: str, literals: tuple[str, ...]) -> str:
    masked = text
    for literal in literals:
        if literal in masked:
            masked = masked.replace(literal, " " * len(literal))
    return masked


def scan_text(
    *,
    rel: str,
    text: str,
    policy: Policy,
) -> list[Violation]:
    violations: list[Violation] = []
    is_markdown = rel.endswith(".md")
    lines = text.splitlines()
    if is_markdown:
        # Rebuild line-aligned segments: for each source line emit one scan pass.
        source_lines = lines
        in_fence = False
        in_deprecated = False
        for line_number, line in enumerate(source_lines, start=1):
            stripped = line.strip()
            if stripped.startswith("```"):
                in_fence = not in_fence
                scan_line = _mask_frozen(line, policy.frozen_literals)
                for rule in policy.identifier_rules:
                    if rule.owner_allow and not any(
                        token in rel for token in rule.owner_allow
                    ):
                        # identifier rules ignore owner_allow
                        pass
                    if rule.pattern.search(scan_line):
                        violations.append(
                            Violation(rel, line_number, rule.rule_id, rule.prefer, line)
                        )
                continue
            if stripped == "## Deprecated phrases":
                in_deprecated = True
                continue
            if in_deprecated and stripped.startswith("## "):
                in_deprecated = False
            if in_deprecated:
                continue
            scan_line = _mask_frozen(line, policy.frozen_literals)
            for rule in policy.identifier_rules:
                if rule.pattern.search(scan_line):
                    violations.append(
                        Violation(rel, line_number, rule.rule_id, rule.prefer, line)
                    )
            if in_fence:
                continue
            prose_line = re.sub(r"`[^`]*`", lambda m: " " * len(m.group(0)), scan_line)
            for rule in policy.prose_rules:
                if rule.owner_allow and any(token in rel for token in rule.owner_allow):
                    continue
                if not rel.startswith("docs/") and rel not in {
                    "README.md",
                    "AGENTS.md",
                    "CONTRIBUTING.md",
                }:
                    continue
                if rule.pattern.search(prose_line):
                    violations.append(
                        Violation(rel, line_number, rule.rule_id, rule.prefer, line)
                    )
        return violations

    for line_number, line in enumerate(lines, start=1):
        scan_line = _mask_frozen(line, policy.frozen_literals)
        for rule in policy.identifier_rules:
            if rule.pattern.search(scan_line):
                violations.append(
                    Violation(rel, line_number, rule.rule_id, rule.prefer, line)
                )
    return violations


def scan_repo(
    policy: Policy | None = None, repo_root: Path = REPO_ROOT
) -> list[Violation]:
    policy = policy or load_policy()
    violations: list[Violation] = []
    for path in iter_scan_paths(policy, repo_root=repo_root):
        rel = path.relative_to(repo_root).as_posix()
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        violations.extend(scan_text(rel=rel, text=text, policy=policy))
    return violations


def parse_core_terms_from_doc(text: str | None = None) -> set[str]:
    doc = text if text is not None else TERMINOLOGY_DOC.read_text(encoding="utf-8")
    terms: set[str] = set()
    in_table = False
    for line in doc.splitlines():
        if line.strip() == "## Core vocabulary":
            in_table = True
            continue
        if in_table and line.startswith("## "):
            break
        if not in_table or not line.startswith("|"):
            continue
        if "Bucket" in line or line.startswith("| ---"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 2:
            continue
        for match in re.finditer(r"\*\*([^*]+)\*\*", cells[1]):
            terms.add(match.group(1).strip().lower())
    return terms


def parse_deprecated_table(text: str | None = None) -> list[tuple[str, str]]:
    doc = text if text is not None else TERMINOLOGY_DOC.read_text(encoding="utf-8")
    rows: list[tuple[str, str]] = []
    in_table = False
    for line in doc.splitlines():
        if line.strip() == "## Deprecated phrases":
            in_table = True
            continue
        if in_table and line.startswith("## "):
            break
        if not in_table or not line.startswith("|"):
            continue
        if "Avoid" in line or line.startswith("| ---"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) >= 2:
            rows.append((cells[0], cells[1]))
    return rows

"""Fail closed when retired terminology reappears in active first-party surfaces."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.support.terminology_policy import (
    POLICY_PATH,
    REPO_ROOT,
    TERMINOLOGY_DOC,
    load_policy,
    parse_core_terms_from_doc,
    parse_deprecated_table,
    scan_repo,
    scan_text,
)

pytestmark = pytest.mark.repo_check


def test_policy_loads_and_respects_budgets():
    policy = load_policy()
    assert policy.meta["version"] == 1
    assert len(policy.all_core_terms) == 34
    assert policy.path_exception_count <= int(policy.meta["max_path_exceptions"])
    assert len(policy.identifier_rules) <= int(
        policy.meta["max_retired_identifier_rules"]
    )
    assert len(policy.prose_rules) <= int(policy.meta["max_retired_prose_rules"])
    assert "allow_line" not in POLICY_PATH.read_text(encoding="utf-8")


def test_core_terms_match_terminology_doc():
    policy = load_policy()
    doc_terms = parse_core_terms_from_doc()
    assert doc_terms == {term.lower() for term in policy.all_core_terms}


def test_local_owners_exist_and_mention_topics():
    policy = load_policy()
    for topic, rel in policy.local_owners.items():
        path = REPO_ROOT / rel
        assert path.is_file(), f"missing local owner for {topic!r}: {rel}"
        text = path.read_text(encoding="utf-8").lower()
        needle = topic.lower()
        # Contract IDs and multi-word topics should appear literally.
        assert needle in text or needle.replace(" ", "-") in text, (
            f"{rel} does not mention {topic!r}"
        )


def test_deprecated_table_present():
    rows = parse_deprecated_table()
    assert rows
    avoids = " ".join(avoid for avoid, _ in rows).lower()
    assert "minutely" in avoids
    assert "universe" in avoids
    assert "market_scope_registry" in avoids or "scopestep" in avoids.lower()


def test_active_surfaces_avoid_retired_terminology():
    violations = scan_repo()
    assert not violations, "Retired terminology found:\n" + "\n".join(
        v.format() for v in violations[:80]
    )


def test_scanner_rejects_synthetic_identifier_violation(tmp_path: Path):
    policy = load_policy()
    text = "model graph_export should not land\n"
    violations = scan_text(rel="src/example.py", text=text, policy=policy)
    assert any(v.rule_id == "retired_graph_export_product" for v in violations)


def test_scanner_skips_deprecated_table_rows():
    policy = load_policy()
    text = TERMINOLOGY_DOC.read_text(encoding="utf-8")
    violations = [
        v
        for v in scan_text(
            rel="docs/reference/terminology.md", text=text, policy=policy
        )
        if v.rule_id
        in {
            "retired_graph_export_product",
            "ambiguous_graph_bundle",
            "retired_minutely_config",
            "retired_universe_models",
        }
    ]
    # The deprecated table may quote retired phrases; they must not fail the scan.
    assert violations == []


def test_terminology_reference_is_linked_from_glossary_and_naming():
    glossary = (REPO_ROOT / "docs/concepts/glossary.md").read_text(encoding="utf-8")
    naming = (REPO_ROOT / "docs/reference/naming.md").read_text(encoding="utf-8")
    terminology = TERMINOLOGY_DOC.read_text(encoding="utf-8")
    assert (
        "reference/terminology.md" in glossary
        or "../reference/terminology.md" in glossary
    )
    assert "terminology.md" in naming
    assert "Pipeline" in terminology
    assert "working set" in terminology.lower()
    assert "**contract**" in terminology.lower() or "| **Contract**" in terminology
    assert "34" in terminology

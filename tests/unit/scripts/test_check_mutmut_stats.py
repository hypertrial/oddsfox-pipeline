"""Tests for the strict Mutmut statistics gate."""

from __future__ import annotations

import json

import pytest
from scripts.check_mutmut_stats import COUNTERS, main, validate_stats


def _clean_stats() -> dict[str, int]:
    return {name: 0 for name in COUNTERS} | {
        "killed": 9,
        "skipped": 1,
        "total": 10,
    }


def test_validate_stats_accepts_resolved_mutants() -> None:
    validate_stats(_clean_stats())


@pytest.mark.parametrize(
    "counter",
    [
        "survived",
        "no_tests",
        "suspicious",
        "timeout",
        "check_was_interrupted_by_user",
        "segfault",
    ],
)
def test_validate_stats_rejects_unresolved_mutants(counter: str) -> None:
    stats = _clean_stats()
    stats[counter] = 1

    with pytest.raises(ValueError, match=rf"{counter}=1"):
        validate_stats(stats)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda stats: stats.pop("killed"), "missing counters: killed"),
        (lambda stats: stats.update(total=0, killed=0, skipped=0), "no mutants"),
        (lambda stats: stats.update(total=11), r"killed \+ skipped"),
        (lambda stats: stats.update(killed=True), "non-negative integers"),
        (lambda stats: stats.update(skipped=-1), "non-negative integers"),
    ],
)
def test_validate_stats_rejects_incomplete_or_invalid_reports(
    mutation, message: str
) -> None:
    stats = _clean_stats()
    mutation(stats)

    with pytest.raises(ValueError, match=message):
        validate_stats(stats)


def test_validate_stats_rejects_non_object() -> None:
    with pytest.raises(ValueError, match="JSON object"):
        validate_stats([])


def test_main_reports_success(tmp_path, capsys) -> None:
    report = tmp_path / "stats.json"
    report.write_text(json.dumps(_clean_stats()), encoding="utf-8")

    assert main([str(report)]) == 0
    assert "9 killed, 1 skipped" in capsys.readouterr().out


@pytest.mark.parametrize("contents", [None, "{bad json"])
def test_main_rejects_missing_or_malformed_report(tmp_path, capsys, contents) -> None:
    report = tmp_path / "stats.json"
    if contents is not None:
        report.write_text(contents, encoding="utf-8")

    assert main([str(report)]) == 1
    assert "Mutmut statistics failed:" in capsys.readouterr().err

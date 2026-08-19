from __future__ import annotations

from datetime import date, timedelta

import pytest

from oddsfox_pipeline.features.pre_match_elo.elo import (
    EloParameters,
    HistoricalMatch,
    compute_pre_match_ratings,
    expected_score,
    tune_parameters,
)


def match(
    match_id: str,
    day: date,
    home: str = "a",
    away: str = "b",
    score: tuple[int, int] = (1, 0),
    *,
    pool: str = "club_men",
    neutral: bool = False,
    friendly: bool = False,
) -> HistoricalMatch:
    return HistoricalMatch(
        match_id,
        day,
        home,
        away,
        score[0],
        score[1],
        pool,
        neutral,
        friendly,
    )


PARAMS = {"club_men": EloParameters(400, 20, 0)}


def test_expected_score_and_golden_updates() -> None:
    assert expected_score(1500, 1500, d=400, home_advantage=0) == 0.5
    day = date(2024, 1, 1)
    ratings = compute_pre_match_ratings(
        [match("win", day)],
        PARAMS,
        [
            ("club_men", day + timedelta(days=1), "a"),
            ("club_men", day + timedelta(days=1), "b"),
        ],
    )
    assert ratings[("club_men", day + timedelta(days=1), "a")].rating == 1510
    assert ratings[("club_men", day + timedelta(days=1), "b")].rating == 1490


@pytest.mark.parametrize(
    ("score", "neutral", "friendly", "expected_home"),
    [
        ((0, 1), False, False, 1490),
        ((0, 0), True, False, 1500),
        ((1, 0), False, True, 1505),
    ],
)
def test_loss_draw_neutral_and_friendly_updates(
    score: tuple[int, int], neutral: bool, friendly: bool, expected_home: float
) -> None:
    day = date(2024, 1, 1)
    rows = [match("m", day, score=score, neutral=neutral, friendly=friendly)]
    rating = compute_pre_match_ratings(
        rows, PARAMS, [("club_men", day + timedelta(days=1), "a")]
    )[("club_men", day + timedelta(days=1), "a")]
    assert rating.rating == expected_home


def test_same_date_batching_is_order_independent() -> None:
    day = date(2024, 1, 1)
    rows = [
        match("one", day, "a", "b", (1, 0)),
        match("two", day, "a", "c", (0, 1)),
    ]
    target = [("club_men", day + timedelta(days=1), team) for team in "abc"]
    assert compute_pre_match_ratings(rows, PARAMS, target) == compute_pre_match_ratings(
        reversed(rows), PARAMS, target
    )
    assert compute_pre_match_ratings(rows, PARAMS, target)[target[0]].rating == 1500


def test_later_result_cannot_change_earlier_capture() -> None:
    first = date(2024, 1, 1)
    target = ("club_men", date(2024, 2, 1), "a")
    before = compute_pre_match_ratings([match("early", first)], PARAMS, [target])
    after = compute_pre_match_ratings(
        [match("early", first), match("later", date(2024, 3, 1), score=(0, 9))],
        PARAMS,
        [target],
    )
    assert before[target] == after[target]


def test_quality_age_components_and_pool_isolation() -> None:
    start = date(2023, 1, 1)
    rows = [
        match(f"m{index}", start + timedelta(days=index), score=(1, 1))
        for index in range(10)
    ]
    rows.append(
        match(
            "national",
            start,
            "a",
            "b",
            pool="national_men",
        )
    )
    params = {
        "club_men": EloParameters(400, 20, 0),
        "national_men": EloParameters(400, 20, 0),
    }
    stable_day = start + timedelta(days=20)
    old_day = start + timedelta(days=500)
    ratings = compute_pre_match_ratings(
        rows,
        params,
        [
            ("club_men", stable_day, "a"),
            ("club_men", stable_day, "b"),
            ("club_men", old_day, "a"),
            ("national_men", start + timedelta(days=1), "a"),
        ],
    )
    assert ratings[("club_men", stable_day, "a")].quality == "stable"
    assert ratings[("club_men", old_day, "a")].quality == "provisional"
    assert (
        ratings[("club_men", stable_day, "a")].connected_component_id
        == ratings[("club_men", stable_day, "b")].connected_component_id
    )
    assert ratings[("club_men", stable_day, "a")].rating == 1500
    assert ratings[("national_men", start + timedelta(days=1), "a")].rating == 1510


def test_parameter_fallback_and_deterministic_tie_break() -> None:
    fallback = tune_parameters([match("one", date(2022, 1, 1))])
    assert fallback["club_men"] == EloParameters(400, 20, 60, 1, None, True)
    assert fallback["club_women"].validation_matches == 0

    rows = [
        match(
            f"draw-{index}",
            date(2022, 1, 1) + timedelta(days=index),
            score=(0, 0),
        )
        for index in range(500)
    ]
    selected = tune_parameters(rows)["club_men"]
    assert (selected.d, selected.k, selected.home_advantage) == (400, 10, 0)
    assert selected.validation_mse == 0
    assert selected.used_fallback is False

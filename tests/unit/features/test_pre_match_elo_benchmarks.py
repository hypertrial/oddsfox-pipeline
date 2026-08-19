from datetime import date

from oddsfox_pipeline.features.pre_match_elo.benchmarks import (
    BenchmarkIndex,
    BenchmarkRating,
    reconstruct_eloratings,
)


def test_clubelo_lookup_is_strictly_before_match_date() -> None:
    index = BenchmarkIndex(
        [
            BenchmarkRating(
                "ClubElo", "club:a", 1500, date(2024, 1, 1), "one", "exact"
            ),
            BenchmarkRating(
                "ClubElo", "club:a", 1510, date(2024, 1, 2), "two", "exact"
            ),
        ]
    )
    assert index.latest_before("ClubElo", "club:a", date(2024, 1, 2)).rating == 1500


def test_eloratings_reconstruction_allows_proven_pre_match_same_date() -> None:
    rows = reconstruct_eloratings(
        [
            {
                "match_date": date(2024, 1, 2),
                "home_team_id": "national:a",
                "away_team_id": "national:b",
                "home_post_rating": 1612,
                "away_post_rating": 1488,
                "home_rating_change": 12,
                "snapshot_id": "snapshot",
                "mapping_method": "exact",
            }
        ]
    )
    assert [(row.team_id, row.rating) for row in rows] == [
        ("national:a", 1600),
        ("national:b", 1500),
    ]
    index = BenchmarkIndex(rows)
    assert (
        index.latest_before("EloRatings", "national:a", date(2024, 1, 2)).rating == 1600
    )

from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def _nav_targets(items):
    for item in items:
        if isinstance(item, str):
            yield item
        elif isinstance(item, dict):
            for value in item.values():
                if isinstance(value, str):
                    yield value
                else:
                    yield from _nav_targets(value)


def test_mkdocs_nav_targets_exist():
    config = yaml.safe_load((REPO_ROOT / "mkdocs.yml").read_text())
    docs_dir = REPO_ROOT / "docs"

    for target in _nav_targets(config["nav"]):
        assert (docs_dir / target).is_file(), target


def test_mkdocs_uses_material_theme_contract():
    config = yaml.safe_load((REPO_ROOT / "mkdocs.yml").read_text())
    features = set(config["theme"]["features"])

    assert config["theme"]["name"] == "material"
    assert config["theme"]["logo"] == "assets/images/oddsfox-white.png"
    assert config["theme"]["favicon"] == "assets/images/oddsfox-white.png"
    assert config["extra_css"] == ["assets/stylesheets/oddsfox-dark.css"]
    assert config["plugins"] == ["search"]
    assert {
        "navigation.tabs",
        "navigation.sections",
        "navigation.expand",
        "navigation.indexes",
        "navigation.top",
        "toc.follow",
        "search.suggest",
        "search.highlight",
        "content.code.copy",
    } <= features


def test_readme_links_to_project_docs():
    readme = (REPO_ROOT / "README.md").read_text()

    assert "(docs/index.md)" in readme
    assert "(docs/analyst-guide.md)" in readme
    assert "(docs/query-cookbook.md)" in readme
    assert "(docs/data-dictionary.md)" in readme
    assert "(docs/system-overview.md)" in readme
    assert "(docs/operator-runbook.md)" in readme
    assert "(docs/quickstart.md)" in readme
    assert "(docs/architecture.md)" in readme
    assert "(docs/data-contracts.md)" in readme
    assert "(CONTRIBUTING.md)" in readme
    assert "(SECURITY.md)" in readme
    assert "(CHANGELOG.md)" in readme
    assert "(LICENSE)" in readme


def test_readme_covers_first_run_analyst_path():
    readme = (REPO_ROOT / "README.md").read_text()

    required_terms = [
        "uv run make docs-serve",
        "http://127.0.0.1:8000",
        "(docs/analyst-guide.md)",
        "(docs/query-cookbook.md)",
        "(docs/data-dictionary.md)",
        "`*_marts`",
        "`*_observability`",
        "is_actionable_live_market",
        "current_price_status",
    ]

    for term in required_terms:
        assert term in readme


def test_landing_docs_describe_shipped_kalshi_support():
    texts = {
        "README.md": (REPO_ROOT / "README.md").read_text(),
        "docs/index.md": (REPO_ROOT / "docs" / "index.md").read_text(),
        "CONTRIBUTING.md": (REPO_ROOT / "CONTRIBUTING.md").read_text(),
    }

    for text in texts.values():
        assert "Kalshi WC2026" in text

    combined = "\n".join(texts.values()).lower()
    assert "future adapter contributions may cover kalshi" not in combined
    assert (
        "kalshi and traditional bookmaker adapters are welcome future" not in combined
    )


def test_operator_docs_describe_scoped_runner():
    texts = {
        "docs/quickstart.md": (REPO_ROOT / "docs" / "quickstart.md").read_text(),
        "docs/operations.md": (REPO_ROOT / "docs" / "operations.md").read_text(),
        "docs/scripts.md": (REPO_ROOT / "docs" / "scripts.md").read_text(),
        "docs/development.md": (REPO_ROOT / "docs" / "development.md").read_text(),
    }
    combined = "\n".join(texts.values())

    assert "scripts/run_scope.py" in combined
    assert "polymarket:wc2026" in combined
    assert "polymarket:us_midterms_2026" in combined
    assert "kalshi:wc2026" in combined
    assert "ScopeSpec" in combined


def test_github_templates_exist():
    required = [
        ".github/ISSUE_TEMPLATE/bug_report.yml",
        ".github/ISSUE_TEMPLATE/feature_request.yml",
        ".github/ISSUE_TEMPLATE/documentation.yml",
        ".github/PULL_REQUEST_TEMPLATE.md",
    ]

    for target in required:
        assert (REPO_ROOT / target).is_file(), target


def test_docs_make_targets_suppress_material_warning():
    makefile = (REPO_ROOT / "Makefile").read_text()

    assert "NO_MKDOCS_2_WARNING=true" in makefile


def test_cross_repo_operator_docs_are_in_nav():
    config = yaml.safe_load((REPO_ROOT / "mkdocs.yml").read_text())
    targets = set(_nav_targets(config["nav"]))

    assert "system-overview.md" in targets
    assert "operator-runbook.md" in targets


def test_analyst_docs_are_in_nav_and_linked_from_homepage():
    config = yaml.safe_load((REPO_ROOT / "mkdocs.yml").read_text())
    targets = set(_nav_targets(config["nav"]))
    homepage = (REPO_ROOT / "docs" / "index.md").read_text()

    assert "analyst-guide.md" in targets
    assert "query-cookbook.md" in targets
    assert "data-dictionary.md" in targets
    assert "[Analyst Guide](analyst-guide.md)" in homepage
    assert "[Query Cookbook](query-cookbook.md)" in homepage
    assert "[Data Dictionary](data-dictionary.md)" in homepage


def test_analyst_docs_cover_public_marts_and_trust_fields():
    texts = {
        "docs/analyst-guide.md": (REPO_ROOT / "docs" / "analyst-guide.md").read_text(),
        "docs/query-cookbook.md": (
            REPO_ROOT / "docs" / "query-cookbook.md"
        ).read_text(),
        "docs/data-dictionary.md": (
            REPO_ROOT / "docs" / "data-dictionary.md"
        ).read_text(),
    }
    combined = "\n".join(texts.values())

    required_terms = [
        "polymarket_wc2026_marts.polymarket_wc2026_knockout_markets",
        ("polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds"),
        ("polymarket_wc2026_marts.polymarket_wc2026_graph_token_hourly_odds"),
        ("international_results_wc2026_marts.international_results_wc2026_matches"),
        ("international_results_wc2026_marts.international_results_wc2026_team_status"),
        "kalshi_wc2026_marts.kalshi_wc2026_stage_markets",
        "kalshi_wc2026_marts.kalshi_wc2026_stage_market_hourly_odds",
        "kalshi_wc2026_marts.kalshi_wc2026_group_winner_markets",
        ("kalshi_wc2026_marts.kalshi_wc2026_group_winner_market_hourly_odds"),
        (
            "polymarket_us_midterms_2026_marts."
            "polymarket_us_midterms_2026_market_token_hourly_odds"
        ),
        "is_actionable_live_market",
        "current_price_status",
        "price_represents",
    ]

    for term in required_terms:
        assert term in combined


def test_docs_logo_asset_exists():
    logo = REPO_ROOT / "docs" / "assets" / "images" / "oddsfox-white.png"

    assert logo.is_file()
    assert logo.stat().st_size > 0


def test_built_docs_use_material_homepage():
    homepage = REPO_ROOT / "site" / "index.html"
    if not homepage.exists():
        pytest.skip("Run make docs-check before asserting generated HTML.")

    html = homepage.read_text()

    assert "md-header" in html
    assert "md-sidebar" in html
    assert "md-search" in html
    assert "Open-source prediction-market data pipeline" in html
    assert "oddsfox-dark.css" in html
    assert "assets/images/oddsfox-white.png" in html
    assert "Operator Manual" not in html
    assert "navbar-dark" not in html
    assert "bootstrap" not in html.lower()

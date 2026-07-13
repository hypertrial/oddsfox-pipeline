import json
import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = REPO_ROOT / "docs"

REDIRECTS = {
    "/quickstart": "/getting-started/",
    "/operator-runbook": "/guides/deploy-hosted-stack/",
    "/hosted-graph-deployment": "/guides/deploy-hosted-stack/",
    "/operations": "/reference/orchestration/",
    "/troubleshooting": "/guides/troubleshooting/",
    "/analyst-guide": "/guides/query-the-warehouse/",
    "/query-cookbook": "/guides/query-recipes/",
    "/configuration": "/reference/configuration/",
    "/warehouse": "/reference/warehouse/",
    "/data-contracts": "/reference/data-contracts/",
    "/data-dictionary": "/reference/data-dictionary/",
    "/scripts": "/reference/scripts/",
    "/naming": "/reference/naming/",
    "/system-overview": "/concepts/system-overview/",
    "/architecture": "/concepts/architecture/",
    "/community": "/development/community/",
    "/changelog": "/development/changelog/",
}


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


def _config():
    text = (REPO_ROOT / "mkdocs.yml").read_text()
    text = re.sub(r"!!python/name:([^\s]+)", r"\1", text)
    return yaml.safe_load(text)


def test_navigation_contains_every_docs_page():
    targets = set(_nav_targets(_config()["nav"]))
    pages = {path.relative_to(DOCS_DIR).as_posix() for path in DOCS_DIR.rglob("*.md")}

    assert targets == pages
    for target in targets:
        assert (DOCS_DIR / target).is_file(), target


def test_material_theme_uses_native_navigation_and_dark_palette():
    config = _config()
    features = set(config["theme"]["features"])
    extension_names = {
        item if isinstance(item, str) else next(iter(item))
        for item in config["markdown_extensions"]
    }

    assert config["theme"]["name"] == "material"
    assert config["site_name"] == "OddsFox Pipeline"
    assert config["repo_url"] == "https://github.com/hypertrial/oddsfox-pipeline"
    assert config["repo_name"] == "hypertrial/oddsfox-pipeline"
    assert config["theme"]["logo"] == "assets/images/oddsfox-favicon.png"
    assert config["theme"]["font"] is False
    assert config["extra_css"] == ["assets/stylesheets/extra.css"]
    assert config["extra_javascript"] == [
        "https://cdn.jsdelivr.net/npm/mermaid@11.15.0/dist/mermaid.min.js",
        "assets/javascripts/mermaid.js",
    ]
    assert "navigation.expand" not in features
    assert "navigation.instant" not in features
    assert {
        "navigation.tabs",
        "navigation.tabs.sticky",
        "navigation.sections",
        "navigation.path",
        "navigation.tracking",
        "navigation.footer",
        "content.code.copy",
    } <= features
    assert {
        "admonition",
        "attr_list",
        "md_in_html",
        "pymdownx.details",
        "pymdownx.highlight",
        "pymdownx.superfences",
        "pymdownx.tabbed",
        "toc",
    } == extension_names

    assert config["theme"]["palette"] == {
        "scheme": "slate",
        "primary": "custom",
        "accent": "custom",
    }


def test_vercel_redirects_cover_every_moved_page():
    config = json.loads((REPO_ROOT / "vercel.json").read_text())
    redirects = config["redirects"]
    actual = {item["source"]: item["destination"] for item in redirects}

    assert config["trailingSlash"] is True
    assert len({item["source"] for item in redirects}) == len(redirects)
    assert actual == REDIRECTS
    assert all(item["permanent"] is True for item in redirects)
    assert not (set(actual) & set(actual.values()))

    nav_urls = {
        f"/{Path(target).parent.as_posix()}/"
        if Path(target).name == "index.md" and target != "index.md"
        else f"/{Path(target).with_suffix('').as_posix()}/"
        for target in _nav_targets(_config()["nav"])
    }
    assert set(actual.values()) <= nav_urls


def test_every_page_starts_with_a_visible_h1():
    for path in DOCS_DIR.rglob("*.md"):
        text = path.read_text()
        if text.startswith("---\n"):
            text = text.split("---\n", 2)[2]
        assert re.search(r"^# [^#]", text, re.MULTILINE), path.relative_to(DOCS_DIR)


def test_homepage_uses_parsed_markdown_and_operator_actions():
    homepage = (DOCS_DIR / "index.md").read_text()

    assert "# OddsFox Pipeline" in homepage
    assert 'class="of-hero" markdown' in homepage
    assert "[Get started](getting-started/index.md)" in homepage
    assert "[Query the warehouse](guides/query-the-warehouse.md)" in homepage
    assert homepage.count('class="of-task-card"') == 3
    assert "of-brand-lockup" not in homepage
    assert "of-badges" not in homepage
    assert "of-capability-grid" not in homepage


def test_readme_links_to_canonical_docs_and_live_reload():
    readme = (REPO_ROOT / "README.md").read_text()
    required = [
        "uv run make docs-serve",
        "http://127.0.0.1:8000",
        "(docs/getting-started/index.md)",
        "(docs/guides/query-the-warehouse.md)",
        "(docs/guides/query-recipes.md)",
        "(docs/reference/data-dictionary.md)",
        "(docs/reference/data-contracts.md)",
        "(docs/concepts/system-overview.md)",
        "(docs/development/index.md)",
    ]

    for term in required:
        assert term in readme


def test_repository_docs_do_not_reference_moved_markdown_paths():
    policy_docs = [
        REPO_ROOT / "README.md",
        REPO_ROOT / "AGENTS.md",
        REPO_ROOT / "CONTRIBUTING.md",
        REPO_ROOT / "CHANGELOG.md",
        REPO_ROOT / "dbt/README.md",
        REPO_ROOT / "tests/README.md",
        *DOCS_DIR.rglob("*.md"),
    ]
    legacy_paths = {f"docs{source}.md" for source in REDIRECTS}

    for path in policy_docs:
        text = path.read_text()
        for legacy_path in legacy_paths:
            assert legacy_path not in text, (
                f"{path.relative_to(REPO_ROOT)}: {legacy_path}"
            )


def test_shipped_scopes_and_public_marts_remain_documented():
    combined = "\n".join(path.read_text() for path in DOCS_DIR.rglob("*.md"))
    required = [
        "polymarket:wc2026",
        "polymarket:us_midterms_2026",
        "kalshi:wc2026",
        "scripts/run_scope.py",
        "polymarket_wc2026_marts.polymarket_wc2026_knockout_markets",
        "polymarket_wc2026_marts.polymarket_wc2026_knockout_token_hourly_odds",
        "polymarket_wc2026_marts.polymarket_wc2026_graph_token_hourly_odds",
        "international_results_wc2026_marts.international_results_wc2026_matches",
        "wc2026_marts.wc2026_knockout_match_hourly_odds",
        "kalshi_wc2026_marts.kalshi_wc2026_stage_markets",
        "kalshi_wc2026_marts.kalshi_wc2026_group_winner_market_hourly_odds",
        "polymarket_us_midterms_2026_marts.polymarket_us_midterms_2026_market_token_hourly_odds",
        "is_actionable_live_market",
        "current_price_status",
        "price_represents",
    ]

    for term in required:
        assert term in combined


def test_brand_assets_and_compact_styles_exist():
    assets = [
        "assets/images/oddsfox-white.png",
        "assets/images/oddsfox-favicon.png",
        "assets/fonts/inter-latin-variable.woff2",
        "assets/fonts/jetbrains-mono-latin-variable.woff2",
        "assets/stylesheets/extra.css",
        "assets/javascripts/mermaid.js",
    ]

    for target in assets:
        asset = DOCS_DIR / target
        assert asset.is_file(), target
        assert asset.stat().st_size > 0, target

    assert not (DOCS_DIR / "assets/stylesheets/oddsfox-dark.css").exists()
    css = (DOCS_DIR / "assets/stylesheets/extra.css").read_text()
    assert css.count("@font-face") == 2
    assert ".of-hero" in css
    assert ".of-task-grid" in css
    assert ".md-search" not in css
    assert ".md-footer" not in css
    assert "::-webkit-scrollbar" not in css
    assert '[data-md-color-scheme="default"]' not in css


def test_built_homepage_and_diagrams_are_semantic():
    homepage = REPO_ROOT / "site/index.html"
    architecture = REPO_ROOT / "site/concepts/architecture/index.html"
    if not homepage.exists() or not architecture.exists():
        pytest.skip("Run make docs-build before checking generated HTML.")

    home_html = homepage.read_text()
    architecture_html = architecture.read_text()

    assert re.search(r'<h1[^>]+id="oddsfox-pipeline"[^>]*>OddsFox Pipeline', home_html)
    assert 'class="md-source"' in home_html
    assert 'href="https://github.com/hypertrial/oddsfox-pipeline"' in home_html
    assert "hypertrial/oddsfox-pipeline" in home_html
    assert 'href="getting-started/"' in home_html
    assert 'href="guides/query-the-warehouse/"' in home_html
    assert "[Choose a scope]" not in home_html
    assert "assets/stylesheets/extra.css" in home_html
    assert "mermaid@11.15.0/dist/mermaid.min.js" in home_html
    assert architecture_html.count('class="mermaid"') == 2
    assert 'class="language-mermaid"' not in architecture_html


def test_docs_make_targets_suppress_material_warning():
    makefile = (REPO_ROOT / "Makefile").read_text()
    assert "NO_MKDOCS_2_WARNING=true" in makefile


def test_github_templates_exist():
    for target in [
        ".github/ISSUE_TEMPLATE/bug_report.yml",
        ".github/ISSUE_TEMPLATE/feature_request.yml",
        ".github/ISSUE_TEMPLATE/documentation.yml",
        ".github/PULL_REQUEST_TEMPLATE.md",
    ]:
        assert (REPO_ROOT / target).is_file(), target

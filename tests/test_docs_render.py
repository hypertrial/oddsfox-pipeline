import functools
import http.server
import threading
from pathlib import Path

import pytest
from playwright.sync_api import sync_playwright

REPO_ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = REPO_ROOT / "site"
pytestmark = pytest.mark.repo_check


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass


@pytest.fixture(scope="module")
def docs_url():
    handler = functools.partial(_QuietHandler, directory=SITE_DIR)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture(scope="module")
def chromium():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            yield browser
        finally:
            browser.close()


def _has_horizontal_overflow(page):
    return page.evaluate(
        "document.documentElement.scrollWidth > document.documentElement.clientWidth"
    )


def _new_page(chromium, viewport):
    page = chromium.new_page(viewport=viewport)
    page.route(
        "https://api.github.com/**",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body='{"stargazers_count":0,"forks_count":0,"tag_name":"v0.0.0"}',
        ),
    )
    return page


def test_homepage_desktop_geometry_and_actions(chromium, docs_url):
    page = _new_page(chromium, {"width": 1440, "height": 900})
    errors = []
    page.on(
        "console",
        lambda message: (
            errors.append(f"{message.text} @ {message.location['url']}")
            if message.type == "error"
            else None
        ),
    )

    page.goto(docs_url, wait_until="networkidle")

    assert not _has_horizontal_overflow(page)
    assert page.locator("h1", has_text="OddsFox Pipeline").is_visible()
    assert page.locator(
        ".md-header .md-source[href='https://github.com/hypertrial/oddsfox-pipeline']"
    ).is_visible()
    assert page.locator(".of-hero .md-button[href='getting-started/']").is_visible()
    assert page.locator(
        ".of-hero .md-button[href='guides/query-the-warehouse/']"
    ).is_visible()
    assert page.locator("body").get_attribute("data-md-color-scheme") == "slate"
    assert (
        page.locator("body").evaluate(
            "element => getComputedStyle(element).backgroundColor"
        )
        == "rgb(15, 23, 42)"
    )
    assert (
        page.locator("h1", has_text="OddsFox Pipeline").evaluate(
            "element => getComputedStyle(element).color"
        )
        == "rgb(248, 250, 252)"
    )
    assert (
        page.locator(
            ".of-hero .md-button[href='guides/query-the-warehouse/']"
        ).evaluate("element => getComputedStyle(element).color")
        == "rgb(34, 211, 238)"
    )
    assert (
        page.locator(".of-hero__copy > p:nth-of-type(2)").evaluate(
            "element => getComputedStyle(element).color"
        )
        == "rgb(203, 213, 225)"
    )
    assert (
        page.locator(".of-task-card").first.evaluate(
            "element => getComputedStyle(element).backgroundColor"
        )
        == "rgb(23, 35, 56)"
    )
    assert not page.locator(".md-sidebar--primary").is_visible()
    assert not page.locator(".md-sidebar--secondary").is_visible()
    assert page.locator(".md-content").bounding_box()["width"] >= 1000
    assert page.locator(".of-hero").bounding_box()["height"] <= 430
    assert page.locator(".of-task-grid").bounding_box()["y"] < 900
    assert page.locator("[data-md-component='palette']").count() == 0
    assert not errors

    page.close()


def test_homepage_mobile_geometry(chromium, docs_url):
    page = _new_page(chromium, {"width": 390, "height": 844})
    page.goto(docs_url, wait_until="networkidle")

    assert not _has_horizontal_overflow(page)
    assert page.locator("h1", has_text="OddsFox Pipeline").is_visible()
    assert page.locator(".of-hero .md-button[href='getting-started/']").is_visible()
    assert page.locator(".of-hero").bounding_box()["height"] <= 640
    columns = page.locator(".of-task-grid").evaluate(
        "element => getComputedStyle(element).gridTemplateColumns"
    )
    assert " " not in columns

    page.close()


@pytest.mark.parametrize(
    "path",
    [
        "/getting-started/",
        "/reference/orchestration/",
        "/reference/data-dictionary/",
        "/concepts/architecture/",
    ],
)
def test_representative_pages_do_not_overflow(chromium, docs_url, path):
    page = _new_page(chromium, {"width": 390, "height": 844})
    page.goto(f"{docs_url}{path}", wait_until="networkidle")
    assert not _has_horizontal_overflow(page), path
    assert page.locator("h1").is_visible(), path
    page.close()


def test_architecture_diagrams_render(chromium, docs_url):
    page = _new_page(chromium, {"width": 1440, "height": 900})
    errors = []
    page.on(
        "console",
        lambda message: (
            errors.append(f"{message.text} @ {message.location['url']}")
            if message.type == "error"
            else None
        ),
    )
    page.goto(f"{docs_url}/concepts/architecture/", wait_until="networkidle")

    page.locator(".mermaid svg").first.wait_for(state="visible", timeout=10_000)
    assert page.locator(".mermaid svg").count() == 2
    assert not errors

    page.close()

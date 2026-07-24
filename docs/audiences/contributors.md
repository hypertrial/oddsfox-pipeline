# Contributors

Use this hub when changing code, dbt models, docs, or orchestration. For
operator setup, start with [Quickstart](../getting-started/index.md).

## Setup

```bash
uv sync --extra dev
cp .env.example .env
```

Keep schedules disabled unless you intentionally test live ingestion. Docs
contributors should install Chromium into the Makefile runtime browser cache
once:

```bash
uv run make runtime-dirs
PLAYWRIGHT_BROWSERS_PATH="$PWD/.cache/runtime/ms-playwright" \
  uv run playwright install chromium
```

## Which Quality Gate?

| Change | Gate |
| --- | --- |
| Docs, styles, or `mkdocs.yml` only | `uv run make docs-check` |
| Ordinary code or test PR | `uv run make ci-fast` |
| Dependency, Docker, Dagster, dbt, or data-quality changes; pre-release | `uv run make release-gate` |
| Live network acceptance (local only) | `live-smoke`, `match-minute-live-smoke`, or `polygon-settlement-live-smoke` — never add these to GitHub Actions |

The full command tables and layout guardrails live in
[AGENTS.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/AGENTS.md).
Do not duplicate them elsewhere.

## Contribution Checklists

See [Development](../development/index.md) for:

- Which quality gate to run
- Add a market adapter
- Add a public mart
- Add a fixed scope
- Targeted Make commands

Also read [CONTRIBUTING.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/CONTRIBUTING.md),
[tests/README.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/tests/README.md),
and [dbt/README.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/dbt/README.md).

## Design Decisions

v0.1.x intentionally has no warehouse migrations, no runtime scope selector, and
an isolated Polygon path. Read [Design decisions](../concepts/decisions.md)
before proposing compatibility shims.

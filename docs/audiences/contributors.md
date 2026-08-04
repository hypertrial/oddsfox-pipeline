# Contributors

Use this hub when changing code, dbt models, docs, or orchestration. For
operator setup, start with [Quickstart](../getting-started/index.md).

Setup, quality gates, and targeted Make commands live in
[Development](../development/index.md) and [CONTRIBUTING.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/CONTRIBUTING.md).

## Contribution Checklists

See [Development](../development/index.md) for:

- Which quality gate to run
- Add a market adapter
- Add a documented mart
- Add a fixed scope

## Data And IP Hygiene

- Do not contribute production data, scraped dumps, populated seeds,
  attestations, or non-synthetic warehouse rows.
- Keep tracked seed shells header-only; use synthetic fixtures under
  `tests/fixtures/`.
- AI-assisted PRs still require you have rights to submit the material.
- Complete the provenance checklist in the pull-request template.
- Read [Operator responsibilities](../concepts/operator-responsibilities.md)
  and [CONTRIBUTING.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/CONTRIBUTING.md).

Also read [tests/README.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/tests/README.md)
and [dbt/README.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/dbt/README.md).

## Design Decisions

v0.1.x intentionally has no warehouse migrations, no runtime scope selector, and
an isolated Polygon path. Read [Design decisions](../concepts/decisions.md)
before proposing compatibility shims. Use [Terminology](../reference/terminology.md)
for product vocabulary (pipeline, working set, marts vs strategy
contract `wc2026.v1`).

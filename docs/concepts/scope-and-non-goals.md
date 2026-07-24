# Scope And Non-Goals

OddsFox Pipeline is MIT-licensed, local-first prediction-market pipeline
software. This page is the short human summary. The authoritative licence and
third-party boundary is
[THIRD_PARTY_NOTICES.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/THIRD_PARTY_NOTICES.md).
For the operator checklist, see
[Operator responsibilities](operator-responsibilities.md).

## What This Repository Ships

- Source code, dbt models, Dagster jobs, operator scripts, documentation, and
  signed software container images.
- Three fixed `run_scope.py` market scopes in `v0.1.x`:
  `polymarket:wc2026`, `polymarket:us_midterms_2026`, and `kalshi:wc2026`.
- Supporting FIFA fixture/results ingestion used to validate real-team scope on
  WC2026 market graphs.
- An optional, isolated Polygon settlement-history flow with its own
  unscheduled job and dbt tag (not a `run_scope.py` scope).

## What It Does Not Ship Or Operate

- No bundled production datasets or operator data in the canonical repository
  or newly published images.
- No hosted continuous live ingestion, hosted production pipeline, or hosted
  data service operated by Hypertrial.
- No trade execution, strategy, or order admission runtime (those live in other
  repositories; see [System overview](system-overview.md)).
- No investment, betting, or trading advice.
- No prediction-market venue, brokerage, oracle, custody, or KYC/AML service.

## Operator Ownership

Every operator supplies source inputs, runs ingestion against source APIs or
operator-local files, and stores results in their own DuckDB file or
self-managed warehouse. Operators remain responsible for their inputs and
outputs.

Tracked seed paths that look like data files may be **header-only schema
shells**. Complete manifests, attestations, and exports stay operator-local and
untracked. Restore shells with `git restore dbt/seeds` after local overlays; see
[dbt/seeds/README.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/dbt/seeds/README.md).

## De-Identification Is Not Anonymity

The optional Polygon technical export omits wallets and many chain locators.
Sparse public blockchain aggregates can still be reverse-linked. Treat exports
as de-identified operator artifacts, not anonymous public datasets.

The internal audit bundle retains verification locators and must stay
operator-local. The allowlisted technical export is a narrower dossier and still
is not an anonymous public dataset. See
[Operator responsibilities](operator-responsibilities.md).

## Related Pages

- [Operator responsibilities](operator-responsibilities.md)
- [FAQ](faq.md)
- [Design decisions](decisions.md)
- [Integration](integration.md)

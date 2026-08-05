# Scope And Non-Goals

OddsFox Pipeline is MIT-licensed, local-first prediction-market pipeline
software. This page is the short human summary. The authoritative licence and
third-party boundary is
[THIRD_PARTY_NOTICES.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/THIRD_PARTY_NOTICES.md).
For the operator checklist, see
[Operator responsibilities](operator-responsibilities.md).

## What This Repository Ships

- Source code, dbt models, jobs, operator scripts, and documentation
  for local macOS-first operation.
- Two fixed `run_scope.py` market scopes in `v0.2.x`:
  `polymarket:wc2026` and `kalshi:wc2026`.
- FIFA fixture/results and OpenFootball schedule ingestion for Kalshi WC2026 and
  match-minute real-team validation; not required for the Polymarket golden-mart
  path.
- An optional, isolated Polygon settlement-history pipeline with its own
  unscheduled job and dbt tag (not a `run_scope.py` scope).

## What It Does Not Ship Or Operate

- No bundled production datasets or operator data in the canonical repository.
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

Polygon exports are de-identified, not anonymous. See
[Operator responsibilities](operator-responsibilities.md#privacy-and-re-identification).

## Related Pages

- [Operator responsibilities](operator-responsibilities.md)
- [FAQ](faq.md)
- [Design decisions](decisions.md)
- [Integration](integration.md)

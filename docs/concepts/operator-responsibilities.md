# Operator Responsibilities

This page is the operational checklist for legal and distribution hygiene.
The authoritative licence and third-party boundary remains
[THIRD_PARTY_NOTICES.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/THIRD_PARTY_NOTICES.md).
This page does not grant rights, certify compliance, or interpret third-party
terms.

## Authority

MIT covers Hypertrial-authored code and docs only — not operator or third-party
data, and not OddsFox marks beyond unmodified documentation display. See
[Scope and non-goals](scope-and-non-goals.md).

## Operator Checklist

- Confirm you are authorized to access and use each source you configure
  (Polymarket, Kalshi, Polygon JSON-RPC providers, and any supplied immutable
  artifacts). Non-market source acquisition is configured in OddsFox Scraper.
- Keep populated seed overlays, reviewed attestations, DuckDB files, parquet or
  CSV exports, and authoring evidence operator-local and untracked.
- Restore header-only shells after local overlays:
  `git restore dbt/seeds`
- Never commit `.env`, CLOB credentials, RPC URLs or tokens, wallets, or
  attestation contents.
- Treat redistribution of warehouses, exports, and derived odds as your
  responsibility under third-party terms that apply to you.

See [dbt/seeds/README.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/dbt/seeds/README.md)
and [Scope and non-goals](scope-and-non-goals.md).

## Not Advice And Not A Venue

Not investment, betting, or trading advice; not a venue, broker, oracle, or
custodian. Order execution is separate — see
[Scope and non-goals](scope-and-non-goals.md#what-it-does-not-ship-or-operate)
and [System overview](system-overview.md).

## Export And Redistribution Matrix

| Artifact | Ships in git? | Redistribution |
| --- | --- | --- |
| MIT code and docs | Yes | Per [LICENSE](https://github.com/hypertrial/oddsfox-pipeline/blob/main/LICENSE) |
| Header-only seed shells | Yes | Yes (empty schema shells only) |
| Populated seeds, attestations, source documents | No | Only if the operator has independent rights |
| Local DuckDB / parquet / CSV exports | No | Operator's responsibility |
| Polygon internal audit bundle | No | Operator-local; retain carefully |
| Polygon allowlisted technical export | No | Operator's responsibility; de-identified, not anonymous |
| OddsFox name and visual marks | Limited docs display | Not licensed under MIT for reuse |

## Privacy And Re-Identification

The optional Polygon technical export omits wallets and many chain locators.
That is de-identification, not anonymity: sparse public blockchain aggregates
can still be reverse-linked.

- The internal audit bundle retains verification locators needed for audit; keep
  it operator-local.
- The allowlisted technical export is a narrower operator-controlled dossier; it
  still is not an anonymous public dataset.
- Do not commit or paste into public issues: wallet addresses, RPC URLs or
  tokens, order hashes, raw topics/data/calldata, or attestation contents.

See [SECURITY.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/SECURITY.md).

## Third-Party Terms (Non-Authoritative)

The following links are for operator review only. This project provides no
publication clearance, terms snapshot, or conclusion about third-party terms,
and it does not grant rights in operator or third-party data:

- [Polymarket](https://polymarket.com/) site and developer materials as published
  by Polymarket
- [Kalshi](https://kalshi.com/) site and API materials as published by Kalshi
- [Polygon PoS RPC documentation](https://docs.polygon.technology/pos/reference/rpc-endpoints)
  and your provider's acceptable-use terms
- OddsFox Scraper reference-bundle provenance and source licenses, recorded in
  the supplied `oddsfox.reference.v1` manifest

## Technical Success Is Not Certification

Local `dbt` builds, CI gates, smoke targets, and exact row-count checks verify
technical shape against project contracts. They are not Hypertrial warranties of
completeness, accuracy, third-party authorization, or fitness for trading.

## Related Pages

- [Scope and non-goals](scope-and-non-goals.md)
- [Operators](../audiences/operators.md)
- [FAQ](faq.md)
- [THIRD_PARTY_NOTICES.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/THIRD_PARTY_NOTICES.md)

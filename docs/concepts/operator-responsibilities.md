# Operator Responsibilities

This page is the operational checklist for legal and distribution hygiene.
The authoritative licence and third-party boundary remains
[THIRD_PARTY_NOTICES.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/THIRD_PARTY_NOTICES.md).
This page does not grant rights, certify compliance, or interpret third-party
terms.

## Authority

OddsFox Pipeline is MIT-licensed software and documentation. The MIT grant
covers Hypertrial-authored code and docs. It does **not** grant rights in data
an operator obtains, generates, or supplies, and it does not licence OddsFox
marks beyond unmodified documentation display.

Public APIs, public blockchain state, and technical interoperability do not
themselves authorize access, retention, or redistribution of third-party data
or derived outputs.

## Operator Checklist

- Confirm you are authorized to access and use each source you configure
  (Polymarket, Kalshi, FIFA schedule materials, OpenFootball, Polygon JSON-RPC
  providers, and any private snapshots).
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

- Documentation, SQL examples, and marts are analytics tooling for operators who
  already have rights to their data. They are not investment, betting, or
  trading advice.
- OddsFox Pipeline is not a prediction market, exchange, broker, oracle,
  custodian, or KYC/AML service. It does not hold funds, match orders, or settle
  markets.
- Order execution belongs to separate systems such as `oddsfox-execution`; see
  [System overview](system-overview.md).

## Export And Redistribution Matrix

| Artifact | Ships in git / published image? | Redistribution |
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
- [OpenFootball worldcup](https://github.com/openfootball/worldcup) (CC0 at the
  pinned revision cited in project notices)
- FIFA World Cup schedule materials used for authoring: the PDF is not
  redistributed by this repository

## Technical Success Is Not Certification

Local `dbt` builds, CI gates, smoke targets, and exact row-count checks verify
technical shape against project contracts. They are not Hypertrial warranties of
completeness, accuracy, third-party authorization, or fitness for trading.

## Related Pages

- [Scope and non-goals](scope-and-non-goals.md)
- [Operators](../audiences/operators.md)
- [FAQ](faq.md)
- [THIRD_PARTY_NOTICES.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/THIRD_PARTY_NOTICES.md)

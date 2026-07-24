# Third-Party Notices and Distribution Scope

## Copyright ownership and MIT scope

Hypertrial owns and licenses the existing first-party software and associated
documentation authored for OddsFox Pipeline under the [MIT License](LICENSE).
Accepted external contributors retain copyright in their contributions and
license those contributions under MIT.

Canonical source archives, Python packages, documentation, and newly published
container images contain no bundled production datasets or operator data.

Tracked CSV seed shells contain column headers only. Small contract constants,
aliases, and market-scope identifiers are executable project configuration.
Fixtures under `tests/fixtures/` are synthetic, Hypertrial-authored test inputs
unless a file-specific notice says otherwise.

Operator data and third-party code, services, fonts, documents, dependencies,
and marks are outside Hypertrial's MIT grant.

## Hypertrial operations

Hypertrial develops, tests, researches, and validates OddsFox Pipeline locally.
Hypertrial does not operate continuous live ingestion, a hosted production
pipeline, or a hosted data service.

This statement describes Hypertrial's operations. It does not modify or
restrict any permission in the MIT License and is not a warranty, safety
certification, or conclusion about a recipient's use.

## Operator and source responsibilities

The MIT License does not grant rights in data that an operator obtains,
generates, or supplies. Software availability, public endpoints, public
blockchain state, and technical interoperability do not themselves grant
permission to access a provider or redistribute its data or derived outputs.
Operators are responsible for third-party authorization and for the
acquisition, licensing, retention, handling, use, export, and distribution of
their own data.

This repository provides no dataset licence, publication clearance, terms
snapshot, or repository-authored conclusion about third-party terms.

## Independent protocol interfaces

The Polygon settlement decoder for `OrderFilled` and `OrdersMatched` is an
independently written implementation of publicly observable event topics and
ABI/interface facts. No source code from
[Polymarket CTF Exchange V2](https://github.com/Polymarket/ctf-exchange-v2/tree/ccc0596074f4dfd62c944fbca4de252893b82b4b)
is included, copied, or adapted. The pinned upstream repository is cited for
transparent interface provenance and is licensed under
[BUSL-1.1](https://github.com/Polymarket/ctf-exchange-v2/blob/ccc0596074f4dfd62c944fbca4de252893b82b4b/LICENSE.md).
The project's MIT status for its independently authored implementation does not
rely on Hypertrial's operational statement above.

## Third-party material

Documentation fonts under `docs/assets/fonts/` are distributed under their
included SIL Open Font License notices.

The project references these independently governed upstream materials:

- [OpenFootball World Cup](https://github.com/openfootball/worldcup/tree/bd46a148289f9930da66c140d4d7d2325e95d387)
  and [international_results](https://github.com/martj42/international_results)
  data sources under CC0-1.0;
- [Gnosis Conditional Tokens](https://github.com/gnosis/conditional-tokens-contracts/tree/eeefca66eb46c800a9aaab88db2064a99026fde5)
  interfaces under LGPL-3.0;
- the [UMA CTF Adapter](https://github.com/Polymarket/uma-ctf-adapter/tree/8b76cc9e0d46c6f7450a0adb0ddc0f5b0568c9cc)
  and [Neg Risk CTF Adapter](https://github.com/Polymarket/neg-risk-ctf-adapter/tree/f78b35b0863b4308a431ca307d06f49b2ea65e78)
  interface sources under their upstream MIT notices; and
- [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md), adapted from
  [Contributor Covenant 2.1](https://github.com/EthicalSource/contributor_covenant/tree/2.1)
  under
  [CC BY 4.0](https://github.com/EthicalSource/contributor_covenant/blob/2.1/LICENSE.md).
  The file identifies the adaptation and retains the required attribution.

Runtime and development dependencies and the container base image remain
governed by their own licences. Installed distributions retain their licence
metadata, and published container images include an SBOM. The OCI `MIT` licence
label describes the Hypertrial-owned application, not every component in the
image.

No third-party material is relicensed under the project's MIT License.

## Names and visual marks

The OddsFox name, logo, favicon, and other visual marks are not licensed under
the MIT License, except that unmodified copies may be displayed as part of the
project documentation.

FIFA, FIFA World Cup, Polymarket, Polygon, Kalshi, OpenFootball, and related
names and marks belong to their respective owners. They are used only to
identify sources, protocols, and interoperability targets. OddsFox Pipeline is
independent and is not affiliated with, sponsored by, or endorsed by those
owners.

## Disclaimer

The project makes no representation that operator-supplied data may be used or
redistributed. The warranty and liability disclaimer in [`LICENSE`](LICENSE)
applies to the MIT-licensed material. Operators remain responsible for their
third-party authorization, data handling, retention, use, and distribution.

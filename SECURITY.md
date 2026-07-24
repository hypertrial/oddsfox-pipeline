# Security Policy

## Supported versions

| Version | Supported |
| ------- | --------- |
| 0.1.x   | Yes       |

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Report security issues through **GitHub Private Vulnerability Reporting**:
open the repository on GitHub, go to **Security** → **Report a vulnerability**,
and submit a private report.

Include:

- A description of the issue and potential impact
- Steps to reproduce (proof of concept if available)
- Affected versions or commits
- Suggested fix or mitigation, if you have one

We will acknowledge receipt and work with you on a timeline for investigation and disclosure.

## Scope notes

OddsFox Pipeline is a **local-first** prediction-market data pipeline. Local
operation does not remove operator responsibility for secrets and sensitive
artifacts.

Secret classes that must never be committed or pasted into public issues:

- Polymarket CLOB credentials and related `.env` values
- Polygon JSON-RPC URLs, API tokens, and provider credentials
- Wallet addresses, order hashes, signatures, and raw topics/data/calldata
- Reviewed attestation contents and complete operator seed rows

Optional Polygon internal audit bundles and allowlisted technical exports are
operator-local. Treat them as sensitive even when fields are redacted;
de-identification is not anonymity. See
[Operator responsibilities](docs/concepts/operator-responsibilities.md) and
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

The current implementation may read public Polymarket/Gamma and CLOB APIs,
Kalshi public trade APIs, FIFA/results feeds, and optional finalized Polygon
logs, then store results in an operator-controlled DuckDB warehouse.

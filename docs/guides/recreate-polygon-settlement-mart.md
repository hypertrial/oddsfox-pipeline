# Recreate the Polygon settlement mart

Build
`polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds` from a
clean clone or preserved raw warehouse. Complete
[shared setup](recreate-local-marts.md#shared-setup-every-route) first.

## Configure the Polygon RPC

Export the endpoint and a non-secret provider/plan label:

```bash
export POLYGON_RPC_URL="https://your-authorized-archive-rpc"
export POLYGON_RPC_PROVIDER_LABEL="provider-plan"
```

The endpoint value may contain credentials. Do not paste it into a tracked
file, terminal transcript, issue, or log. The software records only the label
and sanitized HTTPS origin.

The RPC must support:

- Polygon PoS mainnet, chain ID 137;
- `eth_getBlockByNumber` with the `finalized` block tag;
- historical `eth_getLogs` over the fixed authoring ranges; and
- historical `eth_call` at the referenced event blocks.

A free public endpoint may not provide the required archive history.

## Generate and review the Polygon inputs

Choose an unused plain SemVer and record the UTC review minute:

```bash
export POLYGON_SEED_MANIFEST_VERSION="1.0.0"
export POLYGON_SEED_REVIEWED_AT="$(date -u +%Y-%m-%dT%H:%M:00Z)"
export REFERENCE_BUNDLE_DIR="/absolute/path/to/scraper-reference-bundle"
```

Generate the candidate:

```bash
uv run make polygon-settlement-seed-candidate
```

The command refuses to overwrite an existing candidate directory. If the
version already exists, choose another unused SemVer; do not amend an existing
candidate.

The tool automatically:

1. validates the immutable Scraper reference bundle and consumes its fixture
   table;
2. records the bundle, table, row, and checksum provenance for each fixture;
3. discovers the relevant Polygon question and condition events;
4. derives and verifies Yes/No token orientation;
5. verifies the standard and neg-risk contract relationships;
6. verifies finalized resolution evidence; and
7. writes a candidate manifest, attestation, and evidence report below:

```text
artifacts/polygon_settlement_seed_candidates/<version>/
```

Set a convenient path to that directory:

```bash
export POLYGON_CANDIDATE_DIR="$PWD/artifacts/polygon_settlement_seed_candidates/$POLYGON_SEED_MANIFEST_VERSION"
```

Before installing anything, confirm:

```bash
test ! -f "$POLYGON_CANDIDATE_DIR/FAILED"
test -f "$POLYGON_CANDIDATE_DIR/EVIDENCE.json"
test -f "$POLYGON_CANDIDATE_DIR/resolution_attestation.yml"
test "$(wc -l < "$POLYGON_CANDIDATE_DIR/polymarket_wc2026_polygon_settlement_markets.csv" | tr -d ' ')" = "249"
```

Review `EVIDENCE.json` and the 248 candidate rows. Approve them only when the
fixture inventory, proposition semantics, source revisions and hashes,
question/condition locators, token orientation, duplicate overrides, and
resolution evidence match the intended WC2026 scope.

## Install and validate the reviewed Polygon inputs

After review approval, copy the matching pair:

```bash
cp \
  "$POLYGON_CANDIDATE_DIR/polymarket_wc2026_polygon_settlement_markets.csv" \
  dbt/seeds/polymarket_wc2026_polygon_settlement_markets.csv

cp \
  "$POLYGON_CANDIDATE_DIR/resolution_attestation.yml" \
  config/polygon-settlement-resolution-attestation.yml
```

Validate the installed pair:

```bash
uv run make polygon-settlement-seed-validate
```

Do not continue unless it reports:

- 248 propositions;
- 248 resolved conditions;
- the selected manifest version; and
- a manifest SHA-256 matching the reviewed evidence.

## Create the Polygon settlement mart

For a new disposable warehouse, run:

```bash
POLYGON_SETTLEMENT_LIVE_SMOKE_RESET=true \
  uv run make polygon-settlement-live-smoke
```

The job validates the installed manifest and attestation, scans finalized
Polygon V2 settlement logs, publishes the raw snapshot, builds the isolated dbt
graph, and fails closed on incomplete coverage or invalid output.

On success, the mart is in:

```text
.cache/polygon_settlement/benchmarks/v4/live_smoke.duckdb
```

The relation is:

```text
polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds
```

The Make target asserts the 39,120-row contract and exits nonzero on failure.

For a shorter path when raw warehouses already exist, use the
[completed-warehouse route](recreate-local-marts.md#alternative-rebuild-completed-raw-warehouses).

## Troubleshooting

| Failure | What to check |
| --- | --- |
| Candidate directory already exists | Use a new unused plain SemVer. Candidate evidence is immutable and is never overwritten. |
| RPC rejects `finalized` | Use a Polygon mainnet provider that implements the finalized block tag. |
| Historical `eth_getLogs` or `eth_call` fails | Use an archive-capable endpoint and confirm the provider permits the required historical ranges and request volume. |
| `FAILED` appears in the candidate directory | Do not install any candidate output. Correct the reported source/RPC/evidence failure and generate a new version. |
| Polygon seed validation reports a hash mismatch | The manifest and attestation came from different candidate runs. Reinstall one reviewed matching pair. |

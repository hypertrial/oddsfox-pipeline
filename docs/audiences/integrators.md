# Integrators

<p class="of-personas" markdown><span class="of-persona of-persona--integrator">Integrator</span></p>

Use this hub when another repository or offline tool consumes OddsFox Pipeline
outputs. Public marts are analytics inputs,
not execution orders. Canonical vocabulary lives in
[Terminology](../reference/terminology.md).

## Checklist

1. **Consume public surfaces only** — public `*_marts` relations. Do not treat
   `*_raw`, `*_ops`, staging, or intermediate schemas as APIs. Start with
   [Data contracts](../reference/data-contracts.md) and the
   [Data dictionary](../reference/data-dictionary.md).
2. **Pin versions** — read `wc2026_marts.contract_metadata` where present and
   track
   [CHANGELOG.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/CHANGELOG.md).
   `v0.2.x` mart layouts may break between releases.
3. **Strategy / raw.v1 consumers** — if you load private canonical snapshots or
   the strategy clean-data set (`wc2026.v1`), fail closed on readiness using
   [Strategy contracts](../reference/strategy-contracts.md). Ordinary public
   mart consumers do not need that page.
4. **Polygon boundary** — operator-local Polygon audit bundles and technical
   exports are not `wc2026.v1` signal inputs and must not feed intents or
   execution.
5. **Execution stays elsewhere** — order admission belongs to
   `oddsfox-execution`. See [Integration](../concepts/integration.md) and
   [System overview](../concepts/system-overview.md) for repository roles.

See [Scope and non-goals](../concepts/scope-and-non-goals.md) for what this
repository ships and what it does not host.

## Consumption contract

1. Read [Data contracts](../reference/data-contracts.md) for grain and
   guarantees.
2. Query only `*_marts` (column semantics live in the
   [Data dictionary](../reference/data-dictionary.md)).
3. Pin pipeline version via `wc2026_marts.contract_metadata` and track
   [CHANGELOG.md](https://github.com/hypertrial/oddsfox-pipeline/blob/main/CHANGELOG.md).
4. Run observability checks before trusting live filters (see
   [Query the warehouse](../guides/query-the-warehouse.md#trust-before-analysis)).
5. Never depend on `*_raw`, audit bundles, or strategy `wc2026.v1` unless you
   are explicitly on that path.

## Version pinning

```sql
select
    contract_name,
    contract_version,
    contract_fingerprint,
    pipeline_git_sha,
    built_at
from wc2026_marts.contract_metadata;
```

`wc2026_marts.contract_metadata` is published with the shared WC2026 strategy
clean-data graph (for example after Kalshi or match-minute ingest that rebuilds
those marts). A Polymarket-only golden-mart quickstart may not populate this
relation. Strategy consumers should fail closed using
[Strategy contracts](../reference/strategy-contracts.md). Ordinary public mart
consumers still pin the pipeline release via CHANGELOG and documented mart
grains.

## Integration anti-patterns

| Anti-pattern | Why it breaks |
| --- | --- |
| Treating `*_observability` as a stable API | Diagnostic surface; columns may change |
| Joining Kalshi/Polymarket on team display strings | Use `canonical_team_name` / documented bridges |
| Assuming `fifa_match_id` equals `match_id` | Different grains; see the [Analysts](analysts.md) join map |
| Feeding Polygon exports into signals or execution | Explicit non-goal; see checklist item 4 |
| Expecting semver-stable mart layouts in `v0.2.x` | Pin + read CHANGELOG; no migration shims |

## See also

- [Data contracts](../reference/data-contracts.md)
- [Strategy contracts](../reference/strategy-contracts.md)
- [Integration](../concepts/integration.md)
- [System overview](../concepts/system-overview.md)
- [Terminology](../reference/terminology.md)

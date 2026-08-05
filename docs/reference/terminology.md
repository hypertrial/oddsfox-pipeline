# Terminology

Normative vocabulary for OddsFox Pipeline. Exactly **34** global terms.
Identifier construction lives in [Naming](naming.md). Product contract IDs,
atlas semantics, and analyst shortcuts live in the local docs linked below.

Breaking identifier renames and prose cleanup follow these definitions. There
are no compatibility aliases in `v0.2.x`; operators with older warehouses
delete `oddsfox.duckdb*` and rebuild.

Machine-readable inventory and retirement rules live in the repository at
`config/terminology_policy.toml` (enforced by `make check-terminology`).

## Core vocabulary

| Bucket | Terms |
| --- | --- |
| Execution | **pipeline**, **job**, **run**, **schedule**, **backfill** |
| Identity | **source**, **scope**, **catalog**, **registry**, **working set** |
| Observation | **grain**, **cadence**, **snapshot**, **history**, **latest** |
| Storage | **relation**, **mart**, **artifact**, **bundle**, **release** |
| Actions | **ingest**, **refresh**, **enrich**, **build**, **export**, **activate** |
| Domain | **event**, **market**, **outcome**, **token**, **proposition**, **fixture**, **match** |
| Guarantee | **contract** |

## Definitions

### Execution

| Term | Meaning | Example |
| --- | --- | --- |
| **Pipeline** | Coherent source-to-output data path. | Polymarket WC2026 match-minute odds pipeline |
| **Job** | Registered executable that runs part or all of a pipeline. | `polymarket_wc2026_match_minute_odds_backfill` |
| **Run** | One execution of a job. | A run of the hourly odds job |
| **Schedule** | Automatic trigger for a job. | Hourly odds schedule (stopped by default) |
| **Backfill** | Historical or gap-filling execution mode. | Match-minute odds backfill |

Do not use **flow** for a product path. Qualify third-party objects: **dlt
pipeline**, **OddsFox Pipeline**. Jobs named `*_full_pipeline` remain valid.

Entry-point jobs (`*_full_pipeline`, or the sole job for single-job isolated
pipelines such as Polygon settlement) are pipelines. Narrower jobs such as
`*_dbt_build`, `*_market_scope_registry_refresh`, and `*_hourly_odds_ingest` run
one step of a pipeline, not a separate pipeline. See the
[Pipeline registry](orchestration.md#pipeline-registry) for the full inventory
and maturity tiers.

### Identity

| Term | Meaning | Example |
| --- | --- | --- |
| **Source** | Upstream provider adapter. | `polymarket`, `kalshi` |
| **Scope** | Fixed shipped product slice within a source. | `wc2026` |
| **Catalog** | Source-discovered inventory. Always qualify: market catalog, event catalog. | Gamma event catalog |
| **Registry** | Admitted persisted set the pipeline operates on. Always qualify: market scope registry. | `market_scope_registry` |
| **Working set** | Derived bounded set built from a registry or catalog. | Match-minute admitted markets |

Chooser strings (`polymarket:wc2026`) and flat prefixes (`polymarket_wc2026`)
are encodings documented in [Naming](naming.md), not separate ontology terms.
Never say “the catalog” or “the registry” without the grain qualifier.

### Observation

| Term | Meaning |
| --- | --- |
| **Grain** | What one row uniquely represents. Prefer “minute-grain” or “hourly-grain.” |
| **Cadence** | How often a job is intended to run. |
| **Snapshot** | Point-in-time observation. Always qualify. |
| **History** | Retained observations over time. |
| **Latest** | Derived mechanical head of that history. |

Prefer **minute-grain** or **match-minute**. **Latest** is a mechanical head;
lifecycle-valid
“current” (for example an activated release symlink or live-market flag) is a
separate product concept, not a synonym.

Upstream observation buckets such as CLOB `fidelity=60` are configuration
details in [Configuration](configuration.md).

### Storage and outputs

| Term | Meaning |
| --- | --- |
| **Relation** | Any DuckDB/dbt table or view. |
| **Mart** | Curated consumer-facing relation in a `*_marts` schema. |
| **Artifact** | Filesystem output outside the warehouse. |
| **Bundle** | Manifest-bound collection of artifacts. |
| **Release** | Validated immutable version of a bundle or export set. |

Supported mart query surfaces are documented in
[Data contracts](data-contracts.md). “Public” in that page means supported,
not redistributable.

### Actions

| Verb | Use when |
| --- | --- |
| **Ingest** | Land external data into raw storage through a routine job. |
| **Refresh** | Update a current snapshot or registry from already-discovered inputs. |
| **Enrich** | Fill missing metadata on already-admitted rows. |
| **Build** | Materialize dbt models or other warehouse relations. |
| **Export** | Serialize warehouse data to operator-local files. |
| **Activate** | Repoint “current” to a published release. |

**Enrich** replaces metadata “backfill” when the work is not historical
gap-filling. Keep **backfill** for historical jobs. Framework verbs such as
Dagster “materialize” or local “sync” helpers stay framework-local and are
not global ontology.

Atomic table promotion that lands a scan into a canonical raw relation may
still say **publish** locally; that is not the same as creating an immutable
**release** or **activate** step.

### Domain

| Term | Grain |
| --- | --- |
| **Event** | Gamma event container (`event_id`). |
| **Market** | Venue market (`market_id`). |
| **Outcome** | Source-provided answer label. |
| **Token** | Tradable CLOB side (`clob_token_id`). |
| **Proposition** | Normalized claim parsed from an outcome. |
| **Fixture** | Scheduled contest and schedule identity (`fifa_match_id`). |
| **Match** | Contest in its played, live, or result state. |

Always say **test fixture** for synthetic CI or unit-test input. Preserve
upstream Gamma fields such as `game_id` verbatim. Derived first-party fields
use **match_*** names.

### Guarantee

| Term | Meaning |
| --- | --- |
| **Contract** | Named guarantee about a relation set, bundle, or collector format. |

Concrete contract IDs live in local docs:

- Mart contracts → [Data contracts](data-contracts.md)
- Strategy contract `wc2026.v1` and raw snapshot contract `oddsfox.raw.v1` →
  [Strategy contracts](strategy-contracts.md)

## Local vocabulary (not global terms)

| Topic | Owner |
| --- | --- |
| Scope reference, namespace, pipeline-policy seed names | [Naming](naming.md) |
| API fidelity, threshold windows | [Configuration](configuration.md) |
| Raw layer / raw snapshot collector format | [Strategy contracts](strategy-contracts.md), [Warehouse](warehouse.md) |
| Analyst column shortcuts | [Glossary](../concepts/glossary.md) |

## Frozen exceptions

These names stay as written:

- Product and package: `OddsFox Pipeline`, `oddsfox_pipeline`, `oddsfox-pipeline`
- Library API: `dlt.pipeline`
- Qualified tooling: `dbt graph`, `asset graph`
- Vendor fields: Gamma `game_id`, `game_start_time`, and similar upstream columns
- Test infrastructure: “test fixture”, pytest fixtures
- Ops telemetry table: `sync_run_metrics`
- Released CHANGELOG history (do not rewrite old release notes)
- Historical filesystem roles already documented for Polygon audit bundles

## Deprecated phrases

| Avoid | Prefer |
| --- | --- |
| flow (product path) | pipeline |
| minutely | minute-grain / match-minute |
| graph odds / graph export / graph bundle | retired logical-atlas export surface |
| graph contract (internal) | mart contract |
| universe / market universe / token universe | working set |
| Dagster job (general prose) | job |
| public mart | mart + [Data contracts](data-contracts.md) |
| bare snapshot | qualified snapshot |
| bare catalog / registry | market catalog / event catalog / market scope registry |
| metadata backfill (non-historical) | metadata enrichment |
| pipeline run events (ops telemetry) | ingestion run events |
| sync run observability | ingestion run observability |
| knockout fixtures (OpenFootball 1–104) | schedule fixtures |
| ScopeStep `market_registry` | ScopeStep `market_scope_registry` |
| `publish_current` (symlink repoint) | `activate_current` |

## See also

- [Glossary](../concepts/glossary.md)
- [Orchestration](orchestration.md)

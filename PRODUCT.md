# Product

## 1. Purpose

OddsFox Pipeline turns operator-configured prediction-market and football data
into trusted local analytical relations and immutable research releases.

- Problem: Source data is fragmented, mutable, and unsafe to analyze or consume
  until identity, schema, completeness, and quality are validated.
- Target user: Local data operators, analysts, and downstream integrators.
- Core value: Reproducible ingestion, canonical DuckDB storage, documented dbt
  marts, and explicit data-quality evidence under operator control.
- Success looks like: An operator can build a warehouse whose supported marts
  and releases are complete, queryable, provenance-bound, and safe for
  downstream research.

---

## 2. Product Principles

Rules that guide product and engineering decisions.

1. Contracts, grain, provenance, and data quality are product behavior, not
   implementation details.
2. Keep data and operation local: operators supply inputs, control credentials,
   and own every warehouse and release.
3. Fail closed on incomplete inputs, ambiguous identity, invalid revisions, or
   broken publication state.

---

## 3. Users

### Primary Users

Data operators who ingest sources and maintain a local warehouse, plus analysts
who query supported marts.

### Secondary Users

Strategy developers consuming versioned contracts, integrators consuming
documented artifacts, and contributors extending supported pipelines.

---

## 4. User Outcomes

- Users can ingest configured sources and validated canonical snapshots into a
  local DuckDB warehouse.
- Users can query stable, documented marts at known grains with associated
  readiness and quality checks.
- Users can export validated, immutable operator-local bundles for offline
  research and integration.

---

## 5. Scope

### In Scope

- Safe-source ingestion, canonical snapshot validation, and local persistence.
- Dagster jobs, dbt transformations, marts, observability, and data quality.
- Versioned data contracts and validated operator-local exports and releases.

### Out of Scope

- Private source authorization or private collector implementation.
- Forecasting, portfolio allocation, order admission, or execution.
- A hosted data service or bundled production datasets.

---

## 6. Core Capabilities

### Ingestion and Canonical Storage

**Purpose:**
Convert configured source observations into validated local history and
qualified snapshots.

**Responsibilities:**
- Discover, ingest, normalize, and persist supported source data.
- Validate schema, identity, completeness, hashes, audits, and retry state.

**Non-responsibilities:**
- Owning private collector authorization.
- Choosing trades or strategy policy.

### Analytical Warehouse

**Purpose:**
Build consumer-facing relations with explicit contracts and grains.

**Responsibilities:**
- Materialize dbt staging, intermediate, mart, and observability relations.
- Publish readiness and quality evidence for supported scopes.

**Non-responsibilities:**
- Writing downstream research logic into warehouse models.
- Guaranteeing source availability outside operator-controlled runs.

### Research Releases

**Purpose:**
Produce immutable, independently verifiable inputs for offline analysis.

**Responsibilities:**
- Validate release inventory, schemas, relationships, hashes, and provenance.
- Publish atomically without committing operator data.
- Preserve source-time and receipt-time semantics for reconstructed execution
  evidence, with diagnostic trades kept separate from fill evidence.

**Non-responsibilities:**
- Distributing production data.
- Claiming that historical prices or books guarantee executable profit.

---

## 7. Product Model

Core domain concepts and their relationships.

```text
Operator
 └── Source Scope
      ├── Market Catalog
      ├── Market Scope Registry
      ├── Observation History
      │    └── Qualified Snapshot
      ├── Warehouse Relation
      │    ├── Mart
      │    └── Observability Result
      └── Immutable Release
           ├── Contract
           └── Provenance Manifest
```

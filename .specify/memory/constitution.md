# ROBO Data Catalog Constitution

<!--
  Principles the robo-data-catalog codebase ALREADY embodies, derived 2026-06-15 by
  reverse-engineering the implementation. Every new spec/plan's "Constitution Check" must comply.
  Amending a principle requires bumping the version below with a rationale.
-->

## Core Principles

### I. Read-Model Over the Analyzer Graph (분석 그래프의 조회·편집 계층)

The catalog does NOT run analysis. It browses, edits, enriches, and serves the Neo4j graph that
**robo-data-analyzer produced** (`Table`/`Column`/`MODULE`/`FUNCTION`/`PROCEDURE` + `READS`/`WRITES`
/`FK_TO_TABLE`/lineage). New features MUST stay on the consume/serve side, not re-derive the graph.

**Rationale**: one writer (the analyzer), many readers (catalog, architect). Keeping the catalog a
read/edit layer prevents two services fighting over graph ownership.

### II. User Edits Isolated From Analysis Output (사용자 편집 격리)

Human edits (`description` with `description_source='user'`) MUST NOT overwrite the analyzer's
machine-written fields (`analyzed_description`, Statement `ai_description`). The two coexist; the UI
chooses which to show.

**Rationale**: a re-analysis must never silently destroy curated human descriptions, and a human
edit must never be mistaken for machine output.

### III. Catalog Is Also a Provider Back to Analysis (양방향 계약)

Two endpoints are contracts the analyzer consumes **back**: `POST /robo/tables/sample-context`
(real sample rows for step5) and `GET /robo/lineage/` (DBMS lineage enrichment). Their request/
response shapes are boundaries — changing them MUST flag robo-data-analyzer as affected.

**Rationale**: the dependency is bidirectional; a field rename here breaks an analysis run there.
⚠️ *Current debt*: the `/robo/lineage/` response (`{nodes, edges, stats}`) does NOT match what the
analyzer's `enrich_lineage_from_catalog_step` expects (`{flows:[{source, table_fqn, relation,
dml_type}]}`), and relation naming drifts (`DATA_FLOW_TO` vs `DATA_FLOWS_TO`). A plan touching
lineage MUST reconcile this contract (see spec 004).

### IV. Config-Driven API Surface (설정 주도)

The `/robo` route prefix comes from `settings.api_prefix`; Neo4j/embedding/datasource config comes
from `config/settings.py`. Endpoints MUST NOT hardcode the prefix or connection details.

**Rationale**: one settings source keeps deploys (local/docker/embedded) reconfigurable without code
edits.

### V. Fuzzy Name Resolution at the Boundary (경계에서 이름 정규화 매칭)

Raw table names from upstream (e.g. analyzer-extracted names) are resolved to real DB tables via
normalized fuzzy matching (rapidfuzz) above a `similarity_threshold`, returning the resolved name +
score. Unmatched inputs return `null`, never a fabricated match.

**Rationale**: legacy names rarely match the DB exactly; explicit score + null-on-miss keeps the
matching auditable instead of silently wrong.

## Cross-Service Contracts

- **↑ robo-data-analyzer (writer)**: catalog reads the graph the analyzer wrote.
- **↓ robo-data-analyzer (consumer)**: `POST /robo/tables/sample-context` (step5 samples) +
  `GET /robo/lineage/` (DBMS lineage). ⚠️ lineage contract currently mismatched (see III / spec 004).
- **↔ datasource / text2sql backend**: sample rows are fetched via `SELECT * FROM <table> LIMIT n`.
- **↓ frontend / robo-architect**: schema browse/edit, semantic search, DW star-schema endpoints.

## Governance

New features go through `/speckit-specify → /speckit-plan`; each `plan.md` MUST include a
Constitution Check verifying the principles above. A principle is amended only by editing this file
and bumping the version, with the rationale recorded.

**Version**: 1.0.0 | **Ratified**: 2026-06-15 | **Last Amended**: 2026-06-15

# Specifications — ROBO Data Catalog

> **Backfilled** on 2026-06-15 by reverse-engineering the existing codebase using the
> [GitHub Spec Kit](https://github.com/github/spec-kit) format. Each `spec.md` was derived from the
> actual FastAPI router and services — **not** from a prior design doc. The backfill surfaced
> several real bugs and a cross-service contract mismatch; specs follow the **code** (see below).

## What this is

robo-data-catalog (Python/FastAPI, port 5503) browses, edits, enriches, and serves the Neo4j graph
that robo-data-analyzer produced — schema/relationships, semantic search, lineage, DW star-schemas,
and the sample-context the analyzer reads back during a run.

## How to use these specs

- **Change planning**: update the relevant `spec.md` first; the spec is the contract.
- **New features**: `/speckit-specify → /speckit-plan → /speckit-tasks → /speckit-implement`.
  The constitution at [`.specify/memory/constitution.md`](../.specify/memory/constitution.md) states
  the 5 principles every new plan's Constitution Check must satisfy.

## Feature index

| # | Spec | One-liner |
|---|------|-----------|
| 001 | [Graph Query & Data Lifecycle](001-graph-query-lifecycle/spec.md) | historical graph/delete contract; owner lifecycle superseded by 010 |
| 002 | [Schema Browse & Edit](002-schema-browse-edit/spec.md) | tables/columns/relationships/descriptions browse + edit (user edits isolated from analysis) |
| 003 | [Semantic Search & Schema Vectorization](003-semantic-search-vectorize/spec.md) | embed table descriptions; cosine semantic search |
| 004 | [Data Lineage & ETL Analysis](004-data-lineage-etl/spec.md) | lineage graph + ETL-SQL lineage extraction (⚠ analyzer contract mismatch) |
| 005 | [DW Star-Schema Registration](005-dw-star-schema/spec.md) | register/delete cube (fact + dimensions) |
| 006 | [Sample Context & FK Inference Provider](006-sample-context-provider/spec.md) | `POST /robo/tables/sample-context` — the contract analyzer step5 reads |

## Cross-service contracts

- **↑ writer** robo-data-analyzer wrote the graph catalog reads.
- **↓ consumer** robo-data-analyzer reads `POST /robo/tables/sample-context` (spec 006 ↔ analyzer
  spec 008/012) and `GET /robo/lineage/` (spec 004 ↔ analyzer spec 012).
- **↔ datasource / text2sql** sample rows via `SELECT * FROM <table> LIMIT n`.

## Discrepancies & latent bugs found (code is truth)

The backfill surfaced real issues — specs document the code and flag these:

- **004 (cross-service contract mismatch — important)**: analyzer's `enrich_lineage_from_catalog_step`
  expects `data["flows"]` with `source`/`table_fqn`/`relation`/`dml_type`, but catalog returns
  `{nodes, edges, stats}` (no `flows` key). Relation naming also drifts (`DATA_FLOW_TO` read vs
  `DATA_FLOWS_TO` written). The lineage integration is effectively broken until reconciled.
- **002 (latent bug)**: `delete_schema_relationship` builds its query dict with key `"params"` while
  every other call uses `"parameters"` — if the client only reads `"parameters"`, DELETE binds no
  params and matches nothing.
- **003**: `/vectorize` and `/semantic-search` are **decoupled** — search re-embeds every table
  description on each call and never reads the persisted `Table.embedding`, so running `/vectorize`
  has no effect on search results. `include_columns` is accepted but dead (`columns_processed`=0).
- **005**: `DELETE /robo/schema/dw-tables/{cube}` actually **requires** `schema` + `db_name` query
  params (400 if missing) — the router docstring implies path-only. `X-API-Key` is enforced
  unconditionally even when no embedding is created.
- **001**: `DELETE /robo/delete/` preserves `DataSource` nodes (not a full wipe). Node labels are
  PascalCase `:Table`/`:Column` (not `TABLE`).
- **006**: FK candidate inference lives **here** (`FkInferenceService`) — this is the home of the
  "candidate FK" that the analyzer's DDL step deliberately does NOT compute.

Specs are intentionally short (~90-140 lines each) — they describe the contract, not the code.

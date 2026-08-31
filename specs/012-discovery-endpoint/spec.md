# Feature Specification: lossless Catalog table discovery pagination

**Created**: 2026-07-30
**Updated**: 2026-09-01
**Status**: Complete
**Consumer**: `robo-data-analyzer` spec 131 C05

## Problem

The former endpoint eagerly returned at most `max_tables` rows plus a `truncated` count.
That prefix could not prove the datasource population complete and had no immutable
snapshot, deterministic continuation, cursor binding, or terminal closure.

## Contract

The cross-service wire authority is:

`../../../robo-data-analyzer/specs/131-cross-node-semantic-grounding/contracts/catalog-table-discovery-pagination-v1.md`

Catalog implements `GET /robo/tables/discovery` as follows:

- The first request accepts `datasource`, non-negative `sample_limit`, and positive
  `page_size`; it contains neither `snapshot` nor `cursor`.
- Catalog reads the complete Fabric table identity population once, parses one- to
  three-part quote-aware names, rejects malformed or duplicate identities, sorts them
  deterministically, and stores the immutable population in a bounded TTL snapshot.
- Every continuation supplies both opaque `snapshot` and `cursor`. They are bound to the
  original datasource, sample limit, page size, and page index. Unknown, expired,
  cross-snapshot, or mismatched controls fail instead of relisting or returning a prefix.
- Page details retain table and column quoted identity. Transport failure for one table's
  schema/sample produces `degraded=true` without dropping the table. A successful but
  malformed or contradictory Fabric response aborts the page.
- The versioned response contains fixed `total_tables`, contiguous `page_index`, ordered
  `tables`, and a `next_cursor` only before the terminal page. `max_tables`, `truncated`,
  offset paging, and legacy aliases are absent.
- Snapshot/cursor state is in-process transport bookkeeping only. It is not written to
  Neo4j and is not model input/output. Expiry and bounded-capacity eviction fail closed.

## Ownership

- Data Fabric owns datasource registration, metadata browse, and read-only SQL execution.
- Catalog owns immutable identity snapshot creation, deterministic paging, cursor
  validation, table-detail degradation, and the HTTP response.
- Analyzer owns full-page aggregation, cross-page exact-once validation, DDL union, and
  publication gating.

## Acceptance

- Unit: deterministic multi-page closure, zero population, quote-aware identity,
  duplicate/malformed provider rejection, degraded table preservation, cursor-control
  binding, expiry, and 404 propagation.
- Cross-service: the production Analyzer consumer reads the real Catalog ASGI endpoint
  across at least three pages; Fabric population listing occurs once and each table detail
  is consumed once in deterministic order.
- Full Catalog pytest/unittest and Analyzer pytest/unittest regressions pass.
- A local isolated HTTP run proves the same wire without LLM, GPU, Neo4j write, or a new DB.

## Completion evidence

- Catalog unit and contract regression: pytest `73 passed, 28 subtests passed`;
  authoritative unittest `46 tests, OK`.
- Analyzer regression with the production consumer: pytest `1494 passed, 5 skipped,
  359 subtests passed`; authoritative unittest `1261 tests, OK (skipped=2)`.
- `tests/contract/test_analyzer_discovery_integration.py` exercises three pages through
  the real Catalog FastAPI route and through an isolated localhost TCP server. Fabric
  population listing occurs once and every table detail is consumed exactly once.
- The verification made no LLM, embedding, GPU, Neo4j write, or database-creation call.

# Feature Specification: Runtime Boundaries

**Created**: 2026-07-21
**Status**: Specified

## Goal

Verify every Catalog runtime folder against its consumers and give outbound service adapters an unambiguous owner name.

## Requirements

- Keep `api`, `contracts`, `enrichment`, `graph`, `lineage`, `samples`, `search`, and `shared`; their contents match their domains.
- Rename `external/` to `integrations/`; it contains Data Fabric, embedding, and LLM outbound adapters rather than analyzed external-file data.
- Keep configuration and observability under `shared` because both cross multiple Catalog domains.
- Add no compatibility package or duplicate import path.

## Acceptance

Full 40-test suite, focused integration consumers, compile/import, old-path grep, CodeGraph, and startup pass.

# Feature Specification: Shared Layout

**Created**: 2026-07-21
**Status**: Specified

## Goal

Supersede the spec 011 root-file layout for infrastructure now proven to be consumed by multiple Catalog domains. Shared code is grouped under single-word responsibility folders and domain behavior remains unchanged.

## Requirements

- Move root `settings.py` to `shared/config/settings.py`.
- Move cross-domain `observability.py` to `shared/observability/logger.py`.
- Rename the domain folder `table_samples/` to the single-word `samples/`.
- Keep API, contracts, graph, enrichment, external, lineage, and search code with their current owners.
- Add no aliases or duplicated compatibility imports.
- Preserve Catalog API, Neo4j ownership, Analyzer provider contracts, and user-edited metadata behavior.

## Acceptance

Full Catalog tests, compileall, structure verifier, imports, and active old-path searches pass. `settings`, `observability`, and `table_samples` old imports are zero outside historical specs.

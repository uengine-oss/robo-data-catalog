# Tasks

- [x] T001 Freeze source/legacy/API/Cypher/external-consumer inventory.
- [x] T002 Build complete file and symbol audit ledger with no unclassified rows.
- [x] T003 Apply explicit Catalog-owned paths and symbol names with git history preservation.
- [x] T004 Remove proven ignored legacy residue and dead dependencies.
- [x] T005 Update README/contracts and run static/full/integration/live/UI verification.
- [x] T006 Bind every current Catalog runtime file to GOAL-D §2 with post-WIP actions.
- [x] T007 Split API/contracts/enrichment and materialize the final graph/search/sample/external owners.
- [x] T008 Delete proven dead/legacy paths and pass missing 0, forbidden 0, unclassified 0.

## Final evidence

- Final-tree ledger: `final-audit-ledger.tsv` (47 existing flattened runtime/delivery
  files); the pre-existing
  v10 ledger remains separate.
- Full unit/contract suite 40/40, compileall and diff-check pass.
- Live final Catalog: `/health` 200; `/robo/lineage/` 200 with 154 nodes and
  165 edges against the isolated `test` graph.
- Workspace final-tree verifier after the root flattening: missing 0, forbidden 0, unexpected 0,
  unclassified 0.

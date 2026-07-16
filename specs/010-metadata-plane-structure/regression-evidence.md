# R1B verification evidence

Verified on 2026-07-17 against the shared Workspace Neo4j and restarted Catalog/Data Fabric processes.

- Catalog: 17 unit/contract tests, compileall, OpenAPI import with 21 paths.
- Data Fabric: 10 unit/contract tests and compileall; live OpenAPI has 12 paths and 0 retired metadata/model/job/knowledge-base paths.
- Audit: Catalog baseline/current union 120 files, pending 0, blank principle fields 0, verdict/existence mismatches 0.
- Legacy migration dry count: 3,377 Analyzer candidates and one excluded Architect `Proposal`; migration tagged only the 3,377 candidates.
- Live Catalog graph: 3,375 visible Analyzer nodes, 10,666 relationships, zero Architect/system nodes, zero relationship endpoints outside the visible node set.
- Live schema/lineage: 22 schema tables; 107 lineage nodes and 128 lineage edges.
- `CALLS`: one self-call was traced to `dispatch_call` source recursion; API reports it as `recursive=true`, while 661 non-self calls report no recursive true value.
- Workspace environment and process-ownership PowerShell contract tests passed. Catalog and Data Fabric were restarted through the Workspace service controller using their new entrypoints.

T007 remains open because the current Data Fabric registry has no datasource. Therefore a real target-DB query, timeout/compensation exercise, datasource-scoped sample/enrichment request, destructive mixed-owner reset followed by reanalysis, and central UI interaction E2E were not claimed as complete.

# R1B verification evidence

Verified on 2026-07-17 against the shared Workspace Neo4j and restarted Catalog/Data Fabric processes.

- Catalog: 17 unit/contract tests, compileall, OpenAPI import with 21 paths.
- Data Fabric: 16 unit/contract tests and compileall; live OpenAPI has 12 paths and 0 retired metadata/model/job/knowledge-base paths.
- Audit: Catalog baseline/current union 120 files, pending 0, blank principle fields 0, verdict/existence mismatches 0.
- Legacy migration dry count: 3,377 Analyzer candidates and one excluded Architect `Proposal`; migration tagged only the 3,377 candidates.
- Live Catalog graph: 3,375 visible Analyzer nodes, 10,666 relationships, zero Architect/system nodes, zero relationship endpoints outside the visible node set.
- Live schema/lineage: 22 schema tables; 107 lineage nodes and 128 lineage edges.
- MindsDB의 기존 `shopmall` 연결로 실제 target DB `SELECT 1`과 22개 table 조회가 통과했다. Graph TABLE 22개와 이름 집합이 완전 일치한 경우에만 누락된 legacy `db=shopmall`을 보완했고, Catalog sample-context가 `comm_code`, `product_stock` 각각 column 6개와 sample row 2개를 반환했다.
- invalid PostgreSQL port 1 probe가 약 0.6초 안에 `success=false`를 반환했고 임시 `robo_probe_*` connection 누수는 0이었다. MindsDB connection 등록과 실제 DB 접속을 혼동하던 false-positive를 native target `SELECT 1` 검증으로 수정했다.
- `CALLS`: one self-call was traced to `dispatch_call` source recursion; API reports it as `recursive=true`, while 661 non-self calls report no recursive true value.
- Workspace environment and process-ownership PowerShell contract tests passed. Catalog and Data Fabric were restarted through the Workspace service controller using their new entrypoints.

T007 remains open because the Data Fabric registry itself has no datasource row even though MindsDB retains `shopmall`. Therefore timeout/compensation, datasource-scoped LLM enrichment, destructive mixed-owner reset followed by reanalysis, and central UI interaction E2E were not claimed as complete.

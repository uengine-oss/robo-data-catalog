# Implementation Plan: Catalog 메타데이터 plane·소유 수명주기 정리

## Target Structure

```text
app/
├── main.py
├── http/{catalog_endpoints,catalog_requests}.py
├── graph/{client,neo4j_context,graph_queries,schema_queries,schema_edits,dw_schema}.py
├── metadata/{description_enrichment,fk_inference,sample_context,semantic_search}.py
├── lineage/{lineage_service,sql_lineage_extractor}.py
├── external/{data_fabric_client,embedding_client}.py
└── system/{settings,logging}.py
tests/{unit,integration,contract,e2e}/
```

작은 파일 수보다 책임·의존 방향을 우선하되 한 책임을 다시 얇은 파일로 쪼개지는 않는다. HTTP→use
responsibility→external/graph 방향만 허용한다. graph owner 판정과 delete query는 단일 정의처로 둔다.

## Verification

- 현재 전 파일 inventory와 공개 API/Neo4j query/cross-service 소비자 동결.
- import/compile/unit 및 Data Fabric contract test.
- 혼합 owner fixture에서 check/delete 격리 검증.
- Analyzer 재분석→Catalog sample/enrich/search/lineage→중앙 UI E2E.


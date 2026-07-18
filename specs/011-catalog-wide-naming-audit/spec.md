# Feature Specification: Catalog 전 파일 책임·명명 재감사

**Created**: 2026-07-17
**Status**: Complete

## Requirements

- 모든 tracked runtime/test/script 파일을 다시 감사하며 spec010 체크를 최신 완료 증거로 간주하지 않는다.
- Catalog는 metadata browse/edit, description/embedding/search, FK, lineage, sample context만 소유한다.
- 파일·클래스·함수·변수는 Catalog 소유와 구체 책임을 단독 식별해야 한다. `client.py`,
  `lineage_service.py`, generic `Settings`, `Neo4jClient`, 축약 `dw/fk` 등은 직접 소비자와 의미를 확인해
  명확한 이름으로 바꾼다.
- HTTP→capability→graph/external 의존 방향을 유지하고 graph owner/delete 단일 진실을 보존한다.
- ignored 과거 구조 잔재는 tracked/WIP가 아님과 실행 소비자 0을 확인한 뒤 제거한다.
- `project_temp`에서 실행 중인 사용자 서버·포트는 건드리지 않으며 live 검증은 별도 포트로 격리한다.

## Acceptance

전체 inventory/ledger 일치, old ambiguous paths 0, compile/full tests, mixed-owner Neo4j, Data Fabric contract,
metadata enrichment/search/lineage 실제 API와 중앙 UI가 통과한다.

## Final target-tree binding

`D:/work/robo/세서비스-최종트리.md` §2 is the target-tree contract. It is a
static target rather than an authority over source code, so the completed
implementation, audit ledger, and executable verifier remain the final judges.

- Move settings and logging to root `settings.py` and `observability.py`.
- Split the monolithic router into `api/{graph,lineage,schema,schema_edit,
  search,table_samples,enrichment,graph_connection,errors}.py`.
- Split request contracts by domain under `contracts/` and keep relation
  type data in one owner.
- Move the enrichment use case to `enrichment/{orchestrator,events,
  description,foreign_keys}.py`; endpoint code only streams the use case.
- Materialize `search/semantic.py`, `table_samples/{resolver,context}.py`,
  `lineage/{queries,sql_extract}.py`, the final graph modules, and explicit
  `external/{data_fabric,llm,embedding}.py`.
- Delete the consumer-zero DW star-schema implementation and ignored legacy
  roots only after consumer proof. Every runtime file must be classified and
  machine verification must report missing 0, forbidden 0, unclassified 0.

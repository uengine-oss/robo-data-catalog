# Tasks

- [x] T001 현재 파일·API·Cypher·writer/reader·외부 소비자 inventory를 동결한다.
- [x] T002 graph owner/check/delete/상호작용 조회 계약 테스트를 먼저 작성한다.
- [x] T003 책임 기반 package로 `git mv`하고 Catalog 소유 경로·파일·클래스의 `analyzer` 이름을 제거한다.
- [x] T004 metadata/search/enrichment/FK/lineage/sample 계약을 새 구조로 이동하고 모든 graph read/write에 owner 경계를 적용한다.
- [x] T005 내부 deploy·cache·확인된 잡파일과 죽은 dependency를 제거한다.
- [x] T006 README·실행 진입점·환경 template·SDD 문서를 실제 구조와 계약으로 갱신한다.
- [x] T007 혼합 owner 실제 Neo4j, Data Fabric 장애·timeout, Analyzer→Catalog→중앙 UI 전체 E2E를 통과한다.
- [x] T008 OpenAI-compatible provider 설정을 단일 external client 경계로 모으고 실제 metadata 보강을 검증한다.

T007은 mock 기반 24개 unit/contract, compile, OpenAPI 검증과 별개다. 실제 서비스·DB 증거가 없으면 완료로 표시하지 않는다.

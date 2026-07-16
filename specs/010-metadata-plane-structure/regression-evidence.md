# R1B 검증 증거

2026-07-17 공유 Workspace Neo4j와 재기동한 Catalog/Data Fabric/중앙 UI를 기준으로 검증했다.

- Catalog는 unit/contract 24개와 compileall, Data Fabric은 unit/contract 22개와 compileall을 통과했다. 중앙 Frontend 프로덕션 빌드도 통과했으며 전체 type-check의 기존 오류와 별개로 변경 파일 `schemaCanvas.ts` 직접 오류는 0건이다.
- 전체 파일 감사 장부는 Catalog baseline/current 합집합 124개, Data Fabric 89개이며 양쪽 모두 pending 0이다.
- legacy migration dry-run은 Analyzer 후보 3,377개와 제외 대상 Architect `Proposal` 1개를 분리했고 후보만 owner migration했다. 실제 Catalog graph에는 Analyzer 사용자 노드 3,375개, 관계 10,666개가 보이며 Architect/system 노드와 표시 집합 밖 관계 끝점은 0이다.
- 실제 `shopmall` 대상 DB에서 `SELECT 1`, 테이블 22개, Catalog sample-context의 `comm_code`와 `product_stock` 각각 column 6개/sample row 2개를 확인했다. 잘못된 PostgreSQL port 1 연결 검사는 약 0.6초에 `success=false`, 임시 `robo_probe_*` 누수 0을 반환했다.
- 구조화 query 계약은 `datasource=shopmall`, 대상 DB `SELECT`, `max_rows=2`로 정확히 2행을 반환했다. mutation과 다중 statement는 HTTP 400이었고 문자열 안의 `DROP`은 정상 값으로 처리됐다. `pg_sleep(60)`은 30.2초 후 명시적 timeout 오류를 반환했다.
- 실제 별칭 두 개를 서로 다른 registry identity로 생성하고 각각 조회했다. 하나를 삭제한 뒤에도 다른 하나는 조회됐으며, 최종 registry와 MindsDB 별칭 누수는 모두 0이고 다른 owner Neo4j 수량도 변하지 않았다.
- 실제 MindsDB 등록 뒤 registry만 통제 실패시킨 보상 검증은 HTTP 500을 전달하고 MindsDB 별칭을 제거했다(`mindsdb_leaks=0`, target probe=real).
- 중앙 UI에서 폼으로 datasource를 생성·목록 확인하고, Schema 기능에서 `comm_code` 2행/6열을 새 구조화 query 계약으로 조회한 뒤 UI에서 삭제했다. 대상 CRUD/query 응답은 모두 200이고 registry/MindsDB 누수는 0이다.
- 코드 그래프 UI는 기본 IF/PARENT_OF 0, 숨김 표시 후 IF 1/PARENT_OF 1과 canvas 5 nodes/7 relationships, 복원 후 다시 0을 확인했다. `product_stock` 확장은 1/0에서 19/25로 바뀌었고 모든 노드는 실제 1-hop, 모든 관계는 원본 graph의 ID·양 끝·유형과 일치했다.
- 선택적인 text2sql alarm SSE를 명시적 opt-in으로 바꾼 뒤 코드 그래프와 Fabric UI smoke 모두 console/network 오류 0으로 통과했다.
- `CALLS` 자기연결 1개는 `dispatch_call` 원문의 실제 재귀 호출로 확인돼 `recursive=true`이며, 일반 661개 관계에는 true가 없다.
- 두 번째 mixed-owner 재분석은 datasource namespace를 포함해 440.0초·8,205,386 bytes·실패 0으로 종료됐다. Proposal과 DataSource를 보존했고 최종 검증에서 `proposal_preserved=true`, `datasource_preserved=true`, sentinel cleanup=true였다.
- 실제 OpenAI-compatible provider와 `shopmall` Fabric sample을 사용해 `comm_code` 설명을 생성했다. TABLE 22개 중 통제 대상 1개가 실제 저장됐고 `description_source=sample_data_inference`, wrong owner 0, 다른 owner ID 보존을 확인한 뒤 원래 설명을 복원했다.
- 실검증 중 발견한 빈 provider content를 성공 0건으로 숨기던 경로를 제거했다. `LLM_MAX_COMPLETION_TOKENS` 기본 4096을 공용 설정으로 두고, 빈/비객체 응답은 명시적 오류로 전달하며 Neo4j가 반환한 실제 update count만 성공으로 집계한다.

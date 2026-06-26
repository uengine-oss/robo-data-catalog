# Tasks: 데이터 리니지 전략무관 범용화

**Spec**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md)

변경 = `service/data_lineage_service.py::fetch_lineage_graph` 1함수. 계약·프론트·스키마 무변경.

- [ ] **T001** `fetch_lineage_graph` 재작성: `(f)-[:READS|WRITES]->(:TABLE)` 쿼리 → SOURCE/ETL/TARGET 노드 + DATA_FLOW_TO 엣지 도출(dict dedup). 표시명=logical_name 우선. [FR-001/002/004]
- [ ] **T002** stats(etlCount/sourceCount/targetCount/flowCount) 도출값 기준 산출. R/W 0이면 빈 결과 정상 반환. [FR-006]
- [ ] **T003** 응답 형태(노드 type=SOURCE/ETL/TARGET, 엣지 필드 id/source/target/type/properties) 기존과 동일 유지 확인. [FR-004/SC-003]
- [ ] **T004** py_compile + catalog 백엔드 재기동.
- [ ] **T005** 라이브 검증: framework(zapamcom) 데이터로 `curl /robo/lineage/` → nodes/edges 0 아님, 도출 흐름이 그래프 R/W와 일치(SC-001/002). dbms 무회귀(코드/계약 기준).
- [ ] **T006** 커밋(catalog 서브모듈 — detached HEAD·WIP 주의: 내 파일만, 브랜치 정리 후).

## 의존
T001→T002→T003→T004→T005→T006.

# Implementation Plan: 데이터 리니지 전략무관 범용화

**Date**: 2026-06-26 | **Spec**: [spec.md](./spec.md)

## Summary

catalog `/robo/lineage/` (GET → `data_lineage_service.fetch_lineage_graph`) 가 현재 dbms 전용 `:DataSource`/`:ETLProcess`/`DATA_FLOW_TO` 별도 모델만 읽어 framework 분석은 빈 결과. → **분석 그래프의 `(f)-[:READS|WRITES]->(:TABLE)` 에서 전략무관으로 리니지를 도출**: 읽은 테이블=SOURCE, 함수=ETL, 쓴 테이블=TARGET. 응답 형태(SOURCE/ETL/TARGET 노드 + edges + stats) 유지 → **프론트 무변경**.

## 기술 접근 (read-side 도출)
- 변경 = `service/data_lineage_service.py::fetch_lineage_graph` 단일 함수. analyzer·프론트·Neo4j 스키마 무변경(신규 영속 0). dbms도 graph R/W(`link_dbms_tables` 산출) 보유 → 같은 경로로 동작 = 단일화(spec 004 별도 ETL 모델은 이 뷰에서 불필요 → supersede).
- 노드 id: `src:{elementId(t)}` / `etl:{elementId(f)}` / `tgt:{elementId(t)}` (같은 테이블이 읽기·쓰기 양쪽이면 source·target 각각 = ETL 표준 동작). 엣지 id 결정론 → 자동 dedup.

## Constitution Check
- 신규 영속 스키마/노드/관계 **0**(read-side 도출, MERGE 없음). 
- 계약(`/robo/lineage/` 응답)·프론트 무변경 → cross-service 안전.
- silent fail 금지: R/W 0이면 빈 결과 정상 반환(예외 아님). PASS.

## Project Structure
```
specs/007-strategy-agnostic-lineage/ : spec.md ✓ plan.md(이 파일) tasks.md checklists/ ✓
변경 소스: service/data_lineage_service.py (fetch_lineage_graph 1함수)
```

## Phase 설계
- 쿼리: `MATCH (f)-[r:READS|WRITES]->(t:TABLE) RETURN elementId(f),f.name,f.logical_name,type(r),elementId(t),coalesce(t.logical_name,t.name)`.
- 빌드: ETL=함수(읽기/쓰기 주체), READS→ `src:{t} -[DATA_FLOW_TO]-> etl:{f}`, WRITES→ `etl:{f} -[DATA_FLOW_TO]-> tgt:{t}`. 노드/엣지 dict dedup.
- stats: etlCount/sourceCount/targetCount/flowCount.
- 표시명: 함수=logical_name 우선·없으면 name / 테이블=logical_name 우선·없으면 name.

## Complexity Tracking
없음. (대량 R/W 시 컴포넌트 단위 집계는 후속 최적화 — 이번 스코프 밖, spec edge case 기재.)

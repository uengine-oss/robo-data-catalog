# Feature Specification: Catalog 메타데이터 plane·소유 수명주기 정리

**Created**: 2026-07-17  
**Status**: Implementing

## Problem

Catalog는 metadata/search/enrichment/FK/lineage를 담당하지만 현재 `analyzer/strategy/.../lineage_analyzer.py`
같은 잘못된 소유 이름과 `api`, `service`, `util`의 평면 구조를 갖는다. `check-data`는 DataSource까지 전체
노드를 세어 빈 분석 그래프를 데이터 있음으로 오판하고, delete는 DataSource 외 모든 노드를 지워 공유
Neo4j의 Architect·Fabric 데이터를 훼손할 수 있다.

## Requirements

- Catalog는 schema metadata 조회·사용자 편집·description/embedding/search·FK·lineage·sample context를 소유한다.
- 대상 DB SQL은 Data Fabric client 한 경로로만 실행하고 credential을 받거나 저장하지 않는다.
- Catalog 소유 경로·파일·클래스에서 `analyzer` 이름을 제거하고 책임 기반 package로 이동한다.
- `check-data`는 Analyzer 소유 분석 노드만 세며 DataSource와 Architect session node를 제외한다.
- reset/delete는 Analyzer 소유 그래프만 삭제하고 다른 owner는 보존한다.
- 새 owner marker가 없는 과거 분석 노드는 UPPER_SNAKE 분석 라벨 계약으로만 제한 분류하며 PascalCase
  Architect/Fabric 노드를 포괄 삭제하지 않는다.
- Catalog가 편집·보강한 Analyzer graph node의 owner는 Analyzer로 유지한다.
- 내부 deploy와 확인된 잡파일을 제거하고 README를 실제 구조·계약·검증으로 갱신한다.

## Acceptance

1. DataSource만 존재하면 `check-data={hasData:false,nodeCount:0}`이다.
2. reset 전후 Fabric DataSource와 Architect session node count가 동일하다.
3. Catalog의 sample/search/enrichment/FK/lineage 계약이 이동 전과 동일하거나 명시된 supersede 계약을 따른다.
4. Catalog 소유 경로·클래스명에서 `analyzer`가 0이다.
5. 전체 runtime 파일이 수정·삭제·위반 없음 중 하나로 근거와 함께 분류된다.


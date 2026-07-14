# Implementation Plan: 시스템 노드 그래프 API 차단

`Neo4j(all nodes) → catalog graph query(system predicate) → user graph payload`

- 시스템 라벨 집합과 Cypher predicate를 서비스 한곳에서 관리한다.
- 노드 조회, 관계 조회, 존재 여부 조회가 같은 predicate를 사용한다.
- Neo4j 검색/용어집 소비 경로는 변경하지 않는다.

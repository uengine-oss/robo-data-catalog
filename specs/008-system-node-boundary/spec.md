# Feature Specification: 시스템 노드 그래프 API 차단

**Created**: 2026-07-14  
**Status**: Implementing

## Requirements

- `GLOSSARY`, `EMBED_META`는 Neo4j 내부 저장에는 유지한다.
- 일반 그래프 조회 API의 Nodes/Relationships에는 두 시스템 노드와 연결 관계를 반환하지 않는다.
- 시스템 노드만 존재할 때 사용자 그래프 데이터가 존재한다고 판단하지 않는다.


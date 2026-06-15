# Feature Specification: Semantic Search & Schema Vectorization

**Feature Branch**: `003-semantic-search-vectorize`

**Created**: 2026-06-15

**Status**: Backfilled (reverse-engineered)

**Input**: User description: "스키마 요소(테이블 설명)를 벡터로 임베딩하고, 자연어 질의로 의미 기반 검색한다 — `POST /robo/schema/semantic-search`, `POST /robo/schema/vectorize`."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - 자연어로 테이블 찾기 (시멘틱 검색) (Priority: P1)

데이터 사용자가 정확한 테이블명을 모른 채 "고객 주문 이력" 같은 자연어로 질의하면, 시스템은 테이블 설명(description)의 의미적 유사도를 기준으로 가장 관련 높은 테이블 목록을 돌려준다.

**Why this priority**: 카탈로그의 핵심 가치. 키워드 일치(LIKE)로는 못 찾는 테이블을 의미 기반으로 찾아주는 것이 이 피처의 존재 이유다. 검색은 벡터화 여부와 무관하게(쿼리 시점에 즉석 임베딩) 단독 동작 가능하다.

**Independent Test**: 설명이 채워진 `:Table` 노드가 있는 상태에서 `POST /robo/schema/semantic-search` 에 `{query, limit}` 와 `X-API-Key` 헤더를 보내 유사도 내림차순 결과를 받는 것으로 단독 검증 가능.

**Acceptance Scenarios**:

1. **Given** description 이 채워진 테이블들이 존재, **When** 유효한 `X-API-Key` 와 `{"query":"고객 주문", "limit":10}` 으로 호출, **Then** `[{name, schema, description, similarity}]` 가 similarity 내림차순으로, 상위 limit 개 이내, similarity ≥ 0.3 인 항목만 반환된다.
2. **Given** description 이 있는 테이블이 하나도 없음, **When** 검색 호출, **Then** 빈 배열 `[]` 을 반환한다.
3. **Given** `X-API-Key` 헤더 없음, **When** 검색 호출, **Then** HTTP 400 으로 거부된다.

---

### User Story 2 - 스키마 벡터화(임베딩 영속화) (Priority: P2)

운영자가 전체(또는 특정 스키마) 테이블의 설명을 임베딩으로 변환해 Neo4j `Table.embedding` 속성에 저장한다. 이미 임베딩이 있는 테이블은 기본적으로 건너뛰며, 옵션으로 재임베딩할 수 있다.

**Why this priority**: 검색 자체는 쿼리 시점 즉석 임베딩으로 동작하므로 P1보다 후순위지만, 임베딩을 미리 영속화해 두면 후속 유사도/추천 기능의 기반이 된다.

**Independent Test**: `POST /robo/schema/vectorize` 에 `{schema, reembed_existing, batch_size}` 와 `X-API-Key` 를 보내 `{message, stats:{tables_processed,...}}` 를 받고, Neo4j 에서 `Table.embedding` 이 채워졌는지 확인.

**Acceptance Scenarios**:

1. **Given** 임베딩이 없는 테이블들, **When** `reembed_existing=false` 로 벡터화 호출, **Then** `embedding IS NULL` 인 테이블만 batch_size 한도 내에서 처리되고 `stats.tables_processed` 가 증가한다.
2. **Given** `schema` 가 지정됨, **When** 벡터화 호출, **Then** 해당 스키마 테이블만 대상으로 처리된다.
3. **Given** `X-API-Key` 헤더 없음, **When** 벡터화 호출, **Then** HTTP 400 으로 거부된다.

---

### Edge Cases

- 설명이 비었거나 NULL 인 테이블: 검색 대상에서 제외(`description IS NOT NULL AND <> ''`). 벡터화 시 설명이 없으면 `format_table_text` 가 테이블명만으로 텍스트를 구성한다.
- 매우 긴 설명: 검색 시 임베딩 입력은 500자, 응답 description 은 200자로 절단된다.
- 유효하지 않은 OpenAI API 키: 검색은 클라이언트 생성/호출 시점에 HTTP 400 으로 거부한다.
- 유사도 0 분모(임베딩 노름이 0): cosine 유사도를 0.0 으로 처리한다.
- 검색 대상 테이블 과다: 설명 있는 테이블 최대 200개만 조회(`LIMIT 200`)하여 그 안에서 순위화한다.
- 벡터화 중 특정 테이블 실패: 해당 배치 처리는 `RuntimeError` 로 중단된다(부분 실패 전파).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: 시스템은 `POST /robo/schema/semantic-search` 로 `{query, limit(기본 10)}` 를 받아 테이블 설명 기반 의미 검색 결과를 반환해야 한다.
- **FR-002**: 시스템은 검색·벡터화 요청에서 OpenAI 키를 `X-API-Key` 요청 헤더로 받아야 하며, 없으면 HTTP 400 으로 거부해야 한다.
- **FR-003**: 시스템은 검색 시 description 이 있는 `:Table` 노드를 최대 200개 조회하고, 각 설명과 질의를 임베딩하여 cosine 유사도로 순위화해야 한다.
- **FR-004**: 시스템은 검색 결과를 유사도 내림차순으로 정렬하고, 상위 `limit` 개 중 유사도 ≥ 0.3 인 항목만 `{name, schema, description, similarity(소수 4자리)}` 형태로 반환해야 한다. schema 누락 시 `"public"` 으로 채운다.
- **FR-005**: 시스템은 임베딩 입력 설명을 500자, 반환 description 을 200자로 절단해야 한다.
- **FR-006**: 시스템은 `POST /robo/schema/vectorize` 로 `{db_name, schema, include_tables, include_columns, reembed_existing, batch_size(기본 100)}` 를 받아 테이블 설명을 임베딩하고 `:Table.embedding` 속성에 저장해야 한다.
- **FR-007**: 시스템은 `reembed_existing=false` 일 때 `embedding IS NULL` 인 테이블만 대상으로 하고, `schema` 지정 시 해당 스키마로 한정하며, `batch_size` 로 1회 처리량을 제한해야 한다.
- **FR-008**: 시스템은 벡터화 결과로 `{message, stats:{tables_processed, columns_processed, errors}}` 를 반환해야 한다.
- **FR-009**: 시스템은 텍스트 임베딩에 OpenAI 임베딩 모델 `text-embedding-3-small`(설정 `EMBEDDING_MODEL` 로 재정의 가능)을 사용해야 한다.
- **FR-010**: 벡터화 임베딩 텍스트는 `format_table_text` 규칙(`Table: <name> | Description: <desc>` [| Columns: ...])으로 구성해야 한다.

> 참고(현 구현 한계, NEEDS CLARIFICATION): `include_columns` 파라미터를 받지만 컬럼 벡터화 로직은 미구현이며 `columns_processed` 는 항상 0이다. 검색은 영속화된 `Table.embedding` 을 사용하지 않고 쿼리 시점에 설명을 매번 즉석 임베딩한다 — 즉 벡터화와 검색이 현재 결합되어 있지 않다.

### Key Entities *(include if feature involves data)*

- **Table (`:Table` 노드)**: 검색·벡터화의 대상. 주요 속성 `name`, `schema`, `description`, `embedding`(벡터화 결과 저장 위치).
- **임베딩 벡터 (embedding)**: 텍스트의 의미를 표현하는 float 배열. 검색 시 질의·설명 각각에 대해 생성되고, 벡터화 시 `Table.embedding` 으로 영속된다.
- **임베딩 모델/소스**: OpenAI `text-embedding-3-small`(기본). 검색 경로는 `AsyncOpenAI` 를 직접 호출, 벡터화 경로는 `EmbeddingClient`(`client/embedding_client.py`, 설정 `embedding_model` 사용)를 경유한다.
- **벡터화 대상 필드**: 테이블 설명(description)이 임베딩의 원천 텍스트. (컬럼 설명은 모델상 자리만 있고 미구현.)
- **유사도(similarity)**: 질의 임베딩과 설명 임베딩 간 cosine 유사도(`dot / (‖a‖·‖b‖)`, numpy 계산). 임계값 0.3.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 정확한 테이블명을 모르는 사용자가 자연어 질의 1회로 관련 테이블을 유사도 순으로 받아본다(유사도 ≥ 0.3 항목만).
- **SC-002**: 검색 응답은 항상 상위 `limit` 개 이내로 제한된다(기본 10).
- **SC-003**: 벡터화 1회 호출로 대상 테이블의 `Table.embedding` 이 채워지며, 처리 건수가 `stats.tables_processed` 로 보고된다.
- **SC-004**: 설명이 없는 테이블은 검색 결과에 결코 포함되지 않는다.
- **SC-005**: API 키가 없는 요청은 100% HTTP 400 으로 차단된다.

## Assumptions

- OpenAI 임베딩 API 에 접근 가능하며, 호출자가 유효한 키를 `X-API-Key` 헤더로 제공한다(서버는 키를 저장하지 않고 요청별로 전달받는다).
- 검색 대상 테이블 모집단은 200개 이내로 의미 있게 동작한다고 가정(현 구현은 설명 있는 테이블 상위 200개만 순위화).
- Neo4j 가 `:Table` 노드와 `name/schema/description/embedding` 속성을 보유한다(스키마 적재는 본 피처 범위 밖, 선행 피처가 채운다).
- 라우트는 라우터 prefix 하에 `POST /robo/schema/semantic-search`, `POST /robo/schema/vectorize` 로 노출된다(서비스 포트 5503).
- 검색은 쿼리 시점 즉석 임베딩으로 동작하므로 벡터화 선행 없이도 단독 사용 가능하다.

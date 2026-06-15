# Feature Specification: DW Star-Schema Registration

**Feature Branch**: `005-dw-star-schema`

**Created**: 2026-06-15

**Status**: Backfilled (reverse-engineered)

**Input**: 기존 코드에서 역공학. `POST /robo/schema/dw-tables`, `DELETE /robo/schema/dw-tables/{cube_name}` — 데이터 웨어하우스(OLAP) 스타스키마(큐브) 정의를 카탈로그 그래프에 등록/삭제. 소스: `service/dw_schema_service.py`, `api/catalog_router.py`, `api/request_models.py`.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - 스타스키마(큐브) 등록 (Priority: P1)

데이터 모델러가 OLAP 분석을 위한 스타스키마를 정의한다. 하나의 팩트 테이블과 N개의 디멘전 테이블, 그리고 팩트→디멘전 외래키(FK) 관계를 한 번의 요청으로 카탈로그 그래프에 등록한다. 등록 시 각 테이블은 의미 검색을 위해 자동으로 임베딩(벡터화)된다.

**Why this priority**: 스타스키마를 그래프에 올리는 것이 이 기능의 본질이며, 등록이 없으면 삭제·검색 대상도 존재하지 않는다. 단독으로 가치(분석용 큐브 카탈로그화)를 제공한다.

**Independent Test**: cube_name·fact_table·dimensions를 담은 `POST /robo/schema/dw-tables`(+ `X-API-Key` 헤더)를 호출하고, 응답의 `stats`(tables_created / columns_created / relationships_created / embeddings_created)가 입력 규모와 일치하는지, 그래프에 `table_type` FACT/DIMENSION 노드가 생겼는지로 검증한다.

**Acceptance Scenarios**:

1. **Given** 유효한 큐브 정의(팩트 1 + 디멘전 N)와 OpenAI API 키, **When** `POST /robo/schema/dw-tables`를 호출, **Then** 팩트·디멘전 테이블과 컬럼이 생성되고 FK 관계·임베딩이 만들어지며 `message`와 `stats`가 반환된다.
2. **Given** `create_embeddings=true`이지만 `X-API-Key` 헤더 없음, **When** 등록 요청, **Then** 400 오류로 임베딩에 API 키가 필요함을 알린다.

---

### User Story 2 - 스타스키마(큐브) 삭제 (Priority: P2)

데이터 모델러가 더 이상 필요 없거나 잘못 정의된 큐브를 큐브 이름으로 삭제한다. 해당 큐브에 속한 모든 팩트/디멘전 테이블, 컬럼, FK 관계가 함께 제거된다.

**Why this priority**: 등록의 역연산으로 라이프사이클을 완성하지만, 등록 없이는 의미가 없으므로 P2.

**Independent Test**: 등록된 큐브에 대해 `DELETE /robo/schema/dw-tables/{cube_name}?schema=dw&db_name=postgres`를 호출하고 응답 `deleted_tables` 수와 그래프에서 해당 `cube_name` 노드가 사라졌는지로 검증한다.

**Acceptance Scenarios**:

1. **Given** 등록된 큐브 `cube_name`, **When** `schema`·`db_name` 쿼리 파라미터를 포함해 삭제 요청, **Then** 해당 큐브의 모든 테이블·컬럼·관계가 제거되고 `deleted_tables` 수가 반환된다.
2. **Given** 삭제 요청에 `schema` 또는 `db_name` 파라미터 누락, **When** 호출, **Then** 400 오류로 해당 파라미터가 필요함을 알린다.

---

### Edge Cases

- 동일한 `cube_name`(또는 동일 fqn 테이블)으로 재등록하면? → MERGE 기반이라 기존 노드를 갱신(upsert)하며 중복 노드를 만들지 않는다.
- FK 컬럼의 `fk_target_table`이 가리키는 테이블이 그래프에 없으면? → MATCH 실패로 해당 FK 관계만 만들어지지 않고 나머지는 정상 처리된다.
- 임베딩 생성 중 일부 테이블에서 실패하면? → 오류 로깅 후 예외를 올려 등록을 실패로 처리한다(이미 생성된 노드는 트랜잭션 외부라 잔존할 수 있음).
- 존재하지 않는 `cube_name` 삭제? → 삭제 자체는 성공하며 `deleted_tables=0`을 반환한다.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: 시스템은 단일 요청으로 1개의 팩트 테이블, 0개 이상의 디멘전 테이블, 각 테이블의 컬럼을 받아 카탈로그 그래프에 등록 MUST.
- **FR-002**: 시스템은 팩트 테이블은 `table_type=FACT`로, 디멘전 테이블은 `table_type=DIMENSION`으로 구분하여 표기 MUST, 그리고 모든 테이블에 `cube_name`을 태깅 MUST.
- **FR-003**: 시스템은 팩트 컬럼이 FK(`is_fk=true`이고 `fk_target_table` 지정)일 때 팩트→대상 테이블 간 FK 관계를 생성 MUST.
- **FR-004**: 시스템은 `create_embeddings=true`일 때 팩트·디멘전 테이블에 대해 임베딩을 생성하여 의미 검색 가능하게 MUST.
- **FR-005**: 시스템은 `create_embeddings=true`인데 OpenAI API 키(`X-API-Key` 헤더)가 없으면 400 오류로 거부 MUST.
- **FR-006**: 시스템은 등록 결과로 생성된 테이블/컬럼/관계/임베딩 수 통계(`stats`)와 메시지를 반환 MUST.
- **FR-007**: 시스템은 동일 식별자(fqn `schema.name`) 재등록 시 신규 노드를 만들지 않고 갱신(upsert) MUST.
- **FR-008**: 시스템은 `cube_name`으로 해당 큐브에 속한 모든 테이블·컬럼·FK 관계를 일괄 삭제 MUST.
- **FR-009**: 시스템은 삭제 요청 시 `schema`·`db_name` 파라미터가 누락되면 400 오류로 거부 MUST.
- **FR-010**: 시스템은 삭제 결과로 제거된 테이블 수(`deleted_tables`)와 메시지를 반환 MUST.

### Key Entities *(include if feature involves data)*

- **Cube (큐브)**: 하나의 스타스키마 단위. `cube_name`으로 식별되며 한 개의 팩트 테이블과 여러 디멘전 테이블을 묶는 논리적 집합. 큐브에 속한 모든 테이블 노드에 `cube_name` 속성으로 표시된다.
- **Fact Table (팩트 테이블, dw-table 노드)**: `table_type=FACT`인 Table 노드. 측정값·외래키 컬럼을 가지며 큐브당 1개. fqn = `{dw_schema}.{name}`.
- **Dimension Table (디멘전 테이블, dw-table 노드)**: `table_type=DIMENSION`인 Table 노드. 팩트가 참조하는 분석 축. 큐브당 0~N개.
- **Column (컬럼)**: 테이블에 `HAS_COLUMN`으로 연결된 Column 노드. `dtype`, `description`, 팩트의 경우 `is_pk`/`is_fk` 속성 보유.
- **FK 관계 (FK_TO_TABLE)**: 팩트 테이블에서 디멘전 테이블로 향하는 관계. `sourceColumn`과 `source='user'` 속성을 가진다.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 유효한 큐브 정의(팩트 1 + 디멘전 N) 등록 시 `stats.tables_created`가 1 + 디멘전 수와 일치한다.
- **SC-002**: FK로 표시된 모든 팩트 컬럼에 대해 대상 테이블이 존재하면 `relationships_created`가 FK 컬럼 수와 일치한다.
- **SC-003**: `create_embeddings=true` 등록 시 `embeddings_created`가 임베딩 가능한 테이블 수와 일치한다.
- **SC-004**: 등록된 큐브 삭제 후 동일 `cube_name`을 가진 노드가 그래프에 0개 남는다.

## Assumptions

- 카탈로그는 Neo4j 그래프를 영속 저장소로 사용하며 기존 `Table`/`Column` 라벨·`HAS_COLUMN`/`FK_TO_TABLE` 관계 스키마를 재사용한다(신규 라벨 없음).
- 임베딩은 OpenAI 임베딩 API(`AsyncOpenAI` + `EmbeddingClient`)로 생성하며 API 키는 호출자가 `X-API-Key` 헤더로 전달한다.
- 컬럼 `dtype` 미지정 시 기본값 `VARCHAR`, 삭제 시 `schema` 기본 `dw`·`db_name` 기본 `postgres`(서비스 시그니처 기준)를 가정한다. 단 라우터는 삭제 시 두 파라미터를 명시적으로 요구한다.
- 본 기능은 큐브 정의의 등록·삭제만 다루며 OLAP 쿼리 실행이나 큐브 조회/목록 API는 범위 밖이다.

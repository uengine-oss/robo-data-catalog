# Feature Specification: Schema Browse & Edit

**Feature Branch**: `002-schema-browse-edit`

**Created**: 2026-06-15

**Status**: Backfilled (reverse-engineered)

**Input**: Reverse-engineered from existing code — `api/catalog_router.py`, `service/schema_query_service.py`, `service/schema_edit_service.py`, `service/table_description_service.py`, `api/request_models.py` (robo-data-catalog, FastAPI, port 5503).

## User Scenarios & Testing *(mandatory)*

robo-data-catalog는 robo-data-analyzer가 코드/DDL을 분석해 Neo4j에 적재한 스키마 그래프(`:Table`, `:Column`, 테이블 간 관계, 프로시저 Statement)를 **사람이 탐색하고 직접 보정**하는 화면을 제공한다. 자동 분석이 놓치거나 부정확하게 채운 부분을 사람이 읽고, 빠진 관계를 추가/삭제하고, 설명을 다듬는 것이 핵심 가치다.

### User Story 1 - 스키마 탐색 (Priority: P1)

데이터 분석가가 분석 결과로 적재된 테이블 목록을 둘러보고, 한 테이블을 골라 컬럼·외부 참조·관계·(프로시저인 경우)Statement까지 단계적으로 펼쳐 본다.

**Why this priority**: 읽기 없이는 편집 대상조차 식별할 수 없다. 이 스토리만으로도 "분석 결과를 사람이 확인한다"는 MVP가 성립한다.

**Independent Test**: Neo4j에 분석 데이터가 적재된 상태에서 `GET /robo/schema/tables`로 목록을, `.../tables/{name}/columns`로 컬럼을, `.../tables/{name}/references`로 참조를, `.../procedures/{name}/statements`로 Statement를, `GET /robo/schema/relationships`로 관계를 조회해 모두 반환되면 통과.

**Acceptance Scenarios**:

1. **Given** 적재된 테이블들, **When** 사용자가 `search`/`schema` 필터로 테이블 목록을 조회, **Then** 이름/설명 부분일치 + 스키마 일치로 필터된 테이블이 `column_count`와 함께 반환된다.
2. **Given** 한 테이블, **When** 컬럼을 조회, **Then** 각 컬럼의 dtype·nullable·description·analyzed_description이 반환된다(schema 미지정/`public`이면 이름·fqn으로만 매칭).
3. **Given** 한 프로시저, **When** Statement를 조회, **Then** 각 Statement의 라인 범위·타입·AI 설명(`ai_description`)이 라인 순으로 반환된다.

---

### User Story 2 - 관계 추가/삭제 (Priority: P2)

분석가가 자동 추론으로 누락된 테이블 간 관계(FK 등)를 직접 추가하거나, 잘못 추론된 관계를 삭제한다.

**Why this priority**: 관계는 그래프 탐색·리니지의 토대이며, 자동 FK 추론의 빈틈을 사람이 메우는 것이 본 기능의 차별 가치다. 단, 읽기(P1) 위에서 동작한다.

**Independent Test**: `POST /robo/schema/relationships`로 두 기존 테이블 간 관계를 만들고 `GET`으로 보인 뒤, `DELETE`로 제거되는지 확인하면 통과.

**Acceptance Scenarios**:

1. **Given** 두 기존 테이블, **When** from/to 테이블·컬럼·관계타입으로 관계 추가 요청, **Then** 관계가 `source='user'`로 생성(MERGE)되고 성공 응답을 받는다.
2. **Given** 존재하지 않는 from/to 테이블, **When** 관계 추가 요청, **Then** 404("테이블을 찾을 수 없습니다.")를 받는다.
3. **Given** 기존 관계, **When** from/to 테이블·컬럼으로 삭제 요청, **Then** 삭제된 관계 수가 반환된다.

---

### User Story 3 - 테이블/컬럼 설명 편집 (Priority: P2)

분석가가 테이블 또는 컬럼의 사람용 설명(`description`)을 직접 수정해 카탈로그 품질을 끌어올린다.

**Why this priority**: 설명 보정은 데이터 카탈로그의 핵심 산출물이지만 읽기·관계가 갖춰진 위에서 의미가 있어 P2.

**Independent Test**: `PUT /robo/schema/tables/{name}/description`와 `PUT /robo/schema/tables/{name}/columns/{col}/description`로 설명을 바꾼 뒤 재조회하면 새 값과 `description_source='user'`가 보이면 통과.

**Acceptance Scenarios**:

1. **Given** 한 테이블, **When** 새 설명으로 PUT(헤더 `X-API-Key` 포함), **Then** `description`이 갱신되고 `description_source='user'`로 표시되며, API 키가 있으면 임베딩도 재생성된다.
2. **Given** 한 컬럼, **When** 새 설명으로 PUT, **Then** 컬럼 `description`이 갱신되고 `description_source='user'`로 표시된다.
3. **Given** `X-API-Key` 미포함, **When** 설명 PUT, **Then** 400("X-API-Key 헤더가 필요합니다.")을 받는다.

---

### Edge Cases

- `references` 조회 시 `schema` 쿼리 파라미터가 없으면 → 400("schema 파라미터가 필요합니다.").
- 컬럼 조회에서 노드의 `schema`가 비어있는 legacy 데이터 → 이름/fqn 기준 fallback 매칭으로 0건 방지.
- 관계 추가는 MERGE라 동일 from/to·타입 재요청 시 중복 생성 없이 속성만 갱신.
- 설명 PUT에서 임베딩 재생성 실패 → RuntimeError로 500 처리(설명 자체는 이미 저장됨).
- 존재하지 않는 테이블/컬럼 설명 PUT → 404.
- 모든 조회/편집 실패는 `{detail, error_type}` 형태 본문과 함께 500으로 반환.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST `GET /robo/schema/tables`로 테이블 목록을 반환하며, `search`(이름·설명 부분일치), `schema`(정확일치), `limit`(기본 100) 필터를 지원해야 한다. 각 항목은 name·schema·datasource·description·description_source·analyzed_description·column_count를 포함한다.
- **FR-002**: System MUST `GET /robo/schema/tables/{name}/columns`로 컬럼 목록(name·dtype·nullable·description·description_source·analyzed_description)을 반환하고, `schema` 미지정 또는 `public`이면 이름·fqn 기준으로만 매칭해야 한다.
- **FR-003**: System MUST `GET /robo/schema/tables/{name}/references`로 해당 테이블/컬럼을 참조하는 프로시저 참조와 프레임워크 참조(`{references, framework_references}`)를 반환해야 하며, `schema` 파라미터가 없으면 400을 반환해야 한다.
- **FR-004**: System MUST `GET /robo/schema/procedures/{name}/statements`로 프로시저 하위 Statement(라인 범위·타입·summary·`ai_description`)를 라인 순으로 반환해야 한다.
- **FR-005**: System MUST `GET /robo/schema/relationships`로 테이블 간 관계 목록을 반환해야 하며, 대상 관계 타입은 `FK_TO_TABLE`·`ONE_TO_ONE`·`ONE_TO_MANY`·`MANY_TO_ONE`·`MANY_TO_MANY`이고 from/to 테이블·스키마·컬럼·타입·설명을 포함한다.
- **FR-006**: Users MUST be able to `POST /robo/schema/relationships`로 두 기존 테이블 간 관계를 추가할 수 있어야 하며(MERGE, `source='user'` 기록), 테이블이 없으면 404를 반환해야 한다.
- **FR-007**: Users MUST be able to `DELETE /robo/schema/relationships`로 from/to 테이블·컬럼이 일치하는 관계를 삭제하고 삭제 건수를 받을 수 있어야 한다.
- **FR-008**: Users MUST be able to `PUT /robo/schema/tables/{name}/description`로 테이블 설명을 수정할 수 있어야 하며, 저장 시 `description_source='user'`로 표시하고 `X-API-Key`가 있으면 테이블 임베딩을 재생성해야 한다.
- **FR-009**: Users MUST be able to `PUT /robo/schema/tables/{name}/columns/{col}/description`로 컬럼 설명을 수정할 수 있어야 하며, 저장 시 `description_source='user'`로 표시해야 한다.
- **FR-010**: System MUST 설명 편집 엔드포인트(테이블/컬럼 description)에서 `X-API-Key` 헤더를 요구하고, 없으면 400을 반환해야 한다.
- **FR-011**: System MUST 사람이 편집한 `description`/`description_source`를 분석기가 채운 `analyzed_description`과 **분리**해 보존해야 한다. 즉 사용자 편집이 분석 결과를 덮어쓰지 않고, 두 값이 응답에 함께 노출된다.
- **FR-012**: System MUST 모든 요청에서 Electron `X-Neo4j-*` 헤더(있을 경우)를 해당 요청의 Neo4j 연결로 적용하고, 없으면 설정(.env) 연결로 폴백해야 한다.

### Key Entities *(include if feature involves data)*

- **Table** (`:Table`): 분석으로 적재된 테이블. 주요 속성 name·schema·fqn·datasource·`description`(사람용, 편집 대상)·`description_source`('user'/'sample_data_inference' 등)·`analyzed_description`(분석기 산출, 읽기 전용)·`embedding`.
- **Column** (`:Column`): `(:Table)-[:HAS_COLUMN]->(:Column)`. 속성 name·dtype·nullable·`description`(편집 대상)·`description_source`·`analyzed_description`.
- **Relationship** (테이블 간): `FK_TO_TABLE` 및 `ONE_TO_*`/`MANY_TO_*` 타입의 `(:Table)-[r]->(:Table)`. 속성 `sourceColumn`·`targetColumn`·`description`·`source`('user'면 사람이 추가). 추가/삭제 대상.
- **SQL_STATEMENT** (프로시저 Statement): 프로시저/함수 하위에서 `(p)-[:PARENT_OF*]->(s)`로 도달하는 Statement 노드. 속성 start_line·end_line·type·summary·`ai_description`(AI 생성, 읽기 전용).
- **편집 가능 필드(요약)**: 테이블 `description`, 컬럼 `description` — 두 곳 모두 편집 시 `description_source`가 `'user'`로 기록된다. `analyzed_description`과 Statement의 `ai_description`은 본 기능에서 **편집하지 않는다**.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 사용자가 테이블 목록 → 컬럼 → 참조 → 관계 → (프로시저)Statement까지 추가 도구 없이 API만으로 끝까지 펼쳐 볼 수 있다.
- **SC-002**: 두 기존 테이블 간 관계를 추가한 직후 `GET /robo/schema/relationships`에서 그 관계가 즉시 보인다.
- **SC-003**: 테이블/컬럼 설명을 PUT한 직후 재조회 시 새 설명과 `description_source='user'`가 반영되고, `analyzed_description`은 변하지 않는다.
- **SC-004**: 잘못된 입력(존재하지 않는 테이블, schema 누락, API 키 누락)에 대해 적절한 4xx(400/404)와 명확한 사유 메시지를 받는다.

## Assumptions

- Neo4j 그래프는 robo-data-analyzer가 선행 분석으로 적재한 상태이며, 본 기능은 그 위에서 동작한다(빈 그래프면 빈 목록 반환).
- 모든 경로는 `settings.api_prefix`(예: `/robo`) 하위에 마운트된다.
- 사람이 입력하는 `description`은 분석기의 `analyzed_description`과 별개의 필드로, 서로 덮어쓰지 않는다.
- 관계 타입은 Cypher 파라미터화 불가로 화이트리스트(`FK_TO_TABLE`·`ONE_TO_*`·`MANY_TO_*`) 내 값만 안전하게 다뤄진다고 가정한다.
- `X-API-Key`는 임베딩/LLM 연동(OpenAI)용으로, 설명 편집·벡터화 계열 엔드포인트에서 요구된다.
- 시멘틱 검색·벡터화·DW 스타스키마·메타데이터 보강 엔드포인트는 본 스펙(브라우즈/편집) 범위 밖이다.

# Feature Specification: Graph Query & Data Lifecycle

**Feature Branch**: `001-graph-query-lifecycle`

**Created**: 2026-06-15

**Status**: Backfilled (reverse-engineered)

**Input**: 기존 구현으로부터 역설계. robo-data-catalog(FastAPI, 포트 5503)의 그래프 조회·생애주기 엔드포인트 4종 — `GET /robo/check-data/`, `GET /robo/graph/`, `GET /robo/graph/related-tables/{table_name}`, `DELETE /robo/delete/`.

## User Scenarios & Testing *(mandatory)*

<!-- 우선순위(P1=가장 중요). 각 스토리는 독립 테스트 가능. -->

### User Story 1 - 그래프 데이터 존재 확인 (Priority: P1)

프런트엔드(또는 Electron 데스크톱)가 카탈로그 화면을 열 때, robo-data-analyzer가 Neo4j에 그래프를 이미 만들어 두었는지 먼저 확인해야 한다. 데이터가 없으면 "분석을 먼저 실행하세요" 안내를, 있으면 곧바로 그래프 화면을 띄운다.

**Why this priority**: 다른 모든 조회의 진입점(gate)이다. 이 한 호출만으로도 "분석 결과가 준비됐는가"라는 핵심 가치를 단독 제공한다.

**Independent Test**: 빈 Neo4j에서 `GET /robo/check-data/` → `{"hasData": false, "nodeCount": 0}`. analyzer가 인제스천을 한 뒤 같은 호출 → `hasData: true`, 양수 `nodeCount`.

**Acceptance Scenarios**:

1. **Given** Neo4j에 노드가 0개, **When** `GET /robo/check-data/` 호출, **Then** `{"hasData": false, "nodeCount": 0}` 반환
2. **Given** analyzer가 노드를 적재한 상태, **When** 같은 호출, **Then** `hasData: true`이고 `nodeCount`가 실제 전체 노드 수와 일치

---

### User Story 2 - 전체 그래프 및 관련 테이블 조회 (Priority: P1)

분석가가 카탈로그 화면에서 analyzer가 만든 코드·데이터 그래프 전체(노드+관계)를 보고, 특정 테이블을 클릭하면 그 테이블과 FK/프로시저로 엮인 테이블들만 추려서 본다.

**Why this priority**: 카탈로그의 본질적 읽기 기능. 전체 그래프 렌더링과 테이블 중심 탐색이 사용자가 실제로 쓰는 주 화면이다.

**Independent Test**: 적재된 DB에서 `GET /robo/graph/` → `{"Nodes":[...], "Relationships":[...]}`(벡터/임베딩 속성 제외됨). `GET /robo/graph/related-tables/{name}` → 기준 테이블과 FK_TO_TABLE·CO_REFERENCED로 연결된 테이블 목록.

**Acceptance Scenarios**:

1. **Given** 적재된 그래프, **When** `GET /robo/graph/`, **Then** 모든 노드(`Node ID`/`Labels`/`Properties`)와 모든 관계(`Type`/시작·끝 노드 ID/`Properties`)를 반환하고, 고차원 숫자 벡터·`*vector*`/`*embedding*` 속성은 응답에서 제외
2. **Given** `ORDERS` 테이블이 다른 테이블을 FK로 참조, **When** `GET /robo/graph/related-tables/ORDERS`, **Then** `base_table`, 연결 테이블 `tables[]`, `FK_TO_TABLE`/`CO_REFERENCED` `relationships[]` 반환
3. **Given** 존재하지 않는 테이블명, **When** related-tables 호출, **Then** `tables`/`relationships`가 빈 배열(에러 아님)

---

### User Story 3 - 데이터 삭제(생애주기 리셋) (Priority: P2)

새 분석을 시작하기 전에 분석가가 이전 결과를 비운다. Neo4j 그래프만 비우거나(파일 유지), 파일 시스템까지 함께 정리한다. 단, 데이터소스 등록 정보(`DataSource` 노드)는 인제스천과 무관하므로 보존한다.

**Why this priority**: 재분석 워크플로의 위생 단계. 조회 기능 없이도 단독으로 호출·검증 가능하지만 일상 빈도는 조회보다 낮다.

**Independent Test**: 적재된 DB에서 `DELETE /robo/delete/` → 메시지 반환, 이후 `check-data`가 `hasData:false`(단 `DataSource` 노드만 남을 수 있음). `?include_files=true`면 데이터 디렉터리도 재생성.

**Acceptance Scenarios**:

1. **Given** 적재된 그래프, **When** `DELETE /robo/delete/`(기본), **Then** `DataSource`를 제외한 모든 노드를 DETACH DELETE하고 "파일은 유지됨" 메시지 반환
2. **Given** `?include_files=true`, **When** 삭제, **Then** 데이터 디렉터리를 삭제 후 재생성하고 "파일 + Neo4j" 메시지 반환

### Edge Cases

- Neo4j가 비어 있을 때: 모든 조회는 빈 배열/0을 반환하며 실패하지 않는다.
- 노드/관계에 대용량 임베딩(벡터) 속성이 있을 때: 응답 비대화·렌더 부하 방지를 위해 `_sanitize_graph_properties`가 제외한다. DB의 실제 vector 속성 키는 `db.propertyKeys()`로 발견해 map projection에서 null로 덮어쓴다.
- related-tables의 테이블명: `name` 정확 일치 또는 `fqn`의 끝 일치(`ENDS WITH`)로 매칭 — 스키마 접두사 없이도 찾는다.
- Neo4j 연결 실패/쿼리 오류: `RuntimeError`로 감싸 HTTP 500 + `{detail, error_type}` 본문 반환.
- 삭제는 `DataSource` 노드를 항상 보존한다(완전 초기화 아님).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST `GET /robo/check-data/`에서 전체 노드 수를 세어 `{"hasData": bool, "nodeCount": int}`를 반환해야 한다.
- **FR-002**: System MUST `GET /robo/graph/`에서 모든 노드와 모든 관계를 `{"Nodes":[...], "Relationships":[...]}` 형태로 반환해야 한다. 각 노드는 `Node ID`(elementId), `Labels`, `Properties`를 갖고, 각 관계는 `Relationship ID`, `Start Node ID`, `End Node ID`, `Type`, `Properties`를 갖는다.
- **FR-003**: System MUST 그래프 응답에서 대용량 벡터/임베딩 속성(키에 `vector`/`embedding` 포함하거나 길이 ≥128 숫자 배열)을 제외해야 한다.
- **FR-004**: System MUST `GET /robo/graph/related-tables/{table_name}`에서 `FK_TO_TABLE` 관계와 동일 프로시저 공동참조(`CO_REFERENCED`, `FROM`/`WRITES`+`PARENT_OF*` 경로로 도출)로 연결된 테이블을 `{base_table, tables[], relationships[]}`로 반환해야 한다.
- **FR-005**: System MUST related-tables 매칭 시 `Table.name` 정확 일치 또는 `Table.fqn ENDS WITH` 일치를 모두 허용해야 한다.
- **FR-006**: System MUST `DELETE /robo/delete/`에서 `DataSource` 라벨을 제외한 모든 노드를 DETACH DELETE해야 하며, `include_files=true`이면 데이터 디렉터리를 비우고 재생성해야 한다.
- **FR-007**: System MUST 이 그래프를 읽기 전용으로 소비해야 한다 — 그래프를 **쓰는 주체는 robo-data-analyzer**이고 카탈로그는 그 산출물을 조회·삭제만 한다(이 피처는 새 도메인 노드를 생성하지 않음).
- **FR-008**: System MUST Neo4j 연결을 요청별 `X-Neo4j-*` 헤더 override(Electron) → 없으면 `.env`(settings) 폴백 순으로 결정해야 한다.
- **FR-009**: System MUST 조회/삭제 실패 시 HTTP 500과 `{detail, error_type}` 본문을 반환해야 한다.

### Key Entities *(read-only; analyzer가 적재)*

- **MODULE / FUNCTION / VARIABLE / CONSTANT / PROCEDURE**: analyzer의 코드 분석 산출 노드(UPPERCASE 라벨). 코드 구조와 SQL 사용처를 표현. 관계: `HAS_FUNCTION`, `HAS_VARIABLE`, `PARENT_OF`, `CALLS`.
- **Table**: DB 테이블 노드(PascalCase `:Table`). 속성 `name`, `schema`/`schema_name`, `fqn`, `description`. analyzer가 식별·적재.
- **Column**: 컬럼 노드(`:Column`). `(:Table)-[:HAS_COLUMN]->(:Column)`.
- **SQL_STATEMENT**: 분석된 SQL 구문 노드. 테이블에 `FROM`/`READS`/`WRITES`로 연결.
- **DataSource**: 데이터소스 등록 노드. 삭제 시 항상 보존(인제스천과 무관).
- **관계**: `FK_TO_TABLE`(`(:Table)->(:Table)`, 속성 `sourceColumn`/`targetColumn`/`source`), `HAS_COLUMN`, `FROM`/`READS`/`WRITES`, `PARENT_OF`, `REFER_TO`, `FK_TO_COLUMN`.
- **그래프 응답 형태**: `Nodes`(각 `{Node ID, Labels[], Properties{}}`) + `Relationships`(각 `{Relationship ID, Start Node ID, End Node ID, Type, Properties{}}`). related-tables 응답은 `{base_table, tables[{name,schema,description}], relationships[{from_table,to_table,type,source,column_pairs[]}]}`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 빈 DB든 적재된 DB든 네 엔드포인트가 예외 없이 정상 응답(빈 결과는 빈 배열/0)을 돌려준다.
- **SC-002**: `GET /robo/graph/` 응답에 어떤 고차원 벡터/임베딩 속성도 포함되지 않는다(필터링 100%).
- **SC-003**: `related-tables`가 스키마 접두사 유무와 무관하게 `name` 또는 `fqn` 끝 일치로 대상 테이블을 찾아낸다.
- **SC-004**: `DELETE /robo/delete/` 후 `DataSource` 노드는 보존되고 그 외 노드 수는 0이 된다.

## Assumptions

- robo-data-analyzer가 Neo4j 그래프를 **이미 적재**했다고 가정한다. 카탈로그는 그 결과를 읽고/삭제만 하는 다운스트림 소비자다(contract: catalog READS what analyzer WROTE).
- 코드가 실제로 읽는 라벨 케이싱은 `:Table`/`:Column`(PascalCase)과 `:MODULE`/`:FUNCTION`/`:PROCEDURE`/`:VARIABLE`(UPPERCASE)이다. README의 `TABLE`(대문자) 표기는 사용하지 않는다.
- Electron 데스크톱은 요청 헤더 `X-Neo4j-*`로 연결을 주입하고, 브라우저/CLI/테스트는 `.env`(settings.neo4j) 폴백을 쓴다.
- 삭제는 분석 결과 리셋 용도이며 데이터소스 등록(`DataSource`)은 별도 생애주기로 보존된다.
- 이 피처는 새 Neo4j 스키마를 생성하지 않는다(읽기/삭제 전용).

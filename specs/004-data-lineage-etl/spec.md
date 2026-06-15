# Feature Specification: Data Lineage & ETL Analysis

**Feature Branch**: `004-data-lineage-etl`

**Created**: 2026-06-15

**Status**: Backfilled (reverse-engineered)

**Input**: Reverse-engineered from existing code — `service/data_lineage_service.py`, `analyzer/strategy/dbms/linking/lineage_analyzer.py`, `api/catalog_router.py` (lineage routes), `api/request_models.py`.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - 리니지 그래프 조회 (Priority: P1)

데이터 거버넌스 사용자(또는 호출 서비스)가 카탈로그에 이미 저장된 데이터 흐름(소스 테이블 → ETL 프로시저 → 타겟 테이블)을 한 번에 조회해, 어떤 데이터가 어디에서 와서 어디로 가는지 파악한다.

**Why this priority**: 리니지의 핵심 가치는 "이미 적재된 흐름을 보여주는 것"이다. 분석(US2)이 없어도 그래프 조회만으로 기존 ETL 흐름을 시각화·감사할 수 있으므로 MVP 의 중심이다. 또한 robo-data-analyzer 가 이 엔드포인트를 크로스서비스로 소비한다(아래 크로스서비스 노트 참고).

**Independent Test**: Neo4j 에 `DataSource`/`ETLProcess` 노드와 `DATA_FLOW_TO`/`TRANSFORMS_TO` 관계가 있는 상태에서 `GET /robo/lineage/` 를 호출하면 `nodes`/`edges`/`stats` 가 채워져 반환되는지로 단독 검증 가능.

**Acceptance Scenarios**:

1. **Given** Neo4j 에 DataSource·ETLProcess 노드와 흐름 관계가 존재할 때, **When** `GET /robo/lineage/` 를 호출하면, **Then** `{nodes, edges, stats}` 형태로 노드 목록(id/name/type/properties), 엣지 목록(id/source/target/type/properties), 통계(etlCount/sourceCount/targetCount/flowCount)를 반환한다.
2. **Given** 리니지 노드가 하나도 없을 때, **When** 조회하면, **Then** 빈 `nodes`/`edges` 와 0 값 통계를 200 으로 반환한다(오류 아님).
3. **Given** Neo4j 연결 실패가 발생할 때, **When** 조회하면, **Then** HTTP 500 과 `{detail, error_type}` 을 반환한다.

---

### User Story 2 - ETL SQL 에서 리니지 추출 (Priority: P2)

사용자가 ETL SQL 소스(프로시저/함수)를 제출하면, 시스템이 SQL 을 파싱해 소스 테이블과 타겟 테이블을 식별하고 source→target 리니지를 도출하여 Neo4j 에 저장한다.

**Why this priority**: US1 의 그래프를 채우는 공급 경로지만, 기존 그래프가 이미 있으면 조회는 독립적으로 동작하므로 P2.

**Independent Test**: `INSERT INTO TGT SELECT * FROM SRC` 같은 SQL 을 `POST /robo/lineage/analyze/` 로 보내면 `lineages` 에 source/target 가 분리되어 반환되는지로 검증.

**Acceptance Scenarios**:

1. **Given** `CREATE PROCEDURE` 안에 `INSERT INTO ... SELECT ... FROM ...` 가 있는 SQL, **When** `POST /robo/lineage/analyze/` 에 `sqlContent` 를 보내면, **Then** 각 프로시저별 `etl_name`·`source_tables`·`target_tables`·`operation_type` 을 담은 `lineages` 와 저장 `stats` 를 반환한다.
2. **Given** 소스와 타겟이 모두 식별된 프로시저, **When** 분석하면, **Then** 해당 프로시저는 ETL 패턴으로 표시되고 Neo4j 에 기존 PROCEDURE/FUNCTION·Table 노드를 매칭하여 ETL_READS / ETL_WRITES / DATA_FLOWS_TO 관계를 MERGE 한다.

---

### User Story 3 - 이름 표기·DBMS 옵션 적용 (Priority: P3)

사용자가 대상 DBMS(예: oracle)와 식별자 대소문자 처리(uppercase/lowercase/original)를 지정해, 저장될 테이블/프로시저 이름 표기를 기존 카탈로그 노드와 일치시킨다.

**Why this priority**: 매칭 정확도를 높이는 보조 옵션. 기본값(oracle/original)으로도 동작하므로 P3.

**Independent Test**: 동일 SQL 을 `nameCaseOption=uppercase` 로 보냈을 때 매칭 쿼리가 대소문자 무시 매칭으로 기존 Table 노드를 찾는지로 검증.

**Acceptance Scenarios**:

1. **Given** `nameCaseOption` 미지정, **When** 분석하면, **Then** `original` 표기가 적용된다.
2. **Given** `dbms` 미지정, **When** 분석하면, **Then** `oracle` 가 기본 적용된다.

---

### Edge Cases

- SQL 에 `CREATE PROCEDURE/FUNCTION` 이 전혀 없으면 파일 전체를 하나의 ETL 단위(이름=fileName 또는 `UNKNOWN`)로 분석한다.
- 소스 테이블 목록에 타겟 테이블과 동일한 이름이 있으면 자기참조로 보고 소스에서 제외한다.
- `dual`, `sysdate`, `information_schema`, `pg_catalog` 등 시스템 테이블/함수는 소스·타겟 모두에서 제외한다.
- 소스 또는 타겟이 하나도 없는 프로시저는 `lineages` 에 포함되지 않는다(저장도 스킵). 단, 타겟이 2개 이상이면 소스가 없어도 ETL 로 간주.
- 그래프 조회 시 `DataSource` 노드의 `type` 속성으로 SOURCE/TARGET 을 구분하며, 속성이 없으면 `SOURCE` 로 폴백한다.
- Neo4j 에 매칭되는 기존 Table 노드가 없으면 해당 READS/WRITES/FLOW 관계는 생성되지 않는다(MATCH 실패는 무음).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST `GET /robo/lineage/` 에서 Neo4j 의 `DataSource`·`ETLProcess` 노드와 `DATA_FLOW_TO`·`TRANSFORMS_TO` 관계를 조회해 `{nodes, edges, stats}` 로 반환해야 한다.
- **FR-002**: System MUST 응답 노드의 `type` 을 정규화해야 한다 — DataSource 는 그 `properties.type`(SOURCE/TARGET) 으로, ETLProcess 는 `ETL` 로 표기.
- **FR-003**: System MUST `stats` 에 `etlCount`·`sourceCount`·`targetCount`·`flowCount` 를 집계해 포함해야 한다.
- **FR-004**: System MUST `POST /robo/lineage/analyze/` 에서 `sqlContent`(필수)·`fileName`·`dbms`·`nameCaseOption` 을 받아 SQL 을 프로시저/함수 단위로 분할 분석해야 한다.
- **FR-005**: System MUST 타겟 테이블을 INSERT/MERGE/UPDATE/DELETE 절에서, 소스 테이블을 FROM/JOIN/USING 절에서 추출해야 한다.
- **FR-006**: System MUST 소스·타겟이 모두 있는(또는 타겟 2개 이상인) 프로시저를 ETL 패턴으로 식별하고 `operation_type` 을 부여해야 한다.
- **FR-007**: System MUST 식별된 ETL 을 Neo4j 에 저장 시 기존 PROCEDURE/FUNCTION 노드와 Table 노드를 대소문자 무시로 매칭하여 `ETL_READS`(소스→ETL)·`ETL_WRITES`(ETL→타겟)·`DATA_FLOWS_TO`(소스→타겟) 관계를 MERGE 해야 한다.
- **FR-008**: System MUST 시스템 테이블·자기참조를 소스·타겟에서 제외해야 한다.
- **FR-009**: System MUST `nameCaseOption`(uppercase/lowercase/original, 기본 original)·`dbms`(기본 oracle) 를 적용해야 한다.
- **FR-010**: System MUST 두 엔드포인트의 오류를 HTTP 500 + `{detail, error_type}` 형태로 반환해야 한다.
- **FR-011**: System MUST 매 요청마다 `X-Neo4j-*` 헤더(Electron override)를 contextvar 로 반영하고, 없으면 `.env` 설정으로 폴백해야 한다.

### Cross-Service Contract Note

robo-data-analyzer 의 DBMS 후처리 스텝 `enrich_lineage_from_catalog_step`(analyzer spec 012)이 카탈로그의 리니지 엔드포인트를 HTTP 로 소비한다. 분석기는 `{catalog.base_url}/lineage` 를 호출해 응답의 흐름 항목을 코드의 실제 Function 과 매칭하여 `ReadsWrites` 엣지(READS/WRITES)를 생성하고, 매칭 실패 시 `__catalog__:` prefix 가상 노드로 보존한다. 즉 카탈로그는 리니지의 "단일 진실" 공급자이고 분석기는 소비자이다.

> ⚠️ 계약 불일치(코드 기준 발견): 카탈로그 `GET /robo/lineage/` 는 `{nodes, edges, stats}` 를 반환하지만, 분석기 스텝은 응답에서 `data["flows"]` 와 각 flow 의 `source`/`table_fqn`/`relation`/`dml_type` 필드를 읽는다. 두 서비스의 응답 스키마가 다르다 — 별도 `flows` 형태 엔드포인트가 있거나, 한쪽이 미정렬 상태일 수 있으므로 계약 정합 확인이 필요하다. (Assumptions 참조)

### Key Entities *(include if feature involves data)*

- **DataSource**: 데이터의 소스/타겟 테이블을 나타내는 리니지 노드. 속성 `type`(SOURCE 또는 TARGET)·`name`. 그래프 조회의 노드로 노출.
- **ETLProcess**: 소스에서 데이터를 읽어 타겟에 쓰는 ETL 프로시저/함수. 그래프에서 `type=ETL` 로 표기. 분석 시에는 기존 PROCEDURE/FUNCTION 노드에 `is_etl`·`etl_operation`·`etl_source_count`·`etl_target_count` 플래그로 표현.
- **lineage edges (READS/WRITES)**: ETL 과 테이블 간 흐름 관계.
  - `ETL_READS`: ETL → 소스 Table (읽기)
  - `ETL_WRITES`: ETL → 타겟 Table (쓰기)
  - `DATA_FLOWS_TO`: 소스 Table → 타겟 Table (ETL 경유 직접 흐름, `via_etl` 속성 보유)
  - 그래프 조회에서는 `DATA_FLOW_TO`·`TRANSFORMS_TO` 타입의 관계만 엣지로 노출.
- **`/lineage` 응답 형태**: `{ nodes: [{id, name, type, properties}], edges: [{id, source, target, type, properties}], stats: {etlCount, sourceCount, targetCount, flowCount} }`.
- **`/lineage/analyze/` 응답 형태**: `{ lineages: [{etl_name, source_tables[], target_tables[], operation_type}], stats: {etl_nodes, etl_reads, etl_writes, data_flows, matched_tables} }`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 리니지가 적재된 그래프에서 `GET /robo/lineage/` 가 노드·엣지·통계를 단일 응답으로 반환한다(누락 0).
- **SC-002**: `INSERT INTO ... SELECT ... FROM ...` 형태 ETL SQL 제출 시 소스·타겟 테이블이 100% 분리되어 `lineages` 에 나타난다.
- **SC-003**: 식별된 ETL 의 ETL_READS/ETL_WRITES/DATA_FLOWS_TO 관계가 기존 Table 노드 존재 시 멱등하게(MERGE) 저장되어 재호출에도 중복이 생기지 않는다.
- **SC-004**: 시스템 테이블·자기참조가 리니지 결과에 0건 포함된다.
- **SC-005**: robo-data-analyzer 가 `{base_url}/lineage` 호출로 카탈로그 리니지를 받아 `ReadsWrites` 엣지로 변환할 수 있다(크로스서비스 소비 가능).

## Assumptions

- Neo4j 가 가용하며 `DataSource`/`ETLProcess`/`Table`/`PROCEDURE`/`FUNCTION` 라벨 및 관계 스키마가 존재한다고 가정.
- SQL 파싱은 정규식 기반(완전한 SQL 파서 아님)이므로 비정형 SQL·서브쿼리·동적 SQL 은 일부 누락될 수 있다 — best-effort 추출로 간주.
- 테이블 매칭은 이름 기준(스키마 선택적, 대소문자 무시)으로, 동명이표(同名異表)는 구분하지 않는다.
- DBMS 기본값은 oracle, 이름 표기 기본값은 original 이다.
- 크로스서비스 계약: 분석기가 기대하는 `flows`(source/table_fqn/relation/dml_type) 응답 형태와 카탈로그의 현재 `{nodes, edges, stats}` 응답 형태가 정합되어야 한다 — 현재 코드 기준 불일치가 있으며, 이 정합은 본 피처 범위 밖의 후속 확인 대상으로 둔다.
- README 보다 코드를 우선 신뢰하여 본 명세를 역설계했다.

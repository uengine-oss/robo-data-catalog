# Feature Specification: Sample Context & FK Inference Provider

**Feature Branch**: `006-sample-context-provider`

**Created**: 2026-06-15

**Status**: Backfilled (reverse-engineered)

**Input**: Existing-code reverse engineering: robo-data-catalog 가 (1) analyzer 가 분석 세션당 1회 호출하는 `sample-context` 엔드포인트로 실제 테이블 샘플 행·컬럼 메타를 제공하고, (2) 메타데이터 보강 시 값 overlap·컬럼명 유사도·distinct 충분도를 통합한 confidence 기반 foreign-key 후보를 추론·영속화하며 테이블 설명을 생성한다.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - analyzer 가 분석 도중 실제 테이블 샘플을 요청 (Priority: P1)

robo-data-analyzer 가 코드 분석 Phase 2 Linking 을 마치면, 코드에서 추출한 테이블명 목록을 한 번에 catalog 로 전달한다. catalog 는 각 추출명을 DB 에 실제 등록된 테이블명에 유사도로 해소(resolve)하고, 매칭된 테이블마다 컬럼 메타 + 실제 DB 의 샘플 행 몇 줄을 묶어 돌려준다. analyzer 는 이 실데이터를 LLM 컨텍스트로 써서 의미 추론 정확도를 높인다.

**Why this priority**: 이 엔드포인트가 본 피처의 핵심 cross-service 계약이다. analyzer spec 012/008(step5 `fetch_table_samples`)이 이 응답 모양에 직접 의존하므로, 요청·응답 shape 이 바뀌면 analyzer 가 깨진다. 다른 모든 시나리오보다 우선한다.

**Independent Test**: 등록된 datasource 와 (오타 포함) 테이블명 목록으로 `POST /robo/tables/sample-context` 를 호출해, 응답이 요청 원본명을 key 로 하는 map 이고, 매칭된 key 는 `resolved/score/columns/sample_rows` 를 담고 매칭 실패 key 는 `null` 임을 단독으로 검증.

**Acceptance Scenarios**:

1. **Given** datasource 에 `public.ZngmCommCdDtl` 테이블이 등록되어 있고, **When** analyzer 가 `table_names=["zngm_comm_cd_dtl"]` 로 호출하면, **Then** 응답 `{"zngm_comm_cd_dtl": {resolved, score, columns[], sample_rows[]}}` 가 반환된다(camelCase·구분자·스키마 차이를 정규화 후 매칭).
2. **Given** 요청 목록 중 일부 이름이 DB 어떤 테이블과도 임계값 이상 유사하지 않을 때, **When** 호출하면, **Then** 그 key 의 값은 `null` 이고 나머지 매칭 key 는 정상 채워진다(부분 실패가 전체를 막지 않음).
3. **Given** `sample_limit=5`, **When** 매칭된 테이블의 샘플을 조회하면, **Then** 최대 5행이 `SELECT * ... LIMIT 5` 로 text2sql 백엔드 경유 반환된다.

---

### User Story 2 - 메타데이터 보강 시 FK 후보 자동 추론 (Priority: P2)

운영자가 스키마 적재 후 메타데이터 보강을 실행하면, catalog 는 description 생성에 이어 FK 후보를 자동 추론한다. PostgreSQL 내부에서 값 overlap 으로 1차 후보를 뽑고, Python 에서 컬럼명 유사도·overlap 비율·distinct 충분도를 가중평균한 단일 confidence 점수로 채택/거부를 결정한 뒤, 채택분을 Neo4j 에 `FK_TO_COLUMN`/`FK_TO_TABLE` 관계로 영속화한다.

**Why this priority**: 그래프에 추론 FK 관계를 채워 후속 lineage·관계 뷰의 품질을 올린다. P1 계약과 독립이며 보강 파이프라인의 일부라 P2.

**Independent Test**: 대상 PG 에 `public.infer_fk_candidates` 함수가 설치된 상태에서 `POST /robo/schema/enrich-metadata` 를 호출해, NDJSON 스트림에 `fk_query_start → fk_query_done → fk_filter_applied → fk_persisted` 이벤트가 순서대로 나오고, confidence < 임계 후보가 `rejected_examples` 로 분리됨을 검증.

**Acceptance Scenarios**:

1. **Given** PG 에 FK 추론 함수가 설치돼 있고, **When** 보강을 실행하면, **Then** confidence ≥ 0.85 후보만 Neo4j `FK_TO_COLUMN`·`FK_TO_TABLE` 로 `source='inferred'` 와 함께 MERGE 된다.
2. **Given** 함수가 미설치, **When** 보강을 실행하면, **Then** `fk_error` 이벤트로 설치 안내(`scripts/install_fk_function.sql`)를 내보내고 전체 보강은 중단 없이 종료한다.

---

### User Story 3 - 테이블 설명 자동 생성 (Priority: P3)

보강 Phase 1 에서 description 이 비어 있는 테이블에 대해, catalog 는 샘플 10행 + 컬럼 메타를 LLM 에 주어 테이블/컬럼 설명을 생성·영속화한다.

**Why this priority**: analyzer 가 `sample-context` 응답의 `columns[].description` 으로도 일부 소비하나 보강 자체는 부가 가치라 P3.

**Independent Test**: description 이 빈 테이블이 있는 datasource 로 보강 호출 시 `table_done`/`phase_done` 이벤트와 description 영속 수가 보고됨을 검증.

**Acceptance Scenarios**:

1. **Given** description 이 NULL/''/'N/A' 인 테이블, **When** 보강하면, **Then** 샘플 기반 생성 설명이 영속되고 `table_done.description_persisted=true` 가 스트리밍된다.

---

### Edge Cases

- datasource 에 등록 테이블이 0건 → 모든 요청 key 를 `null` 로 채워 반환(예외 아님).
- `datasource` 누락 → 400 (ValueError). `table_names` 빈 배열 → 빈 map `{}`.
- 한 테이블의 샘플 조회 실패(권한·연결) → 해당 `sample_rows` 는 `[]`, 다른 테이블 결과는 보존.
- 동일 정규화로 충돌하는 후보가 여럿 → exact 정규화 일치 우선, 없으면 rapidfuzz WRatio 최고점 1건만 채택.
- FK: distinct 가 작은 동명 컬럼(예: `BR_GUBUN`) → distinct 페널티로 confidence 가 임계 밑이면 거부.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST expose `POST /robo/tables/sample-context` (prefix `/robo`, port 5503) 를 cross-service 계약으로 제공한다.
- **FR-002**: 요청 본문 MUST 은 `datasource`(str, min_length 1), `table_names`(List[str], min_length 1), `sample_limit`(int, 기본 5, 1~20), `similarity_threshold`(float, 기본 85.0, 50.0~100.0) 필드를 가진다.
- **FR-003**: 응답 MUST 은 요청 원본 테이블명을 key 로 하는 map 이며, 각 값은 `{resolved, score, columns, sample_rows}` 객체 또는 `null`(매칭 실패)이다.
- **FR-004**: 매칭값의 `columns` MUST 은 Neo4j 의 `{name, dtype, description, is_primary_key, nullable}` 항목 배열, `sample_rows` MUST 은 실제 DB 행(dict) 배열, `score` MUST 은 0~100 유사도이다.
- **FR-005**: System MUST 은 요청명·DB 테이블명을 정규화(스키마 strip + camelCase 분해 + 구분자 통일 + 소문자)한 뒤 exact 우선·rapidfuzz WRatio(`score_cutoff=similarity_threshold`)로 해소한다.
- **FR-006**: 등록 테이블 목록은 Neo4j `(:Table)` 에서 조회하고, 샘플 행은 text2sql(MindsDB) 백엔드를 `SELECT * FROM <table> LIMIT <sample_limit>` 로 경유 조회해야 한다.
- **FR-007**: System MUST 은 매칭 테이블별 컬럼·샘플 조회를 `asyncio.Semaphore`(기본 동시성 5, `FK_CONCURRENCY`)로 제한한 병렬로 수행한다.
- **FR-008**: System MUST 은 `POST /robo/schema/enrich-metadata` 에서 description 생성과 FK 추론을 NDJSON(`application/x-ndjson`) 스트림으로 제공한다.
- **FR-009**: FK 추론 MUST 은 PG `public.infer_fk_candidates(schema, overlap_threshold, min_src_distinct)` 로 1차 후보를 뽑고, `confidence = name_similarity×0.5 + overlap_ratio×0.2 + distinct_factor×0.3`(distinct 포화=5)로 단일 임계(기본 0.85) 채택을 결정한다.
- **FR-010**: 채택 FK MUST 은 Neo4j `(:Column)-[:FK_TO_COLUMN]->(:Column)` 및 `(:Table)-[:FK_TO_TABLE]->(:Table)` 로 `source='inferred'`·confidence·모든 신호값과 함께 MERGE 영속화한다.
- **FR-011**: FK 추론 함수 미설치 시 System MUST 은 `fk_error` 이벤트로 설치 안내를 내보내고 전체 보강을 중단 없이 마쳐야 한다.

### Key Entities *(include if feature involves data)*

- **SampleContextRequest (계약)**: analyzer→catalog batch 요청. `datasource`, `table_names`, `sample_limit`, `similarity_threshold`. 응답 map 의 key 는 여기 보낸 `table_names` 원본값과 동일.
- **Sample Context 응답값**: 테이블 1건의 매칭 결과. `resolved`(실제 매칭 테이블 fqn), `score`(유사도), `columns`(컬럼 메타 배열), `sample_rows`(실제 행 배열). 매칭 실패 시 `null`.
- **Sample Row**: 실제 DB 한 행의 `{컬럼명: 값}` dict. text2sql 경유 `SELECT * LIMIT N` 산출.
- **Datasource**: MindsDB 에 등록된 데이터원 이름. Neo4j Table 조회 필터이자 text2sql 쿼리 대상.
- **Column 메타**: `{name, dtype, description, is_primary_key, nullable}` (Neo4j `(:Column)`).
- **FK Candidate**: 추론된 외래키 후보. 신호 `name_similarity`(0~100), `overlap_ratio`(0~1), `src_distinct`, 파생 `confidence`(0~1). 채택 시 `FK_TO_COLUMN`/`FK_TO_TABLE` 관계로 영속.

### Cross-Service Contract Note

이 엔드포인트는 robo-data-analyzer 의 step5 `fetch_table_samples`(analyzer spec 012/008)가 읽는 단일 진실 계약이다. **경로 `POST /robo/tables/sample-context`, 요청 4필드, 응답 `Dict[원본테이블명, {resolved,score,columns,sample_rows} | null]`** 중 어느 하나라도 바꾸면 analyzer 의 의미 추론 단계가 깨진다. 변경 시 양 서비스 스펙을 동시에 갱신할 것.

**FK 책임 경계 note**: analyzer 의 DDL/스키마 추출 단계는 의도적으로 "후보(candidate) FK" 추론을 하지 **않는다**. 그 추론은 본 피처(catalog 의 `FkInferenceService`)가 값 overlap·이름 유사도·distinct 기반 confidence 로 담당한다 — 즉 candidate FK 의 home 은 catalog 다.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: analyzer 가 분석 세션당 `sample-context` 를 1회 호출해 식별 테이블 batch 의 샘플·컬럼을 한 응답으로 받는다(N회 왕복 불필요).
- **SC-002**: 표기 차이(camelCase/스키마 접두/구분자)만 있는 테이블명은 similarity_threshold 기본값(85)에서 정확히 해소된다.
- **SC-003**: 부분 매칭 실패가 발생해도 매칭 가능한 모든 테이블 결과가 반환된다(실패 key 만 `null`).
- **SC-004**: confidence ≥ 0.85 FK 후보만 영속화되어, distinct 작은 우연·무관 컬럼 우연 매칭이 그래프에 들어가지 않는다.

## Assumptions

- analyzer 가 `table_names` 로 보내는 값은 코드에서 추출한 원본(정규화 전) 문자열이며, 응답 key 매핑은 이 원본값 기준이다.
- 대상 테이블·컬럼 메타는 이미 Linking 단계에서 Neo4j `(:Table)`/`(:Column)` 으로 적재돼 있다.
- 샘플 행 조회와 FK overlap 조회는 text2sql(MindsDB) 백엔드(`TEXT2SQL_API_URL`)를 통해 수행된다.
- FK 추론은 대상 PG 에 `public.infer_fk_candidates` 함수가 사전 설치(`scripts/install_fk_function.sql`)됐다고 가정한다.
- Neo4j 연결은 Electron `X-Neo4j-*` 헤더 override 또는 `.env` 폴백을 따른다(기존 connection_context 재사용).
- description 생성은 LLM API 키가 제공될 때만 동작하며, 없으면 보강을 skip 한다.

# Feature Specification: 데이터 리니지 전략무관 범용화 (framework 지원)

**Feature Branch**: `007-strategy-agnostic-lineage`

**Created**: 2026-06-26

**Status**: Draft

**Input**: 데이터 리니지(Lineage 탭)가 현재 DBMS ETL 분석으로만 채워져 framework 분석은 빈 화면. 분석 그래프의 TABLE + 함수 READS/WRITES 에서 리니지를 도출해 **전략 무관**으로 Lineage 탭을 채운다.

## 배경 (왜)

데이터 리니지 = "데이터가 어디서 와서 어디로 가나"(테이블↔테이블 흐름). 프론트 **Lineage 탭**은 `source 테이블 → ETL 프로세스 → target 테이블` 3단 모델로 시각화하며, catalog `/robo/lineage/` 가 데이터를 준다.

현재 그 데이터는 **DBMS 전용 ETL SQL 분석**(`:DataSource`/`:ETLProcess`/`DATA_FLOW_TO` 별도 모델, spec 004)으로만 생성된다. 그래서 **framework 분석은 Lineage 탭이 항상 비어 있다**(실측: framework 데이터에서 `/robo/lineage/` = nodes 0·edges 0·etlCount 0).

그러나 분석 그래프는 **전략 무관으로 함수↔테이블 R/W 를 이미 보유**한다: framework `link_deterministic`(코드 SQL grep), dbms `link_dbms_tables`(DML→DDL) 가 각각 `(:FUNCTION)-[:READS|WRITES]->(:TABLE)` 엣지를 만든다(실측: framework READS 16·WRITES 9). 

핵심 통찰: **한 함수가 테이블 A 를 읽고 테이블 B 를 쓰면 = A →(그 함수)→ B 데이터 흐름**. 이는 프론트의 `SOURCE → ETL → TARGET` 모델과 정확히 같은 모양이다 — 읽은 테이블=SOURCE, 함수=ETL, 쓴 테이블=TARGET. 따라서 그래프 R/W 에서 리니지를 도출하면 **전략 무관**으로 Lineage 탭을 채울 수 있다(framework 도출 검증: 읽기+쓰기 함수에서 테이블 흐름 도출됨).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - framework 분석도 데이터 리니지를 본다 (Priority: P1)

framework(코드) 분석을 한 사용자가 Lineage 탭에서 **어떤 테이블이 어떤 처리를 거쳐 어떤 테이블로 흐르는지**를 본다(빈 화면이 아니라). DBMS 분석과 동일한 SOURCE→ETL→TARGET 시각화.

**Why this priority**: 핵심 가치 — framework 사용자가 못 쓰던 리니지 기능을 동등하게 사용.

**Independent Test**: framework 분석 1회 후 `/robo/lineage/` 호출 → 읽기+쓰기 함수에서 도출된 source/etl/target 노드와 흐름 엣지가 1개 이상 반환되는지 확인.

**Acceptance Scenarios**:

1. **Given** framework 분석 그래프(함수→테이블 R/W 보유), **When** Lineage 탭을 연다, **Then** 읽은 테이블(SOURCE) → 함수/처리(ETL) → 쓴 테이블(TARGET) 흐름이 표시된다.
2. **Given** 테이블 A 를 읽고 B 를 쓰는 함수, **When** 리니지를 조회, **Then** A→(함수)→B 흐름이 나타난다.
3. **Given** DBMS 분석, **When** Lineage 탭을 연다, **Then** 기존과 동등하게(또는 더 풍부하게) 리니지가 표시된다(무회귀).

### User Story 2 - 동일 계약·프론트 무변경 (Priority: P2)

Lineage 탭(프론트)은 변경 없이 동작한다 — catalog `/robo/lineage/` 응답 형태(`{nodes:[{id,name,type,properties}], edges:[{id,source,target,type,properties}], stats}`, type=SOURCE/ETL/TARGET)를 유지한다.

**Why this priority**: 범용화를 데이터 제공 계층(catalog)에서 해결해 프론트 변경·계약 파손 0.

**Independent Test**: 응답 스키마(노드 type 집합·엣지 필드)가 기존과 동일한지 + 프론트 Lineage 탭이 코드 변경 없이 framework 리니지를 렌더하는지 확인.

**Acceptance Scenarios**:

1. **Given** 범용 도출 적용, **When** `/robo/lineage/` 응답을 본다, **Then** 노드 type 은 SOURCE/ETL/TARGET, 엣지는 기존 필드 구조를 따른다.

### Edge Cases

- **R/W 가 없는 분석**(테이블 미사용 코드): 리니지 빈 결과를 정상 반환(에러 아님).
- **자기 자신 읽고 씀(A→A)**: 무의미한 자기 루프는 노이즈로 제외하거나 명확히 처리.
- **읽기만/쓰기만 하는 함수**: SOURCE→ETL 또는 ETL→TARGET 한쪽만 — 흐름이 끊긴 경우의 표시 규칙.
- **DBMS 기존 ETL 리니지(spec 004)와의 관계**: 그래프 R/W 도출과 기존 DataSource/ETLProcess 모델이 **중복·충돌하지 않도록** 단일화(둘 중 하나로 수렴 or 명확히 합성).
- **대량 R/W**: 함수·테이블이 많을 때 흐름 폭증 → 적절한 집계/한도(예: 컴포넌트 단위 묶기) 고려.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: catalog 리니지 조회는 분석 그래프의 `(:FUNCTION|컴포넌트)-[:READS|WRITES]->(:TABLE)` 에서 **전략 무관**으로 리니지를 도출해야 한다.
- **FR-002**: 한 함수가 읽은 테이블(SOURCE) → 그 함수/처리(ETL) → 쓴 테이블(TARGET) 의 흐름으로 매핑해야 한다.
- **FR-003**: framework 분석에서도 Lineage 탭에 리니지가 표시되어야 한다(빈 화면 금지, 단 실제 R/W 가 있을 때).
- **FR-004**: `/robo/lineage/` 응답 형태(노드 type=SOURCE/ETL/TARGET, 엣지 필드)는 유지해 **프론트 무변경**이어야 한다(계약 보존).
- **FR-005**: DBMS 분석 리니지는 무회귀(기존 동등 이상)여야 하며, 기존 ETL 모델(spec 004)과 **중복 생성하지 않도록** 단일 출처로 수렴해야 한다.
- **FR-006**: R/W 가 없으면 빈 리니지를 정상 반환(조용한 에러·예외 금지).
- **FR-007**: 자기 루프(A→A) 등 무의미 흐름은 노이즈로 제외하는 결정론 규칙을 둔다.

### Key Entities *(include if feature involves data)*

- **SOURCE(원천 테이블)**: 어떤 처리가 읽는 테이블 — 분석 그래프의 READS 대상 TABLE.
- **ETL(처리)**: 데이터를 변환·이동시키는 단위 — 분석 그래프의 함수(또는 컴포넌트). 읽고 쓰는 주체.
- **TARGET(대상 테이블)**: 어떤 처리가 쓰는 테이블 — WRITES 대상 TABLE.
- **흐름(edge)**: SOURCE→ETL, ETL→TARGET 데이터 이동.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: framework 분석 후 `/robo/lineage/` 가 노드·엣지를 **0이 아닌** 수로 반환(실제 R/W 가 있는 경우).
- **SC-002**: 읽기+쓰기 함수 1개당 SOURCE→ETL→TARGET 흐름이 그래프 R/W 와 **1:1로 일치**(누락·날조 0).
- **SC-003**: 응답 노드 type 집합 = {SOURCE, ETL, TARGET}, 엣지 필드 = 기존과 동일 → 프론트 코드 변경 0.
- **SC-004**: DBMS 분석 리니지가 수정 전후로 무회귀(같은 흐름 표시, 중복 0).

## Assumptions

- 분석 그래프(neo4j)에 함수↔테이블 R/W(`READS`/`WRITES`) 가 양 전략 모두 존재한다(실측 확인).
- 리니지 도출·제공은 **catalog 계층**에서 수행한다(read-side 도출 → 프론트·analyzer 무변경 지향). analyzer 가 별도 리니지 엣지를 생산할지는 plan 에서 결정.
- 프론트 Lineage 탭의 SOURCE→ETL→TARGET 모델·`/robo/lineage/` 응답 계약을 유지한다(계약 = cross-service 경계).
- 기존 dbms ETL 리니지(spec 004)의 DBMS-only 범위는 본 스펙이 **전략무관으로 확장·수렴**(supersede 관계, 004 보존).
- 검증은 framework 실데이터(zapamcom 재분석)로 수행(테이블 R/W 존재 → 리니지 도출 확인). dbms 무회귀는 코드/계약 기준 확인(현재 dbms 라이브 데이터 부재).

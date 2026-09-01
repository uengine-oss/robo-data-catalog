# ROBO Data Catalog

Analyzer가 만든 그래프를 조회하고, 테이블 설명·FK·SQL 계보처럼 Catalog가 맡은 메타데이터를 보완하는 FastAPI 서비스입니다.

## 책임 경계

- Analyzer는 소스 코드, TABLE, COLUMN, RULE과 분석 관계를 생성합니다.
- Catalog는 Analyzer 노드를 조회하고 사람이 입력한 설명을 수정할 수 있습니다.
- Catalog가 새로 만드는 FK·READS·WRITES·DATA_FLOWS_TO 관계에는 `_owner="catalog"`를 기록합니다.
- Analyzer 노드에는 `_owner="analyzer"`가 기록됩니다.
- Catalog는 Analyzer의 노드 ID나 분석 관계를 재작성하지 않습니다.
- 의미 검색은 요청 시 설명을 메모리에서 임베딩합니다. 검색 전용 노드나 저장 임베딩 복사본은 만들지 않습니다.

## 공유 그래프 계약

Catalog가 직접 사용하는 노드는 다음 둘입니다.

| 노드 | 주요 속성 |
|---|---|
| `TABLE` | `_id`, `_owner`, `datasource`, `database`, `schema`, `name`, `qualified_name`, `logical_name`, `table_type`, `description`, `summary` |
| `COLUMN` | `_id`, `_owner`, `name`, `logical_name`, `data_type`, `nullable`, `primary_key`, `unique`, `default_value`, `description`, `summary` |

Catalog가 직접 사용하는 관계는 다음과 같습니다.

| 관계 | 방향 | 용도 |
|---|---|---|
| `HAS_COLUMN` | TABLE → COLUMN | 테이블의 컬럼 |
| `FK` | TABLE → TABLE | `from_column`, `to_column`으로 컬럼 쌍을 표시 |
| `READS` | 코드 노드 → TABLE | 읽는 테이블 |
| `WRITES` | 코드 노드 → TABLE | 쓰는 테이블 |
| `PARENT_OF` | 코드 노드 → 코드 노드 | 프로시저와 하위 구문 탐색 |
| `DATA_FLOWS_TO` | TABLE → TABLE | Catalog가 SQL에서 추출한 데이터 흐름 |

`description`은 사람이 확인하거나 수정할 수 있는 설명이고, `summary`는 Analyzer가 만든 분석 요약입니다. 둘을 한 필드로 합치지 않습니다.

## API

모든 업무 API의 접두사는 `/robo`입니다.

### 그래프

- `GET /robo/check-data/` — Analyzer 소유 노드 존재 여부
- `GET /robo/graph/` — 화면용 노드·관계 조회. 큰 벡터 속성은 반환하지 않음
- `GET /robo/graph/related-tables/{table_name}` — FK와 공통 프로시저 기준 관련 테이블
- `DELETE /robo/delete/` — Analyzer 소유 그래프 삭제. 파일 삭제는 선택 사항

### 스키마

- `GET /robo/schema/tables`
- `GET /robo/schema/tables/{table_name}/columns`
- `GET /robo/schema/tables/{table_name}/references`
- `GET /robo/schema/procedures/{procedure_name}/statements`
- `GET /robo/schema/relationships`
- `POST /robo/schema/relationships` — Catalog 소유 `FK` 생성
- `DELETE /robo/schema/relationships` — Catalog 소유 `FK` 삭제
- `PUT /robo/schema/tables/{table_name}/description`
- `PUT /robo/schema/tables/{table_name}/columns/{column_name}/description`
- `POST /robo/schema/semantic-search` — 설명 기반 요청 시점 검색
- `POST /robo/schema/enrich-metadata` — 샘플 데이터 기반 설명 보완

### 샘플과 Analyzer 연동

- `POST /robo/tables/resolve-context` — 코드에서 발견한 객체명을 실제 테이블과 일괄 연결
- `POST /robo/tables/sample-context` — 테이블 컬럼과 제한된 샘플 행 조회
- `GET /robo/tables/discovery` — 페이지·스냅샷 기반 테이블 구조 전달

### 계보

- `GET /robo/lineage/` — READS·WRITES 기반 화면용 계보
- `POST /robo/lineage/analyze/` — SQL에서 READS·WRITES·DATA_FLOWS_TO 추출

## 실행

```powershell
.venv\Scripts\python.exe -m uvicorn main:app --host 127.0.0.1 --port 15503
```

환경 변수는 `shared/config/settings.py`에서 읽습니다. Neo4j 주소·계정·데이터베이스와 외부 API 키를 소스에 기록하지 않습니다.

## 검증

Neo4j에 연결하지 않는 전체 테스트:

```powershell
uv run --python .venv\Scripts\python.exe --with pytest --with pytest-asyncio python -m pytest -q
.venv\Scripts\python.exe -m compileall -q api contracts enrichment graph lineage samples search shared
```

실제 Neo4j 검증은 명시적으로 만든 격리 데이터베이스에서만 수행해야 합니다. 공용 또는 사용 중인 데이터베이스를 테스트 대상으로 사용하지 않습니다.

## 구조

```text
robo-data-catalog/
├── main.py                         # FastAPI 시작점과 라우터 등록
├── api/                            # HTTP 요청·응답만 담당
│   ├── graph.py                    # 그래프 조회·삭제 API
│   ├── lineage.py                  # 계보 조회·SQL 분석 API
│   ├── schema.py                   # 테이블·컬럼·참조 조회 API
│   ├── schema_edit.py              # 설명·FK 수정 API
│   ├── search.py                   # 의미 검색 API
│   ├── table_samples.py            # 객체 연결·샘플·구조 전달 API
│   ├── enrichment.py               # 설명·FK 보완 스트림 API
│   ├── graph_connection.py         # 요청별 Neo4j 연결 헤더 처리
│   └── errors.py                   # 외부 서비스 오류의 HTTP 변환
├── contracts/                      # API 입력·출력 모델
├── graph/                          # Neo4j 조회·수정과 소유권 경계
├── enrichment/                     # 설명 생성과 FK 추론
├── lineage/                        # SQL 계보 추출과 조회
├── samples/                        # 테이블 발견·이름 연결·샘플 구성
├── search/                         # 요청 시점 의미 검색
├── integrations/                   # Data Fabric·LLM·임베딩 연결
├── shared/                         # 설정과 로그
├── scripts/                        # 외부 DB에 설치할 선택 SQL
├── tests/                          # 단위·계약 테스트
└── specs/                          # 변경 당시의 SDD 기록
```

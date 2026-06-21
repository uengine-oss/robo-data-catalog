# ROBO Data Catalog

데이터 자산 조회·편집·보강·리니지 관리 마이크로서비스 (v2.0.0).

`robo-data-analyzer`에서 분리된 독립 서비스로, Neo4j에 저장된 스키마/그래프 데이터의 **조회·편집·메타데이터 보강**을 담당합니다.

## 주요 기능

- 테이블/컬럼/관계 조회 및 편집
- 시멘틱 검색 (임베딩 기반)
- **메타데이터 보강** (LLM description 생성 + FK 자동 추론, NDJSON 스트리밍)
- **분석 세션용 테이블 샘플 컨텍스트** 제공 (analyzer 연동)
- 데이터 리니지 조회 및 ETL SQL 분석
- DW 스타스키마 등록/삭제
- 스키마 벡터화

## 아키텍처

```
robo-data-catalog (port 5503)
├── api/
│   ├── catalog_router.py        # FastAPI 라우터(prefix /robo)
│   └── request_models.py
├── service/                     # 비즈니스 로직
│   ├── schema_query_service.py      # 테이블/컬럼/관계 조회
│   ├── schema_edit_service.py       # 설명 편집·관계 추가/삭제
│   ├── schema_search_service.py     # 시멘틱 검색·벡터화
│   ├── table_description_service.py # description 생성
│   ├── fk_inference_service.py      # FK 자동 추론
│   ├── sample_context_service.py    # 분석 세션 샘플 컨텍스트
│   ├── graph_query_service.py       # 그래프 조회
│   ├── data_lineage_service.py      # 리니지·ETL 분석
│   ├── dw_schema_service.py         # DW 스타스키마
│   └── text2sql_client.py
├── client/
│   ├── neo4j_client.py
│   ├── connection_context.py        # 요청별 Neo4j override(X-Neo4j-*)
│   └── embedding_client.py
├── config/settings.py
└── main.py                      # 진입점
```

## API 엔드포인트

> 편집/검색/벡터화/DW/보강 엔드포인트는 **`X-API-Key`** 헤더 필수. Electron 임베드 시 요청별 **`X-Neo4j-URI/User/Password/Database`** 헤더로 연결 override 가능(미지정 시 `.env` 폴백).

### 헬스체크
| Method | Path | 설명 |
|--------|------|------|
| GET | `/` | 서비스 상태 |
| GET | `/health` | 헬스 |

### 그래프 데이터
| Method | Path | 설명 |
|--------|------|------|
| GET | `/robo/check-data/` | 데이터 존재 확인 |
| GET | `/robo/graph/` | 전체 그래프 조회 `{Nodes, Relationships}` |
| GET | `/robo/graph/related-tables/{name}` | 관련 테이블 조회 |
| DELETE | `/robo/delete/` | 데이터 삭제 |

### 스키마
| Method | Path | 설명 |
|--------|------|------|
| GET | `/robo/schema/tables` | 테이블 목록 |
| GET | `/robo/schema/tables/{name}/columns` | 컬럼 목록 |
| GET | `/robo/schema/tables/{name}/references` | 참조 프로시저 |
| GET | `/robo/schema/procedures/{name}/statements` | Statement 조회 |
| GET | `/robo/schema/relationships` | 관계 목록 |
| POST | `/robo/schema/relationships` | 관계 추가 |
| DELETE | `/robo/schema/relationships` | 관계 삭제 |
| POST | `/robo/schema/semantic-search` | 시멘틱 검색 |
| PUT | `/robo/schema/tables/{name}/description` | 테이블 설명 수정 |
| PUT | `/robo/schema/tables/{name}/columns/{col}/description` | 컬럼 설명 수정 |
| POST | `/robo/schema/vectorize` | 스키마 벡터화 |

### 메타데이터 보강
| Method | Path | 설명 |
|--------|------|------|
| POST | `/robo/schema/enrich-metadata` | description 생성 + FK 추론 (NDJSON 스트림) |
| POST | `/robo/tables/sample-context` | 테이블명 batch 매칭 + 샘플 행 반환 (analyzer 호출) |

### 리니지
| Method | Path | 설명 |
|--------|------|------|
| GET | `/robo/lineage/` | 리니지 그래프 조회 |
| POST | `/robo/lineage/analyze/` | ETL SQL 리니지 분석 |

### DW 스타스키마
| Method | Path | 설명 |
|--------|------|------|
| POST | `/robo/schema/dw-tables` | 스타스키마 등록 |
| DELETE | `/robo/schema/dw-tables/{cube}` | 스타스키마 삭제 |

## 실행 방법

### 환경 변수 (.env)

```env
# Neo4j
NEO4J_URI=bolt://127.0.0.1:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
NEO4J_DATABASE=neo4j

# LLM (메타데이터 보강 / 임베딩)
LLM_API_KEY=                 # 또는 OPENAI_API_KEY
LLM_MODEL=gpt-4.1
EMBEDDING_MODEL=text-embedding-3-small

# 메타데이터 보강 (Text2SQL 연동)
TEXT2SQL_API_URL=
FK_INFERENCE_ENABLED=true
FK_SAMPLE_SIZE=25
FK_SIMILARITY_THRESHOLD=0.8

# 서버
HOST=0.0.0.0
PORT=5503
```

### 설치 및 실행

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 5503
```

### API 문서
실행 후 http://localhost:5503/docs 에서 Swagger UI 확인 가능.

## 기술 스택

FastAPI 0.115 · uvicorn · Neo4j Python Driver 5.28(async) · OpenAI / LangChain(LLM 보강·임베딩) · rapidfuzz(테이블명 매칭) · lxml(SQL 파싱) · numpy

## Neo4j 노드 (읽기 대상)

분석 서비스(`robo-data-analyzer`)가 생성한 그래프 데이터를 조회/편집합니다. 노드 라벨은 **UPPER_SNAKE**입니다:

- 코드 노드: `FUNCTION`, `VARIABLE`, `PROCEDURE`, `METHOD`, `TRIGGER`
- 스키마 노드: `TABLE`, `COLUMN`
- 리니지 노드: `DataSource`, `ETLProcess`

관계:
- `HAS_COLUMN` (TABLE→COLUMN)
- `FK_TO_TABLE` (TABLE→TABLE), `FK_TO_COLUMN` (COLUMN→COLUMN)
- `READS` / `WRITES` (코드→TABLE), `REFER_TO`, `FROM`
- `PARENT_OF` (프로시저→하위 statement)

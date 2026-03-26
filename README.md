# ROBO Data Catalog

데이터 자산 조회, 편집, 보강, 리니지 관리 마이크로서비스.

`robo-data-analyzer`에서 분리된 독립 서비스로, Neo4j에 저장된 스키마/그래프 데이터의 조회와 편집을 담당합니다.

## 주요 기능

- 테이블/컬럼/관계 조회 및 편집
- 시멘틱 검색 (임베딩 기반)
- 데이터 리니지 조회 및 ETL 분석
- DW 스타스키마 등록/삭제
- 스키마 벡터화

## 아키텍처

```
robo-data-catalog (port 5503)
├── api/                  # FastAPI 라우터
│   ├── catalog_router.py
│   └── request_models.py
├── service/              # 비즈니스 로직
│   ├── schema_manage_service.py
│   ├── graph_query_service.py
│   ├── data_lineage_service.py
│   └── dw_schema_service.py
├── analyzer/             # 리니지 분석 로직
│   └── strategy/dbms/linking/lineage_analyzer.py
├── client/               # 외부 클라이언트
│   ├── neo4j_client.py
│   └── embedding_client.py
├── config/               # 환경 설정
│   └── settings.py
├── util/                 # 유틸리티
│   └── logger.py
└── main.py               # 서비스 진입점
```

## API 엔드포인트

### 그래프 데이터

| Method | Path | 설명 |
|--------|------|------|
| GET | `/robo/check-data/` | 데이터 존재 확인 |
| GET | `/robo/graph/` | 전체 그래프 조회 |
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
NEO4J_URI=bolt://127.0.0.1:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
```

### 설치 및 실행

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 5503
```

### API 문서

실행 후 http://localhost:5503/docs 에서 Swagger UI 확인 가능.

## Neo4j 노드 (읽기 대상)

분석 서비스(`robo-data-analyzer`)가 생성한 그래프 데이터를 조회합니다:

- `MODULE`, `FUNCTION`, `VARIABLE`, `CONSTANT`
- `Table`, `Column`, `SQL_STATEMENT`
- `CALLS`, `REFER_TO`, `READS`, `WRITES`, `FK_TO_TABLE`
- `DataSource`, `ETLProcess` (리니지)

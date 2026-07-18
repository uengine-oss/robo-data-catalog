# ROBO Data Catalog

ROBO Data Catalog는 Analyzer가 만든 분석 그래프를 조회·편집하고, 스키마 검색·설명 보강·FK 추론·샘플 컨텍스트·리니지·스키마 메타데이터를 제공하는 서비스입니다. 대상 데이터베이스 접속 정보와 실제 SQL 실행은 Data Fabric이 소유하며, Catalog는 `DATA_FABRIC_URL`의 `/api/query`에 datasource·대상 SELECT·최대 행 수를 분리해 전달합니다. MindsDB wrapper와 read-only 판정은 Data Fabric만 소유합니다.

## 책임 경계

- Analyzer: 소스와 DDL을 분석하여 `graph_owner="analyzer"`인 그래프를 생성합니다.
- Catalog: 분석 그래프를 조회·편집·보강하고 검색, 리니지, 샘플 컨텍스트 API를 제공합니다. 전체 Neo4j나 DataSource registry를 삭제하지 않습니다.
- Data Fabric: 데이터소스 연결 등록, MindsDB 연결 관리, 실제 DB 쿼리 실행만 담당합니다. 메타데이터 추출이나 별도 UI는 소유하지 않습니다.

Catalog의 `check-data`, 그래프 조회, 초기화는 Analyzer 소유 그래프만 대상으로 합니다. `GLOSSARY`, `EMBED_META` 같은 시스템 노드는 사용자 그래프 응답에서 제외됩니다.

## 구조

```text

  main.py                    FastAPI 진입점과 공통 미들웨어
  api/                       도메인별 HTTP router와 연결 경계
  contracts/                 공개 request/response 계약
  enrichment/                설명·FK 보강 orchestration과 이벤트
  graph/                     Neo4j 연결·조회·편집·삭제·스키마
  lineage/                   리니지 조회와 SQL 리니지 추출
  search/                    메타데이터 시맨틱 검색
  table_samples/             테이블 샘플 context
  external/                  Data Fabric·embedding·LLM 연동
  settings.py                타입화된 환경 설정
  observability.py           운영 로깅
tests/
  unit/                      소유권·가시성 등 국소 검증
  contract/                  외부 HTTP 계약 검증
scripts/                     운영에 필요한 DB 보조 스크립트
specs/                       기능·계약·구조 결정 기록
```

`api`, `service`, `client`, `config`처럼 기술 계층만 나타내는 기존 최상위 패키지는 사용하지 않습니다. Catalog 소유 경로·파일·클래스에도 Analyzer 구현 이름을 두지 않습니다.

## 실행

Python 가상환경을 만든 뒤 다음과 같이 실행합니다.

```powershell
python -m pip install -r requirements.txt
$env:PYTHONPATH = "."
python -m uvicorn main:app --host 0.0.0.0 --port 5503
```

주요 환경 변수는 `.env.example`을 기준으로 설정합니다.

```env
NEO4J_URI=bolt://127.0.0.1:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
NEO4J_DATABASE=neo4j
DATA_FABRIC_URL=http://127.0.0.1:8404
CATALOG_CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173
CATALOG_ALLOW_NEO4J_HEADER_OVERRIDE=false
HOST=0.0.0.0
PORT=5503
```

LLM 설명 보강과 임베딩을 사용할 때만 `LLM_API_KEY` 또는 요청별 API 키를 전달합니다.
OpenAI-compatible provider는 `LLM_API_BASE`, `LLM_MODEL`, `LLM_MAX_COMPLETION_TOKENS`(기본 4096)로 한 곳에서 설정합니다. 대상 DB 비밀번호는 Catalog에 저장하거나 전달하지 않습니다.

Electron이 `X-Neo4j-*` 헤더로 요청별 연결을 선택해야 하는 로컬 환경에서만 `CATALOG_ALLOW_NEO4J_HEADER_OVERRIDE=true`를 설정합니다. 기본값은 꺼짐이며, URI는 Neo4j 전용 scheme과 hostname을 통과해야 합니다.

## 주요 API

- `GET /health`: 서비스 상태
- `GET /robo/check-data/`: Analyzer 소유 그래프 존재 여부
- `GET /robo/graph/`: 사용자에게 표시할 분석 그래프
- `DELETE /robo/delete/`: Analyzer 소유 그래프 삭제
- `/robo/schema/*`: 스키마 조회·편집·검색·벡터화 메타데이터
- `POST /robo/tables/sample-context`: 분석 세션용 테이블 매칭·샘플 컨텍스트
- `GET /robo/lineage/`, `POST /robo/lineage/analyze/`: 리니지 조회·SQL 추출
- `POST /robo/schema/enrich-metadata`: 설명 보강과 FK 추론 NDJSON 스트림

전체 요청·응답 계약은 실행 후 `http://localhost:5503/docs`에서 확인할 수 있습니다. 스키마 편집·검색·보강 API의 인증 헤더와 요청별 Neo4j override 계약은 OpenAPI 정의를 따릅니다.

## 검증

```powershell
$env:PYTHONPATH = "."
.venv\Scripts\python.exe -m unittest discover -s tests -t . -p "test_*.py"
.venv\Scripts\python.exe -m compileall -q main.py settings.py observability.py api contracts enrichment external graph lineage search table_samples tests
.venv\Scripts\python.exe -c "from main import app; print(len(app.openapi()['paths']))"
```

단위·계약 테스트 통과만으로 실제 연동 완료를 주장하지 않습니다. Neo4j, Data Fabric, MindsDB, 대상 DB가 필요한 통합 검증은 각 서비스를 실행한 상태에서 별도로 수행하고 실패·타임아웃·부분 복구 경로까지 확인해야 합니다.

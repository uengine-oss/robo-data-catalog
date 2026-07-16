# Implementation Plan: Catalog Data Fabric Query Client

## Flow

`Catalog API → SampleContext/FkInference → DataFabricClient → POST /api/query {datasource,query,max_rows} → Data Fabric read-only policy/MindsDB wrapper → target DB`

## Design

- HTTP, retry, 구조화 요청, 응답 정규화는 `app/external/data_fabric_client.py`가 단독 소유한다.
- Catalog는 대상 SELECT만 생성하고 Data Fabric이 datasource 식별자 검증·read-only 판정·row limit을 소유한다.
- 상위 서비스는 client protocol만 주입받고 Data Fabric HTTP 형식을 알지 않는다.
- 설정 단일 진실은 `DATA_FABRIC_URL`이며 datasource는 각 요청에서 client 생성 시 고정한다.
- 예전 Text2SQL client와 설정은 호환 별칭 없이 제거한다.
- Spec 006의 Text2SQL transport 설명은 당시 기록으로 남고 이 Spec이 현행 transport를 대체한다.

## Verification

- query 조립·sample SQL·응답 정규화 단위 테스트.
- Python compile/import와 옛 runtime symbol 전수 검색.
- 실제 Data Fabric 연결 상태·샘플 행·FK 추론 E2E.


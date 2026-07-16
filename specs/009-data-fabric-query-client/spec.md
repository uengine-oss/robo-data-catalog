# Feature Specification: Catalog Data Fabric Query Client

**Created**: 2026-07-14  
**Status**: Implementing

## Problem

Catalog의 샘플 행 조회와 FK 추론은 제거되는 Text2SQL 서비스의 전용 endpoint와 응답 형식에
직접 의존한다. Architect가 canonical 대상 DB 접근 서비스로 Data Fabric을 사용해도 Catalog만
옛 서비스에 남으면 메타데이터 보강이 조용히 건너뛰어지거나 실행 배선이 둘로 갈라진다.

## Requirements

- Catalog는 대상 DB에 직접 접속하지 않고 Data Fabric `/api/query`만 사용한다.
- datasource와 대상 DB read-only SQL을 별도 필드로 Data Fabric에 전달한다. MindsDB native query
  조립과 최종 row limit은 Data Fabric만 소유하며 Catalog는 wrapper 문법을 알지 않는다.
- schema-qualified table은 schema와 table 식별자를 각각 인용하고 limit를 보존한다.
- Data Fabric의 `columns`·`data` 응답을 `list[dict]`로 한 곳에서 정규화한다.
- 미설정, 연결 실패, timeout, SQL 오류는 기존 보강 흐름의 skip/None 계약을 유지하며 원인을 로그에 남긴다.
- `TEXT2SQL_API_URL`, `Text2SqlClient`, `/text2sql/direct-sql` 소비자는 제거한다.
- 사용자 비밀번호·대상 DB 자격증명은 Catalog에 전달하거나 기록하지 않는다.

## Acceptance

1. 샘플 조회와 FK 추론이 동일한 `DataFabricClient`를 사용한다.
2. Data Fabric 미설정 시 보강 stream은 명시적인 skip 이유를 반환한다.
3. 응답 열과 행이 순서대로 dict에 매핑되고 빈 응답은 `None`이다.
4. 저장소에서 옛 Text2SQL runtime 참조가 0이다. 과거 Spec 기록은 제외한다.
5. 실제 Data Fabric과 대상 PostgreSQL을 연결한 샘플·FK E2E 결과가 기존 계약과 일치한다.
6. Catalog 요청에는 `datasource/query/max_rows`가 분리되고 mutation SQL을 만들거나 전달하는 경로가 없다.


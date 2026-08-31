# Feature Specification: 데이터 객체 신원 단일 resolver

**Status**: Complete
**Created**: 2026-08-10
**Updated**: 2026-09-01
**Upstream**: robo-data-fabric spec 006
**Consumer**: robo-data-analyzer spec 126

## 문제

현행 `GET /tables/discovery`는 datasource 전체를 eager 열거하고 이름 유사도/전체 sample 경로를
남긴다. schema, database link, alias, requested column을 가진 코드 참조를 truth identity로
해소하는 batch 계약이 없다.

## 계약

- POST `/tables/resolve-context`는 Analyzer의 dedup reference를 한 번 받아 Fabric batch API를
  호출한다.
- exact confirmed와 unresolved를 모두 입력 key 기준으로 반환한다. schema/link/column 근거가
  부족하면 unresolved이며 fuzzy score로 confirmed하지 않는다.
- confirmed sample만 분석 근거로 전달한다. 후보·점수·향후 승인 상태는 truth graph와 분리한다.
- 404 datasource와 provider 실패를 구분하며 활성 서비스 실패를 빈 성공으로 바꾸지 않는다.
- 현행 discovery endpoint는 호환 기간 유지하되 Analyzer production consumer는 새 resolver로
  전환한다.

## 성공 기준

- duplicate reference dedup, exact schema, ambiguous bare name, db link, column mismatch fixture 통과.
- Catalog→Fabric HTTP 1회/bounded chunk이며 metrics를 손실 없이 전달한다.
- 기존 sample-context/discovery 소비자와 전체 Catalog 회귀에 무회귀가 있다.

## 완료 증거

- `tests/unit/test_object_resolver.py`가 중복 reference dedup, quoted case 보존, exact
  schema, ambiguous bare name, database link, column mismatch와 provider 오류를 검증한다.
- Analyzer의 production object-resolution 소비자와 Catalog의 단일 Fabric batch 경계가
  전체 회귀에 포함된다.
- Catalog 전체 pytest `73 passed, 28 subtests passed`, authoritative unittest `46 tests,
  OK`를 통과했다.

# Implementation Plan

**Status**: Complete (2026-09-01)

1. Catalog API contract와 Fabric gateway red fixture를 고정한다.
2. resolve service와 단일 POST proxy를 구현한다.
3. Analyzer DBMS producer/step에 연결하고 discovery production 호출을 제거한다.
4. 오프라인 exact/unresolved 및 5/100/1000 성능을 측정한다.

구현과 Analyzer 연결은 완료됐다. 현재 회귀는 quoted identity의 case-sensitive dedup도
포함하며, discovery pagination은 별도 spec 012의 lossless snapshot 계약이 소유한다.

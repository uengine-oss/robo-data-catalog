# Implementation Plan

1. 모든 tracked source와 ignored legacy 잔재, API/Cypher/import/Workspace 소비자를 동결한다.
2. 파일별 이름·책임 감사를 수행하고 target move map을 확정한다.
3. git mv와 같은 슬라이스의 import/test/docs 갱신으로 compatibility package 없이 이동한다.
4. 잔재는 consumer 0과 HEAD 비추적을 증명한 뒤 제거한다.
5. 정적/전체 테스트 후 실제 Fabric·Neo4j·provider·중앙 UI를 검증한다.

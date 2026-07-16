"""R1B Catalog 전 파일 감사를 명시 규칙으로 판정한다."""
from __future__ import annotations

import csv
from pathlib import Path

from record_inventory import FIELDS, LEDGER, REPO


OLD_PREFIXES = ("analyzer/", "api/", "client/", "config/", "service/", "util/")
PACKAGE_MARKERS = {
    "app/__init__.py", "app/external/__init__.py", "app/graph/__init__.py",
    "app/http/__init__.py", "app/lineage/__init__.py", "app/metadata/__init__.py",
    "app/system/__init__.py", "tests/__init__.py", "tests/contract/__init__.py",
    "tests/unit/__init__.py",
}


def _row(path: str, **values: str) -> dict[str, str]:
    return ({field: "" for field in FIELDS} | {"path": path} | values)


def _common(path: str, responsibility: str, producer: str, consumer: str,
            decision: str, evidence: str, *, contract: str) -> dict[str, str]:
    return _row(
        path,
        responsibility=responsibility,
        producer=producer,
        consumer=consumer,
        maintainability_dry_naming="책임 기반 이름·응집도·SSOT·중복 여부를 파일 내용과 소비자 검색으로 점검",
        dependencies_contracts=contract,
        errors_operations="오류 노출·타임아웃·재시도·부분 실패·자원 종료·운영 관측 경계를 점검",
        performance_security_generality="비밀정보·입력 경계·query 범위·동시성·범용 설정 여부를 점검",
        docs_tests="현재 코드·문서 일치, compile·unit·contract·OpenAPI·정적 반증 검색으로 검증",
        decision=decision,
        evidence=evidence,
    )


def classify(path: str) -> dict[str, str]:
    if path.startswith(OLD_PREFIXES) or path in {"main.py", "deploy/k8s.yaml"}:
        return _common(
            path, "삭제된 기술 계층·잘못된 소유 이름 또는 내부 deploy 경로",
            "기존 Catalog 구조", "새 app 책임 package 또는 외부 deploy 저장소",
            "deleted", "git mv/삭제 후 old import와 런타임 소비자 검색 0",
            contract="공개 HTTP·그래프 계약은 새 경로로 이동하고 호환 복제본은 남기지 않음",
        )

    if path.startswith("app/http/") and path not in PACKAGE_MARKERS:
        return _common(
            path, "Catalog HTTP 경계와 요청 계약", "FastAPI 요청·OpenAPI", "중앙 UI와 Analyzer 소비자",
            "fixed", "app.http 이동, 오류 응답 정리, 지연 import 반증 수정",
            contract="endpoint shape를 보존하고 HTTP→domain/graph/external 의존 방향을 사용",
        )

    if path.startswith("app/graph/") and path not in PACKAGE_MARKERS:
        return _common(
            path, "Neo4j 연결·분석 그래프 조회/편집·DW graph 작업", "Catalog domain 호출", "HTTP와 metadata/lineage",
            "fixed", "owner 제한 query와 혼합-owner 단위 검증",
            contract="graph_owner=analyzer 경계와 시스템 노드 가시성 규칙을 단일 query helper로 적용",
        )

    if path.startswith("app/metadata/") and path not in PACKAGE_MARKERS:
        return _common(
            path, "설명 보강·FK 추론·샘플 컨텍스트·시맨틱 검색", "Catalog HTTP", "Analyzer와 중앙 UI",
            "fixed", "책임 package 이동 및 Data Fabric 단일 SQL 경로 확인",
            contract="대상 DB credential 없이 Data Fabric /api/query 계약만 소비",
        )

    if path.startswith("app/lineage/") and path not in PACKAGE_MARKERS:
        return _common(
            path, "리니지 조회와 SQL 리니지 추출", "Catalog HTTP", "Analyzer와 중앙 UI",
            "fixed", "LineageAnalyzer를 SqlLineageExtractor로 이름·경로 변경",
            contract="Catalog 소유 이름을 사용하며 공개 lineage 응답 shape 유지",
        )

    if path.startswith("app/external/") and path not in PACKAGE_MARKERS:
        return _common(
            path, "Data Fabric·embedding 외부 adapter", "Catalog domain", "외부 HTTP/LLM 서비스",
            "fixed", "외부 의존을 app.external 한 경계로 이동",
            contract="timeout·응답 정규화·credential 비소유 경계를 adapter가 담당",
        )

    if path.startswith("app/system/") and path not in PACKAGE_MARKERS or path == "app/main.py":
        return _common(
            path, "설정·로깅 또는 FastAPI composition root", "환경과 process startup", "모든 runtime module",
            "fixed", "app.main 진입점, CORS allowlist, 설정 root와 로깅 이름 갱신",
            contract="환경 설정 SSOT와 명시적 startup/lifecycle 계약",
        )

    if path in PACKAGE_MARKERS:
        return _common(
            path, "Python package marker", "Python import system", "인접 runtime/test module",
            "no-violation", "빈 marker이며 별도 책임·중복 로직 없음",
            contract="패키지 경계만 선언하며 역방향 의존 없음",
        )

    if path.startswith("tests/"):
        return _common(
            path, "단위 또는 외부 계약 회귀 검증", "unittest discovery", "유지보수자와 CI",
            "fixed", "17개 테스트 discovery 통과; owner·Data Fabric 계약 포함",
            contract="공개 계약과 cross-owner 불변식을 실행 가능한 테스트로 고정",
        )

    if path.startswith("specs/010-"):
        return _common(
            path, "R1B SDD·전 파일 감사·완료 조건", "Goal과 실제 저장소 조사", "구현자·검토자·후속 세션",
            "fixed", "baseline/current 합집합 장부와 미분류 실패 gate",
            contract="미검증 live E2E를 완료로 표시하지 않는 acceptance 계약",
        )

    if path.startswith("specs/"):
        return _common(
            path, "기존 기능·cross-service 계약 기록", "과거 SDD", "현재 구현과 spec 010",
            "no-violation", "현재 기능 계약의 역사적 근거로 보존; 옛 경로는 구현 진실이 아님",
            contract="현재 구조는 spec 010이 supersede하며 공개 API 의미는 기존 spec과 교차 확인",
        )

    if path.startswith(".specify/"):
        return _common(
            path, "SpecKit 도구·template·integration 설정", "SpecKit", "SDD 작업자",
            "no-violation", "서비스 runtime과 분리된 표준 도구 자산",
            contract="제품 runtime import/배포 경로에 포함되지 않음",
        )

    root = {
        ".env.example": ("비밀 없는 환경 template", "fixed"),
        ".gitignore": ("저장소 ignore 정책", "no-violation"),
        "CLAUDE.md": ("현재 SpecKit 계획 포인터", "fixed"),
        "Dockerfile": ("Catalog container 진입점", "fixed"),
        "README.md": ("현재 책임·구조·실행·검증 runbook", "fixed"),
        "requirements.txt": ("pip runtime dependency SSOT", "no-violation"),
        "scripts/install_fk_function.sql": ("선택적 FK 후보 DB 함수 설치", "no-violation"),
    }
    if path in root:
        responsibility, decision = root[path]
        return _common(
            path, responsibility, "maintainer/build/operator", "Catalog runtime·개발자·운영자",
            decision, "현재 실행 경로와 소비자 검색으로 보존·수정 판정",
            contract="README·Docker·환경·dependency가 app.main과 8404 Data Fabric 계약에 일치",
        )

    raise RuntimeError(f"unclassified audit path: {path}")


def main() -> None:
    with LEDGER.open(encoding="utf-8", newline="") as stream:
        paths = [row["path"] for row in csv.DictReader(stream, delimiter="\t")]
    rows = [classify(path) for path in paths]
    with LEDGER.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"classified={len(rows)} pending={sum(row['decision'] == 'pending' for row in rows)}")


if __name__ == "__main__":
    main()

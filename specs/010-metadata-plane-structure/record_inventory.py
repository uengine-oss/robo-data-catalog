"""현재 저장소 파일을 R1B 전 파일 감사 장부에 빠짐없이 기록한다."""
from __future__ import annotations

import csv
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
LEDGER = Path(__file__).with_name("audit-ledger.tsv")
FIELDS = (
    "path", "responsibility", "producer", "consumer",
    "maintainability_dry_naming", "dependencies_contracts", "errors_operations",
    "performance_security_generality", "docs_tests", "decision", "evidence",
)


def current_files() -> set[str]:
    output = subprocess.run(
        ["git", "-c", "core.quotePath=false", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=REPO, check=True, capture_output=True, text=True, encoding="utf-8",
    ).stdout.splitlines()
    return {path for path in output if path and (REPO / path).is_file()}


def baseline_files() -> set[str]:
    """현재 HEAD의 파일도 포함해 이동·삭제된 파일의 판정이 장부에서 사라지지 않게 한다."""
    output = subprocess.run(
        ["git", "-c", "core.quotePath=false", "ls-tree", "-r", "--name-only", "HEAD"],
        cwd=REPO, check=True, capture_output=True, text=True, encoding="utf-8",
    ).stdout.splitlines()
    return {path for path in output if path}


def main() -> None:
    existing: dict[str, dict[str, str]] = {}
    if LEDGER.exists():
        with LEDGER.open(encoding="utf-8", newline="") as stream:
            existing = {row["path"]: row for row in csv.DictReader(stream, delimiter="\t")}
    rows = []
    for path in sorted(current_files() | baseline_files()):
        row = existing.get(path, {
            "path": path, "responsibility": "", "producer": "", "consumer": "",
            "decision": "pending", "evidence": "",
        })
        rows.append({field: row.get(field, "") for field in FIELDS})
    with LEDGER.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"files={len(rows)} pending={sum(row['decision'] == 'pending' for row in rows)}")


if __name__ == "__main__":
    main()

"""공유 Neo4j에서 Catalog가 소비·편집하는 분석 그래프의 소유권 계약."""

import re


ANALYZER_OWNER = "analyzer"
CATALOG_OWNER = "catalog"
_CYPHER_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def owner_predicate(alias: str) -> str:
    if not _CYPHER_IDENTIFIER_PATTERN.fullmatch(alias):
        raise ValueError(f"invalid Cypher alias: {alias!r}")
    return f"{alias}._owner = '{ANALYZER_OWNER}'"

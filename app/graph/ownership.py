"""공유 Neo4j에서 Catalog가 소비·편집하는 분석 그래프의 소유권 계약."""

import re


ANALYSIS_GRAPH_OWNER = "analyzer"
SYSTEM_GRAPH_NODE_LABELS: frozenset[str] = frozenset({"GLOSSARY", "EMBED_META"})
_CYPHER_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def owner_predicate(alias: str) -> str:
    if not _CYPHER_IDENTIFIER_PATTERN.fullmatch(alias):
        raise ValueError(f"invalid Cypher alias: {alias!r}")
    return f"{alias}.graph_owner = '{ANALYSIS_GRAPH_OWNER}'"


def visible_predicate(alias: str) -> str:
    if not _CYPHER_IDENTIFIER_PATTERN.fullmatch(alias):
        raise ValueError(f"invalid Cypher alias: {alias!r}")
    return " AND ".join(f"NOT {alias}:{label}" for label in sorted(SYSTEM_GRAPH_NODE_LABELS))

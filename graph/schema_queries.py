"""스키마 조회 서비스

테이블·컬럼·관계·참조를 읽기 전용으로 조회.
쓰기 작업은 ``schema_metadata_commands``가 담당한다.
"""

from __future__ import annotations

import logging
from typing import Optional

from graph.database import CatalogGraphDatabase
from graph.scope import ANALYSIS_GRAPH_OWNER

logger = logging.getLogger(__name__)


# =============================================================================
# 테이블 / 컬럼 조회
# =============================================================================

def metadata_enrichment_targets_query(datasource: str) -> dict:
    """Build the owner-scoped query for tables that need descriptions."""
    return {
        "query": """
            MATCH (t:TABLE)
            WHERE t.graph_owner = $graph_owner
              AND coalesce(t.db, t.datasource) = $datasource
              AND (t.description IS NULL OR t.description = '' OR t.description = 'N/A')
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:COLUMN {graph_owner: $graph_owner})
            RETURN t.name AS table_name,
                   t.schema AS schema_name,
                   collect({
                     name: c.name,
                     dtype: c.dtype,
                     description: c.description
                   }) AS columns
        """,
        "parameters": {
            "datasource": datasource,
            "graph_owner": ANALYSIS_GRAPH_OWNER,
        },
    }


async def fetch_schema_tables(
    search: Optional[str] = None,
    schema: Optional[str] = None,
    limit: int = 100,
) -> list:
    """테이블 목록 조회

    Args:
        search: 테이블명/설명 검색
        schema: 스키마 필터
        limit: 결과 제한
    """
    client = CatalogGraphDatabase()
    try:
        where_conditions: list[str] = ["t.graph_owner = $graph_owner"]
        safe_limit = max(1, min(int(limit), 1000))
        params: dict = {"limit": safe_limit, "graph_owner": ANALYSIS_GRAPH_OWNER}

        if schema:
            where_conditions.append("t.schema = $schema")
            params["schema"] = schema
        if search:
            where_conditions.append(
                "(toLower(t.name) CONTAINS toLower($search) "
                "OR toLower(t.description) CONTAINS toLower($search))"
            )
            params["search"] = search

        where_clause = " AND ".join(where_conditions) if where_conditions else "true"

        query = {
            "query": f"""
                MATCH (t:TABLE)
                WHERE {where_clause}
                OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:COLUMN {{graph_owner: $graph_owner}})
                WITH t, count(c) AS col_count
                RETURN t.name                 AS name,
                       t.schema               AS schema,
                       coalesce(t.db, t.datasource) AS datasource,
                       t.logical_name         AS logical_name,
                       t.description          AS description,
                       t.description_source   AS description_source,
                       t.analyzed_description AS analyzed_description,
                       col_count              AS column_count
                ORDER BY t.datasource, t.schema, t.name
                LIMIT $limit
            """,
            "parameters": params,
        }

        results = await client.execute_queries([query])
        return results[0] if results else []
    finally:
        await client.close()


async def fetch_table_columns(
    table_name: str,
    schema: str = "",
) -> list:
    """테이블 컬럼 목록 조회

    schema 매칭 정책:
      - "" / "public" : schema 무시. 이름 또는 fqn 으로만 매칭.
        (프론트가 fallback 으로 'public' 을 보내는데, Neo4j 노드에는
         schema 속성이 비어있거나 다른 값일 수 있어 0 건이 되는 문제 방지)
      - 그 외 : 해당 schema 에 정확 매칭하되, 노드에 schema 가 비어있는
        legacy 데이터도 함께 잡히도록 fallback.
    """
    client = CatalogGraphDatabase()
    try:
        params: dict = {"table_name": table_name, "graph_owner": ANALYSIS_GRAPH_OWNER}
        name_match = "(t.name = $table_name OR t.id ENDS WITH $table_name)"

        if schema and schema.lower() != "public":
            where_clause = (
                f"{name_match} AND "
                "(t.schema = $schema OR t.schema IS NULL OR t.schema = '')"
            )
            params["schema"] = schema
        else:
            where_clause = name_match

        query = {
            "query": f"""
                MATCH (t:TABLE)-[:HAS_COLUMN]->(c:COLUMN)
                WHERE t.graph_owner = $graph_owner AND c.graph_owner = $graph_owner
                  AND ({where_clause})
                RETURN c.name                 AS name,
                       t.name                 AS table_name,
                       c.dtype                AS dtype,
                       COALESCE(c.nullable, true) AS nullable,
                       c.description          AS description,
                       c.description_source   AS description_source,
                       c.analyzed_description AS analyzed_description
                ORDER BY c.name
            """,
            "parameters": params,
        }

        results = await client.execute_queries([query])
        rows = results[0] if results else []
        logger.info(
            "[schema_query] fetch_table_columns | table=%s | schema=%s | rows=%d",
            table_name, schema, len(rows),
        )
        return rows
    finally:
        await client.close()


# =============================================================================
# 참조 조회 (프로시저 / 프레임워크)
# =============================================================================

async def fetch_table_references(
    table_name: str,
    schema: str = "",
    column_name: Optional[str] = None,
) -> dict:
    """테이블 또는 컬럼이 참조된 프로시저 + 프레임워크 목록 조회

    Returns:
        {"references": [...], "framework_references": [...]}
    """
    client = CatalogGraphDatabase()
    try:
        # ── (A) 프로시저 전략 참조 ──
        proc_query = {
            "query": """
                MATCH (s)-[:FROM|WRITES]->(t:TABLE)
                WHERE t.graph_owner = $graph_owner
                  AND s.graph_owner = $graph_owner
                  AND (t.name = $table_name OR t.id ENDS WITH $table_name)
                  AND NOT s:FUNCTION AND NOT s:VARIABLE
                OPTIONAL MATCH (p)-[:PARENT_OF*]->(s)
                    WHERE p.graph_owner = $graph_owner
                      AND (p:PROCEDURE OR p:FUNCTION OR p:METHOD OR p:TRIGGER)
                RETURN DISTINCT
                    p.name           AS procedure_name,
                    labels(p)[0]     AS procedure_type,
                    p.start_line     AS start_line,
                    p.end_line       AS end_line,
                    s.type           AS statement_type,
                    s.start_line     AS statement_line,
                    s.start_line     AS evidence_line,
                    p.file_name      AS file_name,
                    p.file_directory AS file_directory,
                    p.file_path      AS file_path
                ORDER BY p.name, s.start_line
            """,
            "parameters": {"table_name": table_name, "graph_owner": ANALYSIS_GRAPH_OWNER},
        }

        # ── (B) 프레임워크 전략 참조 ──
        fw_query = {
            "query": """
                MATCH (src)-[r:READS|WRITES]->(t:TABLE)
                WHERE src.graph_owner = $graph_owner
                  AND t.graph_owner = $graph_owner
                  AND (t.name = $table_name OR t.id ENDS WITH $table_name)
                OPTIONAL MATCH (m)-[:HAS_MEMBER]->(src)
                WHERE m.graph_owner = $graph_owner
                RETURN DISTINCT
                    COALESCE(src.name, src.id) AS source_name,
                    labels(src)[0]   AS source_type,
                    type(r)          AS access_type,
                    src.start_line   AS start_line,
                    src.end_line     AS end_line,
                    r.evidence_line  AS evidence_line,
                    src.file_name    AS file_name,
                    src.file_path    AS file_path,
                    src.directory    AS file_directory,
                    m.name           AS module_name
                ORDER BY source_name

                UNION ALL

                MATCH (m)-[r:REFER_TO]->(t:TABLE)
                WHERE m.graph_owner = $graph_owner
                  AND t.graph_owner = $graph_owner
                  AND (t.name = $table_name OR t.id ENDS WITH $table_name)
                RETURN DISTINCT
                    m.name           AS source_name,
                    labels(m)[0]     AS source_type,
                    'REFER_TO'       AS access_type,
                    m.start_line     AS start_line,
                    m.end_line       AS end_line,
                    r.evidence_line  AS evidence_line,
                    m.file_name      AS file_name,
                    m.file_path      AS file_path,
                    m.file_directory AS file_directory,
                    m.name           AS module_name
                ORDER BY source_name
            """,
            "parameters": {"table_name": table_name, "graph_owner": ANALYSIS_GRAPH_OWNER},
        }

        results = await client.execute_queries([proc_query, fw_query])
        proc_records = results[0] if len(results) > 0 else []
        fw_records = results[1] if len(results) > 1 else []

        return {
            "references": proc_records,
            "framework_references": fw_records,
        }
    finally:
        await client.close()


async def fetch_procedure_statements(
    procedure_name: str,
    file_directory: Optional[str] = None,
) -> list:
    """프로시저의 모든 Statement와 AI 설명 조회"""
    client = CatalogGraphDatabase()
    try:
        params: dict = {"procedure_name": procedure_name, "graph_owner": ANALYSIS_GRAPH_OWNER}
        where_clause = "p.name = $procedure_name"

        if file_directory:
            where_clause += " AND p.file_directory = $file_directory"
            params["file_directory"] = file_directory

        query = {
            "query": f"""
                MATCH (p)-[:PARENT_OF*]->(s)
                WHERE p.graph_owner = $graph_owner AND s.graph_owner = $graph_owner
                  AND ({where_clause})
                  AND (s:Statement OR s.type IS NOT NULL)
                RETURN s.start_line     AS start_line,
                       s.end_line       AS end_line,
                       s.type           AS statement_type,
                       s.summary        AS summary,
                       s.ai_description AS ai_description
                ORDER BY s.start_line
            """,
            "parameters": params,
        }

        results = await client.execute_queries([query])
        return results[0] if results else []
    finally:
        await client.close()


# =============================================================================
# 관계 조회
# =============================================================================

async def fetch_schema_relationships() -> list:
    """테이블 관계 목록 조회 (FK_TO_TABLE, ONE_TO_*, MANY_TO_*)"""
    client = CatalogGraphDatabase()
    try:
        query = f"""
            MATCH (t1:TABLE)-[r]->(t2:TABLE)
            WHERE t1.graph_owner = '{ANALYSIS_GRAPH_OWNER}'
              AND t2.graph_owner = '{ANALYSIS_GRAPH_OWNER}'
              AND type(r) IN ['FK_TO_TABLE', 'ONE_TO_ONE',
                              'ONE_TO_MANY', 'MANY_TO_ONE', 'MANY_TO_MANY']
            RETURN t1.name       AS from_table,
                   t1.schema     AS from_schema,
                   r.sourceColumn AS from_column,
                   t2.name       AS to_table,
                   t2.schema     AS to_schema,
                   r.targetColumn AS to_column,
                   type(r)       AS relationship_type,
                   r.description AS description
            ORDER BY t1.name, t2.name
        """

        results = await client.execute_queries([query])
        return results[0] if results else []
    finally:
        await client.close()

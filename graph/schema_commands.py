"""스키마 편집 서비스

테이블 관계 CRUD (create/delete) + 테이블/컬럼 description 수정.
읽기 작업은 ``schema_metadata_queries``, 검색·벡터화는 ``metadata_semantic_search``가 담당한다.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import HTTPException
from integrations.embedding import CatalogEmbeddingGateway
from integrations.llm import create_metadata_llm_client
from graph.database import CatalogGraphDatabase
from graph.scope import ANALYSIS_GRAPH_OWNER

logger = logging.getLogger(__name__)
ALLOWED_RELATIONSHIP_TYPES = frozenset({
    "FK_TO_TABLE", "ONE_TO_ONE", "ONE_TO_MANY", "MANY_TO_ONE", "MANY_TO_MANY",
})


# =============================================================================
# 관계 생성 / 삭제
# =============================================================================

async def create_schema_relationship(
    from_table: str,
    from_schema: str,
    from_column: str,
    to_table: str,
    to_schema: str,
    to_column: str,
    relationship_type: str = "FK_TO_TABLE",
    description: str = "",
) -> dict:
    """테이블 관계 생성."""
    if relationship_type not in ALLOWED_RELATIONSHIP_TYPES:
        raise HTTPException(400, "지원하지 않는 관계 타입입니다.")
    client = CatalogGraphDatabase()
    try:
        # NOTE: 관계 타입은 Cypher 파라미터로 전달 불가 → f-string
        query = {
            "query": f"""
                MATCH (t1:TABLE {{name: $from_table}})
                MATCH (t2:TABLE {{name: $to_table}})
                WHERE t1.graph_owner = $graph_owner AND t2.graph_owner = $graph_owner
                  AND ($from_schema = '' OR t1.schema = $from_schema)
                  AND ($to_schema = '' OR t2.schema = $to_schema)
                MERGE (t1)-[r:{relationship_type}]->(t2)
                SET r.sourceColumn = $from_column,
                    r.targetColumn = $to_column,
                    r.description  = $description,
                    r.source       = 'user'
                RETURN t1.name AS from_table,
                       t2.name AS to_table
            """,
            "parameters": {
                "from_table": from_table,
                "to_table": to_table,
                "from_column": from_column,
                "to_column": to_column,
                "description": description,
                "from_schema": from_schema,
                "to_schema": to_schema,
                "graph_owner": ANALYSIS_GRAPH_OWNER,
            },
        }

        results = await client.execute_queries([query])
        if results and results[0]:
            return {"message": "관계가 생성되었습니다.", "created": True}
        raise HTTPException(404, "테이블을 찾을 수 없습니다.")
    finally:
        await client.close()


async def delete_schema_relationship(
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
    from_schema: str = "",
    to_schema: str = "",
) -> dict:
    """테이블 관계 삭제 (모든 관계 타입 대상)."""
    client = CatalogGraphDatabase()
    try:
        query = {
            "query": """
                MATCH (t1:TABLE {name: $from_table})
                      -[r]->
                      (t2:TABLE {name: $to_table})
                WHERE type(r) IN ['FK_TO_TABLE', 'ONE_TO_ONE',
                                  'ONE_TO_MANY', 'MANY_TO_ONE', 'MANY_TO_MANY']
                  AND t1.graph_owner = $graph_owner
                  AND t2.graph_owner = $graph_owner
                  AND ($from_schema = '' OR t1.schema = $from_schema)
                  AND ($to_schema = '' OR t2.schema = $to_schema)
                  AND r.sourceColumn = $from_column
                  AND r.targetColumn = $to_column
                DELETE r
                RETURN count(*) AS deleted
            """,
            "parameters": {
                "from_table": from_table,
                "to_table": to_table,
                "from_column": from_column,
                "to_column": to_column,
                "graph_owner": ANALYSIS_GRAPH_OWNER,
                "from_schema": from_schema,
                "to_schema": to_schema,
            },
        }

        results = await client.execute_queries([query])
        deleted = results[0][0]["deleted"] if results and results[0] else 0
        return {"message": f"{deleted}개 관계가 삭제되었습니다.", "deleted": deleted}
    finally:
        await client.close()


# =============================================================================
# 설명 업데이트
# =============================================================================

async def update_table_description(
    table_name: str,
    schema: str,
    description: str,
    api_key: Optional[str] = None,
) -> dict:
    """테이블 설명 업데이트 + (선택) 임베딩 재생성."""
    client = CatalogGraphDatabase()
    try:
        query = {
            "query": """
                MATCH (t:TABLE)
                WHERE t.name = $table_name
                  AND t.graph_owner = $graph_owner
                  AND (t.schema = $schema OR t.schema IS NULL)
                SET t.description        = $description,
                    t.description_source  = 'user'
                RETURN t.name AS name
            """,
            "parameters": {
                "table_name": table_name,
                "schema": schema,
                "description": description,
                "graph_owner": ANALYSIS_GRAPH_OWNER,
            },
        }

        results = await client.execute_queries([query])
        if not results or not results[0]:
            raise HTTPException(404, "테이블을 찾을 수 없습니다.")

        embedding_updated: Optional[bool] = None
        if api_key:
            embedding_updated = False
            try:
                openai_client = create_metadata_llm_client(api_key)
                embedding_gateway = CatalogEmbeddingGateway(openai_client)
                embedding = await embedding_gateway.embed_text(description)
                if embedding:
                    await client.execute_queries([{
                        "query": """
                            MATCH (t:TABLE {name: $table_name})
                            WHERE t.graph_owner = $graph_owner
                              AND (t.schema = $schema OR t.schema IS NULL)
                            SET t.embedding = $embedding
                        """,
                        "parameters": {
                            "table_name": table_name,
                            "embedding": embedding,
                            "schema": schema,
                            "graph_owner": ANALYSIS_GRAPH_OWNER,
                        },
                    }])
                    embedding_updated = True
            except Exception as e:
                logger.error("테이블 임베딩 업데이트 실패 | error_type=%s", type(e).__name__)

        return {
            "message": "테이블 설명이 업데이트되었습니다.",
            "updated": True,
            "embedding_updated": embedding_updated,
        }
    finally:
        await client.close()


async def update_column_description(
    table_name: str,
    table_schema: str,
    column_name: str,
    description: str,
    api_key: Optional[str] = None,
) -> dict:
    """컬럼 설명 업데이트."""
    client = CatalogGraphDatabase()
    try:
        query = {
            "query": """
                MATCH (t:TABLE)-[:HAS_COLUMN]->(c:COLUMN)
                WHERE t.name = $table_name
                  AND t.graph_owner = $graph_owner
                  AND c.graph_owner = $graph_owner
                  AND ($table_schema = '' OR t.schema = $table_schema)
                  AND c.name = $column_name
                SET c.description        = $description,
                    c.description_source  = 'user'
                RETURN c.name AS name
            """,
            "parameters": {
                "table_name": table_name,
                "column_name": column_name,
                "description": description,
                "table_schema": table_schema,
                "graph_owner": ANALYSIS_GRAPH_OWNER,
            },
        }

        results = await client.execute_queries([query])
        if not results or not results[0]:
            raise HTTPException(404, "컬럼을 찾을 수 없습니다.")

        return {"message": "컬럼 설명이 업데이트되었습니다.", "updated": True}
    finally:
        await client.close()

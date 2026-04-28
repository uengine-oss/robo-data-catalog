"""스키마 편집 서비스

테이블 관계 CRUD (create/delete) + 테이블/컬럼 description 수정.
읽기 작업은 schema_query_service, 검색·벡터화는 schema_search_service 참조.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import HTTPException
from openai import AsyncOpenAI

from client.embedding_client import EmbeddingClient
from client.neo4j_client import Neo4jClient

logger = logging.getLogger(__name__)


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
    client = Neo4jClient()
    try:
        # NOTE: 관계 타입은 Cypher 파라미터로 전달 불가 → f-string
        query = {
            "query": f"""
                MATCH (t1:Table {{name: $from_table}})
                MATCH (t2:Table {{name: $to_table}})
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
) -> dict:
    """테이블 관계 삭제 (모든 관계 타입 대상)."""
    client = Neo4jClient()
    try:
        query = {
            "query": """
                MATCH (t1:Table {name: $from_table})
                      -[r]->
                      (t2:Table {name: $to_table})
                WHERE type(r) IN ['FK_TO_TABLE', 'ONE_TO_ONE',
                                  'ONE_TO_MANY', 'MANY_TO_ONE', 'MANY_TO_MANY']
                  AND r.sourceColumn = $from_column
                  AND r.targetColumn = $to_column
                DELETE r
                RETURN count(*) AS deleted
            """,
            "params": {
                "from_table": from_table,
                "to_table": to_table,
                "from_column": from_column,
                "to_column": to_column,
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
    client = Neo4jClient()
    try:
        query = {
            "query": """
                MATCH (t:Table)
                WHERE t.name = $table_name
                  AND (t.schema = $schema OR t.schema IS NULL)
                SET t.description        = $description,
                    t.description_source  = 'user'
                RETURN t.name AS name
            """,
            "parameters": {
                "table_name": table_name,
                "schema": schema,
                "description": description,
            },
        }

        results = await client.execute_queries([query])
        if not results or not results[0]:
            raise HTTPException(404, "테이블을 찾을 수 없습니다.")

        if api_key:
            try:
                openai_client = AsyncOpenAI(api_key=api_key)
                embedding_client = EmbeddingClient(openai_client)
                embedding = await embedding_client.embed_text(description)
                if embedding:
                    await client.execute_queries([{
                        "query": """
                            MATCH (t:Table {name: $table_name})
                            SET t.embedding = $embedding
                        """,
                        "parameters": {
                            "table_name": table_name,
                            "embedding": embedding,
                        },
                    }])
            except Exception as e:
                error_msg = f"임베딩 업데이트 실패: {e}"
                logger.error(error_msg)
                raise RuntimeError(error_msg) from e

        return {"message": "테이블 설명이 업데이트되었습니다.", "updated": True}
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
    client = Neo4jClient()
    try:
        query = {
            "query": """
                MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
                WHERE t.name = $table_name
                  AND c.name = $column_name
                SET c.description        = $description,
                    c.description_source  = 'user'
                RETURN c.name AS name
            """,
            "parameters": {
                "table_name": table_name,
                "column_name": column_name,
                "description": description,
            },
        }

        results = await client.execute_queries([query])
        if not results or not results[0]:
            raise HTTPException(404, "컬럼을 찾을 수 없습니다.")

        return {"message": "컬럼 설명이 업데이트되었습니다.", "updated": True}
    finally:
        await client.close()

"""스키마 검색·벡터화 서비스

- 시멘틱 검색: 테이블 설명 임베딩 기반 cosine 유사도
- 벡터화: 테이블 설명 → 임베딩 생성 후 Neo4j Table.embedding 저장
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
from fastapi import HTTPException
from openai import AsyncOpenAI

from client.embedding_client import EmbeddingClient
from client.neo4j_client import Neo4jClient

logger = logging.getLogger(__name__)


# =============================================================================
# 시멘틱 검색
# =============================================================================

async def search_tables_by_semantic(
    query: str,
    limit: int,
    api_key: str,
) -> list:
    """테이블 설명의 의미적 유사도 기반 검색.

    Returns:
        [{"name", "schema", "description", "similarity"}, ...]
        similarity >= 0.3 필터링, 상위 limit개.
    """
    if not api_key:
        raise HTTPException(400, {"error": "OpenAI API 키가 필요합니다."})

    client = Neo4jClient()
    try:
        openai_client = AsyncOpenAI(api_key=api_key)
    except Exception as e:
        raise HTTPException(400, {"error": f"OpenAI API 키가 유효하지 않습니다: {e}"})

    try:
        # 설명이 있는 테이블 최대 200개 조회
        cypher_query = """
            MATCH (t:Table)
            WHERE t.description IS NOT NULL
              AND t.description <> ''
            RETURN t.name        AS name,
                   t.schema      AS schema,
                   t.description AS description
            ORDER BY t.name
            LIMIT 200
        """

        results = await client.execute_queries([cypher_query])
        records = results[0] if results else []
        if not records:
            return []

        # 쿼리 임베딩
        query_response = await openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=query,
        )
        query_embedding = np.array(query_response.data[0].embedding)

        # 설명 임베딩 (배치)
        descriptions = [
            (r.get("description") or "no description")[:500]
            for r in records
        ]
        desc_response = await openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=descriptions,
        )

        # 코사인 유사도
        scored = []
        for i, record in enumerate(records):
            desc_embedding = np.array(desc_response.data[i].embedding)
            norm_product = (
                np.linalg.norm(query_embedding) * np.linalg.norm(desc_embedding)
            )
            similarity = (
                0.0 if norm_product == 0
                else float(np.dot(query_embedding, desc_embedding) / norm_product)
            )
            scored.append({
                "name": record["name"],
                "schema": record["schema"] or "public",
                "description": record["description"][:200],
                "similarity": round(similarity, 4),
            })

        scored.sort(key=lambda x: x["similarity"], reverse=True)
        return [r for r in scored[:limit] if r["similarity"] >= 0.3]
    finally:
        await client.close()


# =============================================================================
# 벡터화
# =============================================================================

async def vectorize_schema_tables(
    db_name: str = "postgres",
    schema: Optional[str] = None,
    include_tables: bool = True,
    include_columns: bool = True,
    reembed_existing: bool = False,
    batch_size: int = 100,
    api_key: Optional[str] = None,
) -> dict:
    """전체 스키마 벡터화 (Table.embedding 저장)."""
    if not api_key:
        raise HTTPException(400, {"error": "OpenAI API 키가 필요합니다."})

    openai_client = AsyncOpenAI(api_key=api_key)
    embedding_client = EmbeddingClient(openai_client)

    client = Neo4jClient()
    stats = {"tables_processed": 0, "columns_processed": 0, "errors": 0}

    try:
        if include_tables:
            where_conditions: list[str] = []
            params: dict = {"batch_size": batch_size}

            if schema:
                where_conditions.append("t.schema = $schema")
                params["schema"] = schema
            if not reembed_existing:
                where_conditions.append("t.embedding IS NULL")

            where_clause = (
                "WHERE " + " AND ".join(where_conditions)
                if where_conditions else ""
            )

            query = {
                "query": f"""
                    MATCH (t:Table)
                    {where_clause}
                    RETURN t.name        AS name,
                           t.description AS description,
                           t.schema      AS schema
                    LIMIT $batch_size
                """,
                "parameters": params,
            }

            results = await client.execute_queries([query])
            tables = results[0] if results else []

            for table in tables:
                try:
                    text = EmbeddingClient.format_table_text(
                        table["name"],
                        table.get("description") or "",
                    )
                    embedding = await embedding_client.embed_text(text)
                    if embedding:
                        await client.execute_queries([{
                            "query": """
                                MATCH (t:Table {name: $table_name})
                                SET t.embedding = $embedding
                            """,
                            "parameters": {
                                "table_name": table["name"],
                                "embedding": embedding,
                            },
                        }])
                        stats["tables_processed"] += 1
                except Exception as e:
                    error_msg = f"테이블 '{table['name']}' 벡터화 실패: {e}"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg) from e

        return {"message": "벡터화가 완료되었습니다.", "stats": stats}
    finally:
        await client.close()

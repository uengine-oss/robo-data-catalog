"""On-demand semantic search over human table descriptions."""
from __future__ import annotations

from typing import Any

import numpy as np
from fastapi import HTTPException

from graph.database import CatalogGraphDatabase
from graph.scope import ANALYZER_OWNER
from integrations.embedding import CatalogEmbeddingGateway
from integrations.llm import create_metadata_llm_client


_SIMILARITY_THRESHOLD = 0.3


async def search_tables_by_semantic(
    query: str,
    limit: int,
    api_key: str,
) -> list[dict[str, Any]]:
    """Embed one request in memory; do not create a second stored vector corpus."""
    if not api_key:
        raise HTTPException(400, {"error": "OpenAI API 키가 필요합니다."})
    if not query.strip():
        raise HTTPException(400, {"error": "검색어가 필요합니다."})

    try:
        gateway = CatalogEmbeddingGateway(create_metadata_llm_client(api_key))
    except Exception as exc:
        raise HTTPException(400, {"error": "OpenAI API 키를 사용할 수 없습니다."}) from exc

    client = CatalogGraphDatabase()
    try:
        results = await client.execute_queries([{
            "query": """
                MATCH (table:TABLE)
                WHERE table._owner = $owner
                  AND table.description IS NOT NULL
                  AND table.description <> ''
                RETURN table.name AS name,
                       table.schema AS schema,
                       table.description AS description
                ORDER BY table.name
                LIMIT 200
            """,
            "parameters": {"owner": ANALYZER_OWNER},
        }])
        records = results[0] if results else []
        if not records:
            return []

        descriptions = [(record.get("description") or "")[:500] for record in records]
        embeddings = await gateway.embed_texts([query, *descriptions])
        query_embedding = np.asarray(embeddings[0], dtype=float)
        scored: list[dict[str, Any]] = []
        for record, raw_embedding in zip(records, embeddings[1:]):
            description_embedding = np.asarray(raw_embedding, dtype=float)
            norm_product = (
                np.linalg.norm(query_embedding) * np.linalg.norm(description_embedding)
            )
            similarity = (
                0.0 if norm_product == 0 else
                float(np.dot(query_embedding, description_embedding) / norm_product)
            )
            if similarity < _SIMILARITY_THRESHOLD:
                continue
            scored.append({
                "name": record["name"],
                "schema": record["schema"] or "public",
                "description": record["description"][:200],
                "similarity": round(similarity, 4),
            })
        scored.sort(key=lambda item: item["similarity"], reverse=True)
        return scored[:max(1, min(int(limit), 100))]
    finally:
        await client.close()

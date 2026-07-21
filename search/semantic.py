"""Semantic schema search and explicit table/column vectorization."""

from __future__ import annotations

from typing import Any, Optional

import numpy as np
from fastapi import HTTPException

from integrations.embedding import CatalogEmbeddingGateway
from integrations.llm import create_metadata_llm_client
from graph.database import CatalogGraphDatabase
from graph.scope import ANALYSIS_GRAPH_OWNER


_SIMILARITY_THRESHOLD = 0.3


async def search_tables_by_semantic(query: str, limit: int, api_key: str) -> list[dict[str, Any]]:
    """Return description-based table matches ordered by cosine similarity."""
    if not api_key:
        raise HTTPException(400, {"error": "OpenAI API 키가 필요합니다."})
    if not query.strip():
        raise HTTPException(400, {"error": "검색어가 필요합니다."})

    try:
        embedding_gateway = CatalogEmbeddingGateway(create_metadata_llm_client(api_key))
    except Exception as exc:
        raise HTTPException(400, {"error": "OpenAI API 키를 사용할 수 없습니다."}) from exc

    client = CatalogGraphDatabase()
    try:
        results = await client.execute_queries(
            [
                {
                    "query": """
                        MATCH (t:TABLE)
                        WHERE t.graph_owner = $graph_owner
                          AND t.description IS NOT NULL
                          AND t.description <> ''
                        RETURN t.name AS name,
                               t.schema AS schema,
                               t.description AS description
                        ORDER BY t.name
                        LIMIT 200
                    """,
                    "parameters": {"graph_owner": ANALYSIS_GRAPH_OWNER},
                }
            ]
        )
        records = results[0] if results else []
        if not records:
            return []

        descriptions = [(record.get("description") or "")[:500] for record in records]
        embeddings = await embedding_gateway.embed_texts([query, *descriptions])
        query_embedding = np.asarray(embeddings[0], dtype=float)

        scored: list[dict[str, Any]] = []
        for record, raw_embedding in zip(records, embeddings[1:]):
            description_embedding = np.asarray(raw_embedding, dtype=float)
            norm_product = np.linalg.norm(query_embedding) * np.linalg.norm(description_embedding)
            similarity = (
                0.0
                if norm_product == 0
                else float(np.dot(query_embedding, description_embedding) / norm_product)
            )
            if similarity < _SIMILARITY_THRESHOLD:
                continue
            scored.append(
                {
                    "name": record["name"],
                    "schema": record["schema"] or "public",
                    "description": record["description"][:200],
                    "similarity": round(similarity, 4),
                }
            )

        scored.sort(key=lambda item: item["similarity"], reverse=True)
        return scored[: max(1, min(int(limit), 100))]
    finally:
        await client.close()


def _target_query(kind: str) -> str:
    if kind == "tables":
        return """
            MATCH (n:TABLE)
            WHERE n.graph_owner = $graph_owner
              AND ($schema IS NULL OR n.schema = $schema)
              AND ($db_name IS NULL OR coalesce(n.db, n.datasource) = $db_name)
              AND ($reembed_existing OR n.embedding IS NULL)
            RETURN elementId(n) AS node_id,
                   n.name AS name,
                   n.description AS description,
                   null AS table_name
            ORDER BY n.name
            LIMIT $batch_size
        """
    if kind == "columns":
        return """
            MATCH (t:TABLE)-[:HAS_COLUMN]->(n:COLUMN)
            WHERE t.graph_owner = $graph_owner
              AND n.graph_owner = $graph_owner
              AND ($schema IS NULL OR t.schema = $schema)
              AND ($db_name IS NULL OR coalesce(t.db, t.datasource) = $db_name)
              AND ($reembed_existing OR n.embedding IS NULL)
            RETURN elementId(n) AS node_id,
                   n.name AS name,
                   n.description AS description,
                   t.name AS table_name
            ORDER BY t.name, n.name
            LIMIT $batch_size
        """
    raise ValueError(f"unsupported vectorization kind: {kind}")


def _embedding_text(kind: str, target: dict[str, Any]) -> str:
    if kind == "tables":
        return CatalogEmbeddingGateway.format_table_text(
            target["name"], target.get("description") or ""
        )
    return " | ".join(
        part
        for part in (
            f"Table: {target.get('table_name') or ''}",
            f"Column: {target['name']}",
            f"Description: {target.get('description') or ''}",
        )
        if not part.endswith(": ")
    )


async def _vectorize_kind(
    *,
    client: CatalogGraphDatabase,
    embedding_gateway: CatalogEmbeddingGateway,
    kind: str,
    params: dict[str, Any],
) -> int:
    results = await client.execute_queries(
        [{"query": _target_query(kind), "parameters": params}]
    )
    targets = results[0] if results else []
    if not targets:
        return 0

    embeddings = await embedding_gateway.embed_texts(
        [_embedding_text(kind, target) for target in targets]
    )
    update_results = await client.execute_queries(
        [
            {
                "query": """
                    UNWIND $items AS item
                    MATCH (n)
                    WHERE elementId(n) = item.node_id
                      AND n.graph_owner = $graph_owner
                    SET n.embedding = item.embedding
                    RETURN count(n) AS persisted
                """,
                "parameters": {
                    "items": [
                        {"node_id": target["node_id"], "embedding": embedding}
                        for target, embedding in zip(targets, embeddings)
                    ],
                    "graph_owner": ANALYSIS_GRAPH_OWNER,
                },
            }
        ]
    )
    rows = update_results[0] if update_results else []
    return int(rows[0]["persisted"]) if rows else 0


async def vectorize_schema_tables(
    db_name: Optional[str] = "postgres",
    schema: Optional[str] = None,
    include_tables: bool = True,
    include_columns: bool = True,
    reembed_existing: bool = False,
    batch_size: int = 100,
    api_key: Optional[str] = None,
) -> dict[str, Any]:
    """Vectorize requested schema node kinds in bounded provider batches."""
    if not api_key:
        raise HTTPException(400, {"error": "OpenAI API 키가 필요합니다."})
    if not include_tables and not include_columns:
        raise HTTPException(400, {"error": "벡터화할 노드 종류가 필요합니다."})

    embedding_gateway = CatalogEmbeddingGateway(create_metadata_llm_client(api_key))
    client = CatalogGraphDatabase()
    params = {
        "batch_size": max(1, min(int(batch_size), 1000)),
        "graph_owner": ANALYSIS_GRAPH_OWNER,
        "schema": schema,
        "db_name": db_name,
        "reembed_existing": reembed_existing,
    }
    stats = {"tables_processed": 0, "columns_processed": 0, "errors": 0}
    try:
        if include_tables:
            stats["tables_processed"] = await _vectorize_kind(
                client=client,
                embedding_gateway=embedding_gateway,
                kind="tables",
                params=params,
            )
        if include_columns:
            stats["columns_processed"] = await _vectorize_kind(
                client=client,
                embedding_gateway=embedding_gateway,
                kind="columns",
                params=params,
            )
        return {"message": "벡터화가 완료되었습니다.", "stats": stats}
    finally:
        await client.close()

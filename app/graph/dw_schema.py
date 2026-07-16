"""DW 스타스키마 서비스

OLAP 스타스키마 등록 및 삭제 기능을 제공합니다.

주요 기능:
- DW 스타스키마 등록 (팩트/디멘전 테이블 + 벡터화)
- DW 스타스키마 삭제
"""

import logging
from typing import List

from fastapi import HTTPException

from app.graph.client import Neo4jClient
from app.graph.ownership import ANALYSIS_GRAPH_OWNER
from app.external.embedding_client import EmbeddingClient


logger = logging.getLogger(__name__)


async def register_star_schema(
    cube_name: str,
    db_name: str,
    dw_schema: str,
    fact_table: dict,
    dimensions: List[dict],
    create_embeddings: bool = True,
    api_key: str = None
) -> dict:
    """DW 스타스키마 등록
    
    Args:
        cube_name: 큐브 이름
        db_name: 데이터베이스 이름
        dw_schema: DW 스키마 이름
        fact_table: 팩트 테이블 정보
        dimensions: 디멘전 테이블 정보 리스트
        create_embeddings: 임베딩 자동 생성 여부
        api_key: OpenAI API 키 (create_embeddings가 True일 때 필수)
        
    Returns:
        등록 결과 통계
    """
    if create_embeddings and not api_key:
        raise HTTPException(400, {"error": "임베딩 생성을 위해 OpenAI API 키가 필요합니다."})
    
    client = Neo4jClient()
    stats = {
        "tables_created": 0,
        "columns_created": 0,
        "relationships_created": 0,
        "embeddings_created": 0,
        "embedding_errors": 0,
    }
    
    try:
        embedding_client = None
        if create_embeddings and api_key:
            from app.external.openai_client import create_openai_client
            openai_client = create_openai_client(api_key)
            embedding_client = EmbeddingClient(openai_client)
        graph_queries = []
        
        # 1. 팩트 테이블 생성
        fact_name = fact_table.get("name", "")
        fact_fqn = f"{dw_schema}.{fact_name}"
        
        fact_query = {
            "query": """
                MERGE (__cy_t__:TABLE {id:$fqn, graph_owner:$graph_owner})
                SET __cy_t__.name = $name,
                    __cy_t__.schema = $schema,
                    __cy_t__.db_name = $db_name,
                    __cy_t__.table_type = 'FACT',
                    __cy_t__.cube_name = $cube_name
                RETURN __cy_t__.name AS name
            """,
            "parameters": {
                "fqn": fact_fqn,
                "name": fact_name,
                "schema": dw_schema,
                "db_name": db_name,
                "cube_name": cube_name,
                "graph_owner": ANALYSIS_GRAPH_OWNER,
            }
        }
        
        graph_queries.append(fact_query)
        stats["tables_created"] += 1
        
        # 팩트 테이블 컬럼 생성
        for col in fact_table.get("columns", []):
            col_fqn = f"{fact_fqn}.{col['name']}"
            col_query = {
                "query": """
                    MATCH (__cy_t__:TABLE {id:$table_fqn, graph_owner:$graph_owner})
                    MERGE (__cy_t__)-[:HAS_COLUMN]->(__cy_c__:COLUMN {id:$col_fqn, graph_owner:$graph_owner})
                    SET __cy_c__.name = $col_name,
                        __cy_c__.dtype = $dtype,
                        __cy_c__.description = $description,
                        __cy_c__.is_pk = $is_pk,
                        __cy_c__.is_fk = $is_fk
                """,
                "parameters": {
                    "table_fqn": fact_fqn,
                    "col_fqn": col_fqn,
                    "col_name": col["name"],
                    "dtype": col.get("dtype", "VARCHAR"),
                    "description": col.get("description") or "",
                    "is_pk": col.get("is_pk", False),
                    "is_fk": col.get("is_fk", False),
                    "graph_owner": ANALYSIS_GRAPH_OWNER,
                }
            }
            graph_queries.append(col_query)
            stats["columns_created"] += 1
        
        # 2. 디멘전 테이블 생성
        for dim in dimensions:
            dim_name = dim.get("name", "")
            dim_fqn = f"{dw_schema}.{dim_name}"
            
            dim_query = {
                "query": """
                    MERGE (__cy_t__:TABLE {id:$fqn, graph_owner:$graph_owner})
                    SET __cy_t__.name = $name,
                        __cy_t__.schema = $schema,
                        __cy_t__.db_name = $db_name,
                        __cy_t__.table_type = 'DIMENSION',
                        __cy_t__.cube_name = $cube_name
                    RETURN __cy_t__.name AS name
                """,
                "parameters": {
                    "fqn": dim_fqn,
                    "name": dim_name,
                    "schema": dw_schema,
                    "db_name": db_name,
                    "cube_name": cube_name,
                    "graph_owner": ANALYSIS_GRAPH_OWNER,
                }
            }
            
            graph_queries.append(dim_query)
            stats["tables_created"] += 1
            
            # 디멘전 테이블 컬럼 생성
            for col in dim.get("columns", []):
                col_fqn = f"{dim_fqn}.{col['name']}"
                col_query = {
                    "query": """
                        MATCH (__cy_t__:TABLE {id:$table_fqn, graph_owner:$graph_owner})
                        MERGE (__cy_t__)-[:HAS_COLUMN]->(__cy_c__:COLUMN {id:$col_fqn, graph_owner:$graph_owner})
                        SET __cy_c__.name = $col_name,
                            __cy_c__.dtype = $dtype,
                            __cy_c__.description = $description
                    """,
                    "parameters": {
                        "table_fqn": dim_fqn,
                        "col_fqn": col_fqn,
                        "col_name": col["name"],
                        "dtype": col.get("dtype", "VARCHAR"),
                        "description": col.get("description") or "",
                        "graph_owner": ANALYSIS_GRAPH_OWNER,
                    }
                }
                graph_queries.append(col_query)
                stats["columns_created"] += 1
        
        # 3. FK 관계 생성 (팩트 → 디멘전)
        for col in fact_table.get("columns", []):
            if col.get("is_fk") and col.get("fk_target_table"):
                target_table = col["fk_target_table"]
                fk_query = {
                    "query": """
                        MATCH (__cy_f__:TABLE {id:$fact_fqn, graph_owner:$graph_owner})
                        MATCH (__cy_d__:TABLE {id:$target_fqn, graph_owner:$graph_owner})
                        MERGE (__cy_f__)-[__cy_r__:FK_TO_TABLE]->(__cy_d__)
                        SET __cy_r__.sourceColumn = $source_column,
                            __cy_r__.source = 'user'
                    """,
                    "parameters": {
                        "fact_fqn": fact_fqn,
                        "target_fqn": target_table,
                        "source_column": col["name"],
                        "graph_owner": ANALYSIS_GRAPH_OWNER,
                    }
                }
                graph_queries.append(fk_query)
                stats["relationships_created"] += 1

        # 구조 write는 한 Neo4j transaction으로 실행해 중간 실패 시 전부 rollback한다.
        if graph_queries:
            await client.execute_queries(graph_queries)
        
        # 4. 임베딩 생성
        if embedding_client:
            # 팩트 테이블 임베딩
            try:
                fact_text = EmbeddingClient.format_table_text(
                    fact_name,
                    "",
                    [col["name"] for col in fact_table.get("columns", [])]
                )
                fact_embedding = await embedding_client.embed_text(fact_text)
                
                if fact_embedding:
                    embed_query = {
                        "query": """
                            MATCH (__cy_t__:TABLE {id:$fqn, graph_owner:$graph_owner})
                            SET __cy_t__.embedding = $embedding
                        """,
                        "parameters": {
                            "fqn": fact_fqn,
                            "embedding": fact_embedding,
                            "graph_owner": ANALYSIS_GRAPH_OWNER,
                        }
                    }
                    await client.execute_queries([embed_query])
                    stats["embeddings_created"] += 1
            except Exception as e:
                logger.error("팩트 테이블 임베딩 실패 | error_type=%s", type(e).__name__)
                stats["embedding_errors"] += 1
            
            # 디멘전 테이블 임베딩
            for dim in dimensions:
                try:
                    dim_fqn = f"{dw_schema}.{dim['name']}"
                    dim_text = EmbeddingClient.format_table_text(
                        dim["name"],
                        "",
                        [col["name"] for col in dim.get("columns", [])]
                    )
                    dim_embedding = await embedding_client.embed_text(dim_text)
                    
                    if dim_embedding:
                        embed_query = {
                            "query": """
                                MATCH (__cy_t__:TABLE {id:$fqn, graph_owner:$graph_owner})
                                SET __cy_t__.embedding = $embedding
                            """,
                            "parameters": {
                                "fqn": dim_fqn,
                                "embedding": dim_embedding,
                                "graph_owner": ANALYSIS_GRAPH_OWNER,
                            }
                        }
                        await client.execute_queries([embed_query])
                        stats["embeddings_created"] += 1
                except Exception as e:
                    logger.error(
                        "디멘전 테이블 임베딩 실패 | table=%s error_type=%s",
                        dim["name"], type(e).__name__,
                    )
                    stats["embedding_errors"] += 1
        
        return {
            "message": f"스타스키마 '{cube_name}'이 등록되었습니다.",
            "stats": stats
        }
    finally:
        await client.close()


async def delete_star_schema(
    cube_name: str,
    schema: str = "dw",
    db_name: str = "postgres"
) -> dict:
    """DW 스타스키마 삭제
    
    Args:
        cube_name: 큐브 이름
        schema: 스키마 이름
        db_name: 데이터베이스 이름
        
    Returns:
        삭제 결과
    """
    client = Neo4jClient()
    try:
        # 큐브에 속한 모든 테이블과 컬럼, 관계 삭제
        query = {
            "query": """
                MATCH (__cy_t__:TABLE {cube_name: $cube_name})
                WHERE __cy_t__.graph_owner = $graph_owner
                OPTIONAL MATCH (__cy_t__)-[:HAS_COLUMN]->(__cy_c__:COLUMN {graph_owner: $graph_owner})
                OPTIONAL MATCH (__cy_t__)-[__cy_r__:FK_TO_TABLE]->()
                DETACH DELETE __cy_t__, __cy_c__
                RETURN count(DISTINCT __cy_t__) AS deleted_tables
            """,
            "parameters": {"cube_name": cube_name, "graph_owner": ANALYSIS_GRAPH_OWNER}
        }
        
        results = await client.execute_queries([query])
        deleted = results[0][0]["deleted_tables"] if results and results[0] else 0
        
        return {
            "message": f"스타스키마 '{cube_name}'이 삭제되었습니다.",
            "deleted_tables": deleted
        }
    finally:
        await client.close()


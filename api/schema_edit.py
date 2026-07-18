"""Domain router extracted from the Catalog HTTP boundary."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request

from api.errors import error_body as _error_body
from api.graph_connection import apply_neo4j_override
from contracts import (
    AddRelationshipRequest,
    ColumnDescriptionUpdateRequest,
    TableDescriptionUpdateRequest,
    VectorizeRequest,
)
from graph import schema_commands as schema_metadata_commands
from search import semantic as metadata_semantic_search
from settings import CATALOG_SETTINGS

router = APIRouter(
    prefix=CATALOG_SETTINGS.api_prefix,
    dependencies=[Depends(apply_neo4j_override)],
)
logger = logging.getLogger(__name__)

@router.post("/schema/relationships")
async def add_schema_relationship(body: AddRelationshipRequest):
    """테이블 관계 추가"""
    logger.info("[API] 관계 추가 | %s -> %s", body.from_table, body.to_table)
    try:
        return await schema_metadata_commands.create_schema_relationship(
            from_table=body.from_table,
            from_schema=body.from_schema,
            from_column=body.from_column,
            to_table=body.to_table,
            to_schema=body.to_schema,
            to_column=body.to_column,
            relationship_type=body.relationship_type,
            description=body.description,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 관계 추가 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.delete("/schema/relationships")
async def remove_schema_relationship(
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
    from_schema: str = "",
    to_schema: str = "",
):
    """테이블 관계 삭제"""
    logger.info("[API] 관계 삭제 | %s.%s -> %s.%s", from_table, from_column, to_table, to_column)
    try:
        return await schema_metadata_commands.delete_schema_relationship(
            from_table, from_column, to_table, to_column, from_schema, to_schema
        )
    except Exception as e:
        logger.error("[API] 관계 삭제 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.put("/schema/tables/{table_name}/description")
async def update_table_description(
    request: Request, table_name: str, body: TableDescriptionUpdateRequest,
):
    """테이블 설명 업데이트"""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(400, "X-API-Key 헤더가 필요합니다.")
    logger.info("[API] 테이블 설명 업데이트 | table=%s | schema=%s", table_name, body.table_schema)
    try:
        return await schema_metadata_commands.update_table_description(
            table_name=table_name,
            schema=body.table_schema,
            description=body.description,
            api_key=api_key,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 테이블 설명 업데이트 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.put("/schema/tables/{table_name}/columns/{column_name}/description")
async def update_column_description(
    request: Request,
    table_name: str,
    column_name: str,
    body: ColumnDescriptionUpdateRequest,
):
    """컬럼 설명 업데이트"""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(400, "X-API-Key 헤더가 필요합니다.")
    logger.info("[API] 컬럼 설명 업데이트 | table=%s | column=%s", table_name, column_name)
    try:
        return await schema_metadata_commands.update_column_description(
            table_name=table_name,
            table_schema=body.table_schema,
            column_name=column_name,
            description=body.description,
            api_key=api_key,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 컬럼 설명 업데이트 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.post("/schema/vectorize")
async def vectorize_schema(request: Request, body: VectorizeRequest):
    """전체 스키마 벡터화"""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(400, "X-API-Key 헤더가 필요합니다.")
    logger.info("[API] 스키마 벡터화 요청")
    try:
        return await metadata_semantic_search.vectorize_schema_tables(
            db_name=body.db_name,
            schema=body.table_schema,
            include_tables=body.include_tables,
            include_columns=body.include_columns,
            reembed_existing=body.reembed_existing,
            batch_size=body.batch_size,
            api_key=api_key,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 스키마 벡터화 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))

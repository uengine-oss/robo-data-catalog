"""Domain router extracted from the Catalog HTTP boundary."""
from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException

from api.errors import error_body as _error_body
from api.graph_connection import apply_neo4j_override
from contracts import SchemaColumnInfo, SchemaRelationshipInfo, SchemaTableInfo
from graph import schema_queries as schema_metadata_queries
from shared.config.settings import CATALOG_SETTINGS

router = APIRouter(
    prefix=CATALOG_SETTINGS.api_prefix,
    dependencies=[Depends(apply_neo4j_override)],
)
logger = logging.getLogger(__name__)

@router.get("/schema/tables", response_model=List[SchemaTableInfo])
async def list_schema_tables(
    search: Optional[str] = None,
    schema: Optional[str] = None,
    limit: int = 100,
):
    """테이블 목록 조회"""
    logger.info("[API] 테이블 목록 조회")
    try:
        records = await schema_metadata_queries.fetch_schema_tables(search, schema, limit)
        tables = [
            SchemaTableInfo(
                name=r["name"],
                table_schema=r["schema"] or "",
                datasource=r.get("datasource") or "",
                logical_name=r.get("logical_name") or "",
                description=r["description"] or "",
                description_source=r.get("description_source") or "",
                analyzed_description=r.get("analyzed_description") or "",
                column_count=r["column_count"] or 0,
            )
            for r in records
        ]
        logger.info("[API] 테이블 목록 조회 완료 | count=%d", len(tables))
        return tables
    except Exception as e:
        logger.error("[API] 테이블 목록 조회 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.get("/schema/tables/{table_name}/columns", response_model=List[SchemaColumnInfo])
async def get_table_columns(table_name: str, schema: Optional[str] = None):
    """테이블 컬럼 목록 조회.

    schema 파라미터는 선택. 미지정/'public' 이면 이름/fqn 기준으로만 매칭.
    """
    logger.info("[API] 컬럼 조회 | table=%s | schema=%s", table_name, schema)
    try:
        records = await schema_metadata_queries.fetch_table_columns(table_name, schema or "")
        columns = [
            SchemaColumnInfo(
                name=r["name"],
                table_name=r["table_name"],
                dtype=r["dtype"] or "",
                nullable=r.get("nullable", True),
                description=r.get("description") or "",
                description_source=r.get("description_source") or "",
                analyzed_description=r.get("analyzed_description") or "",
            )
            for r in records
        ]
        return columns
    except Exception as e:
        logger.error("[API] 컬럼 조회 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.get("/schema/tables/{table_name}/references")
async def get_table_references(
    table_name: str,
    schema: Optional[str] = None,
    column_name: Optional[str] = None,
):
    """테이블 또는 컬럼이 참조된 프로시저 목록 조회"""
    if schema is None:
        raise HTTPException(400, "schema 파라미터가 필요합니다.")
    logger.info("[API] 테이블 참조 조회 | table=%s | schema=%s | column=%s",
                table_name, schema, column_name)
    try:
        return await schema_metadata_queries.fetch_table_references(table_name, schema, column_name)
    except Exception as e:
        logger.error("[API] 테이블 참조 조회 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.get("/schema/procedures/{procedure_name}/statements")
async def get_procedure_statements(procedure_name: str, file_directory: Optional[str] = None):
    """프로시저의 모든 Statement와 AI 설명 조회"""
    logger.info("[API] Statement 조회 | procedure=%s", procedure_name)
    try:
        records = await schema_metadata_queries.fetch_procedure_statements(procedure_name, file_directory)
        return {"statements": records}
    except Exception as e:
        logger.error("[API] Statement 조회 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.get("/schema/relationships", response_model=List[SchemaRelationshipInfo])
async def list_schema_relationships():
    """테이블 관계 목록 조회"""
    logger.info("[API] 관계 조회")
    try:
        records = await schema_metadata_queries.fetch_schema_relationships()
        return [
            SchemaRelationshipInfo(
                from_table=r["from_table"],
                from_schema=r.get("from_schema") or "",
                from_column=r.get("from_column") or "",
                to_table=r["to_table"],
                to_schema=r.get("to_schema") or "",
                to_column=r.get("to_column") or "",
                relationship_type=r.get("relationship_type") or "FK_TO_TABLE",
                description=r.get("description") or "",
            )
            for r in records
        ]
    except Exception as e:
        logger.error("[API] 관계 조회 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))

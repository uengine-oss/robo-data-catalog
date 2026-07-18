"""Domain router extracted from the Catalog HTTP boundary."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from api.errors import error_body as _error_body
from api.graph_connection import apply_neo4j_override
from graph import deletes as graph_deletes
from graph import queries as analysis_graph_queries
from settings import CATALOG_SETTINGS

router = APIRouter(
    prefix=CATALOG_SETTINGS.api_prefix,
    dependencies=[Depends(apply_neo4j_override)],
)
logger = logging.getLogger(__name__)

@router.get("/check-data/")
async def check_existing_data():
    """Neo4j에 기존 데이터 존재 여부 확인"""
    logger.info("[API] 데이터 존재 확인 요청")
    try:
        return await analysis_graph_queries.check_graph_data_exists()
    except Exception as e:
        logger.error("[API] 데이터 확인 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.get("/graph/")
async def get_graph_data():
    """Neo4j에서 기존 그래프 데이터 조회"""
    logger.info("[API] 그래프 데이터 조회")
    try:
        result = await analysis_graph_queries.fetch_graph_data()
        logger.info(
            "[API] 그래프 데이터 조회 완료 | nodes=%d | relationships=%d",
            len(result["Nodes"]), len(result["Relationships"]),
        )
        return result
    except Exception as e:
        logger.error("[API] 그래프 조회 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.get("/graph/related-tables/{table_name}")
async def get_related_tables(table_name: str):
    """특정 테이블과 연결된 모든 테이블 조회"""
    logger.info("[API] 관련 테이블 조회 요청 | table=%s", table_name)
    try:
        result = await analysis_graph_queries.fetch_related_tables(table_name)
        logger.info(
            "[API] 관련 테이블 조회 완료 | table=%s | tables=%d | rels=%d",
            table_name, len(result["tables"]), len(result["relationships"]),
        )
        return result
    except Exception as e:
        logger.error("[API] 관련 테이블 조회 실패 | table=%s | error_type=%s", table_name, type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.delete("/delete/")
async def delete_user_data(include_files: bool = False):
    """사용자 데이터 삭제"""
    logger.info("[API] 데이터 삭제 요청 | include_files=%s", include_files)
    try:
        result = await graph_deletes.delete_graph_data(include_files)
        logger.info("[API] 데이터 삭제 완료 | include_files=%s", include_files)
        return result
    except Exception as e:
        logger.error("[API] 데이터 삭제 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))

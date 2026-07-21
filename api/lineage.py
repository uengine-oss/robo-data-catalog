"""Domain router extracted from the Catalog HTTP boundary."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from api.errors import error_body as _error_body
from api.graph_connection import apply_neo4j_override
from contracts import LineageAnalyzeRequest
from lineage import queries as lineage_graph_queries
from shared.config.settings import CATALOG_SETTINGS

router = APIRouter(
    prefix=CATALOG_SETTINGS.api_prefix,
    dependencies=[Depends(apply_neo4j_override)],
)
logger = logging.getLogger(__name__)

@router.get("/lineage/")
async def get_lineage_graph():
    """데이터 리니지 그래프 조회"""
    logger.info("[API] 리니지 조회 요청")
    try:
        result = await lineage_graph_queries.fetch_lineage_graph()
        logger.info("[API] 리니지 조회 완료 | nodes=%d | edges=%d",
                    len(result["nodes"]), len(result["edges"]))
        return result
    except Exception as e:
        logger.error("[API] 리니지 조회 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.post("/lineage/analyze/")
async def analyze_lineage(body: LineageAnalyzeRequest):
    """ETL 코드에서 데이터 리니지 추출"""
    logger.info("[API] 리니지 분석 요청 | file=%s", body.fileName)
    try:
        result = await lineage_graph_queries.analyze_sql_lineage(
            sql_content=body.sqlContent,
            file_name=body.fileName,
            dbms=body.dbms,
            name_case=body.nameCaseOption,
        )
        logger.info("[API] 리니지 분석 완료 | lineages=%d", len(result["lineages"]))
        return result
    except Exception as e:
        logger.error("[API] 리니지 분석 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))

"""Domain router extracted from the Catalog HTTP boundary."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request

from api.errors import error_body as _error_body
from api.graph_connection import apply_neo4j_override
from contracts import SemanticSearchRequest
from search import semantic as metadata_semantic_search
from shared.config.settings import CATALOG_SETTINGS

router = APIRouter(
    prefix=CATALOG_SETTINGS.api_prefix,
    dependencies=[Depends(apply_neo4j_override)],
)
logger = logging.getLogger(__name__)

@router.post("/schema/semantic-search")
async def semantic_search_tables(request: Request, body: SemanticSearchRequest):
    """시멘틱 검색: 테이블 설명의 의미적 유사도 기반 검색"""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(400, "X-API-Key 헤더가 필요합니다.")
    logger.info("[API] 시멘틱 검색 요청 | query=%s", body.query[:50])
    try:
        result = await metadata_semantic_search.search_tables_by_semantic(
            query=body.query, limit=body.limit, api_key=api_key,
        )
        logger.info("[API] 시멘틱 검색 완료 | results=%d", len(result))
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 시멘틱 검색 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))

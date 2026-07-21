"""Domain router extracted from the Catalog HTTP boundary."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from api.errors import error_body as _error_body
from api.graph_connection import apply_neo4j_override
from contracts import SampleContextRequest
from integrations.data_fabric import DataFabricQueryGateway
from graph.database import CatalogGraphDatabase
from shared.config.settings import CATALOG_SETTINGS
from samples.context import TableSampleContextBuilder

router = APIRouter(
    prefix=CATALOG_SETTINGS.api_prefix,
    dependencies=[Depends(apply_neo4j_override)],
)
logger = logging.getLogger(__name__)

@router.post("/tables/sample-context")
async def get_table_sample_context(body: SampleContextRequest):
    """analyzer Phase 2 Linking 완료 후 식별 테이블명 batch 전달 → 매칭·샘플 반환.

    응답 map:
      { 요청 테이블명 원본: { resolved, score, columns, sample_rows } | null }
    매칭 실패 → 값이 null.
    """
    logger.info(
        "[API] 샘플 컨텍스트 | datasource=%s tables=%d",
        body.datasource, len(body.table_names),
    )
    neo4j = CatalogGraphDatabase()
    try:
        sample_context_builder = TableSampleContextBuilder(
            neo4j_client=neo4j,
            db_client=DataFabricQueryGateway(
                base_url=CATALOG_SETTINGS.metadata_enrichment.data_fabric_url,
                datasource=body.datasource,
            ),
            concurrency=CATALOG_SETTINGS.metadata_enrichment.fk_concurrency,
        )
        result = await sample_context_builder.fetch(
            table_names=body.table_names,
            sample_limit=body.sample_limit,
            similarity_threshold=body.similarity_threshold,
        )
        resolved_count = sum(1 for v in result.values() if v is not None)
        logger.info(
            "[API] 샘플 컨텍스트 완료 | 매칭=%d/%d",
            resolved_count, len(body.table_names),
        )
        return result
    except ValueError as e:
        raise HTTPException(400, _error_body(e))
    except Exception as e:
        logger.error("[API] 샘플 컨텍스트 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))
    finally:
        await neo4j.close()

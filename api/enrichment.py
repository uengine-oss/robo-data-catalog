"""Domain router extracted from the Catalog HTTP boundary."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse

from api.graph_connection import apply_neo4j_override
from contracts import MetadataEnrichmentRequest
from enrichment.orchestrator import enrichment_events
from settings import CATALOG_SETTINGS

router = APIRouter(
    prefix=CATALOG_SETTINGS.api_prefix,
    dependencies=[Depends(apply_neo4j_override)],
)
logger = logging.getLogger(__name__)

@router.post("/schema/enrich-metadata")
async def enrich_metadata(request: Request, body: MetadataEnrichmentRequest):
    """메타데이터 보강 (스트리밍) — description 생성 + FK 추론.

    응답: NDJSON (application/x-ndjson). 한 줄 = 한 이벤트 dict.
    이벤트 종류:
      - {event: start, phase: description, total: N}
      - {event: table_done, i, total, table, description_persisted: bool}
      - {event: error, phase: description, table, error_type}
      - {event: phase_done, phase: description, enriched: N}
      - {event: fk_query_start, schema, threshold, min_src_distinct}
      - {event: fk_query_done, candidate_count}
      - {event: fk_persisted, fk_to_column, fk_to_table}
      - {event: complete, description_enriched, fk_persisted}
      - {event: error, message} / {event: skip, reason}
    """
    datasource_name = body.datasource_name
    api_key = request.headers.get("OpenAI-Api-Key") or request.headers.get("X-API-Key") or ""

    logger.info("[API] 메타데이터 보강 요청(스트림) | datasource=%s", datasource_name)

    if not api_key:
        api_key = CATALOG_SETTINGS.llm.api_key

    fabric_url = CATALOG_SETTINGS.metadata_enrichment.data_fabric_url

    return StreamingResponse(
        enrichment_events(
            datasource_name=datasource_name, api_key=api_key, fabric_url=fabric_url,
        ),
        media_type="application/x-ndjson",
    )

"""Domain router extracted from the Catalog HTTP boundary."""
from __future__ import annotations

import logging

import aiohttp

from fastapi import APIRouter, Depends, HTTPException, Query

from api.errors import error_body as _error_body
from api.graph_connection import apply_neo4j_override
from contracts import (
    ObjectResolutionRequest,
    ObjectResolutionResponse,
    SampleContextRequest,
    TableDiscoveryPageResponse,
)
from integrations.data_fabric import (
    DataFabricDatasourceNotFoundError,
    DataFabricQueryGateway,
    DataFabricGatewayError,
)
from graph.database import CatalogGraphDatabase
from shared.config.settings import CATALOG_SETTINGS
from samples.context import TableSampleContextBuilder
from samples.discovery import DiscoverySnapshotStore, TableDiscoveryService
from samples.object_resolver import ObjectIdentityResolver

router = APIRouter(
    prefix=CATALOG_SETTINGS.api_prefix,
    dependencies=[Depends(apply_neo4j_override)],
)
logger = logging.getLogger(__name__)
_DISCOVERY_SNAPSHOTS = DiscoverySnapshotStore(
    ttl_seconds=CATALOG_SETTINGS.discovery.snapshot_ttl_seconds,
    capacity=CATALOG_SETTINGS.discovery.snapshot_capacity,
)


@router.post("/tables/resolve-context", response_model=ObjectResolutionResponse)
async def resolve_table_context(body: ObjectResolutionRequest):
    """Deduplicate code references and call the single Fabric exact resolver batch."""
    try:
        resolver = ObjectIdentityResolver(
            gateway=DataFabricQueryGateway(
                base_url=CATALOG_SETTINGS.metadata_enrichment.data_fabric_url,
                datasource=body.datasource,
            ),
            concurrency=CATALOG_SETTINGS.metadata_enrichment.fk_concurrency,
        )
        async with aiohttp.ClientSession() as session:
            return await resolver.fetch(
                session, body.references, sample_limit=body.sample_limit,
            )
    except DataFabricDatasourceNotFoundError as e:
        raise HTTPException(404, _error_body(e))
    except ValueError as e:
        raise HTTPException(400, _error_body(e))
    except DataFabricGatewayError as e:
        logger.error("[API] 객체 신원 해소 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(502, _error_body(e))

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


@router.get("/tables/discovery", response_model=TableDiscoveryPageResponse)
async def discover_datasource_tables(
    datasource: str = Query(
        ..., min_length=1, max_length=128,
        pattern=r"^[A-Za-z_][A-Za-z0-9_-]*$",
    ),
    sample_limit: int = Query(5, ge=0, le=50),
    page_size: int = Query(1000, ge=1, le=10000),
    snapshot: str | None = Query(None, min_length=1, max_length=256),
    cursor: str | None = Query(None, min_length=1, max_length=256),
):
    """한 immutable snapshot의 table identity와 page 상세를 반환한다.

    최초 요청만 Fabric table population을 열거·봉인한다. 후속 요청은 같은 snapshot과
    opaque cursor를 함께 보내야 하며, page size는 전송 단위일 뿐 모집단 상한이 아니다.
    """
    logger.info(
        "[API] 테이블 발견 | datasource=%s page_size=%d continuation=%s",
        datasource, page_size, snapshot is not None or cursor is not None,
    )
    try:
        service = TableDiscoveryService(
            db_client=DataFabricQueryGateway(
                base_url=CATALOG_SETTINGS.metadata_enrichment.data_fabric_url,
                datasource=datasource,
            ),
            snapshots=_DISCOVERY_SNAPSHOTS,
            concurrency=CATALOG_SETTINGS.metadata_enrichment.fk_concurrency,
        )
        result = await service.fetch_page(
            sample_limit=sample_limit,
            page_size=page_size,
            snapshot_token=snapshot,
            cursor=cursor,
        )
        logger.info(
            "[API] 테이블 발견 완료 | page=%d tables=%d total=%d terminal=%s",
            result["page_index"], len(result["tables"]), result["total_tables"],
            result["next_cursor"] is None,
        )
        return result
    except DataFabricDatasourceNotFoundError as e:
        raise HTTPException(404, _error_body(e))
    except ValueError as e:
        raise HTTPException(400, _error_body(e))
    except DataFabricGatewayError as e:
        logger.error("[API] 테이블 발견 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(502, _error_body(e))

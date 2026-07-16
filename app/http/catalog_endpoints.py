"""ROBO Data Catalog API 라우터

엔드포인트 그룹:
  [그래프]     GET  /check-data/                           Neo4j 데이터 존재 여부 확인
               GET  /graph/                                그래프 데이터 조회
               GET  /graph/related-tables/{table_name}     관련 테이블 조회
               DELETE /delete/                             사용자 데이터 삭제
  [리니지]     GET  /lineage/                              리니지 그래프 조회
               POST /lineage/analyze/                      ETL 코드 리니지 추출
  [스키마]     GET  /schema/tables                         테이블 목록 조회
               GET  /schema/tables/{name}/columns          컬럼 조회
               GET  /schema/tables/{name}/references       참조 조회
               GET  /schema/relationships                  관계 목록 조회
               POST /schema/relationships                  관계 추가
               DELETE /schema/relationships                관계 삭제
               GET  /schema/procedures/{name}/statements   프로시저 Statement 조회
               POST /schema/semantic-search                시멘틱 검색
  [스키마편집] PUT  /schema/tables/{name}/description       테이블 설명 수정
               PUT  /schema/tables/{name}/columns/{col}/description  컬럼 설명 수정
               POST /schema/vectorize                      스키마 벡터화
  [DW]         POST /schema/dw-tables                      DW 스타스키마 등록
               DELETE /schema/dw-tables/{cube_name}        DW 스타스키마 삭제
"""

import json
import logging
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from app.http.catalog_requests import (
    LineageAnalyzeRequest,
    SchemaTableInfo,
    SchemaColumnInfo,
    SchemaRelationshipInfo,
    AddRelationshipRequest,
    SemanticSearchRequest,
    TableDescriptionUpdateRequest,
    ColumnDescriptionUpdateRequest,
    VectorizeRequest,
    DWStarSchemaRequest,
    SampleContextRequest,
    MetadataEnrichmentRequest,
)
from app.graph.neo4j_context import Neo4jOverride, set_override
from app.graph.client import Neo4jClient
from app.graph.ownership import ANALYSIS_GRAPH_OWNER
from app.system.settings import settings
from app.graph import graph_queries, dw_schema, schema_queries, schema_edits
from app.lineage import lineage_service
from app.metadata import semantic_search
from app.metadata.fk_inference import FkInferenceService
from app.external.data_fabric_client import DataFabricClient
from app.metadata.sample_context import SampleContextService


def _ndjson(payload: dict) -> bytes:
    return (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")


async def _apply_neo4j_override(request: Request) -> None:
    """매 요청마다 ``X-Neo4j-*`` 헤더(Electron)를 contextvar 에 설정 → 핸들러/서비스의
    Neo4jClient 가 그 연결을 사용. 헤더 없으면 None → settings(.env) 폴백."""
    try:
        override = Neo4jOverride.from_headers(request.headers)
    except ValueError as e:
        raise HTTPException(400, "Invalid Neo4j override headers") from e
    if override is not None and not settings.allow_neo4j_header_override:
        raise HTTPException(403, "Neo4j header override is disabled")
    set_override(override)


router = APIRouter(prefix=settings.api_prefix, dependencies=[Depends(_apply_neo4j_override)])
logger = logging.getLogger(__name__)


def _error_body(e: Exception) -> dict:
    return {"detail": "Catalog operation failed", "error_type": type(e).__name__}


# =============================================================================
# 그래프 데이터 API
# =============================================================================

@router.get("/check-data/")
async def check_existing_data():
    """Neo4j에 기존 데이터 존재 여부 확인"""
    logger.info("[API] 데이터 존재 확인 요청")
    try:
        return await graph_queries.check_graph_data_exists()
    except Exception as e:
        logger.error("[API] 데이터 확인 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.get("/graph/")
async def get_graph_data():
    """Neo4j에서 기존 그래프 데이터 조회"""
    logger.info("[API] 그래프 데이터 조회")
    try:
        result = await graph_queries.fetch_graph_data()
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
        result = await graph_queries.fetch_related_tables(table_name)
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
        result = await graph_queries.delete_graph_data(include_files)
        logger.info("[API] 데이터 삭제 완료 | include_files=%s", include_files)
        return result
    except Exception as e:
        logger.error("[API] 데이터 삭제 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


# =============================================================================
# 데이터 리니지 API
# =============================================================================

@router.get("/lineage/")
async def get_lineage_graph():
    """데이터 리니지 그래프 조회"""
    logger.info("[API] 리니지 조회 요청")
    try:
        result = await lineage_service.fetch_lineage_graph()
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
        result = await lineage_service.analyze_sql_lineage(
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


# =============================================================================
# 스키마 조회 API
# =============================================================================

@router.get("/schema/tables", response_model=List[SchemaTableInfo])
async def list_schema_tables(
    search: Optional[str] = None,
    schema: Optional[str] = None,
    limit: int = 100,
):
    """테이블 목록 조회"""
    logger.info("[API] 테이블 목록 조회")
    try:
        records = await schema_queries.fetch_schema_tables(search, schema, limit)
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
        records = await schema_queries.fetch_table_columns(table_name, schema or "")
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
        return await schema_queries.fetch_table_references(table_name, schema, column_name)
    except Exception as e:
        logger.error("[API] 테이블 참조 조회 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.get("/schema/procedures/{procedure_name}/statements")
async def get_procedure_statements(procedure_name: str, file_directory: Optional[str] = None):
    """프로시저의 모든 Statement와 AI 설명 조회"""
    logger.info("[API] Statement 조회 | procedure=%s", procedure_name)
    try:
        records = await schema_queries.fetch_procedure_statements(procedure_name, file_directory)
        return {"statements": records}
    except Exception as e:
        logger.error("[API] Statement 조회 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.get("/schema/relationships", response_model=List[SchemaRelationshipInfo])
async def list_schema_relationships():
    """테이블 관계 목록 조회"""
    logger.info("[API] 관계 조회")
    try:
        records = await schema_queries.fetch_schema_relationships()
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


@router.post("/schema/relationships")
async def add_schema_relationship(body: AddRelationshipRequest):
    """테이블 관계 추가"""
    logger.info("[API] 관계 추가 | %s -> %s", body.from_table, body.to_table)
    try:
        return await schema_edits.create_schema_relationship(
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
        return await schema_edits.delete_schema_relationship(
            from_table, from_column, to_table, to_column, from_schema, to_schema
        )
    except Exception as e:
        logger.error("[API] 관계 삭제 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.post("/schema/semantic-search")
async def semantic_search_tables(request: Request, body: SemanticSearchRequest):
    """시멘틱 검색: 테이블 설명의 의미적 유사도 기반 검색"""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(400, "X-API-Key 헤더가 필요합니다.")
    logger.info("[API] 시멘틱 검색 요청 | query=%s", body.query[:50])
    try:
        result = await semantic_search.search_tables_by_semantic(
            query=body.query, limit=body.limit, api_key=api_key,
        )
        logger.info("[API] 시멘틱 검색 완료 | results=%d", len(result))
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 시멘틱 검색 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


# =============================================================================
# 스키마 편집 API
# =============================================================================

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
        return await schema_edits.update_table_description(
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
        return await schema_edits.update_column_description(
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
        return await semantic_search.vectorize_schema_tables(
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


# =============================================================================
# DW 스타스키마 API
# =============================================================================

@router.post("/schema/dw-tables")
async def register_dw_star_schema(request: Request, body: DWStarSchemaRequest):
    """DW 스타스키마 등록"""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(400, "X-API-Key 헤더가 필요합니다.")
    logger.info("[API] DW 스타스키마 등록 | cube=%s", body.cube_name)
    try:
        return await dw_schema.register_star_schema(
            cube_name=body.cube_name,
            db_name=body.db_name,
            dw_schema=body.dw_schema,
            fact_table=body.fact_table.model_dump(),
            dimensions=[d.model_dump() for d in body.dimensions],
            create_embeddings=body.create_embeddings,
            api_key=api_key,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] DW 스타스키마 등록 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


@router.delete("/schema/dw-tables/{cube_name}")
async def delete_dw_star_schema(
    cube_name: str,
    schema: Optional[str] = None,
    db_name: Optional[str] = None,
):
    """DW 스타스키마 삭제"""
    if schema is None:
        raise HTTPException(400, "schema 파라미터가 필요합니다.")
    if db_name is None:
        raise HTTPException(400, "db_name 파라미터가 필요합니다.")
    logger.info("[API] DW 스타스키마 삭제 | cube=%s", cube_name)
    try:
        return await dw_schema.delete_star_schema(cube_name, schema, db_name)
    except Exception as e:
        logger.error("[API] DW 스타스키마 삭제 실패 | error_type=%s", type(e).__name__)
        raise HTTPException(500, _error_body(e))


# =============================================================================
# 샘플 컨텍스트 API — analyzer 가 분석 세션당 1회 호출
# =============================================================================

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
    neo4j = Neo4jClient()
    try:
        service = SampleContextService(
            neo4j_client=neo4j,
            db_client=DataFabricClient(
                base_url=settings.metadata_enrichment.data_fabric_url,
                datasource=body.datasource,
            ),
            concurrency=settings.metadata_enrichment.fk_concurrency,
        )
        result = await service.fetch(
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


# =============================================================================
# 메타데이터 보강 API
# =============================================================================

@router.post("/schema/enrich-metadata")
async def enrich_metadata(request: Request, body: MetadataEnrichmentRequest):
    """메타데이터 보강 (스트리밍) — description 생성 + FK 추론.

    응답: NDJSON (application/x-ndjson). 한 줄 = 한 이벤트 dict.
    이벤트 종류:
      - {event: start, phase: description, total: N}
      - {event: table_done, i, total, table, description_persisted: bool}
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

    import aiohttp
    from openai import AsyncOpenAI
    from app.metadata.description_enrichment import TableDescriptionService

    if not api_key:
        api_key = settings.llm.api_key

    fabric_url = settings.metadata_enrichment.data_fabric_url

    async def stream():
        # ---- precondition checks ----
        if not api_key:
            yield _ndjson({"event": "skip", "reason": "API 키 없음, 보강 생략"})
            return
        if not fabric_url:
            yield _ndjson({"event": "skip", "reason": "DATA_FABRIC_URL 미설정, 보강 생략"})
            return

        client = Neo4jClient()
        try:
            db = DataFabricClient(base_url=fabric_url, datasource=datasource_name)
            description_service = TableDescriptionService(
                client=client, openai_client=AsyncOpenAI(api_key=api_key)
            )

            # 보강 대상 테이블 조회
            tables_query = {
                "query": f"""
                MATCH (t:TABLE)
                WHERE t.graph_owner = '{ANALYSIS_GRAPH_OWNER}'
                  AND coalesce(t.db, t.datasource) = $datasource
                  AND (t.description IS NULL OR t.description = '' OR t.description = 'N/A')
                OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:COLUMN {{graph_owner: '{ANALYSIS_GRAPH_OWNER}'}})
                RETURN t.name AS table_name, t.schema AS schema_name,
                       collect({
                         name: c.name,
                         dtype: c.dtype,
                         description: c.description
                       }) AS columns
                """,
                "parameters": {"datasource": datasource_name},
            }
            results = await client.execute_queries([tables_query])
            tables = results[0] if results else []

            if not tables:
                yield _ndjson({"event": "skip", "reason": "보강할 테이블 없음"})
                return

            # ---- Phase 1: description 생성 ----
            yield _ndjson({"event": "start", "phase": "description", "total": len(tables)})

            async with aiohttp.ClientSession() as session:
                if not await db.check_available(session):
                    yield _ndjson({"event": "skip", "reason": "data-fabric 연결 불가"})
                    return

                enriched = 0
                schemas_seen: set = set()

                for i, table in enumerate(tables, start=1):
                    tname = table["table_name"]
                    sname = table["schema_name"] or "public"
                    schemas_seen.add(sname)
                    cols = table["columns"]
                    persisted = False
                    try:
                        sample = await db.fetch_rows(
                            session,
                            DataFabricClient.sample_sql(f"{sname}.{tname}", 10),
                            max_rows=10,
                        )
                        if sample:
                            descs = await description_service.generate(tname, sname, sample, cols)
                            if descs:
                                t_upd, _ = await description_service.persist(tname, sname, descs)
                                enriched += t_upd
                                persisted = bool(t_upd)
                    except Exception as e:
                        # 한 테이블 실패가 전체 흐름을 막지 않음
                        logger.warning("[ENRICH] table=%s failed | error_type=%s", tname, type(e).__name__)

                    yield _ndjson({
                        "event": "table_done",
                        "i": i,
                        "total": len(tables),
                        "table": tname,
                        "schema": sname,
                        "description_persisted": persisted,
                    })

                yield _ndjson({
                    "event": "phase_done",
                    "phase": "description",
                    "enriched": enriched,
                })

                # ---- Phase 2: FK 추론 ----
                fk_service = FkInferenceService(neo4j_client=client, db_client=db)
                fk_persisted_total = 0
                for schema in sorted(schemas_seen):
                    async for evt in fk_service.infer_and_persist(session, schema):
                        if evt.get("event") == "fk_persisted":
                            fk_persisted_total += int(evt.get("fk_to_column", 0))
                        yield _ndjson(evt)

                yield _ndjson({
                    "event": "complete",
                    "description_enriched": enriched,
                    "fk_persisted": fk_persisted_total,
                })

        except Exception as e:
            logger.error("[API] 메타데이터 보강 실패 | error_type=%s", type(e).__name__)
            yield _ndjson({"event": "error", "message": "Metadata enrichment failed"})
        finally:
            await client.close()

    return StreamingResponse(stream(), media_type="application/x-ndjson")

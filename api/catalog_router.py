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

import logging
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Request
from api.request_models import (
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
)
from config.settings import settings
from service import (
    graph_query_service,
    schema_manage_service,
    data_lineage_service,
    dw_schema_service,
)

router = APIRouter(prefix=settings.api_prefix)
logger = logging.getLogger(__name__)


def _error_body(e: Exception) -> dict:
    return {"detail": str(e), "error_type": type(e).__name__}


# =============================================================================
# 그래프 데이터 API
# =============================================================================

@router.get("/check-data/")
async def check_existing_data():
    """Neo4j에 기존 데이터 존재 여부 확인"""
    logger.info("[API] 데이터 존재 확인 요청")
    try:
        return await graph_query_service.check_graph_data_exists()
    except Exception as e:
        logger.error("[API] 데이터 확인 실패 | error=%s", e)
        raise HTTPException(500, _error_body(e))


@router.get("/graph/")
async def get_graph_data():
    """Neo4j에서 기존 그래프 데이터 조회"""
    logger.info("[API] 그래프 데이터 조회")
    try:
        result = await graph_query_service.fetch_graph_data()
        logger.info(
            "[API] 그래프 데이터 조회 완료 | nodes=%d | relationships=%d",
            len(result["Nodes"]), len(result["Relationships"]),
        )
        return result
    except Exception as e:
        logger.error("[API] 그래프 조회 실패 | error=%s", e)
        raise HTTPException(500, _error_body(e))


@router.get("/graph/related-tables/{table_name}")
async def get_related_tables(table_name: str):
    """특정 테이블과 연결된 모든 테이블 조회"""
    logger.info("[API] 관련 테이블 조회 요청 | table=%s", table_name)
    try:
        result = await graph_query_service.fetch_related_tables(table_name)
        logger.info(
            "[API] 관련 테이블 조회 완료 | table=%s | tables=%d | rels=%d",
            table_name, len(result["tables"]), len(result["relationships"]),
        )
        return result
    except Exception as e:
        logger.error("[API] 관련 테이블 조회 실패 | table=%s | error=%s", table_name, e)
        raise HTTPException(500, _error_body(e))


@router.delete("/delete/")
async def delete_user_data(include_files: bool = False):
    """사용자 데이터 삭제"""
    logger.info("[API] 데이터 삭제 요청 | include_files=%s", include_files)
    try:
        result = await graph_query_service.delete_graph_data(include_files)
        logger.info("[API] 데이터 삭제 완료 | include_files=%s", include_files)
        return result
    except Exception as e:
        logger.error("[API] 데이터 삭제 실패 | error=%s", e)
        raise HTTPException(500, _error_body(e))


# =============================================================================
# 데이터 리니지 API
# =============================================================================

@router.get("/lineage/")
async def get_lineage_graph():
    """데이터 리니지 그래프 조회"""
    logger.info("[API] 리니지 조회 요청")
    try:
        result = await data_lineage_service.fetch_lineage_graph()
        logger.info("[API] 리니지 조회 완료 | nodes=%d | edges=%d",
                    len(result["nodes"]), len(result["edges"]))
        return result
    except Exception as e:
        logger.error("[API] 리니지 조회 실패 | error=%s", e)
        raise HTTPException(500, _error_body(e))


@router.post("/lineage/analyze/")
async def analyze_lineage(body: LineageAnalyzeRequest):
    """ETL 코드에서 데이터 리니지 추출"""
    logger.info("[API] 리니지 분석 요청 | file=%s", body.fileName)
    try:
        result = await data_lineage_service.analyze_sql_lineage(
            sql_content=body.sqlContent,
            file_name=body.fileName,
            dbms=body.dbms,
            name_case=body.nameCaseOption,
        )
        logger.info("[API] 리니지 분석 완료 | lineages=%d", len(result["lineages"]))
        return result
    except Exception as e:
        logger.error("[API] 리니지 분석 실패 | error=%s", e)
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
        records = await schema_manage_service.fetch_schema_tables(search, schema, limit)
        tables = [
            SchemaTableInfo(
                name=r["name"],
                table_schema=r["schema"] or "",
                datasource=r.get("datasource") or "",
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
        logger.error("[API] 테이블 목록 조회 실패 | error=%s", e)
        raise HTTPException(500, _error_body(e))


@router.get("/schema/tables/{table_name}/columns", response_model=List[SchemaColumnInfo])
async def get_table_columns(table_name: str, schema: Optional[str] = None):
    """테이블 컬럼 목록 조회"""
    if schema is None:
        raise HTTPException(400, "schema 파라미터가 필요합니다.")
    logger.info("[API] 컬럼 조회 | table=%s", table_name)
    try:
        records = await schema_manage_service.fetch_table_columns(table_name, schema)
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
        logger.error("[API] 컬럼 조회 실패 | error=%s", e)
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
        return await schema_manage_service.fetch_table_references(table_name, schema, column_name)
    except Exception as e:
        logger.error("[API] 테이블 참조 조회 실패 | error=%s", e)
        raise HTTPException(500, _error_body(e))


@router.get("/schema/procedures/{procedure_name}/statements")
async def get_procedure_statements(procedure_name: str, file_directory: Optional[str] = None):
    """프로시저의 모든 Statement와 AI 설명 조회"""
    logger.info("[API] Statement 조회 | procedure=%s", procedure_name)
    try:
        records = await schema_manage_service.fetch_procedure_statements(procedure_name, file_directory)
        return {"statements": records}
    except Exception as e:
        logger.error("[API] Statement 조회 실패 | error=%s", e)
        raise HTTPException(500, _error_body(e))


@router.get("/schema/relationships", response_model=List[SchemaRelationshipInfo])
async def list_schema_relationships():
    """테이블 관계 목록 조회"""
    logger.info("[API] 관계 조회")
    try:
        records = await schema_manage_service.fetch_schema_relationships()
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
        logger.error("[API] 관계 조회 실패 | error=%s", e)
        raise HTTPException(500, _error_body(e))


@router.post("/schema/relationships")
async def add_schema_relationship(body: AddRelationshipRequest):
    """테이블 관계 추가"""
    logger.info("[API] 관계 추가 | %s -> %s", body.from_table, body.to_table)
    try:
        return await schema_manage_service.create_schema_relationship(
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
        logger.error("[API] 관계 추가 실패 | error=%s", e)
        raise HTTPException(500, _error_body(e))


@router.delete("/schema/relationships")
async def remove_schema_relationship(
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
):
    """테이블 관계 삭제"""
    logger.info("[API] 관계 삭제 | %s.%s -> %s.%s", from_table, from_column, to_table, to_column)
    try:
        return await schema_manage_service.delete_schema_relationship(
            from_table, from_column, to_table, to_column
        )
    except Exception as e:
        logger.error("[API] 관계 삭제 실패 | error=%s", e)
        raise HTTPException(500, _error_body(e))


@router.post("/schema/semantic-search")
async def semantic_search_tables(request: Request, body: SemanticSearchRequest):
    """시멘틱 검색: 테이블 설명의 의미적 유사도 기반 검색"""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(400, "X-API-Key 헤더가 필요합니다.")
    logger.info("[API] 시멘틱 검색 요청 | query=%s", body.query[:50])
    try:
        result = await schema_manage_service.search_tables_by_semantic(
            query=body.query, limit=body.limit, api_key=api_key,
        )
        logger.info("[API] 시멘틱 검색 완료 | results=%d", len(result))
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 시멘틱 검색 실패 | error=%s", e)
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
        return await schema_manage_service.update_table_description(
            table_name=table_name,
            schema=body.table_schema,
            description=body.description,
            api_key=api_key,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 테이블 설명 업데이트 실패 | error=%s", e)
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
        return await schema_manage_service.update_column_description(
            table_name=table_name,
            table_schema=body.table_schema,
            column_name=column_name,
            description=body.description,
            api_key=api_key,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 컬럼 설명 업데이트 실패 | error=%s", e)
        raise HTTPException(500, _error_body(e))


@router.post("/schema/vectorize")
async def vectorize_schema(request: Request, body: VectorizeRequest):
    """전체 스키마 벡터화"""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(400, "X-API-Key 헤더가 필요합니다.")
    logger.info("[API] 스키마 벡터화 요청")
    try:
        return await schema_manage_service.vectorize_schema_tables(
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
        logger.error("[API] 스키마 벡터화 실패 | error=%s", e)
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
        return await dw_schema_service.register_star_schema(
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
        logger.error("[API] DW 스타스키마 등록 실패 | error=%s", e)
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
        return await dw_schema_service.delete_star_schema(cube_name, schema, db_name)
    except Exception as e:
        logger.error("[API] DW 스타스키마 삭제 실패 | error=%s", e)
        raise HTTPException(500, _error_body(e))


# =============================================================================
# 메타데이터 보강 API
# =============================================================================

@router.post("/schema/enrich-metadata")
async def enrich_metadata(request: Request):
    """메타데이터 보강 (Text2SQL 기반 테이블/컬럼 설명 생성 + FK 추론)"""
    body = await request.json()
    datasource_name = body.get("datasource_name", "")
    api_key = request.headers.get("OpenAI-Api-Key") or request.headers.get("X-API-Key") or ""

    logger.info("[API] 메타데이터 보강 요청 | datasource=%s", datasource_name)

    from service.metadata_enrichment_service import MetadataEnrichmentService
    from openai import AsyncOpenAI
    from client.neo4j_client import Neo4jClient
    from config.settings import settings as _s

    if not api_key:
        api_key = _s.llm.api_key
    if not api_key:
        return {"message": "API 키 없음, 보강 생략", "enriched": 0}

    text2sql_url = _s.metadata_enrichment.text2sql_api_url
    if not text2sql_url:
        return {"message": "TEXT2SQL_API_URL 미설정, 보강 생략", "enriched": 0}

    client = Neo4jClient()
    try:
        openai_client = AsyncOpenAI(api_key=api_key)
        service = MetadataEnrichmentService(
            client=client,
            openai_client=openai_client,
            text2sql_base_url=text2sql_url,
            datasource_name=datasource_name,
        )

        query = """
            MATCH (t:Table)
            WHERE t.description IS NULL OR t.description = '' OR t.description = 'N/A'
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
            RETURN t.name AS table_name, t.schema AS schema_name,
                   collect({name: c.name, dtype: c.dtype}) AS columns
        """
        results = await client.execute_queries([query])
        tables = results[0] if results else []

        if not tables:
            return {"message": "보강할 테이블 없음", "enriched": 0}

        import aiohttp
        enriched = 0
        async with aiohttp.ClientSession() as session:
            available = await service.check_text2sql_available(session)
            if not available:
                return {"message": "Text2SQL 서버 연결 불가", "enriched": 0}

            for table in tables:
                tname = table["table_name"]
                sname = table["schema_name"] or "public"
                cols = table["columns"]
                sample_sql = f'SELECT * FROM "{sname}"."{tname}" LIMIT 10'
                sample = await service.fetch_sample_data(session, sample_sql)
                if not sample:
                    continue
                descs = await service.generate_descriptions_from_sample(tname, sname, sample, cols)
                if descs:
                    t_upd, c_upd = await service.update_descriptions_in_neo4j(tname, sname, descs)
                    enriched += t_upd

        return {"message": f"보강 완료: {enriched}개 테이블", "enriched": enriched}
    except Exception as e:
        logger.error("[API] 메타데이터 보강 실패 | error=%s", e)
        raise HTTPException(500, _error_body(e))
    finally:
        await client.close()

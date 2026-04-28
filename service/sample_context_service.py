"""샘플 컨텍스트 서비스 — analyzer 측 1회 요청을 처리.

analyzer 가 Phase 2 Linking 완료 후 식별된 테이블명 목록을 전달하면,
이 서비스가:
  1) Neo4j 에서 해당 datasource 의 실제 테이블 목록 조회
  2) rapidfuzz 유사도로 요청명 → 실제명 매칭
  3) 매칭된 테이블 각각에 대해 컬럼 메타 + 실제 DB 샘플 행 조회
  4) 응답 map 반환 (key = 요청 원본명, 매칭 실패 시 값 None)

호출 경로:
  catalog_router.POST /robo/tables/sample-context → SampleContextService.fetch
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from rapidfuzz import fuzz, process

from client.neo4j_client import Neo4jClient
from service.text2sql_client import Text2SqlClient
from util.logger import log_process

logger = logging.getLogger(__name__)


_CAMEL_SPLIT = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_SEP = re.compile(r"[\s\-]+")
_MULTI_UNDER = re.compile(r"_+")


def _normalize(name: str) -> str:
    """범용 스타일 통일: 스키마 strip + camelCase 분해 + 구분자 통일 + 소문자.

    언어·도메인 중립. 토큰 의미(접두어·접미어·약어)는 건드리지 않음.
    'PUBLIC.ZngmCommCdDtl' / 'zngm-comm cd dtl' / 'zngm_comm_cd_dtl' → 'zngm_comm_cd_dtl'
    """
    name = name.split(".")[-1]
    name = _CAMEL_SPLIT.sub("_", name)
    name = _SEP.sub("_", name)
    name = _MULTI_UNDER.sub("_", name).strip("_")
    return name.lower()


def resolve_name(
    query: str,
    candidates: List[str],
    threshold: float = 85.0,
) -> Optional[Tuple[str, float]]:
    """query 를 candidates 중 best match 로 해소.

    normalize 를 query·candidates 양쪽에 적용 후 비교.
    1) exact match → (원본 candidate, 100.0)
    2) rapidfuzz.process.extractOne (score_cutoff=threshold)
    3) 실패 시 None
    """
    if not candidates:
        return None

    norm_map = {_normalize(c): c for c in candidates}
    norm_q = _normalize(query)
    if norm_q in norm_map:
        return (norm_map[norm_q], 100.0)

    result = process.extractOne(
        norm_q,
        list(norm_map.keys()),
        scorer=fuzz.WRatio,
        score_cutoff=threshold,
    )
    if result is None:
        return None
    matched_norm, score, _ = result
    return (norm_map[matched_norm], float(score))


class SampleContextService:
    """테이블명 batch → 매칭·샘플 조회 결과 batch 반환.

    분석 세션당 1회 호출 가정. 내부에서 asyncio.Semaphore 로 동시성 제어.
    """

    def __init__(
        self,
        neo4j_client: Neo4jClient,
        text2sql_client: Text2SqlClient,
        concurrency: int = 5,
    ):
        self._neo4j = neo4j_client
        self._text2sql = text2sql_client
        self._sem = asyncio.Semaphore(max(1, concurrency))

    async def fetch(
        self,
        datasource: str,
        table_names: List[str],
        sample_limit: int = 5,
        similarity_threshold: float = 85.0,
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """요청 테이블명 → 매칭·샘플 컨텍스트 map.

        응답 key = 요청 원본 테이블명.
        매칭 실패한 key 의 값은 None.
        """
        if not table_names:
            return {}
        if not datasource:
            raise ValueError("datasource is required")

        # 1) DB 에 등록된 테이블 목록 조회 (Linking 단계에서 Neo4j 저장된 것 기준)
        db_tables = await self._list_tables(datasource)
        if not db_tables:
            log_process("SAMPLE_CTX", "NO_TABLES", f"datasource={datasource} 에 등록 테이블 없음", logging.WARNING)
            return {name: None for name in table_names}

        # 2) 이름 해소 (rapidfuzz)
        resolved: Dict[str, Optional[Tuple[str, float]]] = {
            name: resolve_name(name, db_tables, similarity_threshold)
            for name in table_names
        }
        resolved_count = sum(1 for v in resolved.values() if v)
        log_process(
            "SAMPLE_CTX",
            "RESOLVE",
            f"요청={len(table_names)} 매칭={resolved_count} 미매칭={len(table_names) - resolved_count}",
            logging.INFO,
        )

        # 3) 매칭된 테이블 각각에 대해 컬럼 + 샘플 병렬 조회
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_one(session, req_name, matched, datasource, sample_limit)
                for req_name, matched in resolved.items()
            ]
            pairs = await asyncio.gather(*tasks)

        return dict(pairs)

    async def _fetch_one(
        self,
        session: aiohttp.ClientSession,
        req_name: str,
        matched: Optional[Tuple[str, float]],
        datasource: str,
        sample_limit: int,
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        if matched is None:
            return (req_name, None)
        actual, score = matched
        async with self._sem:
            columns_task = self._fetch_columns(actual)
            rows_task = self._fetch_sample_rows(session, actual, datasource, sample_limit)
            columns, sample_rows = await asyncio.gather(columns_task, rows_task)
        return (
            req_name,
            {
                "resolved": actual,
                "score": round(score, 2),
                "columns": columns,
                "sample_rows": sample_rows or [],
            },
        )

    # -------------------------------------------------------------------
    # Neo4j 조회
    # -------------------------------------------------------------------

    async def _list_tables(self, datasource: str) -> List[str]:
        """Neo4j 에 저장된 datasource 의 Table 이름 목록 (schema.name 또는 name)."""
        query = (
            "MATCH (t:Table) "
            "WHERE t.datasource = $ds OR t.datasource IS NULL "
            "RETURN DISTINCT coalesce(t.schema + '.' + t.name, t.name) AS fqn"
        )
        results = await self._neo4j.execute_queries([query], {"ds": datasource})
        if not (results and results[0]):
            return []
        return [r["fqn"] for r in results[0] if r.get("fqn")]

    async def _fetch_columns(self, table_fqn: str) -> List[Dict[str, Any]]:
        """Neo4j 에서 테이블 컬럼 메타 조회.

        table_fqn 은 'schema.name' 또는 'name' 형식. 둘 다 처리.
        """
        # schema + name 분리
        if "." in table_fqn:
            schema, name = table_fqn.rsplit(".", 1)
        else:
            schema, name = "", table_fqn

        query = (
            "MATCH (t:Table)-[:HAS_COLUMN]->(c:Column) "
            "WHERE (t.name = $name AND ($schema = '' OR t.schema = $schema)) "
            "   OR t.fqn = $fqn "
            "RETURN c.name AS name, c.dtype AS dtype, "
            "       c.description AS description, "
            "       c.is_primary_key AS is_primary_key, "
            "       c.nullable AS nullable "
            "ORDER BY c.name"
        )
        params = {"name": name, "schema": schema, "fqn": table_fqn}
        results = await self._neo4j.execute_queries([query], params)
        if not (results and results[0]):
            return []
        return [dict(r) for r in results[0]]

    # -------------------------------------------------------------------
    # 실제 DB 샘플 조회
    # -------------------------------------------------------------------

    async def _fetch_sample_rows(
        self,
        session: aiohttp.ClientSession,
        table_fqn: str,
        datasource: str,
        limit: int,
    ) -> Optional[List[Dict[str, Any]]]:
        """text2sql 경유 SELECT * LIMIT N."""
        # text2sql_client 의 datasource 는 생성 시 고정이므로 재구성
        client = Text2SqlClient(self._text2sql._base_url, datasource)
        sql = f'SELECT * FROM "{table_fqn}" LIMIT {limit}' if "." in table_fqn else f"SELECT * FROM {table_fqn} LIMIT {limit}"
        return await client.fetch_rows(session, sql)

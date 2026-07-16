"""Data Fabric API 클라이언트

robo-data-fabric 의 `/api/query` 를 감싸는 얇은 클라이언트.
SQL 은 MindsDB 네이티브 쿼리로 감싸져 대상 DB 에 **원문 그대로** 전달되므로,
호출자는 대상 DB 방언(PostgreSQL 등) SQL 을 그대로 쓴다.

책임:
- HTTP POST 호출 (재시도 3회, 2·4·6초 백오프)
- 응답 파싱 (columns·data → list[dict])
- Timeout·연결 오류 복원력
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Dict, List, Optional

import aiohttp

from app.system.settings import settings
from app.system.logging import log_process

logger = logging.getLogger(__name__)

QUERY_ENDPOINT = "/api/query"
STATUS_ENDPOINT = "/api/query/status"
_DATASOURCE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class DataFabricClient:
    """data-fabric 을 통해 대상 DB 에 SQL 을 실행하는 얇은 클라이언트.

    Usage:
        client = DataFabricClient(base_url="http://127.0.0.1:8404", datasource="shopmall")
        async with aiohttp.ClientSession() as session:
            rows = await client.fetch_rows(session, "SELECT * FROM orders LIMIT 5")
    """

    def __init__(self, base_url: str, datasource: str = ""):
        if datasource and not _DATASOURCE_PATTERN.fullmatch(datasource):
            raise ValueError("datasource must be a valid connector identifier")
        self._base_url = (base_url or "").rstrip("/")
        self._datasource = datasource or ""
        self._timeout_request = settings.metadata_enrichment.timeout_request

    @property
    def datasource(self) -> str:
        return self._datasource

    @property
    def is_configured(self) -> bool:
        return bool(self._base_url and self._datasource)

    @staticmethod
    def sample_sql(table_fqn: str, limit: int) -> str:
        """테이블 샘플 SQL. fqn 이 'schema.name' 이면 식별자를 각각 인용."""
        bounded_limit = max(1, min(int(limit), 1000))
        identifiers = table_fqn.rsplit(".", 1)
        quoted = ".".join(f'"{part.replace(chr(34), chr(34) * 2)}"' for part in identifiers)
        return f"SELECT * FROM {quoted} LIMIT {bounded_limit}"

    async def check_available(self, session: aiohttp.ClientSession) -> bool:
        """data-fabric 이 응답하고 MindsDB 에 연결돼 있는지 확인."""
        if not self.is_configured:
            return False
        try:
            async with session.get(
                f"{self._base_url}{STATUS_ENDPOINT}",
                timeout=aiohttp.ClientTimeout(total=self._timeout_request),
            ) as resp:
                if resp.status != 200:
                    return False
                return bool((await resp.json()).get("connected"))
        except Exception as e:
            log_process("FABRIC", "UNAVAILABLE", type(e).__name__, logging.DEBUG)
            return False

    async def fetch_rows(
        self,
        session: aiohttp.ClientSession,
        sql: str,
        max_retries: int = 3,
    ) -> Optional[List[Dict[str, Any]]]:
        """SQL 실행 → 행 리스트 반환 (실패·빈 결과 시 None).

        재시도: QueuePool·Timeout·연결 오류는 지수 백오프로 3회 재시도.
        비재시도 오류(SQL 문법 등)는 즉시 None.
        """
        if not self.is_configured:
            return None

        url = f"{self._base_url}{QUERY_ENDPOINT}"
        payload = {"query": self._as_native_query(sql)}

        for attempt in range(max_retries):
            try:
                async with session.post(
                    url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self._timeout_request),
                ) as resp:
                    if resp.status != 200:
                        await resp.read()
                        log_process("FABRIC", "HTTP_ERROR", f"status={resp.status}", logging.DEBUG)
                        return None

                    data = await resp.json()
                    # data-fabric 은 대상 DB 오류도 200 + type=error 로 싣어 보낸다
                    if data.get("type") == "error":
                        err_msg = str(data.get("error", ""))
                        if self._is_retryable(err_msg) and attempt < max_retries - 1:
                            wait = (attempt + 1) * 2
                            log_process("FABRIC", "RETRY", f"remote_error — {wait}초 대기 ({attempt + 1}/{max_retries})", logging.WARNING)
                            await asyncio.sleep(wait)
                            continue
                        log_process("FABRIC", "SQL_ERROR", "remote query rejected", logging.DEBUG)
                        return None

                    return self._parse_rows(data)

            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    wait = (attempt + 1) * 2
                    log_process("FABRIC", "RETRY", f"타임아웃 — {wait}초 대기 ({attempt + 1}/{max_retries})", logging.WARNING)
                    await asyncio.sleep(wait)
                    continue
                log_process("FABRIC", "TIMEOUT", f"{self._timeout_request}초 {max_retries}회 재시도 실패", logging.DEBUG)
            except Exception as e:
                msg = str(e)
                if self._is_retryable(msg) and attempt < max_retries - 1:
                    wait = (attempt + 1) * 2
                    log_process("FABRIC", "RETRY", f"{type(e).__name__} — {wait}초 대기", logging.WARNING)
                    await asyncio.sleep(wait)
                    continue
                log_process("FABRIC", "API_ERROR", type(e).__name__, logging.DEBUG)

        return None

    def _as_native_query(self, sql: str) -> str:
        """MindsDB 네이티브 쿼리 — 대상 DB 가 SQL 원문을 그대로 실행한다."""
        return f"SELECT * FROM {self._datasource} ({sql.rstrip().rstrip(';')})"

    @staticmethod
    def _is_retryable(err_msg: str) -> bool:
        return "QueuePool" in err_msg or "connection timed out" in err_msg

    @staticmethod
    def _parse_rows(data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """data-fabric 응답 → list[dict] 정규화."""
        columns = data.get("columns") or []
        rows = data.get("data") or []
        if not columns or not rows:
            return None
        return [dict(zip(columns, row)) for row in rows]

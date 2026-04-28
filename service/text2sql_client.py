"""Text2SQL API 클라이언트

robo-data-text2sql 서비스의 /text2sql/direct-sql 엔드포인트를 감싸는 얇은 클라이언트.
catalog 내 샘플 조회·FK 검증·스키마 탐지 등에서 공용 사용.

책임:
- HTTP POST 호출 (재시도 3회, 2·4·6초 백오프)
- 응답 파싱 (columns·rows → list[dict])
- QueuePool·Timeout 에러 복원력
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp

from config.settings import settings
from util.logger import log_process

logger = logging.getLogger(__name__)

TEXT2SQL_ENDPOINT = "/text2sql/direct-sql"


class Text2SqlClient:
    """Text2SQL 서비스를 호출하는 얇은 클라이언트.

    Usage:
        client = Text2SqlClient(base_url="http://localhost:8000", datasource="myds")
        async with aiohttp.ClientSession() as session:
            rows = await client.fetch_rows(session, "SELECT * FROM t LIMIT 5")
    """

    def __init__(self, base_url: str, datasource: str = ""):
        self._base_url = (base_url or "").rstrip("/")
        self._datasource = datasource or ""
        self._timeout_connect = settings.metadata_enrichment.timeout_connect
        self._timeout_request = settings.metadata_enrichment.timeout_request

    @property
    def is_configured(self) -> bool:
        return bool(self._base_url)

    async def check_available(self, session: aiohttp.ClientSession) -> bool:
        """서버 응답 여부만 빠르게 확인 (SELECT 1)."""
        if not self.is_configured:
            return False
        try:
            rows = await self.fetch_rows(session, "SELECT 1 AS ok LIMIT 1", max_retries=1)
            return rows is not None
        except Exception:
            return False

    async def fetch_rows(
        self,
        session: aiohttp.ClientSession,
        sql: str,
        max_retries: int = 3,
    ) -> Optional[List[Dict[str, Any]]]:
        """SQL 실행 → 행 리스트 반환 (실패 시 None).

        재시도: QueuePool·Timeout·연결 오류는 지수 백오프로 3회 재시도.
        비재시도 오류(SQL 문법 등)는 즉시 None.
        """
        if not self.is_configured:
            return None

        url = f"{self._base_url}{TEXT2SQL_ENDPOINT}"
        payload: Dict[str, Any] = {"sql": sql}
        if self._datasource:
            payload["datasource"] = self._datasource

        for attempt in range(max_retries):
            try:
                async with session.post(
                    url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self._timeout_request),
                ) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        log_process("TEXT2SQL", "HTTP_ERROR", f"status={resp.status}, body={body[:150]}", logging.DEBUG)
                        return None

                    data = await resp.json()
                    # 서버가 200이지만 내부 에러를 실어 보내는 경우
                    if isinstance(data, dict) and "error" in data:
                        err_msg = str(data.get("error", ""))
                        if self._is_retryable(err_msg) and attempt < max_retries - 1:
                            wait = (attempt + 1) * 2
                            log_process("TEXT2SQL", "RETRY", f"{err_msg[:100]} — {wait}초 대기 ({attempt + 1}/{max_retries})", logging.WARNING)
                            await asyncio.sleep(wait)
                            continue
                        log_process("TEXT2SQL", "SQL_ERROR", err_msg[:150], logging.DEBUG)
                        return None

                    return self._parse_rows(data)

            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    wait = (attempt + 1) * 2
                    log_process("TEXT2SQL", "RETRY", f"타임아웃 — {wait}초 대기 ({attempt + 1}/{max_retries})", logging.WARNING)
                    await asyncio.sleep(wait)
                    continue
                log_process("TEXT2SQL", "TIMEOUT", f"{self._timeout_request}초 {max_retries}회 재시도 실패", logging.DEBUG)
            except Exception as e:
                msg = str(e)
                if self._is_retryable(msg) and attempt < max_retries - 1:
                    wait = (attempt + 1) * 2
                    log_process("TEXT2SQL", "RETRY", f"{msg[:100]} — {wait}초 대기", logging.WARNING)
                    await asyncio.sleep(wait)
                    continue
                log_process("TEXT2SQL", "API_ERROR", msg[:150], logging.DEBUG)

        return None

    @staticmethod
    def _is_retryable(err_msg: str) -> bool:
        return "QueuePool" in err_msg or "connection timed out" in err_msg

    @staticmethod
    def _parse_rows(data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """Text2SQL 응답 → list[dict] 정규화."""
        rows = data.get("rows", [])
        columns = data.get("columns", [])
        if rows and columns:
            return [dict(zip(columns, row)) for row in rows]
        if rows:
            return rows
        return data.get("results", data.get("data", [])) or None

"""Explicit HTTP boundary for read-only queries through robo-data-fabric."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Dict, List

import aiohttp

from shared.observability.logger import log_catalog_operation
from shared.config.settings import CATALOG_SETTINGS


QUERY_ENDPOINT = "/api/query"
STATUS_ENDPOINT = "/api/query/status"
DATASOURCE_ENDPOINT = "/api/datasources"
_DATASOURCE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")


class DataFabricGatewayError(RuntimeError):
    """Base error for an explicit Data Fabric boundary failure."""


class DataFabricUnavailableError(DataFabricGatewayError):
    """The configured Data Fabric endpoint could not be reached."""


class DataFabricQueryError(DataFabricGatewayError):
    """Data Fabric reached the target but rejected or malformed the query."""


class DataFabricQueryGateway:
    """Query a registered datasource through Data Fabric's read-only API."""

    def __init__(self, base_url: str, datasource: str = ""):
        if datasource and not _DATASOURCE_PATTERN.fullmatch(datasource):
            raise ValueError("datasource must be a valid connector identifier")
        self._base_url = (base_url or "").rstrip("/")
        self._datasource = datasource or ""
        self._timeout_request = CATALOG_SETTINGS.metadata_enrichment.timeout_request

    @property
    def datasource(self) -> str:
        return self._datasource

    @property
    def is_configured(self) -> bool:
        return bool(self._base_url and self._datasource)

    @staticmethod
    def sample_sql(table_fqn: str, limit: int) -> str:
        """Build a bounded sample query with every identifier quoted."""
        bounded_limit = max(1, min(int(limit), 1000))
        identifiers = table_fqn.rsplit(".", 1)
        quoted = ".".join(f'"{part.replace(chr(34), chr(34) * 2)}"' for part in identifiers)
        return f"SELECT * FROM {quoted} LIMIT {bounded_limit}"

    async def check_available(self, session: aiohttp.ClientSession) -> bool:
        """Return whether Data Fabric and its MindsDB connection are available."""
        if not self.is_configured:
            return False
        try:
            async with session.get(
                f"{self._base_url}{STATUS_ENDPOINT}",
                timeout=aiohttp.ClientTimeout(total=self._timeout_request),
            ) as response:
                if response.status != 200:
                    log_catalog_operation(
                        "FABRIC", "STATUS_HTTP", f"status={response.status}", logging.DEBUG
                    )
                    return False
                return bool((await response.json()).get("connected"))
        except Exception as exc:
            # Availability is deliberately a boolean probe. The failure remains
            # observable and must not be reused for query execution semantics.
            log_catalog_operation(
                "FABRIC", "UNAVAILABLE", type(exc).__name__, logging.DEBUG
            )
            return False

    async def fetch_rows(
        self,
        session: aiohttp.ClientSession,
        sql: str,
        max_rows: int = 1000,
        max_retries: int = 3,
    ) -> List[Dict[str, Any]]:
        """Execute SQL and return rows; raise on transport or query failure.

        Empty results are ``[]``. They are deliberately distinct from failed
        requests so callers cannot report infrastructure failure as an empty
        but successful query.
        """
        if not self.is_configured:
            raise DataFabricUnavailableError("Data Fabric URL and datasource are required")
        if not 1 <= max_rows <= 1000:
            raise ValueError("max_rows must be between 1 and 1000")
        if max_retries < 1:
            raise ValueError("max_retries must be at least 1")
        normalized_sql = sql.rstrip().rstrip(";")
        if not normalized_sql:
            raise ValueError("sql must not be empty")

        url = f"{self._base_url}{QUERY_ENDPOINT}"
        payload = {
            "datasource": self._datasource,
            "query": normalized_sql,
            "max_rows": max_rows,
        }

        for attempt in range(max_retries):
            try:
                async with session.post(
                    url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self._timeout_request),
                ) as response:
                    if response.status != 200:
                        await response.read()
                        if response.status >= 500 and attempt < max_retries - 1:
                            await self._wait_before_retry(
                                attempt, f"http_status={response.status}"
                            )
                            continue
                        raise DataFabricQueryError(
                            f"Data Fabric query returned HTTP {response.status}"
                        )

                    data = await response.json()
                    if data.get("type") == "error":
                        remote_message = str(data.get("error", ""))
                        if self._is_retryable(remote_message) and attempt < max_retries - 1:
                            await self._wait_before_retry(attempt, "remote_error")
                            continue
                        raise DataFabricQueryError("Data Fabric rejected the query")

                    return self._parse_rows(data)

            except asyncio.TimeoutError as exc:
                if attempt < max_retries - 1:
                    await self._wait_before_retry(attempt, "timeout")
                    continue
                raise DataFabricUnavailableError(
                    f"Data Fabric query timed out after {max_retries} attempt(s)"
                ) from exc
            except DataFabricGatewayError:
                raise
            except aiohttp.ClientError as exc:
                if attempt < max_retries - 1:
                    await self._wait_before_retry(attempt, type(exc).__name__)
                    continue
                raise DataFabricUnavailableError(
                    f"Data Fabric request failed ({type(exc).__name__})"
                ) from exc
            except (TypeError, ValueError, KeyError) as exc:
                raise DataFabricQueryError(
                    f"Invalid Data Fabric response ({type(exc).__name__})"
                ) from exc

        raise AssertionError("retry loop exited without a result")

    async def _wait_before_retry(self, attempt: int, reason: str) -> None:
        wait_seconds = (attempt + 1) * 2
        log_catalog_operation(
            "FABRIC",
            "RETRY",
            f"reason={reason} wait_seconds={wait_seconds}",
            logging.WARNING,
        )
        await asyncio.sleep(wait_seconds)

    @staticmethod
    def _is_retryable(error_message: str) -> bool:
        normalized = error_message.lower()
        return "queuepool" in normalized or "connection timed out" in normalized

    @staticmethod
    def _parse_rows(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Normalize a Data Fabric table response to ``list[dict]``."""
        columns = data.get("columns") or []
        rows = data.get("data") or []
        if not rows:
            return []
        if not columns:
            raise DataFabricQueryError("Data Fabric response has rows without columns")

        normalized: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) != len(columns):
                raise DataFabricQueryError("Data Fabric response row does not match columns")
            normalized.append(dict(zip(columns, row)))
        return normalized


class DataFabricDatasourceNotFoundError(DataFabricGatewayError):
    """The datasource is not registered in Data Fabric (HTTP 404)."""


# ── 발견(discovery) REST 프록시 — fabric 의 datasource 브라우징 API 소비 ──
# (스키마 열거의 원천은 Neo4j 가 아니라 fabric — Neo4j TABLE 은 analyzer 가 쓴
#  그래프의 되읽기라 미발견 테이블을 모른다. specs/discovery-endpoint 계약.)


async def _fabric_get_json(
    gateway: "DataFabricQueryGateway",
    session: aiohttp.ClientSession,
    path: str,
    label: str,
) -> Dict[str, Any]:
    """fabric GET — 404 = 미등록(전용 오류), non-200 = 실패, 200 = JSON."""
    url = f"{gateway._base_url}{path}"
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=gateway._timeout_request),
        ) as response:
            if response.status == 404:
                raise DataFabricDatasourceNotFoundError(
                    f"datasource not registered ({label})"
                )
            if response.status != 200:
                raise DataFabricQueryError(
                    f"Data Fabric {label} returned HTTP {response.status}"
                )
            return await response.json()
    except DataFabricGatewayError:
        raise
    except asyncio.TimeoutError as exc:
        raise DataFabricUnavailableError(f"Data Fabric {label} timed out") from exc
    except aiohttp.ClientError as exc:
        raise DataFabricUnavailableError(
            f"Data Fabric {label} failed ({type(exc).__name__})"
        ) from exc


async def list_datasource_tables(
    gateway: "DataFabricQueryGateway", session: aiohttp.ClientSession,
) -> List[str]:
    """``GET /api/datasources/{ds}/tables`` → 테이블명 목록."""
    if not gateway.is_configured:
        raise DataFabricUnavailableError("Data Fabric URL and datasource are required")
    data = await _fabric_get_json(
        gateway, session,
        f"{DATASOURCE_ENDPOINT}/{gateway.datasource}/tables",
        "table listing",
    )
    tables = data.get("tables")
    if not isinstance(tables, list):
        raise DataFabricQueryError("Invalid Data Fabric table listing response")
    names: list[str] = []
    for table in tables:
        if not isinstance(table, dict):
            raise DataFabricQueryError("Invalid Data Fabric table listing entry")
        name = table.get("name")
        if not isinstance(name, str) or not name.strip():
            raise DataFabricQueryError("Invalid Data Fabric table listing name")
        names.append(name.strip())
    return names


async def fetch_datasource_table_schema(
    gateway: "DataFabricQueryGateway", session: aiohttp.ClientSession, table: str,
) -> List[Dict[str, Any]]:
    """``GET /api/datasources/{ds}/tables/{table}/schema`` → 컬럼 메타 목록."""
    if not gateway.is_configured:
        raise DataFabricUnavailableError("Data Fabric URL and datasource are required")
    data = await _fabric_get_json(
        gateway, session,
        f"{DATASOURCE_ENDPOINT}/{gateway.datasource}/tables/{table}/schema",
        "table schema",
    )
    columns = data.get("columns")
    if not isinstance(columns, list):
        raise DataFabricQueryError("Invalid Data Fabric table schema response")
    return columns


async def resolve_datasource_objects(
    gateway: "DataFabricQueryGateway",
    session: aiohttp.ClientSession,
    references: list[dict[str, Any]],
    *,
    sample_limit: int,
    concurrency: int,
) -> Dict[str, Any]:
    """Single Fabric batch request for exact metadata and confirmed-only samples."""
    if not gateway.is_configured:
        raise DataFabricUnavailableError("Data Fabric URL and datasource are required")
    url = (
        f"{gateway._base_url}{DATASOURCE_ENDPOINT}/{gateway.datasource}"
        "/object-resolution-context"
    )
    try:
        async with session.post(
            url,
            json={
                "references": references,
                "sample_limit": sample_limit,
                "concurrency": concurrency,
            },
            timeout=aiohttp.ClientTimeout(total=gateway._timeout_request),
        ) as response:
            if response.status == 404:
                raise DataFabricDatasourceNotFoundError(
                    "datasource not registered (object resolution)"
                )
            if response.status != 200:
                await response.read()
                raise DataFabricQueryError(
                    f"Data Fabric object resolution returned HTTP {response.status}"
                )
            payload = await response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
                raise DataFabricQueryError("Invalid Data Fabric object resolution response")
            return payload
    except DataFabricGatewayError:
        raise
    except asyncio.TimeoutError as exc:
        raise DataFabricUnavailableError("Data Fabric object resolution timed out") from exc
    except aiohttp.ClientError as exc:
        raise DataFabricUnavailableError(
            f"Data Fabric object resolution failed ({type(exc).__name__})"
        ) from exc

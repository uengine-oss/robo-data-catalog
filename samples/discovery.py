"""Lossless datasource table discovery over an immutable in-process snapshot.

The first request seals the complete, deterministically ordered Fabric table identity
population. Continuation cursors are opaque server-owned tokens bound to that snapshot,
datasource, sample limit, and page size. Page details are fetched only for the requested
slice; no prefix is published as a complete inventory.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import secrets
import time
from typing import Any, Callable

import aiohttp

from contracts.table_discovery import TABLE_DISCOVERY_SCHEMA_VERSION
from integrations.data_fabric import (
    DataFabricDatasourceNotFoundError,
    DataFabricGatewayError,
    DataFabricQueryError,
    DataFabricQueryGateway,
    fetch_datasource_table_schema,
    list_datasource_tables,
)
from shared.observability.logger import log_catalog_operation


logger = logging.getLogger(__name__)


def _identifier_key(value: str, quoted: bool) -> tuple[str, str]:
    return ("Q", value) if quoted else ("U", value.casefold())


def _split_qualified_name(value: object) -> list[tuple[str, bool]]:
    if not isinstance(value, str) or not value.strip():
        raise DataFabricQueryError("Fabric table listing contains an invalid name")
    text = value.strip()
    raw_parts: list[str] = []
    current: list[str] = []
    quoted = False
    index = 0
    while index < len(text):
        char = text[index]
        if char == '"':
            current.append(char)
            if quoted and index + 1 < len(text) and text[index + 1] == '"':
                current.append('"')
                index += 2
                continue
            quoted = not quoted
        elif char == "." and not quoted:
            raw_parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
        index += 1
    if quoted:
        raise DataFabricQueryError("Fabric table listing contains an unclosed quote")
    raw_parts.append("".join(current).strip())
    if not 1 <= len(raw_parts) <= 3 or any(not part for part in raw_parts):
        raise DataFabricQueryError("Fabric table listing contains an invalid identity")

    parts: list[tuple[str, bool]] = []
    for raw in raw_parts:
        if raw.startswith('"'):
            if not raw.endswith('"') or len(raw) < 2:
                raise DataFabricQueryError("Fabric table listing contains an invalid quote")
            inner = raw[1:-1]
            cursor = 0
            decoded: list[str] = []
            while cursor < len(inner):
                if inner[cursor] == '"':
                    if cursor + 1 >= len(inner) or inner[cursor + 1] != '"':
                        raise DataFabricQueryError(
                            "Fabric table listing contains an invalid quoted identifier"
                        )
                    decoded.append('"')
                    cursor += 2
                else:
                    decoded.append(inner[cursor])
                    cursor += 1
            name = "".join(decoded)
            is_quoted = True
        else:
            if '"' in raw:
                raise DataFabricQueryError(
                    "Fabric table listing contains an invalid quoted identifier"
                )
            name = raw
            is_quoted = False
        if not name:
            raise DataFabricQueryError("Fabric table listing contains a blank identifier")
        parts.append((name, is_quoted))
    return parts


@dataclass(frozen=True)
class DiscoveredTableIdentity:
    source_name: str
    catalog: str
    schema: str
    name: str
    catalog_quoted: bool
    schema_quoted: bool
    name_quoted: bool

    @property
    def key(self) -> tuple[tuple[str, str], tuple[str, str], tuple[str, str]]:
        return (
            _identifier_key(self.catalog, self.catalog_quoted),
            _identifier_key(self.schema, self.schema_quoted),
            _identifier_key(self.name, self.name_quoted),
        )


def _table_identity(source_name: str) -> DiscoveredTableIdentity:
    parts = _split_qualified_name(source_name)
    padded = [("", False)] * (3 - len(parts)) + parts
    (catalog, catalog_quoted), (schema, schema_quoted), (name, name_quoted) = padded
    return DiscoveredTableIdentity(
        source_name=source_name.strip(),
        catalog=catalog,
        schema=schema,
        name=name,
        catalog_quoted=catalog_quoted,
        schema_quoted=schema_quoted,
        name_quoted=name_quoted,
    )


def _seal_population(names: list[str]) -> tuple[DiscoveredTableIdentity, ...]:
    tables = [_table_identity(name) for name in names]
    tables.sort(key=lambda table: table.key)
    seen: set[tuple[tuple[str, str], tuple[str, str], tuple[str, str]]] = set()
    for table in tables:
        if table.key in seen:
            raise DataFabricQueryError("Fabric table listing contains a duplicate identity")
        seen.add(table.key)
    return tuple(tables)


@dataclass(frozen=True)
class DiscoverySnapshot:
    token: str
    datasource: str
    sample_limit: int
    page_size: int
    tables: tuple[DiscoveredTableIdentity, ...]
    cursors: tuple[str, ...]
    created_at: float


class DiscoverySnapshotStore:
    """Bounded immutable snapshot owner; expiration always fails closed."""

    def __init__(
        self,
        *,
        ttl_seconds: int = 900,
        capacity: int = 256,
        clock: Callable[[], float] = time.monotonic,
        token_factory: Callable[[], str] = lambda: secrets.token_urlsafe(32),
    ) -> None:
        if ttl_seconds <= 0 or capacity <= 0:
            raise ValueError("snapshot ttl and capacity must be positive")
        self._ttl_seconds = ttl_seconds
        self._capacity = capacity
        self._clock = clock
        self._token_factory = token_factory
        self._snapshots: dict[str, DiscoverySnapshot] = {}
        self._lock = asyncio.Lock()

    def _new_token(self, reserved: set[str]) -> str:
        for _ in range(100):
            token = self._token_factory()
            if isinstance(token, str) and token and token not in reserved:
                return token
        raise RuntimeError("unable to allocate an opaque discovery token")

    def _purge_expired(self, now: float) -> None:
        expired = [
            token for token, snapshot in self._snapshots.items()
            if now - snapshot.created_at >= self._ttl_seconds
        ]
        for token in expired:
            del self._snapshots[token]

    async def create(
        self,
        *,
        datasource: str,
        sample_limit: int,
        page_size: int,
        tables: tuple[DiscoveredTableIdentity, ...],
    ) -> DiscoverySnapshot:
        async with self._lock:
            now = self._clock()
            self._purge_expired(now)
            while len(self._snapshots) >= self._capacity:
                oldest = min(
                    self._snapshots.values(), key=lambda snapshot: snapshot.created_at,
                )
                del self._snapshots[oldest.token]

            reserved = set(self._snapshots)
            for existing in self._snapshots.values():
                reserved.update(existing.cursors)
            token = self._new_token(reserved)
            reserved.add(token)
            page_count = max(1, (len(tables) + page_size - 1) // page_size)
            cursors: list[str] = []
            for _ in range(1, page_count):
                cursor = self._new_token(reserved)
                reserved.add(cursor)
                cursors.append(cursor)
            snapshot = DiscoverySnapshot(
                token=token,
                datasource=datasource,
                sample_limit=sample_limit,
                page_size=page_size,
                tables=tables,
                cursors=tuple(cursors),
                created_at=now,
            )
            self._snapshots[token] = snapshot
            return snapshot

    async def resolve(
        self,
        *,
        token: str,
        cursor: str,
        datasource: str,
        sample_limit: int,
        page_size: int,
    ) -> tuple[DiscoverySnapshot, int]:
        async with self._lock:
            self._purge_expired(self._clock())
            snapshot = self._snapshots.get(token)
            if snapshot is None:
                raise ValueError("discovery snapshot is unknown or expired")
            if (
                snapshot.datasource != datasource
                or snapshot.sample_limit != sample_limit
                or snapshot.page_size != page_size
            ):
                raise ValueError("discovery continuation controls do not match snapshot")
            try:
                page_index = snapshot.cursors.index(cursor) + 1
            except ValueError as exc:
                raise ValueError("discovery cursor does not belong to snapshot") from exc
            return snapshot, page_index


def _normalize_columns(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for column in columns:
        if not isinstance(column, dict):
            raise DataFabricQueryError("Fabric schema contains a non-object column")
        raw_name = column.get("name")
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise DataFabricQueryError("Fabric schema contains a column without a name")
        explicit_quoted = column.get("name_quoted")
        if explicit_quoted is not None and not isinstance(explicit_quoted, bool):
            raise DataFabricQueryError("Fabric schema contains an invalid quoted flag")
        parsed = _split_qualified_name(raw_name)
        if len(parsed) != 1:
            raise DataFabricQueryError("Fabric schema column name must be unqualified")
        name, syntax_quoted = parsed[0]
        if explicit_quoted is False and syntax_quoted:
            raise DataFabricQueryError("Fabric schema column quote evidence conflicts")
        name_quoted = syntax_quoted if explicit_quoted is None else explicit_quoted
        key = _identifier_key(name, name_quoted)
        if key in seen:
            raise DataFabricQueryError("Fabric schema contains a duplicate column identity")
        seen.add(key)
        normalized.append({**column, "name": name, "name_quoted": name_quoted})
    return normalized


def _sample_sql(table: DiscoveredTableIdentity, limit: int) -> str:
    identifiers = [
        value for value in (table.catalog, table.schema, table.name) if value
    ]
    quoted = ".".join(
        f'"{identifier.replace(chr(34), chr(34) * 2)}"'
        for identifier in identifiers
    )
    return f"SELECT * FROM {quoted} LIMIT {limit}"


class TableDiscoveryService:
    """Fabric inventory sealing plus bounded page-detail collection."""

    def __init__(
        self,
        db_client: DataFabricQueryGateway,
        snapshots: DiscoverySnapshotStore,
        concurrency: int = 5,
    ) -> None:
        self._db = db_client
        self._snapshots = snapshots
        self._sem = asyncio.Semaphore(max(1, concurrency))

    async def fetch_page(
        self,
        *,
        sample_limit: int,
        page_size: int,
        snapshot_token: str | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if sample_limit < 0 or page_size <= 0:
            raise ValueError("sample_limit and page_size are out of range")
        if (snapshot_token is None) != (cursor is None):
            raise ValueError("snapshot and cursor must be supplied together")

        async with aiohttp.ClientSession() as session:
            if snapshot_token is None:
                names = await list_datasource_tables(self._db, session)
                snapshot = await self._snapshots.create(
                    datasource=self._db.datasource,
                    sample_limit=sample_limit,
                    page_size=page_size,
                    tables=_seal_population(names),
                )
                page_index = 0
            else:
                snapshot, page_index = await self._snapshots.resolve(
                    token=snapshot_token,
                    cursor=cursor or "",
                    datasource=self._db.datasource,
                    sample_limit=sample_limit,
                    page_size=page_size,
                )

            start = page_index * snapshot.page_size
            selected = snapshot.tables[start:start + snapshot.page_size]
            tables = await asyncio.gather(
                *[self._fetch_one(session, table, sample_limit) for table in selected]
            )

        next_cursor = (
            snapshot.cursors[page_index]
            if page_index < len(snapshot.cursors)
            else None
        )
        log_catalog_operation(
            "DISCOVERY",
            "PAGE",
            f"datasource={self._db.datasource} page={page_index} "
            f"tables={len(tables)} total={len(snapshot.tables)} "
            f"degraded={sum(1 for table in tables if table['degraded'])} "
            f"terminal={next_cursor is None}",
            logging.INFO,
        )
        return {
            "schema_version": TABLE_DISCOVERY_SCHEMA_VERSION,
            "datasource": self._db.datasource,
            "snapshot": snapshot.token,
            "page_index": page_index,
            "total_tables": len(snapshot.tables),
            "tables": tables,
            "next_cursor": next_cursor,
        }

    async def _fetch_one(
        self,
        session: aiohttp.ClientSession,
        table: DiscoveredTableIdentity,
        sample_limit: int,
    ) -> dict[str, Any]:
        columns: list[dict[str, Any]] = []
        sample_rows: list[dict[str, Any]] = []
        degraded = False
        async with self._sem:
            try:
                raw_columns = await fetch_datasource_table_schema(
                    self._db, session, table.source_name,
                )
            except DataFabricDatasourceNotFoundError:
                raise
            except DataFabricGatewayError as exc:
                degraded = True
                log_catalog_operation(
                    "DISCOVERY", "SCHEMA_FAIL",
                    f"table={table.source_name} {type(exc).__name__}", logging.WARNING,
                )
            else:
                # A failed request can degrade one table. A successful request with
                # contradictory identities is provider corruption and aborts the page.
                columns = _normalize_columns(raw_columns)
            if sample_limit:
                try:
                    sample_rows = await self._db.fetch_rows(
                        session,
                        _sample_sql(table, sample_limit),
                        max_rows=sample_limit,
                    ) or []
                except DataFabricDatasourceNotFoundError:
                    raise
                except DataFabricGatewayError as exc:
                    degraded = True
                    log_catalog_operation(
                        "DISCOVERY", "SAMPLE_FAIL",
                        f"table={table.source_name} {type(exc).__name__}", logging.WARNING,
                    )
        if any(not isinstance(row, dict) for row in sample_rows):
            raise DataFabricQueryError("Fabric sample contains a non-object row")
        return {
            "catalog": table.catalog,
            "schema": table.schema,
            "name": table.name,
            "catalog_quoted": table.catalog_quoted,
            "schema_quoted": table.schema_quoted,
            "name_quoted": table.name_quoted,
            "columns": columns,
            "sample_rows": sample_rows,
            "degraded": degraded,
        }

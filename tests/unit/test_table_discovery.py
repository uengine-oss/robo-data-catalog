"""Immutable discovery snapshot, cursor binding, and detail collection contracts."""
from __future__ import annotations

import itertools

import pytest

import samples.discovery as discovery_module
from contracts.table_discovery import TABLE_DISCOVERY_SCHEMA_VERSION
from integrations.data_fabric import (
    DataFabricDatasourceNotFoundError,
    DataFabricQueryError,
    DataFabricQueryGateway,
    fetch_datasource_table_schema,
    list_datasource_tables,
)
from samples.discovery import DiscoverySnapshotStore, TableDiscoveryService


def _gateway(datasource: str = "prod_main") -> DataFabricQueryGateway:
    return DataFabricQueryGateway(base_url="http://fabric", datasource=datasource)


class _JsonResponse:
    status = 200

    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self._payload


class _RecordingSession:
    def __init__(self, responses):
        self._responses = iter(responses)
        self.urls = []

    def get(self, url, **_kwargs):
        self.urls.append(url)
        return _JsonResponse(next(self._responses))


@pytest.mark.asyncio
async def test_datasource_browse_uses_fabric_api_prefix():
    session = _RecordingSession([
        {"tables": [{"name": "public.orders"}]},
        {"columns": [{"name": "order_id"}]},
    ])

    assert await list_datasource_tables(_gateway(), session) == ["public.orders"]
    assert await fetch_datasource_table_schema(
        _gateway(), session, "public.orders",
    ) == [{"name": "order_id"}]
    assert session.urls == [
        "http://fabric/api/datasources/prod_main/tables",
        "http://fabric/api/datasources/prod_main/tables/public.orders/schema",
    ]


@pytest.mark.asyncio
async def test_datasource_browse_rejects_malformed_success_payload():
    with pytest.raises(DataFabricQueryError, match="listing entry"):
        await list_datasource_tables(
            _gateway(), _RecordingSession([{"tables": ["public.orders"]}]),
        )


def _store(**kwargs) -> DiscoverySnapshotStore:
    tokens = (f"opaque-{index}" for index in itertools.count())
    return DiscoverySnapshotStore(
        ttl_seconds=kwargs.pop("ttl_seconds", 900),
        capacity=kwargs.pop("capacity", 256),
        token_factory=lambda: next(tokens),
        **kwargs,
    )


def _patch_fabric(monkeypatch, *, tables, schemas=None, samples=None):
    schemas = schemas or {}
    samples = samples or {}
    calls = {"list": 0, "schema": [], "sample": []}

    async def fake_list(gateway, session):
        calls["list"] += 1
        if isinstance(tables, Exception):
            raise tables
        return list(tables)

    async def fake_schema(gateway, session, table):
        calls["schema"].append(table)
        value = schemas.get(table, [])
        if isinstance(value, Exception):
            raise value
        return value

    async def fake_rows(self, session, sql, max_rows=1000, max_retries=3):
        calls["sample"].append((sql, max_rows))
        for name, value in samples.items():
            if name in sql or name.split(".")[-1].strip('"') in sql:
                if isinstance(value, Exception):
                    raise value
                return value
        return []

    monkeypatch.setattr(discovery_module, "list_datasource_tables", fake_list)
    monkeypatch.setattr(discovery_module, "fetch_datasource_table_schema", fake_schema)
    monkeypatch.setattr(DataFabricQueryGateway, "fetch_rows", fake_rows)
    return calls


@pytest.mark.asyncio
async def test_pages_are_deterministic_complete_and_inventory_is_listed_once(monkeypatch):
    calls = _patch_fabric(
        monkeypatch,
        tables=["PUBLIC.ORDERS", "USERS", "public.AUDIT"],
        schemas={
            "PUBLIC.ORDERS": [{"name": "ORDER_ID"}],
            "USERS": [{"name": "USER_ID"}],
            "public.AUDIT": [{"name": "EVENT_ID"}],
        },
    )
    service = TableDiscoveryService(_gateway(), _store())

    first = await service.fetch_page(sample_limit=0, page_size=2)
    second = await service.fetch_page(
        sample_limit=0,
        page_size=2,
        snapshot_token=first["snapshot"],
        cursor=first["next_cursor"],
    )

    assert first["schema_version"] == TABLE_DISCOVERY_SCHEMA_VERSION
    assert first["page_index"] == 0
    assert first["total_tables"] == 3
    assert [table["name"] for table in first["tables"]] == ["USERS", "AUDIT"]
    assert second["page_index"] == 1
    assert [table["name"] for table in second["tables"]] == ["ORDERS"]
    assert second["next_cursor"] is None
    assert first["snapshot"] == second["snapshot"]
    assert calls["list"] == 1
    assert calls["sample"] == []
    assert "truncated" not in first


@pytest.mark.asyncio
async def test_zero_table_snapshot_closes_on_page_zero(monkeypatch):
    _patch_fabric(monkeypatch, tables=[])
    result = await TableDiscoveryService(_gateway(), _store()).fetch_page(
        sample_limit=0, page_size=10,
    )
    assert result["tables"] == []
    assert result["total_tables"] == 0
    assert result["next_cursor"] is None


@pytest.mark.asyncio
async def test_quote_aware_table_and_column_identity_is_preserved(monkeypatch):
    source = '"Main"."App"."Orders"'
    calls = _patch_fabric(
        monkeypatch,
        tables=[source],
        schemas={source: [{"name": '"OrderId"', "dtype": "NUMBER"}]},
        samples={source: [{"OrderId": 1}]},
    )
    result = await TableDiscoveryService(_gateway(), _store()).fetch_page(
        sample_limit=1, page_size=10,
    )
    table = result["tables"][0]
    assert (table["catalog"], table["schema"], table["name"]) == (
        "Main", "App", "Orders",
    )
    assert table["catalog_quoted"] is True
    assert table["schema_quoted"] is True
    assert table["name_quoted"] is True
    assert table["columns"][0]["name"] == "OrderId"
    assert table["columns"][0]["name_quoted"] is True
    assert calls["sample"] == [
        ('SELECT * FROM "Main"."App"."Orders" LIMIT 1', 1),
    ]


@pytest.mark.asyncio
async def test_unquoted_duplicate_table_identity_fails_before_page_details(monkeypatch):
    calls = _patch_fabric(
        monkeypatch, tables=["PUBLIC.Orders", "public.ORDERS"],
    )
    with pytest.raises(DataFabricQueryError, match="duplicate identity"):
        await TableDiscoveryService(_gateway(), _store()).fetch_page(
            sample_limit=0, page_size=10,
        )
    assert calls["schema"] == []


@pytest.mark.asyncio
async def test_duplicate_column_identity_fails_page(monkeypatch):
    _patch_fabric(
        monkeypatch,
        tables=["ORDERS"],
        schemas={"ORDERS": [{"name": "ID"}, {"name": "id"}]},
    )
    with pytest.raises(DataFabricQueryError, match="duplicate column identity"):
        await TableDiscoveryService(_gateway(), _store()).fetch_page(
            sample_limit=0, page_size=10,
        )


@pytest.mark.asyncio
async def test_per_table_gateway_failure_is_degraded_not_silent(monkeypatch):
    _patch_fabric(
        monkeypatch,
        tables=["GOOD", "BROKEN"],
        schemas={"GOOD": [{"name": "C1"}], "BROKEN": DataFabricQueryError("boom")},
        samples={"GOOD": [{"C1": 1}], "BROKEN": DataFabricQueryError("boom")},
    )
    result = await TableDiscoveryService(_gateway(), _store()).fetch_page(
        sample_limit=5, page_size=10,
    )
    by_name = {table["name"]: table for table in result["tables"]}
    assert set(by_name) == {"GOOD", "BROKEN"}
    assert by_name["BROKEN"]["degraded"] is True
    assert by_name["GOOD"]["degraded"] is False


@pytest.mark.asyncio
async def test_cursor_is_bound_to_all_snapshot_controls(monkeypatch):
    _patch_fabric(monkeypatch, tables=["T1", "T2"])
    store = _store()
    first = await TableDiscoveryService(_gateway(), store).fetch_page(
        sample_limit=0, page_size=1,
    )
    cases = [
        (_gateway("other"), 0, 1, first["snapshot"], first["next_cursor"]),
        (_gateway(), 1, 1, first["snapshot"], first["next_cursor"]),
        (_gateway(), 0, 2, first["snapshot"], first["next_cursor"]),
        (_gateway(), 0, 1, first["snapshot"], "wrong-cursor"),
        (_gateway(), 0, 1, "wrong-snapshot", first["next_cursor"]),
    ]
    for gateway, sample_limit, page_size, snapshot, cursor in cases:
        with pytest.raises(ValueError):
            await TableDiscoveryService(gateway, store).fetch_page(
                sample_limit=sample_limit,
                page_size=page_size,
                snapshot_token=snapshot,
                cursor=cursor,
            )


@pytest.mark.asyncio
async def test_expired_snapshot_fails_closed(monkeypatch):
    now = [10.0]
    _patch_fabric(monkeypatch, tables=["T1", "T2"])
    store = _store(ttl_seconds=5, clock=lambda: now[0])
    first = await TableDiscoveryService(_gateway(), store).fetch_page(
        sample_limit=0, page_size=1,
    )
    now[0] = 15.0
    with pytest.raises(ValueError, match="unknown or expired"):
        await TableDiscoveryService(_gateway(), store).fetch_page(
            sample_limit=0,
            page_size=1,
            snapshot_token=first["snapshot"],
            cursor=first["next_cursor"],
        )


@pytest.mark.asyncio
async def test_capacity_eviction_fails_old_continuation_closed(monkeypatch):
    _patch_fabric(monkeypatch, tables=["T1", "T2"])
    store = _store(capacity=1)
    service = TableDiscoveryService(_gateway(), store)
    first = await service.fetch_page(sample_limit=0, page_size=1)
    await service.fetch_page(sample_limit=0, page_size=1)
    with pytest.raises(ValueError, match="unknown or expired"):
        await service.fetch_page(
            sample_limit=0,
            page_size=1,
            snapshot_token=first["snapshot"],
            cursor=first["next_cursor"],
        )


@pytest.mark.asyncio
async def test_datasource_404_during_page_detail_is_not_degraded(monkeypatch):
    _patch_fabric(
        monkeypatch,
        tables=["T1"],
        schemas={"T1": DataFabricDatasourceNotFoundError("removed")},
    )
    with pytest.raises(DataFabricDatasourceNotFoundError):
        await TableDiscoveryService(_gateway(), _store()).fetch_page(
            sample_limit=0, page_size=10,
        )


@pytest.mark.asyncio
async def test_unknown_datasource_propagates_not_found(monkeypatch):
    _patch_fabric(
        monkeypatch, tables=DataFabricDatasourceNotFoundError("no such datasource"),
    )
    with pytest.raises(DataFabricDatasourceNotFoundError):
        await TableDiscoveryService(_gateway(), _store()).fetch_page(
            sample_limit=0, page_size=10,
        )

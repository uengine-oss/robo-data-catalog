"""Real ASGI wire proof between Catalog pages and the Analyzer consumer."""
from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
import socket
import threading

import httpx
import pytest
import uvicorn

import api.table_samples as table_samples_api
import samples.discovery as discovery_module
from integrations.data_fabric import DataFabricQueryGateway
from main import app
from samples.discovery import DiscoverySnapshotStore


def _load_analyzer_consumer():
    analyzer_file = (
        Path(__file__).resolve().parents[3]
        / "robo-data-analyzer"
        / "integrations"
        / "catalog_discovery.py"
    )
    spec = importlib.util.spec_from_file_location(
        "analyzer_catalog_discovery_contract", analyzer_file,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_analyzer_consumes_every_catalog_page_exactly_once(monkeypatch):
    list_calls = 0
    schema_calls: list[str] = []

    async def fake_list(gateway, session):
        nonlocal list_calls
        list_calls += 1
        return ["PUBLIC.T3", "PUBLIC.T1", "PUBLIC.T2"]

    async def fake_schema(gateway, session, table):
        schema_calls.append(table)
        return [{"name": "ID", "dtype": "NUMBER", "nullable": False}]

    async def fake_rows(self, session, sql, max_rows=1000, max_retries=3):
        return [{"ID": len(schema_calls)}]

    monkeypatch.setattr(discovery_module, "list_datasource_tables", fake_list)
    monkeypatch.setattr(discovery_module, "fetch_datasource_table_schema", fake_schema)
    monkeypatch.setattr(DataFabricQueryGateway, "fetch_rows", fake_rows)
    monkeypatch.setattr(
        table_samples_api,
        "_DISCOVERY_SNAPSHOTS",
        DiscoverySnapshotStore(ttl_seconds=60, capacity=4),
    )

    analyzer = _load_analyzer_consumer()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await analyzer.get_complete_table_discovery(
            client,
            "http://catalog/robo",
            "prod_main",
            sample_limit=1,
            page_size=1,
        )

    assert [table["name"] for table in result["tables"]] == ["T1", "T2", "T3"]
    assert result["total_tables"] == 3
    assert list_calls == 1
    assert schema_calls == ["PUBLIC.T1", "PUBLIC.T2", "PUBLIC.T3"]


@pytest.mark.asyncio
async def test_http_boundary_rejects_partial_or_unknown_continuation(monkeypatch):
    async def fake_list(gateway, session):
        return []

    monkeypatch.setattr(discovery_module, "list_datasource_tables", fake_list)
    monkeypatch.setattr(
        table_samples_api,
        "_DISCOVERY_SNAPSHOTS",
        DiscoverySnapshotStore(ttl_seconds=60, capacity=4),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://catalog") as client:
        partial = await client.get(
            "/robo/tables/discovery",
            params={"datasource": "prod_main", "snapshot": "only-snapshot"},
        )
        unknown = await client.get(
            "/robo/tables/discovery",
            params={
                "datasource": "prod_main",
                "snapshot": "unknown",
                "cursor": "unknown",
            },
        )
    assert partial.status_code == 400
    assert unknown.status_code == 400


@pytest.mark.asyncio
async def test_analyzer_catalog_contract_over_isolated_tcp_http(monkeypatch):
    async def fake_list(gateway, session):
        return ["PUBLIC.B", "PUBLIC.A"]

    async def fake_schema(gateway, session, table):
        return [{"name": "ID"}]

    async def fake_rows(self, session, sql, max_rows=1000, max_retries=3):
        return []

    monkeypatch.setattr(discovery_module, "list_datasource_tables", fake_list)
    monkeypatch.setattr(discovery_module, "fetch_datasource_table_schema", fake_schema)
    monkeypatch.setattr(DataFabricQueryGateway, "fetch_rows", fake_rows)
    monkeypatch.setattr(
        table_samples_api,
        "_DISCOVERY_SNAPSHOTS",
        DiscoverySnapshotStore(ttl_seconds=60, capacity=4),
    )

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(
        app, log_level="warning", access_log=False, lifespan="off",
    ))
    thread = threading.Thread(
        target=server.run, kwargs={"sockets": [listener]}, daemon=True,
    )
    thread.start()
    try:
        for _ in range(100):
            if server.started:
                break
            await asyncio.sleep(0.02)
        assert server.started
        analyzer = _load_analyzer_consumer()
        async with httpx.AsyncClient() as client:
            result = await analyzer.get_complete_table_discovery(
                client,
                f"http://127.0.0.1:{port}/robo",
                "prod_main",
                sample_limit=0,
                page_size=1,
            )
        assert [table["name"] for table in result["tables"]] == ["A", "B"]
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        listener.close()
    assert not thread.is_alive()

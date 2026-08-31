from __future__ import annotations

import pytest

import samples.object_resolver as subject
from contracts.object_resolution import ObjectReferenceRequest
from integrations.data_fabric import DataFabricQueryError, DataFabricQueryGateway
from samples.object_resolver import ObjectIdentityResolver


@pytest.mark.asyncio
async def test_deduplicates_identity_before_one_fabric_batch_and_expands_keys(monkeypatch):
    calls = []

    async def fake_resolve(gateway, session, references, *, sample_limit, concurrency):
        calls.append((references, sample_limit, concurrency))
        return {
            "datasource": gateway.datasource,
            "results": [{
                "key": references[0]["key"], "status": "confirmed",
                "reason": "exact_metadata_match", "object_name": "ORDERS",
            }],
            "metrics": {"provider_calls": 3},
        }

    monkeypatch.setattr(subject, "resolve_datasource_objects", fake_resolve)
    resolver = ObjectIdentityResolver(
        DataFabricQueryGateway("http://fabric", "shop"), concurrency=4,
    )
    refs = [
        ObjectReferenceRequest(key="n1", raw_reference="PUBLIC.ORDERS", schema_name="PUBLIC", object_name="ORDERS", qualified_columns=["ID"]),
        ObjectReferenceRequest(key="n2", raw_reference="PUBLIC.ORDERS", schema_name="public", object_name="orders", qualified_columns=["id"]),
    ]

    result = await resolver.fetch(object(), refs, sample_limit=5)

    assert len(calls) == 1
    assert len(calls[0][0]) == 1
    assert [item["key"] for item in result["results"]] == ["n1", "n2"]
    assert result["metrics"]["input_references"] == 2
    assert result["metrics"]["distinct_references"] == 1


@pytest.mark.asyncio
async def test_missing_fabric_result_is_not_silently_dropped(monkeypatch):
    async def fake_resolve(*args, **kwargs):
        return {"datasource": "shop", "results": [], "metrics": {}}

    monkeypatch.setattr(subject, "resolve_datasource_objects", fake_resolve)
    resolver = ObjectIdentityResolver(DataFabricQueryGateway("http://fabric", "shop"))
    with pytest.raises(DataFabricQueryError, match="missing reference key"):
        await resolver.fetch(object(), [ObjectReferenceRequest(
            key="n1", raw_reference="ORDERS", object_name="ORDERS",
        )], sample_limit=0)


@pytest.mark.asyncio
async def test_case_sensitive_and_folded_identifiers_are_not_deduplicated(monkeypatch):
    captured = []

    async def fake_resolve(gateway, session, references, *, sample_limit, concurrency):
        captured.extend(references)
        return {
            "datasource": gateway.datasource,
            "results": [{"key": item["key"], "status": "unresolved", "reason": "x"}
                        for item in references],
            "metrics": {},
        }

    monkeypatch.setattr(subject, "resolve_datasource_objects", fake_resolve)
    resolver = ObjectIdentityResolver(DataFabricQueryGateway("http://fabric", "shop"))
    refs = [
        ObjectReferenceRequest(key="plain", raw_reference="orders", object_name="orders"),
        ObjectReferenceRequest(
            key="quoted", raw_reference='"orders"', object_name='"orders"',
            case_sensitive_identifiers=True,
        ),
    ]
    result = await resolver.fetch(object(), refs, sample_limit=0)

    assert len(captured) == 2
    assert result["metrics"]["distinct_references"] == 2


@pytest.mark.asyncio
async def test_distinct_case_sensitive_spellings_are_not_deduplicated(monkeypatch):
    captured = []

    async def fake_resolve(gateway, session, references, *, sample_limit, concurrency):
        captured.extend(references)
        return {
            "datasource": gateway.datasource,
            "results": [
                {"key": item["key"], "status": "unresolved", "reason": "x"}
                for item in references
            ],
            "metrics": {},
        }

    monkeypatch.setattr(subject, "resolve_datasource_objects", fake_resolve)
    resolver = ObjectIdentityResolver(DataFabricQueryGateway("http://fabric", "shop"))
    refs = [
        ObjectReferenceRequest(
            key="mixed", raw_reference='"Orders"', object_name="Orders",
            case_sensitive_identifiers=True,
        ),
        ObjectReferenceRequest(
            key="upper", raw_reference='"ORDERS"', object_name="ORDERS",
            case_sensitive_identifiers=True,
        ),
    ]
    result = await resolver.fetch(object(), refs, sample_limit=0)

    assert len(captured) == 2
    assert result["metrics"]["distinct_references"] == 2

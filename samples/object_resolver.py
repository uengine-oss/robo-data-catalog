"""Catalog-owned reference dedup and exact Fabric resolver boundary."""
from __future__ import annotations

from collections import OrderedDict
from typing import Any

import aiohttp

from contracts.object_resolution import ObjectReferenceRequest
from integrations.data_fabric import (
    DataFabricQueryError,
    DataFabricQueryGateway,
    resolve_datasource_objects,
)


class ObjectIdentityResolver:
    def __init__(self, gateway: DataFabricQueryGateway, concurrency: int = 5) -> None:
        self._gateway = gateway
        self._concurrency = max(1, concurrency)

    async def fetch(
        self,
        session: aiohttp.ClientSession,
        references: list[ObjectReferenceRequest],
        *,
        sample_limit: int,
    ) -> dict[str, Any]:
        groups: "OrderedDict[tuple, list[ObjectReferenceRequest]]" = OrderedDict()
        for reference in references:
            normalize = (
                (lambda value: value)
                if reference.case_sensitive_identifiers
                else (lambda value: value.upper())
            )
            identity = (
                normalize(reference.schema_name),
                normalize(reference.object_name),
                normalize(reference.database_link),
                reference.case_sensitive_identifiers,
                tuple(sorted(normalize(name) for name in reference.qualified_columns)),
            )
            groups.setdefault(identity, []).append(reference)
        representatives = [items[0] for items in groups.values()]
        payload = await resolve_datasource_objects(
            self._gateway,
            session,
            [reference.model_dump() for reference in representatives],
            sample_limit=sample_limit,
            concurrency=self._concurrency,
        )
        by_key = {str(item.get("key")): item for item in payload.get("results") or ()}
        expanded_by_key: dict[str, dict[str, Any]] = {}
        for items in groups.values():
            representative = items[0]
            resolved = by_key.get(representative.key)
            if resolved is None:
                raise DataFabricQueryError(
                    f"Fabric response missing reference key: {representative.key}"
                )
            for original in items:
                expanded_by_key[original.key] = {**resolved, "key": original.key}
        expanded = [expanded_by_key[reference.key] for reference in references]
        metrics = dict(payload.get("metrics") or {})
        metrics["input_references"] = len(references)
        metrics["distinct_references"] = len(representatives)
        return {
            "datasource": self._gateway.datasource,
            "results": expanded,
            "metrics": metrics,
        }

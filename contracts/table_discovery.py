"""Versioned Catalog-to-Analyzer table discovery response contract."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


TABLE_DISCOVERY_SCHEMA_VERSION = "catalog-table-discovery-page/v1"


class TableDiscoveryPageResponse(BaseModel):
    schema_version: Literal["catalog-table-discovery-page/v1"] = (
        TABLE_DISCOVERY_SCHEMA_VERSION
    )
    datasource: str
    snapshot: str = Field(..., min_length=1)
    page_index: int = Field(..., ge=0)
    total_tables: int = Field(..., ge=0)
    tables: list[dict[str, Any]]
    next_cursor: str | None = None

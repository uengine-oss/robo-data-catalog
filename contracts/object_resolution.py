"""Analyzer-to-Catalog exact data-object resolution contracts."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator


class ObjectReferenceRequest(BaseModel):
    key: str = Field(..., min_length=1, max_length=512)
    raw_reference: str = Field(..., min_length=1, max_length=512)
    schema_name: str = Field(default="", max_length=256)
    object_name: str = Field(..., min_length=1, max_length=256)
    database_link: str = Field(default="", max_length=256)
    case_sensitive_identifiers: bool = False
    qualified_columns: list[str] = Field(default_factory=list, max_length=1000)


class ObjectResolutionRequest(BaseModel):
    datasource: str = Field(..., min_length=1, max_length=128, pattern=r"^[A-Za-z_][A-Za-z0-9_-]*$")
    references: list[ObjectReferenceRequest] = Field(..., min_length=1, max_length=1000)
    sample_limit: int = Field(default=5, ge=0, le=50)

    @model_validator(mode="after")
    def unique_keys(self) -> "ObjectResolutionRequest":
        keys = [reference.key for reference in self.references]
        if len(keys) != len(set(keys)):
            raise ValueError("reference keys must be unique")
        return self


class ObjectResolutionResponse(BaseModel):
    datasource: str
    results: list[dict[str, Any]]
    metrics: dict[str, int | float]

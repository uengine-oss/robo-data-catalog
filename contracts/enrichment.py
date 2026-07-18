"""API 요청/응답 Pydantic 모델"""

from typing import Annotated, Literal, Optional, List
from pydantic import BaseModel, Field


class MetadataEnrichmentRequest(BaseModel):
    datasource_name: str = Field(
        ..., min_length=1, max_length=128, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$"
    )

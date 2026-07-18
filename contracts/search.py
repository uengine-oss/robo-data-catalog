"""API 요청/응답 Pydantic 모델"""

from typing import Annotated, Literal, Optional, List
from pydantic import BaseModel, Field


class SemanticSearchRequest(BaseModel):
    query: str = Field(min_length=1, max_length=2_000)
    limit: int = Field(default=10, ge=1, le=100)

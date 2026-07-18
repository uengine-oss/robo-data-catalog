"""API 요청/응답 Pydantic 모델"""

from typing import Annotated, Literal, Optional, List
from pydantic import BaseModel, Field


class LineageAnalyzeRequest(BaseModel):
    sqlContent: str = Field(min_length=1, max_length=2_000_000)
    fileName: str = Field(default="", max_length=1024)
    dbms: str = Field(default="oracle", max_length=32)
    nameCaseOption: Literal["uppercase", "lowercase", "original"] = "original"

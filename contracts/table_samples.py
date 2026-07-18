"""API 요청/응답 Pydantic 모델"""

from typing import Annotated, Literal, Optional, List
from pydantic import BaseModel, Field


class SampleContextRequest(BaseModel):
    """analyzer → catalog batch 요청.

    Phase 2 Linking 직후, analyzer 에서 식별된 모든 테이블명을 한 번에 전달.
    응답 map 의 key = 여기 보낸 table_names 의 원본 값.
    """
    datasource: str = Field(
        ..., min_length=1, max_length=128, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$",
        description="MindsDB datasource 이름",
    )
    table_names: List[Annotated[str, Field(min_length=1, max_length=1024)]] = Field(
        ..., min_length=1, max_length=500, description="코드에서 추출한 테이블명 목록"
    )
    sample_limit: int = Field(default=5, ge=1, le=20, description="테이블당 샘플 행 수")
    similarity_threshold: float = Field(default=85.0, ge=50.0, le=100.0, description="매칭 임계값 (rapidfuzz WRatio)")

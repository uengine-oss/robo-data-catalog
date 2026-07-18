"""API 요청/응답 Pydantic 모델"""

from typing import Annotated, Literal, Optional, List
from pydantic import BaseModel, Field


class SchemaTableInfo(BaseModel):
    name: str
    table_schema: str
    datasource: Optional[str] = ""
    logical_name: Optional[str] = ""  # 짧은 도메인 표시명 (물리명과 별개, analyzer spec 025)
    description: str
    description_source: Optional[str] = ""
    analyzed_description: Optional[str] = ""
    column_count: int


class SchemaColumnInfo(BaseModel):
    name: str
    table_name: str
    dtype: str
    nullable: bool
    description: str
    description_source: Optional[str] = ""
    analyzed_description: Optional[str] = ""


class SchemaRelationshipInfo(BaseModel):
    from_table: str
    from_schema: str
    from_column: str
    to_table: str
    to_schema: str
    to_column: str
    relationship_type: str
    description: str

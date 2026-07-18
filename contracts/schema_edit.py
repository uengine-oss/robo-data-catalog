"""API 요청/응답 Pydantic 모델"""

from typing import Annotated, Literal, Optional, List
from pydantic import BaseModel, Field


class AddRelationshipRequest(BaseModel):
    from_table: str = Field(min_length=1, max_length=512)
    from_schema: str = Field(default="", max_length=512)
    from_column: str = Field(min_length=1, max_length=512)
    to_table: str = Field(min_length=1, max_length=512)
    to_schema: str = Field(default="", max_length=512)
    to_column: str = Field(min_length=1, max_length=512)
    relationship_type: Literal[
        "FK_TO_TABLE", "ONE_TO_ONE", "ONE_TO_MANY", "MANY_TO_ONE", "MANY_TO_MANY"
    ] = "FK_TO_TABLE"
    description: str = Field(default="", max_length=10_000)


class TableDescriptionUpdateRequest(BaseModel):
    name: str
    table_schema: str = Field(alias="schema", default="public")
    description: Optional[str] = None
    model_config = {"populate_by_name": True}


class ColumnDescriptionUpdateRequest(BaseModel):
    table_name: str
    table_schema: str = "public"
    column_name: str
    description: Optional[str] = None


class VectorizeRequest(BaseModel):
    db_name: Optional[str] = "postgres"
    table_schema: Optional[str] = Field(alias="schema", default=None)
    model_config = {"populate_by_name": True}
    include_tables: bool = True
    include_columns: bool = True
    reembed_existing: bool = False
    batch_size: int = Field(default=100, ge=1, le=1000)

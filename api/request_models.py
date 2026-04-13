"""API 요청/응답 Pydantic 모델"""

from typing import Optional, List
from pydantic import BaseModel, Field


class LineageAnalyzeRequest(BaseModel):
    sqlContent: str
    fileName: str = ""
    dbms: str = "oracle"
    nameCaseOption: str = "original"


class SchemaTableInfo(BaseModel):
    name: str
    table_schema: str
    datasource: Optional[str] = ""
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


class AddRelationshipRequest(BaseModel):
    from_table: str
    from_schema: str = ""
    from_column: str
    to_table: str
    to_schema: str = ""
    to_column: str
    relationship_type: str = "FK_TO_TABLE"
    description: str = ""


class SemanticSearchRequest(BaseModel):
    query: str
    limit: int = 10


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
    batch_size: int = 100


class DWColumnInfo(BaseModel):
    name: str
    dtype: str = "VARCHAR"
    description: Optional[str] = None
    is_pk: bool = False
    is_fk: bool = False
    fk_target_table: Optional[str] = None


class DWDimensionInfo(BaseModel):
    name: str
    columns: List[DWColumnInfo] = []
    source_tables: List[str] = []


class DWFactTableInfo(BaseModel):
    name: str
    columns: List[DWColumnInfo] = []
    source_tables: List[str] = []


class DWStarSchemaRequest(BaseModel):
    cube_name: str
    db_name: str = "postgres"
    dw_schema: str = "dw"
    fact_table: DWFactTableInfo
    dimensions: List[DWDimensionInfo] = []
    create_embeddings: bool = True

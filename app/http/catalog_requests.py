"""API 요청/응답 Pydantic 모델"""

from typing import Annotated, Literal, Optional, List
from pydantic import BaseModel, Field


class LineageAnalyzeRequest(BaseModel):
    sqlContent: str = Field(min_length=1, max_length=2_000_000)
    fileName: str = Field(default="", max_length=1024)
    dbms: str = Field(default="oracle", max_length=32)
    nameCaseOption: Literal["uppercase", "lowercase", "original"] = "original"


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


class SemanticSearchRequest(BaseModel):
    query: str = Field(min_length=1, max_length=2_000)
    limit: int = Field(default=10, ge=1, le=100)


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


class DWColumnInfo(BaseModel):
    name: str
    dtype: str = "VARCHAR"
    description: Optional[str] = None
    is_pk: bool = False
    is_fk: bool = False
    fk_target_table: Optional[str] = None


class DWDimensionInfo(BaseModel):
    name: str
    columns: List[DWColumnInfo] = Field(default_factory=list, max_length=1000)
    source_tables: List[str] = Field(default_factory=list, max_length=1000)


class DWFactTableInfo(BaseModel):
    name: str
    columns: List[DWColumnInfo] = Field(default_factory=list, max_length=1000)
    source_tables: List[str] = Field(default_factory=list, max_length=1000)


class DWStarSchemaRequest(BaseModel):
    cube_name: str
    db_name: str = "postgres"
    dw_schema: str = "dw"
    fact_table: DWFactTableInfo
    dimensions: List[DWDimensionInfo] = Field(default_factory=list, max_length=1000)
    create_embeddings: bool = True


# =============================================================================
# 샘플 컨텍스트 (analyzer 1회 요청 API)
# =============================================================================

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


class MetadataEnrichmentRequest(BaseModel):
    datasource_name: str = Field(
        ..., min_length=1, max_length=128, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$"
    )

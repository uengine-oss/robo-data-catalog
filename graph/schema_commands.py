"""Write the small shared TABLE/COLUMN/FK product contract."""
from __future__ import annotations

from fastapi import HTTPException

from graph.database import CatalogGraphDatabase
from graph.scope import ANALYZER_OWNER, CATALOG_OWNER


async def create_schema_relationship(
    from_table: str,
    from_schema: str,
    from_column: str,
    to_table: str,
    to_schema: str,
    to_column: str,
    relationship_type: str = "FK",
    description: str = "",
) -> dict:
    """Create one column-qualified FK; parallel composite-column edges are valid."""
    if relationship_type != "FK":
        raise HTTPException(400, "지원하지 않는 관계입니다.")
    client = CatalogGraphDatabase()
    try:
        query = {
            "query": """
                MATCH (source:TABLE {name: $from_table})
                MATCH (target:TABLE {name: $to_table})
                WHERE source._owner = $owner AND target._owner = $owner
                  AND ($from_schema = '' OR source.schema = $from_schema)
                  AND ($to_schema = '' OR target.schema = $to_schema)
                MERGE (source)-[relationship:FK {
                    _owner: $relationship_owner,
                    from_column: $from_column,
                    to_column: $to_column
                }]->(target)
                SET relationship.description = $description
                RETURN source.name AS from_table, target.name AS to_table
            """,
            "parameters": {
                "from_table": from_table,
                "to_table": to_table,
                "from_column": from_column,
                "to_column": to_column,
                "description": description,
                "from_schema": from_schema,
                "to_schema": to_schema,
                "owner": ANALYZER_OWNER,
                "relationship_owner": CATALOG_OWNER,
            },
        }
        results = await client.execute_queries([query])
        if results and results[0]:
            return {"message": "관계를 생성했습니다.", "created": True}
        raise HTTPException(404, "테이블을 찾을 수 없습니다.")
    finally:
        await client.close()


async def delete_schema_relationship(
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
    from_schema: str = "",
    to_schema: str = "",
) -> dict:
    client = CatalogGraphDatabase()
    try:
        query = {
            "query": """
                MATCH (source:TABLE {name: $from_table})
                      -[relationship:FK]->
                      (target:TABLE {name: $to_table})
                WHERE source._owner = $owner AND target._owner = $owner
                  AND relationship._owner = $relationship_owner
                  AND ($from_schema = '' OR source.schema = $from_schema)
                  AND ($to_schema = '' OR target.schema = $to_schema)
                  AND relationship.from_column = $from_column
                  AND relationship.to_column = $to_column
                DELETE relationship
                RETURN count(*) AS deleted
            """,
            "parameters": {
                "from_table": from_table,
                "to_table": to_table,
                "from_column": from_column,
                "to_column": to_column,
                "from_schema": from_schema,
                "to_schema": to_schema,
                "owner": ANALYZER_OWNER,
                "relationship_owner": CATALOG_OWNER,
            },
        }
        results = await client.execute_queries([query])
        deleted = results[0][0]["deleted"] if results and results[0] else 0
        return {"message": f"{deleted}개 관계를 삭제했습니다.", "deleted": deleted}
    finally:
        await client.close()


async def update_table_description(
    table_name: str,
    schema: str,
    description: str | None,
) -> dict:
    client = CatalogGraphDatabase()
    try:
        results = await client.execute_queries([{
            "query": """
                MATCH (table:TABLE)
                WHERE table.name = $table_name AND table._owner = $owner
                  AND ($schema = '' OR table.schema = $schema OR table.schema IS NULL)
                SET table.description = $description
                RETURN table.name AS name
            """,
            "parameters": {
                "table_name": table_name,
                "schema": schema,
                "description": description or "",
                "owner": ANALYZER_OWNER,
            },
        }])
        if not results or not results[0]:
            raise HTTPException(404, "테이블을 찾을 수 없습니다.")
        return {"message": "테이블 설명을 수정했습니다.", "updated": True}
    finally:
        await client.close()


async def update_column_description(
    table_name: str,
    table_schema: str,
    column_name: str,
    description: str | None,
) -> dict:
    client = CatalogGraphDatabase()
    try:
        results = await client.execute_queries([{
            "query": """
                MATCH (table:TABLE)-[:HAS_COLUMN]->(column:COLUMN)
                WHERE table.name = $table_name
                  AND table._owner = $owner AND column._owner = $owner
                  AND ($table_schema = '' OR table.schema = $table_schema)
                  AND column.name = $column_name
                SET column.description = $description
                RETURN column.name AS name
            """,
            "parameters": {
                "table_name": table_name,
                "column_name": column_name,
                "description": description or "",
                "table_schema": table_schema,
                "owner": ANALYZER_OWNER,
            },
        }])
        if not results or not results[0]:
            raise HTTPException(404, "컬럼을 찾을 수 없습니다.")
        return {"message": "컬럼 설명을 수정했습니다.", "updated": True}
    finally:
        await client.close()

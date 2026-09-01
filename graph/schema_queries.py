"""Read the shared TABLE/COLUMN/FK and source-reference product graph."""
from __future__ import annotations

import logging
import re
from typing import Optional

from graph.database import CatalogGraphDatabase
from graph.scope import ANALYZER_OWNER, CATALOG_OWNER


logger = logging.getLogger(__name__)


def _filter_column_references(records: list[dict], column_name: Optional[str]) -> list[dict]:
    """Use the published routine source without inventing column-level graph evidence."""
    requested = (column_name or "").strip()
    if not requested:
        return records
    pattern = re.compile(
        rf"(?<![A-Za-z0-9_]){re.escape(requested)}(?![A-Za-z0-9_])",
        re.IGNORECASE,
    )
    return [
        record for record in records
        if pattern.search(str(record.get("code_text") or ""))
    ]


def metadata_enrichment_targets_query(datasource: str) -> dict:
    return {
        "query": """
            MATCH (table:TABLE)
            WHERE table._owner = $owner
              AND table.datasource = $datasource
              AND (table.description IS NULL OR table.description = '')
            OPTIONAL MATCH (table)-[:HAS_COLUMN]->(column:COLUMN {_owner: $owner})
            RETURN table.name AS table_name,
                   table.schema AS schema_name,
                   collect({
                     name: column.name,
                     data_type: column.data_type,
                     description: column.description
                   }) AS columns
        """,
        "parameters": {"datasource": datasource, "owner": ANALYZER_OWNER},
    }


async def fetch_schema_tables(
    search: Optional[str] = None,
    schema: Optional[str] = None,
    limit: int = 100,
) -> list:
    client = CatalogGraphDatabase()
    try:
        conditions = ["table._owner = $owner"]
        params: dict = {
            "limit": max(1, min(int(limit), 1000)),
            "owner": ANALYZER_OWNER,
        }
        if schema:
            conditions.append("table.schema = $schema")
            params["schema"] = schema
        if search:
            conditions.append(
                "(toLower(table.name) CONTAINS toLower($search) OR "
                "toLower(coalesce(table.description,'')) CONTAINS toLower($search) OR "
                "toLower(coalesce(table.summary,'')) CONTAINS toLower($search))"
            )
            params["search"] = search
        query = {
            "query": f"""
                MATCH (table:TABLE)
                WHERE {' AND '.join(conditions)}
                OPTIONAL MATCH (table)-[:HAS_COLUMN]->(column:COLUMN {{_owner: $owner}})
                WITH table, count(column) AS column_count
                RETURN table.name AS name,
                       table.schema AS schema,
                       table.datasource AS datasource,
                       table.logical_name AS logical_name,
                       table.description AS description,
                       table.summary AS summary,
                       column_count
                ORDER BY table.datasource, table.schema, table.name
                LIMIT $limit
            """,
            "parameters": params,
        }
        results = await client.execute_queries([query])
        return results[0] if results else []
    finally:
        await client.close()


async def fetch_table_columns(table_name: str, schema: str = "") -> list:
    client = CatalogGraphDatabase()
    try:
        params: dict = {
            "table_name": table_name,
            "owner": ANALYZER_OWNER,
        }
        name_match = "(table.name = $table_name OR table._id ENDS WITH $table_name)"
        if schema and schema.lower() != "public":
            where = f"{name_match} AND table.schema = $schema"
            params["schema"] = schema
        else:
            where = name_match
        query = {
            "query": f"""
                MATCH (table:TABLE)-[:HAS_COLUMN]->(column:COLUMN)
                WHERE table._owner = $owner AND column._owner = $owner
                  AND ({where})
                RETURN column.name AS name,
                       table.name AS table_name,
                       column.data_type AS data_type,
                       coalesce(column.nullable, true) AS nullable,
                       coalesce(column.logical_name, '') AS logical_name,
                       coalesce(column.description, '') AS description,
                       coalesce(column.summary, '') AS summary
                ORDER BY column.name
            """,
            "parameters": params,
        }
        results = await client.execute_queries([query])
        rows = results[0] if results else []
        logger.info("schema columns read: table=%s schema=%s rows=%d", table_name, schema, len(rows))
        return rows
    finally:
        await client.close()


async def fetch_table_references(
    table_name: str,
    schema: str = "",
    column_name: Optional[str] = None,
) -> dict:
    """Return routines/statements that directly READ or WRITE the table."""
    del schema
    client = CatalogGraphDatabase()
    try:
        query = {
            "query": """
                MATCH (source)-[access:READS|WRITES]->(table:TABLE)
                WHERE source._owner = $owner AND table._owner = $owner
                  AND (table.name = $table_name OR table._id ENDS WITH $table_name)
                OPTIONAL MATCH (routine)-[:PARENT_OF*0..]->(source)
                WHERE routine._owner = $owner
                  AND any(label IN labels(routine)
                          WHERE label IN ['FUNCTION','PROCEDURE','METHOD','TRIGGER'])
                WITH source, access, coalesce(routine, source) AS owner_node
                RETURN DISTINCT
                    coalesce(owner_node.name, owner_node._id) AS procedure_name,
                    head(labels(owner_node)) AS procedure_type,
                    type(access) AS access_type,
                    owner_node.start_line AS start_line,
                    owner_node.end_line AS end_line,
                    head(labels(source)) AS statement_type,
                    source.start_line AS statement_line,
                    source.start_line AS evidence_line,
                    owner_node.file_path AS file_path,
                    owner_node.code_text AS code_text
                ORDER BY procedure_name, statement_line
            """,
            "parameters": {"table_name": table_name, "owner": ANALYZER_OWNER},
        }
        results = await client.execute_queries([query])
        records = _filter_column_references(results[0] if results else [], column_name)
        return {"references": records}
    finally:
        await client.close()


async def fetch_procedure_statements(
    procedure_name: str,
    file_path: Optional[str] = None,
) -> list:
    client = CatalogGraphDatabase()
    try:
        file_condition = " AND routine.file_path = $file_path" if file_path else ""
        parameters = {
            "procedure_name": procedure_name,
            "owner": ANALYZER_OWNER,
        }
        if file_path:
            parameters["file_path"] = file_path
        query = {
            "query": f"""
                MATCH (routine)-[:PARENT_OF*]->(statement)
                WHERE routine._owner = $owner AND statement._owner = $owner
                  AND routine.name = $procedure_name
                  {file_condition}
                RETURN statement.start_line AS start_line,
                       statement.end_line AS end_line,
                       head(labels(statement)) AS statement_type,
                       statement.summary AS summary
                ORDER BY statement.start_line
            """,
            "parameters": parameters,
        }
        results = await client.execute_queries([query])
        return results[0] if results else []
    finally:
        await client.close()


async def fetch_schema_relationships() -> list:
    client = CatalogGraphDatabase()
    try:
        query = """
            MATCH (source:TABLE)-[relationship:FK]->(target:TABLE)
            WHERE source._owner = $owner AND target._owner = $owner
              AND relationship._owner IN $relationship_owners
            RETURN source.name AS from_table,
                   source.schema AS from_schema,
                   relationship.from_column AS from_column,
                   target.name AS to_table,
                   target.schema AS to_schema,
                   relationship.to_column AS to_column,
                   'FK' AS relationship_type,
                   coalesce(relationship.description, '') AS description
            ORDER BY source.name, target.name, from_column, to_column
        """
        results = await client.execute_queries([{
            "query": query,
            "parameters": {
                "owner": ANALYZER_OWNER,
                "relationship_owners": [ANALYZER_OWNER, CATALOG_OWNER],
            },
        }])
        return results[0] if results else []
    finally:
        await client.close()

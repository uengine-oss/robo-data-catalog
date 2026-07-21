"""Per-request Neo4j connection override boundary."""
from fastapi import HTTPException, Request

from graph.connection import RequestGraphConnection, set_request_graph_connection
from shared.config.settings import CATALOG_SETTINGS


async def apply_neo4j_override(request: Request) -> None:
    try:
        connection = RequestGraphConnection.from_headers(request.headers)
    except ValueError as error:
        raise HTTPException(400, "Invalid Neo4j override headers") from error
    if connection is not None and not CATALOG_SETTINGS.allow_neo4j_header_override:
        raise HTTPException(403, "Neo4j header override is disabled")
    set_request_graph_connection(connection)

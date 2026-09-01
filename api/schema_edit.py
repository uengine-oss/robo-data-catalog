"""Catalog schema write routes."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request

from api.errors import error_body as _error_body
from api.graph_connection import apply_neo4j_override
from contracts import (
    AddRelationshipRequest,
    ColumnDescriptionUpdateRequest,
    TableDescriptionUpdateRequest,
)
from graph import schema_commands
from shared.config.settings import CATALOG_SETTINGS


router = APIRouter(
    prefix=CATALOG_SETTINGS.api_prefix,
    dependencies=[Depends(apply_neo4j_override)],
)
logger = logging.getLogger(__name__)


@router.post("/schema/relationships")
async def add_schema_relationship(body: AddRelationshipRequest):
    try:
        return await schema_commands.create_schema_relationship(**body.model_dump())
    except HTTPException:
        raise
    except Exception as error:
        logger.error("schema relationship create failed: %s", type(error).__name__)
        raise HTTPException(500, _error_body(error))


@router.delete("/schema/relationships")
async def remove_schema_relationship(
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
    from_schema: str = "",
    to_schema: str = "",
):
    try:
        return await schema_commands.delete_schema_relationship(
            from_table, from_column, to_table, to_column, from_schema, to_schema,
        )
    except Exception as error:
        logger.error("schema relationship delete failed: %s", type(error).__name__)
        raise HTTPException(500, _error_body(error))


def _require_write_key(request: Request) -> None:
    if not request.headers.get("X-API-Key"):
        raise HTTPException(400, "X-API-Key 헤더가 필요합니다.")


@router.put("/schema/tables/{table_name}/description")
async def update_table_description(
    request: Request,
    table_name: str,
    body: TableDescriptionUpdateRequest,
):
    _require_write_key(request)
    try:
        return await schema_commands.update_table_description(
            table_name=table_name,
            schema=body.table_schema,
            description=body.description,
        )
    except HTTPException:
        raise
    except Exception as error:
        logger.error("table description update failed: %s", type(error).__name__)
        raise HTTPException(500, _error_body(error))


@router.put("/schema/tables/{table_name}/columns/{column_name}/description")
async def update_column_description(
    request: Request,
    table_name: str,
    column_name: str,
    body: ColumnDescriptionUpdateRequest,
):
    _require_write_key(request)
    try:
        return await schema_commands.update_column_description(
            table_name=table_name,
            table_schema=body.table_schema,
            column_name=column_name,
            description=body.description,
        )
    except HTTPException:
        raise
    except Exception as error:
        logger.error("column description update failed: %s", type(error).__name__)
        raise HTTPException(500, _error_body(error))

"""Metadata description and foreign-key enrichment orchestration."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

import aiohttp

from enrichment.description import TableDescriptionEnricher
from enrichment.events import ndjson as _ndjson
from enrichment.foreign_keys import ForeignKeyInference
from integrations.data_fabric import DataFabricQueryGateway
from integrations.llm import create_metadata_llm_client
from graph import schema_queries as schema_metadata_queries
from graph.database import CatalogGraphDatabase


logger = logging.getLogger(__name__)


@dataclass
class _EnrichmentProgress:
    description_enriched: int = 0
    fk_persisted: int = 0
    error_count: int = 0
    schemas: set[str] = field(default_factory=set)


async def _description_events(
    *,
    datasource_name: str,
    tables: list[dict[str, Any]],
    session: aiohttp.ClientSession,
    gateway: DataFabricQueryGateway,
    enricher: TableDescriptionEnricher,
    progress: _EnrichmentProgress,
) -> AsyncIterator[bytes]:
    """Enrich tables independently while making every failure stream-visible."""
    yield _ndjson({"event": "start", "phase": "description", "total": len(tables)})

    for index, table in enumerate(tables, start=1):
        table_name = table["table_name"]
        schema_name = table["schema_name"] or "public"
        progress.schemas.add(schema_name)
        persisted = False
        try:
            sample = await gateway.fetch_rows(
                session,
                DataFabricQueryGateway.sample_sql(f"{schema_name}.{table_name}", 10),
                max_rows=10,
            )
            if sample:
                descriptions = await enricher.generate(
                    table_name, schema_name, sample, table["columns"]
                )
                if descriptions:
                    table_updates, _ = await enricher.persist(
                        datasource_name, table_name, schema_name, descriptions
                    )
                    progress.description_enriched += table_updates
                    persisted = bool(table_updates)
        except Exception as exc:
            progress.error_count += 1
            error_type = type(exc).__name__
            logger.warning(
                "[ENRICH] table=%s failed | error_type=%s",
                table_name,
                error_type,
            )
            yield _ndjson(
                {
                    "event": "error",
                    "phase": "description",
                    "table": table_name,
                    "error_type": error_type,
                    "message": "Table metadata enrichment failed",
                }
            )

        yield _ndjson(
            {
                "event": "table_done",
                "i": index,
                "total": len(tables),
                "table": table_name,
                "schema": schema_name,
                "description_persisted": persisted,
            }
        )

    yield _ndjson(
        {
            "event": "phase_done",
            "phase": "description",
            "enriched": progress.description_enriched,
            "errors": progress.error_count,
        }
    )


async def _foreign_key_events(
    *,
    client: CatalogGraphDatabase,
    session: aiohttp.ClientSession,
    gateway: DataFabricQueryGateway,
    progress: _EnrichmentProgress,
) -> AsyncIterator[bytes]:
    inference = ForeignKeyInference(neo4j_client=client, db_client=gateway)
    for schema in sorted(progress.schemas):
        async for event in inference.infer_and_persist(session, schema):
            if event.get("event") == "fk_persisted":
                progress.fk_persisted += int(event.get("fk_to_column", 0))
            elif event.get("event") == "fk_error":
                progress.error_count += 1
            yield _ndjson(event)


async def enrichment_events(*, datasource_name: str, api_key: str, fabric_url: str):
    """Stream a complete enrichment run with explicit success/partial semantics."""
    if not api_key:
        yield _ndjson({"event": "skip", "reason": "API key missing; enrichment skipped"})
        return
    if not fabric_url:
        yield _ndjson(
            {"event": "skip", "reason": "DATA_FABRIC_URL missing; enrichment skipped"}
        )
        return

    client: CatalogGraphDatabase | None = None
    try:
        client = CatalogGraphDatabase()
        gateway = DataFabricQueryGateway(base_url=fabric_url, datasource=datasource_name)
        enricher = TableDescriptionEnricher(
            client=client,
            openai_client=create_metadata_llm_client(api_key),
        )
        query = schema_metadata_queries.metadata_enrichment_targets_query(datasource_name)
        results = await client.execute_queries([query])
        tables = results[0] if results else []
        if not tables:
            yield _ndjson({"event": "skip", "reason": "No tables eligible for enrichment"})
            return

        progress = _EnrichmentProgress()
        async with aiohttp.ClientSession() as session:
            if not await gateway.check_available(session):
                yield _ndjson(
                    {
                        "event": "error",
                        "phase": "preflight",
                        "error_type": "DataFabricUnavailableError",
                        "message": "Data Fabric is unavailable or disconnected",
                    }
                )
                return

            async for event in _description_events(
                datasource_name=datasource_name,
                tables=tables,
                session=session,
                gateway=gateway,
                enricher=enricher,
                progress=progress,
            ):
                yield event

            async for event in _foreign_key_events(
                client=client,
                session=session,
                gateway=gateway,
                progress=progress,
            ):
                yield event

        yield _ndjson(
            {
                "event": "complete",
                "status": "success" if progress.error_count == 0 else "partial",
                "description_enriched": progress.description_enriched,
                "fk_persisted": progress.fk_persisted,
                "error_count": progress.error_count,
            }
        )
    except Exception as exc:
        error_type = type(exc).__name__
        logger.error(
            "[API] metadata enrichment failed | error_type=%s",
            error_type,
            exc_info=True,
        )
        yield _ndjson(
            {
                "event": "error",
                "message": "Metadata enrichment failed",
                "error_type": error_type,
            }
        )
    finally:
        if client is not None:
            await client.close()

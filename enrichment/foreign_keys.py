"""Infer probable foreign keys and publish one table-level FK relation."""
from __future__ import annotations

import logging
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

import aiohttp
from rapidfuzz import fuzz

from graph.database import CatalogGraphDatabase
from graph.scope import ANALYZER_OWNER, CATALOG_OWNER
from integrations.data_fabric import DataFabricQueryGateway
from shared.observability.logger import log_catalog_operation


logger = logging.getLogger(__name__)
FK_FUNCTION_NAME = "public.infer_fk_candidates"

FK_QUERY = """
UNWIND $items AS item
MATCH (source:TABLE {schema: item.src_schema, name: item.src_table})
MATCH (target:TABLE {schema: item.tgt_schema, name: item.tgt_table})
WHERE source._owner = $analyzer_owner AND target._owner = $analyzer_owner
MERGE (source)-[relationship:FK {
  _owner: $catalog_owner,
  from_column: item.src_column,
  to_column: item.tgt_column
}]->(target)
  ON CREATE SET relationship.description = 'inferred',
                relationship.confidence = item.confidence,
                relationship.overlap_ratio = item.overlap_ratio,
                relationship.name_similarity = item.name_similarity
  ON MATCH SET relationship.confidence = item.confidence,
               relationship.overlap_ratio = item.overlap_ratio,
               relationship.name_similarity = item.name_similarity
RETURN count(relationship) AS persisted
"""

_W_NAME = 0.5
_W_OVERLAP = 0.2
_W_DISTINCT = 0.3
_DISTINCT_SATURATION = 5


class ForeignKeySimilarityFunctionMissingError(RuntimeError):
    """The source database does not provide the candidate function."""


class ForeignKeyInference:
    def __init__(
        self,
        neo4j_client: CatalogGraphDatabase,
        db_client: DataFabricQueryGateway,
        overlap_threshold: float = 0.8,
        min_src_distinct: int = 2,
        confidence_threshold: float = 0.85,
    ):
        self._neo4j = neo4j_client
        self._db = db_client
        self._overlap_threshold = overlap_threshold
        self._min_src_distinct = min_src_distinct
        self._confidence_threshold = confidence_threshold

    async def infer_and_persist(
        self,
        session: aiohttp.ClientSession,
        schema: str,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        yield {
            "event": "fk_query_start",
            "schema": schema,
            "overlap_threshold": self._overlap_threshold,
            "min_src_distinct": self._min_src_distinct,
            "confidence_threshold": self._confidence_threshold,
        }
        candidates = await self._fetch_fk_candidates(session, schema)
        if candidates is None:
            yield {
                "event": "fk_error",
                "message": f"FK candidate function is unavailable: {FK_FUNCTION_NAME}",
            }
            return
        yield {"event": "fk_query_done", "candidate_count": len(candidates)}
        if not candidates:
            return

        accepted, rejected = self._apply_confidence_filter(candidates)
        yield {
            "event": "fk_filter_applied",
            "before": len(candidates),
            "after": len(accepted),
            "confidence_threshold": self._confidence_threshold,
            "rejected_examples": [{
                "src": f"{row['src_table']}.{row['src_column']}",
                "tgt": f"{row['tgt_table']}.{row['tgt_column']}",
                "confidence": row["confidence"],
                "name_similarity": row["name_similarity"],
                "src_distinct": row["src_distinct"],
            } for row in rejected[:5]],
        }
        persisted = await self._persist_relationships(accepted) if accepted else 0
        yield {"event": "fk_persisted", "fk": persisted}

    async def _fetch_fk_candidates(
        self,
        session: aiohttp.ClientSession,
        schema: str,
    ) -> Optional[List[Dict[str, Any]]]:
        sql = (
            f"SELECT * FROM {FK_FUNCTION_NAME}("
            f"'{schema}', {self._overlap_threshold}, {self._min_src_distinct})"
        )
        try:
            rows = await self._db.fetch_rows(session, sql)
        except Exception as error:
            log_catalog_operation(
                "FK_INFER", "FETCH_ERROR", type(error).__name__, logging.WARNING,
            )
            return None
        return rows or []

    @staticmethod
    def _compute_confidence(candidate: Dict[str, Any]) -> float:
        name = float(candidate["name_similarity"]) / 100.0
        overlap = float(candidate["overlap_ratio"])
        distinct = min(
            1.0,
            float(candidate["src_distinct"]) / _DISTINCT_SATURATION,
        )
        return round(name * _W_NAME + overlap * _W_OVERLAP + distinct * _W_DISTINCT, 3)

    def _apply_confidence_filter(
        self,
        candidates: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        accepted: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []
        for candidate in candidates:
            candidate["name_similarity"] = int(fuzz.WRatio(
                candidate["src_column"], candidate["tgt_column"],
            ))
            candidate["confidence"] = self._compute_confidence(candidate)
            (accepted if candidate["confidence"] >= self._confidence_threshold else rejected).append(
                candidate
            )
        accepted.sort(key=lambda item: -item["confidence"])
        rejected.sort(key=lambda item: -item["confidence"])
        return accepted, rejected

    async def _persist_relationships(self, items: List[Dict[str, Any]]) -> int:
        try:
            results = await self._neo4j.execute_queries([{
                "query": FK_QUERY,
                "parameters": {
                    "items": items,
                    "analyzer_owner": ANALYZER_OWNER,
                    "catalog_owner": CATALOG_OWNER,
                },
            }])
            rows = results[0] if results else []
            return int(rows[0]["persisted"]) if rows else 0
        except Exception as error:
            log_catalog_operation(
                "FK_INFER", "PERSIST_ERROR", type(error).__name__, logging.ERROR,
            )
            raise

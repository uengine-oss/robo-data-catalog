"""FK 추론 서비스 — 값 overlap + 컬럼명 유사도 + distinct 충분도를 통합한 confidence score 기반 자동 검출.

알고리즘 (3단계):

  Stage 1 — 값 overlap 후보 추출 (PostgreSQL 내부)
    information_schema 기반 dtype-호환 cross-table 쌍 enumerate.
    각 쌍의 EXISTS 기반 overlap 비율 >= overlap_threshold 인 후보만 반환.

  Stage 2 — 통합 confidence score 계산 + 임계 필터 (Python)
    각 후보에 대해 3가지 신호 가중평균:
        confidence = name_similarity × W_NAME
                   + overlap_ratio   × W_OVERLAP
                   + distinct_factor × W_DISTINCT
    단일 임계 (confidence_threshold) 로 채택/거부 결정.
    이 한 점수가 false positive 의 모든 패턴을 덮음:
      - 작은 distinct 우연 (distinct_factor 페널티)
      - 다른 의미 컬럼 우연 (name_similarity 낮음)
      - 부분 일치 우연 (가중평균이 0.80 못 넘김)

  Stage 3 — Neo4j 영속화
    (:Column)-[:FK_TO_COLUMN]->(:Column) + (:Table)-[:FK_TO_TABLE]->(:Table)
    각 관계에 confidence + 모든 신호값 저장.

confidence 점수 산식 검증 사례 (RWIS):
  BNB_CODE↔BNB_CODE (name=100, overlap=100, distinct=9) → 1.00  ✓ 확실
  BR_GUBUN↔BR_GUBUN (name=100, overlap=100, distinct=2) → 0.82  ✓ 이름 일치 보너스
  BNB_CODE↔BNB_CD   (name=86,  overlap=100, distinct=9) → 0.93  ✓ 미묘한 차이도 잡음
  GNJ_CODE↔GT_CODE  (name=80,  overlap=100, distinct=3) → 0.78  ✗ 부분일치 + 작은 distinct
  CNT↔TAG_UNIT      (name=60,  overlap=100, distinct=2) → 0.62  ✗ 무관 컬럼

전제:
- 대상 PG 에 public.infer_fk_candidates(schema, threshold, min_distinct) 함수 사전 설치.
  설치 SQL: scripts/install_fk_function.sql
- MindsDB datasource 등록 (text2sql 백엔드 경유).

스트리밍:
- async generator 가 dict 이벤트 yield. 호출자는 NDJSON 으로 변환해 전송.
"""

from __future__ import annotations

import logging
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

import aiohttp
from rapidfuzz import fuzz

from client.neo4j_client import Neo4jClient
from service.text2sql_client import Text2SqlClient
from util.logger import log_process

logger = logging.getLogger(__name__)


FK_FUNCTION_NAME = "public.infer_fk_candidates"

# Cypher: (:Column)-[:FK_TO_COLUMN]->(:Column)
FK_TO_COLUMN_QUERY = """
UNWIND $items AS item
MATCH (c1:Column)
  WHERE toLower(c1.fqn) = toLower(item.src_schema + '.' + item.src_table + '.' + item.src_column)
MATCH (c2:Column)
  WHERE toLower(c2.fqn) = toLower(item.tgt_schema + '.' + item.tgt_table + '.' + item.tgt_column)
MERGE (c1)-[r:FK_TO_COLUMN]->(c2)
  ON CREATE SET r.source = 'inferred',
                r.confidence = item.confidence,
                r.overlap_ratio = item.overlap_ratio,
                r.src_distinct = item.src_distinct,
                r.overlap_count = item.overlap_count,
                r.name_similarity = item.name_similarity,
                r.dtype_family = item.dtype_family,
                r.created_at = datetime()
  ON MATCH  SET r.confidence = item.confidence,
                r.overlap_ratio = item.overlap_ratio,
                r.src_distinct = item.src_distinct,
                r.overlap_count = item.overlap_count,
                r.name_similarity = item.name_similarity,
                r.updated_at = datetime()
RETURN count(r) AS persisted
"""

# 테이블 단위 FK_TO_TABLE
FK_TO_TABLE_QUERY = """
UNWIND $items AS item
MATCH (t1:Table {schema: item.src_schema, name: item.src_table})
MATCH (t2:Table {schema: item.tgt_schema, name: item.tgt_table})
MERGE (t1)-[r:FK_TO_TABLE {sourceColumn: item.src_column, targetColumn: item.tgt_column}]->(t2)
  ON CREATE SET r.source = 'inferred',
                r.type = 'many_to_one',
                r.confidence = item.confidence,
                r.overlap_ratio = item.overlap_ratio,
                r.name_similarity = item.name_similarity,
                r.created_at = datetime()
  ON MATCH  SET r.confidence = item.confidence,
                r.overlap_ratio = item.overlap_ratio,
                r.name_similarity = item.name_similarity,
                r.updated_at = datetime()
RETURN count(r) AS persisted
"""


# Confidence 가중치 (sum = 1.0)
_W_NAME = 0.5      # 컬럼명 유사도가 가장 중요한 의미 신호
_W_OVERLAP = 0.2   # 값 겹침 비율 (이미 1차에서 ≥0.8 보장)
_W_DISTINCT = 0.3  # distinct 충분도 (작은 enum 우연 매칭 방어)
_DISTINCT_SATURATION = 5  # distinct ≥ 이 값이면 만점


class FkFunctionMissingError(RuntimeError):
    """대상 PG 에 infer_fk_candidates 함수가 없을 때."""


class FkInferenceService:
    """값 overlap + 컬럼명 유사도 + distinct 충분도를 통합한 FK 추론 + Neo4j 영속화."""

    def __init__(
        self,
        neo4j_client: Neo4jClient,
        text2sql_client: Text2SqlClient,
        overlap_threshold: float = 0.8,
        min_src_distinct: int = 2,
        confidence_threshold: float = 0.85,
    ):
        """
        Args:
            overlap_threshold: PG 함수의 값 overlap 비율 임계 (0~1).
                Stage 1 에서 미달은 후보 진입 자체 차단.
            min_src_distinct: PG 함수의 source distinct 최소 개수.
                극단 케이스(distinct=1) 만 1차 차단. 본격적인 distinct 페널티는 confidence 가 처리.
            confidence_threshold: Stage 2 통합 점수 임계 (0~1).
                기본 0.85 — 진짜 FK 거의 다 통과 + 의심 케이스 보수적 거름.
                0.80 까지 낮추면 distinct 작은 동명 컬럼(BR_GUBUN 등)도 채택, 정확도 ↓.
        """
        self._neo4j = neo4j_client
        self._text2sql = text2sql_client
        self._overlap_threshold = overlap_threshold
        self._min_src_distinct = min_src_distinct
        self._confidence_threshold = confidence_threshold

    async def infer_and_persist(
        self,
        session: aiohttp.ClientSession,
        schema: str,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """FK 후보 추론 + Neo4j 영속화.

        yield 이벤트 (dict):
          - {"event": "fk_query_start", "schema", "overlap_threshold", "min_src_distinct", "confidence_threshold"}
          - {"event": "fk_query_done",  "candidate_count"}
          - {"event": "fk_filter_applied", "before", "after", "confidence_threshold", "rejected_examples"}
          - {"event": "fk_persisted", "fk_to_column", "fk_to_table"}
          - {"event": "fk_error", "message"}
        """
        yield {
            "event": "fk_query_start",
            "schema": schema,
            "overlap_threshold": self._overlap_threshold,
            "min_src_distinct": self._min_src_distinct,
            "confidence_threshold": self._confidence_threshold,
        }

        # Stage 1 — 값 overlap 후보
        candidates = await self._fetch_fk_candidates(session, schema)
        if candidates is None:
            yield {
                "event": "fk_error",
                "message": (
                    f"FK 추론 함수({FK_FUNCTION_NAME}) 호출 실패. "
                    "대상 PG 에 함수가 설치되어 있는지 확인 (scripts/install_fk_function.sql)."
                ),
            }
            return
        yield {"event": "fk_query_done", "candidate_count": len(candidates)}
        if not candidates:
            return

        # Stage 2 — confidence 계산 + 통합 임계 필터
        accepted, rejected = self._apply_confidence_filter(candidates)

        yield {
            "event": "fk_filter_applied",
            "before": len(candidates),
            "after": len(accepted),
            "confidence_threshold": self._confidence_threshold,
            "rejected_examples": [
                {
                    "src": f"{r['src_table']}.{r['src_column']}",
                    "tgt": f"{r['tgt_table']}.{r['tgt_column']}",
                    "confidence": r["confidence"],
                    "name_similarity": r["name_similarity"],
                    "src_distinct": r["src_distinct"],
                }
                for r in rejected[:5]
            ],
        }

        if not accepted:
            yield {"event": "fk_persisted", "fk_to_column": 0, "fk_to_table": 0}
            return

        # Stage 3 — Neo4j 영속화
        col_persisted = await self._persist_relationships(FK_TO_COLUMN_QUERY, accepted)
        tbl_persisted = await self._persist_relationships(FK_TO_TABLE_QUERY, accepted)

        yield {
            "event": "fk_persisted",
            "fk_to_column": col_persisted,
            "fk_to_table": tbl_persisted,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    async def _fetch_fk_candidates(
        self,
        session: aiohttp.ClientSession,
        schema: str,
    ) -> Optional[List[Dict[str, Any]]]:
        """PG 함수 호출로 FK 후보 일괄 조회."""
        sql = (
            f"SELECT * FROM {FK_FUNCTION_NAME}("
            f"'{schema}', {self._overlap_threshold}, {self._min_src_distinct}"
            f")"
        )
        try:
            rows = await self._text2sql.fetch_rows(session, sql)
        except Exception as e:
            log_process("FK_INFER", "FETCH_ERROR", f"{type(e).__name__}: {e}", logging.WARNING)
            return None
        return rows or []

    @staticmethod
    def _compute_confidence(c: Dict[str, Any]) -> float:
        """3가지 신호 가중평균 → 0.0~1.0 신뢰도 점수.

        - name_similarity (0~100) → 정규화 0~1
        - overlap_ratio (0~1) 그대로
        - src_distinct → distinct_factor = min(1, src_distinct/5)
        """
        name = float(c["name_similarity"]) / 100.0
        overlap = float(c["overlap_ratio"])
        distinct_factor = min(1.0, float(c["src_distinct"]) / _DISTINCT_SATURATION)
        return round(
            name * _W_NAME + overlap * _W_OVERLAP + distinct_factor * _W_DISTINCT,
            3,
        )

    def _apply_confidence_filter(
        self,
        candidates: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """각 후보에 name_similarity, confidence 부여 후 임계 통과 분리."""
        accepted: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []
        for c in candidates:
            c["name_similarity"] = int(fuzz.WRatio(c["src_column"], c["tgt_column"]))
            c["confidence"] = self._compute_confidence(c)
            if c["confidence"] >= self._confidence_threshold:
                accepted.append(c)
            else:
                rejected.append(c)
        # 신뢰도 내림차순 정렬 (영속화 순서 안정)
        accepted.sort(key=lambda x: -x["confidence"])
        rejected.sort(key=lambda x: -x["confidence"])
        return accepted, rejected

    async def _persist_relationships(
        self,
        cypher: str,
        items: List[Dict[str, Any]],
    ) -> int:
        """배치 MERGE 로 Neo4j 관계 생성. 생성/갱신 행 수 반환."""
        if not items:
            return 0
        try:
            results = await self._neo4j.execute_queries(
                [{"query": cypher, "parameters": {"items": items}}]
            )
            rows = results[0] if results else []
            return int(rows[0]["persisted"]) if rows else 0
        except Exception as e:
            log_process("FK_INFER", "PERSIST_ERROR", f"{type(e).__name__}: {e}", logging.ERROR)
            raise

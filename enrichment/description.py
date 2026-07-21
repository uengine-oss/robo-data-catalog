"""테이블/컬럼 description 생성 서비스

샘플 데이터를 기반으로 LLM이 테이블·컬럼 설명을 생성하고 Neo4j에 저장.

책임:
- 샘플 데이터 + 컬럼 정보 → LLM 프롬프트 구성
- LLM 호출 → JSON 파싱
- description 없는 노드에만 UPDATE (기존 설명 보존)
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from openai import AsyncOpenAI

from shared.config.settings import CATALOG_SETTINGS
from graph.database import CatalogGraphDatabase
from graph.scope import ANALYSIS_GRAPH_OWNER
from shared.observability.logger import log_catalog_operation

logger = logging.getLogger(__name__)

DESCRIPTION_SOURCE = "sample_data_inference"
SAMPLE_DATA_LIMIT = 10  # LLM 프롬프트용 샘플 데이터 최대 행 수


class MetadataResponseError(RuntimeError):
    """The provider returned no usable structured metadata response."""


def _updated_count(results: list) -> int:
    if not results or not results[0]:
        return 0
    return int(results[0][0].get("updated", 0))


class TableDescriptionEnricher:
    """샘플 데이터 기반 테이블/컬럼 설명 생성·저장."""

    def __init__(self, client: CatalogGraphDatabase, openai_client: AsyncOpenAI):
        self.client = client
        self.openai_client = openai_client

    async def generate(
        self,
        table_name: str,
        schema_name: str,
        sample_data: List[Dict[str, Any]],
        columns_info: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """샘플 데이터 + 컬럼 → LLM description JSON."""
        prompt = self._build_prompt(table_name, schema_name, sample_data, columns_info)

        try:
            response = await self.openai_client.chat.completions.create(
                model=CATALOG_SETTINGS.llm.model or "gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_completion_tokens=CATALOG_SETTINGS.llm.max_completion_tokens,
                response_format={"type": "json_object"},
            )
            choice = response.choices[0]
            content = choice.message.content
            if not content:
                raise MetadataResponseError(
                    f"provider returned empty content (finish_reason={choice.finish_reason})"
                )
            result = json.loads(content)
            if not isinstance(result, dict):
                raise MetadataResponseError("provider response must be a JSON object")
            log_catalog_operation(
                "METADATA",
                "LLM_OK",
                f"설명 생성 완료: {table_name} (테이블: {bool(result.get('table_description'))}, "
                f"컬럼: {len(result.get('column_descriptions', {}))}개)",
                logging.INFO,
            )
            return result
        except Exception as e:
            logger.exception(
                "Metadata description generation failed | table=%s | error_type=%s",
                table_name,
                type(e).__name__,
            )
            raise

    async def persist(
        self,
        datasource: str,
        table_name: str,
        schema_name: str,
        descriptions: Dict[str, Any],
    ) -> Tuple[int, int]:
        """Neo4j에 description 업데이트. Returns (테이블 업데이트수, 컬럼 업데이트수)."""
        table_updated = 0
        columns_updated = 0

        table_desc = descriptions.get("table_description", "")
        if table_desc:
            table_updated = await self._update_table_description(
                datasource, table_name, schema_name, table_desc
            )

        column_descs = descriptions.get("column_descriptions", {})
        if column_descs:
            columns_updated = await self._update_column_descriptions(
                datasource, table_name, schema_name, column_descs
            )
        return table_updated, columns_updated

    # -------------------------------------------------------------------
    # 내부
    # -------------------------------------------------------------------

    @staticmethod
    def _build_prompt(
        table_name: str,
        schema_name: str,
        sample_data: List[Dict[str, Any]],
        columns_info: List[Dict[str, Any]],
    ) -> str:
        sample_rows = sample_data[:SAMPLE_DATA_LIMIT]
        sample_str = "\n".join(str(row) for row in sample_rows)

        # 컬럼 정보: 이름·dtype·기존 DDL 주석(있으면) 함께 표시
        col_lines = []
        any_ddl_comment = False
        for col in columns_info:
            name = col.get("column_name") or col.get("name") or ""
            dtype = col.get("data_type") or col.get("dtype") or "unknown"
            existing = (col.get("description") or "").strip()
            if existing and existing.lower() not in {"n/a", "none"}:
                col_lines.append(f"- {name} ({dtype}) | DDL 주석: {existing}")
                any_ddl_comment = True
            else:
                col_lines.append(f"- {name} ({dtype})")
        columns_str = "\n".join(col_lines)

        # 컬럼별 처리 방침 (확정 vs 추정)
        ddl_hint = (
            "\n\n## 컬럼 description 작성 규칙\n"
            "\n"
            "### A. DDL 주석이 있는 컬럼 (확정)\n"
            "- 주석 내용은 원본 데이터베이스의 정답입니다. 그대로 사용하거나 자연스럽게 보강.\n"
            "- 단정형 한 줄로 짧고 명확하게.\n"
            "- 임의 추론·추가 해석 금지.\n"
            "- 입력에 실제로 제공된 DDL 주석 밖의 도메인 의미를 덧붙이지 말 것.\n"
            "\n"
            "### B. DDL 주석이 없는 컬럼 (추정)\n"
            "- 절대로 한 줄짜리 단정형(\"○○ 코드\", \"○○ 번호\")로 끝내지 말 것.\n"
            "- 반드시 \"[추정]\" 으로 시작하고 아래 4가지를 모두 포함하여 3~5줄로 상세히 작성:\n"
            "    1) 단정할 수 없음을 명시 (DDL 주석 없음 또는 의미 정보 부족)\n"
            "    2) 컬럼명/접두사/접미사/dtype/샘플 값에서 도출한 추정 근거\n"
            "    3) 가능한 대체 해석 (있다면 1~2개 나열)\n"
            "    4) 정확 의미 확인을 위한 권장 행동 (운영 DBMS 주석/도메인 전문가/관련 SP 코드 확인)\n"
            "- 같은 테이블의 다른 DDL 주석과 샘플 값은 입력에 실제로 나타난 내용만 근거로 사용.\n"
            "- 컬럼명·테이블명의 약어는 여러 뜻일 수 있으므로 사전 지식으로 자동 확장하지 말고,\n"
            "  입력 증거가 없으면 가능한 해석을 대안으로만 제시.\n"
            "- 예: 주석 없는 status_code와 샘플 값 A/I가 제공된 경우\n"
            "    description: \"[추정] DDL 주석이 없어 정확한 의미를 단정할 수 없습니다. \"\n"
            "        \"문자열 컬럼명 suffix '_code'와 샘플 A/I를 근거로 상태 또는 분류 코드로 추정됩니다. \"\n"
            "        \"활성 여부나 처리 단계 등 다른 해석도 가능합니다. \"\n"
            "        \"운영 DBMS 주석이나 관련 코드 정의 확인을 권장합니다.\"\n"
            "\n"
            "### 테이블 description\n"
            "- 입력에 제공된 컬럼 DDL 주석과 샘플 값에서 공통 역할을 종합하되, 없는 도메인을 가정하지 말 것.\n"
            "- 테이블명 약어는 입력 증거로 뜻이 확인되지 않으면 자동 확장하지 말 것.\n"
            "- 역할을 단정할 근거가 부족하면 테이블 설명도 \"[추정]\"으로 시작하고 근거·대안·확인 행동 명시.\n"
        )

        return f"""다음은 테이블 "{schema_name}"."{table_name}"의 정보입니다.

## 컬럼 정보:
{columns_str}

## 샘플 데이터 (최대 {SAMPLE_DATA_LIMIT}행):
{sample_str}{ddl_hint}

위 정보를 분석하여 다음을 JSON 형식으로 응답하세요:

1. "table_description": 테이블이 어떤 데이터를 저장하는지 한국어로 설명 (1-2문장)
2. "column_descriptions": 각 컬럼에 대한 설명을 담은 객체 (컬럼명: 설명)

예시:
{{
  "table_description": "[추정] 상태별 수치 기록을 저장하는 테이블로 보이며, 정확한 역할은 DDL 주석 확인이 필요합니다.",
  "column_descriptions": {{
    "record_id": "레코드 고유 식별자",
    "status_code": "[추정] 샘플 값과 이름을 근거로 상태 또는 분류 코드로 보이며, 코드 정의 확인을 권장합니다."
  }}
}}

JSON만 응답하세요."""

    async def _update_table_description(
        self, datasource: str, table_name: str, schema_name: str, description: str
    ) -> int:
        query = """
        MATCH (t:TABLE {name: $table_name})
        WHERE t.graph_owner = $graph_owner
          AND coalesce(t.db, t.datasource) = $datasource
          AND (t.schema = $schema_name
               OR ($schema_name = 'public' AND coalesce(t.schema, '') = ''))
          AND (t.description IS NULL OR t.description = '' OR t.description = 'N/A')
        SET t.description = $description,
            t.description_source = $source
        RETURN count(t) AS updated
        """
        results = await self.client.execute_queries([{
            "query": query,
            "params": {
                "datasource": datasource,
                "table_name": table_name,
                "schema_name": schema_name,
                "description": description,
                "source": DESCRIPTION_SOURCE,
                "graph_owner": ANALYSIS_GRAPH_OWNER,
            },
        }])
        return _updated_count(results)

    async def _update_column_descriptions(
        self,
        datasource: str,
        table_name: str,
        schema_name: str,
        column_descs: Dict[str, str],
    ) -> int:
        query = """
        MATCH (t:TABLE {name: $table_name})
          -[:HAS_COLUMN]->(c:COLUMN {name: $col_name})
        WHERE t.graph_owner = $graph_owner AND c.graph_owner = $graph_owner
          AND coalesce(t.db, t.datasource) = $datasource
          AND (t.schema = $schema_name
               OR ($schema_name = 'public' AND coalesce(t.schema, '') = ''))
          AND (c.description IS NULL OR c.description = '' OR c.description = 'N/A')
        SET c.description = $description,
            c.description_source = $source
        RETURN count(c) AS updated
        """
        updated = 0
        for col_name, col_desc in column_descs.items():
            results = await self.client.execute_queries([{
                "query": query,
                "params": {
                    "datasource": datasource,
                    "table_name": table_name,
                    "schema_name": schema_name,
                    "col_name": col_name,
                    "description": col_desc,
                    "source": DESCRIPTION_SOURCE,
                    "graph_owner": ANALYSIS_GRAPH_OWNER,
                },
            }])
            updated += _updated_count(results)
        return updated

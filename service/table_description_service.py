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

from client.neo4j_client import Neo4jClient
from config.settings import settings
from util.logger import log_process

logger = logging.getLogger(__name__)

DESCRIPTION_SOURCE = "sample_data_inference"
SAMPLE_DATA_LIMIT = 10  # LLM 프롬프트용 샘플 데이터 최대 행 수


class TableDescriptionService:
    """샘플 데이터 기반 테이블/컬럼 설명 생성·저장."""

    def __init__(self, client: Neo4jClient, openai_client: AsyncOpenAI):
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
                model=settings.llm.model or "gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_completion_tokens=1000,
                response_format={"type": "json_object"},
            )
            result = json.loads(response.choices[0].message.content)
            log_process(
                "METADATA",
                "LLM_OK",
                f"설명 생성 완료: {table_name} (테이블: {bool(result.get('table_description'))}, "
                f"컬럼: {len(result.get('column_descriptions', {}))}개)",
                logging.INFO,
            )
            return result
        except Exception as e:
            log_process("METADATA", "LLM_ERROR", f"LLM 설명 생성 실패: {e}", logging.WARNING)
            return None

    async def persist(
        self,
        table_name: str,
        schema_name: str,
        descriptions: Dict[str, Any],
    ) -> Tuple[int, int]:
        """Neo4j에 description 업데이트. Returns (테이블 업데이트수, 컬럼 업데이트수)."""
        table_updated = 0
        columns_updated = 0

        table_desc = descriptions.get("table_description", "")
        if table_desc:
            await self._update_table_description(table_name, schema_name, table_desc)
            table_updated = 1

        column_descs = descriptions.get("column_descriptions", {})
        if column_descs:
            columns_updated = await self._update_column_descriptions(
                table_name, schema_name, column_descs
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
            "- 예: BNB_CODE | DDL 주석: 본부코드  →  description: \"본부 코드\"\n"
            "\n"
            "### B. DDL 주석이 없는 컬럼 (추정)\n"
            "- 절대로 한 줄짜리 단정형(\"○○ 코드\", \"○○ 번호\")로 끝내지 말 것.\n"
            "- 반드시 \"[추정]\" 으로 시작하고 아래 4가지를 모두 포함하여 3~5줄로 상세히 작성:\n"
            "    1) 단정할 수 없음을 명시 (DDL 주석 없음 또는 의미 정보 부족)\n"
            "    2) 컬럼명/접두사/접미사/dtype/샘플 값에서 도출한 추정 근거\n"
            "    3) 가능한 대체 해석 (있다면 1~2개 나열)\n"
            "    4) 정확 의미 확인을 위한 권장 행동 (운영 DBMS 주석/도메인 전문가/관련 SP 코드 확인)\n"
            "- 같은 테이블 내 다른 컬럼의 DDL 주석에서 도메인 단서를 종합 활용 권장\n"
            "  (예: 같은 테이블에 '수자원' 관련 컬럼들이 보이면 도메인을 '수자원 시스템'으로 가정).\n"
            "- 예: LOCGOV_CODE2 (DDL 주석 없음)\n"
            "    description: \"[추정] 정확한 의미는 DDL 주석이 없어 단정할 수 없습니다. \"\n"
            "        \"컬럼명 패턴 'LOCGOV(Local Government, 지방자치단체)' + suffix '2' 로 보아 \"\n"
            "        \"지방자치단체 보조 코드 또는 두 번째 분류 체계로 추정됩니다. \"\n"
            "        \"샘플 10행이 대부분 NULL이라 값 패턴 검증이 어렵습니다. \"\n"
            "        \"정확한 의미는 운영 DBMS 컬럼 주석 또는 도메인 전문가 확인 권장.\"\n"
            "\n"
            "### 테이블 description\n"
            "- 컬럼 주석들에서 도메인 단서 종합 (예: '본부코드'·'사업장코드' 등 → 본부/사업장 마스터 도메인).\n"
            "- 테이블명 약어도 단서로 활용 (RDIBONBU → BONBU=본부, RDISAUP → SAUP=사업).\n"
            "- 도메인 추정이 어려우면 테이블 설명에도 \"[추정]\" 표기 후 근거 명시.\n"
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
  "table_description": "고객의 주문 정보를 저장하는 테이블입니다.",
  "column_descriptions": {{
    "order_id": "주문 고유 식별자",
    "customer_name": "고객 이름",
    "order_date": "주문 일시"
  }}
}}

JSON만 응답하세요."""

    async def _update_table_description(
        self, table_name: str, schema_name: str, description: str
    ) -> None:
        query = """
        MATCH (t:Table {name: $table_name, schema: $schema_name})
        WHERE t.description IS NULL
           OR t.description = ''
           OR t.description = 'N/A'
        SET t.description = $description,
            t.description_source = $source
        """
        await self.client.execute_queries([{
            "query": query,
            "params": {
                "table_name": table_name,
                "schema_name": schema_name,
                "description": description,
                "source": DESCRIPTION_SOURCE,
            },
        }])

    async def _update_column_descriptions(
        self, table_name: str, schema_name: str, column_descs: Dict[str, str]
    ) -> int:
        query = """
        MATCH (t:Table {name: $table_name, schema: $schema_name})
          -[:HAS_COLUMN]->(c:Column {name: $col_name})
        WHERE c.description IS NULL
           OR c.description = ''
           OR c.description = 'N/A'
        SET c.description = $description,
            c.description_source = $source
        """
        updated = 0
        for col_name, col_desc in column_descs.items():
            await self.client.execute_queries([{
                "query": query,
                "params": {
                    "table_name": table_name,
                    "schema_name": schema_name,
                    "col_name": col_name,
                    "description": col_desc,
                    "source": DESCRIPTION_SOURCE,
                },
            }])
            updated += 1
        return updated

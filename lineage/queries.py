"""데이터 리니지 서비스

데이터 리니지 그래프 조회 및 분석 기능을 제공합니다.

주요 기능:
- 리니지 그래프 조회
- SQL에서 리니지 추출
"""

import logging

from graph.database import CatalogGraphDatabase
from graph.scope import ANALYSIS_GRAPH_OWNER
from lineage.sql_extract import analyze_lineage_from_sql as _analyze_lineage


logger = logging.getLogger(__name__)


async def fetch_lineage_graph() -> dict:
    """데이터 리니지 그래프 조회 — 전략무관(framework·dbms 공통).

    분석 그래프의 ``(f)-[:READS|WRITES]->(:TABLE)`` 에서 도출:
    읽은 테이블=SOURCE, 그 함수/처리=ETL, 쓴 테이블=TARGET. 프론트의 SOURCE→ETL→TARGET
    모델 그대로. (옛 dbms 전용 DataSource/ETLProcess 모델 대체 — spec 007.)

    Returns:
        {"nodes": [...], "edges": [...], "stats": {...}}
    """
    client = CatalogGraphDatabase()
    try:
        query = f"""
            MATCH (__cy_f__)-[__cy_r__:READS|WRITES]->(__cy_t__:TABLE)
            WHERE __cy_f__.graph_owner = '{ANALYSIS_GRAPH_OWNER}'
              AND __cy_t__.graph_owner = '{ANALYSIS_GRAPH_OWNER}'
            RETURN elementId(__cy_f__) AS fid,
                   coalesce(__cy_f__.logical_name, __cy_f__.name, __cy_f__.id) AS fname,
                   type(__cy_r__) AS rel,
                   elementId(__cy_t__) AS tid,
                   coalesce(__cy_t__.logical_name, __cy_t__.name, __cy_t__.id) AS tname
        """
        rows = (await client.execute_queries([query]))[0]

        nodes: dict[str, dict] = {}
        edges: dict[str, dict] = {}
        for r in rows:
            etl_id = f"etl:{r['fid']}"
            nodes.setdefault(etl_id, {"id": etl_id, "name": r["fname"], "type": "ETL", "properties": {}})
            if r["rel"] == "READS":
                src_id = f"src:{r['tid']}"
                nodes.setdefault(src_id, {"id": src_id, "name": r["tname"], "type": "SOURCE", "properties": {}})
                eid = f"{src_id}->{etl_id}"
                edges.setdefault(eid, {"id": eid, "source": src_id, "target": etl_id, "type": "DATA_FLOW_TO", "properties": {}})
            else:  # WRITES
                tgt_id = f"tgt:{r['tid']}"
                nodes.setdefault(tgt_id, {"id": tgt_id, "name": r["tname"], "type": "TARGET", "properties": {}})
                eid = f"{etl_id}->{tgt_id}"
                edges.setdefault(eid, {"id": eid, "source": etl_id, "target": tgt_id, "type": "DATA_FLOW_TO", "properties": {}})

        node_list = list(nodes.values())
        edge_list = list(edges.values())
        stats = {
            "etlCount": sum(1 for n in node_list if n["type"] == "ETL"),
            "sourceCount": sum(1 for n in node_list if n["type"] == "SOURCE"),
            "targetCount": sum(1 for n in node_list if n["type"] == "TARGET"),
            "flowCount": len(edge_list),
        }
        return {"nodes": node_list, "edges": edge_list, "stats": stats}
    finally:
        await client.close()


async def analyze_sql_lineage(
    sql_content: str,
    file_name: str = "",
    dbms: str = "oracle",
    name_case: str = "original",
) -> dict:
    """ETL 코드에서 데이터 리니지 추출 및 Neo4j 저장

    Args:
        sql_content: SQL 소스 코드
        file_name: 파일명
        dbms: DBMS 타입
        name_case: 이름 대소문자 처리 (uppercase, lowercase, original)

    Returns:
        {"lineages": [...], "stats": {...}}
    """
    lineage_list, stats = await _analyze_lineage(
        sql_content=sql_content,
        file_name=file_name,
        dbms=dbms,
        name_case=name_case,
    )
    
    # 응답 변환
    lineages = [
        {
            "etl_name": l.etl_name,
            "source_tables": l.source_tables,
            "target_tables": l.target_tables,
            "operation_type": l.operation_type,
        }
        for l in lineage_list
    ]
    
    return {
        "lineages": lineages,
        "stats": stats
    }


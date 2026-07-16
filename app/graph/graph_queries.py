"""그래프 데이터 조회/삭제 서비스

Neo4j 그래프 데이터의 조회, 삭제, 정리 기능을 제공합니다.

주요 기능:
- 그래프 데이터 존재 확인
- 전체 그래프 데이터 조회
- 관련 테이블 조회
- Neo4j/파일 데이터 삭제
"""

import logging
import re
import shutil
from pathlib import Path
from typing import Any

from app.graph.client import Neo4jClient
from app.graph.ownership import (
    ANALYSIS_GRAPH_OWNER,
    SYSTEM_GRAPH_NODE_LABELS,
    owner_predicate as _analysis_node_predicate,
    visible_predicate as _visible_node_predicate,
)
from app.system.settings import settings


logger = logging.getLogger(__name__)


# 벡터 임베딩은 그래프 렌더링에 불필요하고 응답을 과도하게 키우므로 제외합니다.
_VECTOR_KEY_HINTS = ("vector",)
_EMBEDDING_KEY_HINTS = ("embedding",)
_CYPHER_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_large_numeric_vector(value: Any, min_len: int = 128) -> bool:
    """벡터로 간주할 수 있는 고차원 숫자 배열 여부."""
    if not isinstance(value, (list, tuple)):
        return False
    if len(value) < min_len:
        return False
    # 전체 순회 비용을 줄이기 위해 앞부분 샘플만 검사
    sample = value[: min(32, len(value))]
    return all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in sample)


def _should_exclude_property(key: str, value: Any) -> bool:
    """그래프 응답에서 제외할 대용량 속성 판별."""
    key_lower = key.lower()

    # 현재/미래의 vector 계열 키를 가장 우선적으로 제외
    if any(hint in key_lower for hint in _VECTOR_KEY_HINTS):
        return True

    # 임베딩 계열은 숫자 벡터일 때만 제외(설명 텍스트 등은 유지)
    if any(hint in key_lower for hint in _EMBEDDING_KEY_HINTS) and _is_large_numeric_vector(value, min_len=64):
        return True

    # 키 명명 규칙이 달라져도 고차원 숫자 배열이면 벡터로 간주
    if _is_large_numeric_vector(value):
        return True

    return False


def _sanitize_graph_properties(raw_props: Any) -> dict[str, Any]:
    """그래프 API 응답용 속성 필터링."""
    if not isinstance(raw_props, dict):
        return {}

    return {
        key: value
        for key, value in raw_props.items()
        if not _should_exclude_property(str(key), value)
    }


def _normalize_relationship_properties(record: dict[str, Any], raw_props: Any) -> dict[str, Any]:
    """응답 관계 속성을 정리하고 CALLS 재귀 여부를 끝점 식별자로 검증한다."""
    props = _sanitize_graph_properties(raw_props)
    if record.get("relType") == "CALLS":
        props["recursive"] = record.get("startId") == record.get("endId")
    return props


def _is_valid_cypher_identifier(name: str) -> bool:
    return bool(_CYPHER_IDENTIFIER_PATTERN.match(name))


def _validated_data_dir(base_dir: str) -> Path:
    """파일 초기화 대상을 base_dir 바로 아래의 data 디렉터리로만 제한한다."""
    base = Path(base_dir).resolve()
    candidate = base / "data"
    resolved = candidate.resolve()
    if resolved.parent != base or resolved.name != "data" or candidate.is_symlink():
        raise RuntimeError("unsafe Catalog data directory")
    return resolved


async def _discover_vector_property_keys(client: Neo4jClient) -> list[str]:
    """DB에 존재하는 vector 계열 속성 키 목록을 조회."""
    results = await client.execute_queries([
        "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey"
    ])
    rows = results[0] if results else []
    keys = []
    for row in rows:
        key = row.get("propertyKey")
        if not isinstance(key, str):
            continue
        if any(hint in key.lower() for hint in _VECTOR_KEY_HINTS) and _is_valid_cypher_identifier(key):
            keys.append(key)

    # 쿼리 문자열 안정성을 위해 정렬/중복제거
    return sorted(set(keys))


def _build_null_projection_suffix(property_keys: list[str]) -> str:
    """map projection에서 특정 속성을 null로 덮어쓰기 위한 suffix 생성."""
    if not property_keys:
        return ""
    return "".join(f", {key}: null" for key in property_keys)


# =============================================================================
# 그래프 데이터 조회
# =============================================================================

async def check_graph_data_exists() -> dict:
    """Neo4j에 기존 데이터 존재 여부 확인
    
    Returns:
        {"hasData": bool, "nodeCount": int}
    """
    client = Neo4jClient()
    try:
        result = await client.execute_queries([
            f"MATCH (__cy_n__) WHERE {_analysis_node_predicate('__cy_n__')} "
            f"AND {_visible_node_predicate('__cy_n__')} "
            "RETURN count(__cy_n__) as count"
        ])
        node_count = result[0][0]["count"] if result and result[0] else 0
        
        return {
            "hasData": node_count > 0,
            "nodeCount": node_count
        }
    finally:
        await client.close()


async def fetch_graph_data() -> dict:
    """Neo4j에서 기존 그래프 데이터 조회
    
    Returns:
        {"Nodes": [...], "Relationships": [...]}
    """
    client = Neo4jClient()
    try:
        vector_keys = await _discover_vector_property_keys(client)
        null_projection_suffix = _build_null_projection_suffix(vector_keys)

        # 사용자 그래프 노드 조회 — 검색/분석 시스템 노드는 API 경계에서 제외.
        node_query = f"""
            MATCH (__cy_n__)
            WHERE {_analysis_node_predicate('__cy_n__')} AND {_visible_node_predicate('__cy_n__')}
            RETURN elementId(__cy_n__) AS nodeId, labels(__cy_n__) AS labels, __cy_n__{{.*{null_projection_suffix}}} AS props
        """
        
        # 관계 조회 (전체 관계)
        rel_query = f"""
            MATCH (__cy_a__)-[__cy_r__]->(__cy_b__)
            WHERE {_analysis_node_predicate('__cy_a__')} AND {_analysis_node_predicate('__cy_b__')}
              AND {_visible_node_predicate('__cy_a__')} AND {_visible_node_predicate('__cy_b__')}
            RETURN elementId(__cy_r__) AS relId, 
                   elementId(__cy_a__) AS startId, 
                   elementId(__cy_b__) AS endId, 
                   type(__cy_r__) AS relType, 
                   __cy_r__{{.*{null_projection_suffix}}} AS props
        """
        
        results = await client.execute_queries([node_query, rel_query])
        node_result = results[0] if results else []
        rel_result = results[1] if len(results) > 1 else []
        
        # 응답 형식 변환 (대용량 벡터 속성 제외)
        nodes = []
        filtered_node_prop_count = 0
        for record in node_result:
            raw_props = record.get("props") or {}
            safe_props = _sanitize_graph_properties(raw_props)
            filtered_node_prop_count += max(0, len(raw_props) - len(safe_props))
            nodes.append({
                "Node ID": record["nodeId"],
                "Labels": record["labels"],
                "Properties": safe_props
            })
        
        relationships = []
        filtered_rel_prop_count = 0
        for record in rel_result:
            raw_props = record.get("props") or {}
            safe_props = _normalize_relationship_properties(record, raw_props)
            filtered_rel_prop_count += max(0, len(raw_props) - len(safe_props))
            relationships.append({
                "Relationship ID": record["relId"],
                "Start Node ID": record["startId"],
                "End Node ID": record["endId"],
                "Type": record["relType"],
                "Properties": safe_props
            })

        logger.info(
            "[GraphAPI] graph payload sanitized | nodes=%d rels=%d vector_keys=%d filtered_node_props=%d filtered_rel_props=%d",
            len(nodes), len(relationships), len(vector_keys), filtered_node_prop_count, filtered_rel_prop_count
        )
        
        return {
            "Nodes": nodes,
            "Relationships": relationships
        }
    finally:
        await client.close()


async def fetch_related_tables(table_name: str) -> dict:
    """특정 테이블과 연결된 모든 테이블 조회 (FK_TO_TABLE 관계 포함)
    
    Args:
        table_name: 기준 테이블명
        
    Returns:
        {"base_table": str, "tables": [...], "relationships": [...]}
    """
    client = Neo4jClient()
    try:
        # FK_TO_TABLE 관계 조회
        fk_query = {
            "query": f"""
                MATCH (__cy_t1__:TABLE)-[__cy_r__:FK_TO_TABLE]->(__cy_t2__:TABLE)
                WHERE __cy_t1__.graph_owner = '{ANALYSIS_GRAPH_OWNER}'
                  AND __cy_t2__.graph_owner = '{ANALYSIS_GRAPH_OWNER}'
                  AND (__cy_t1__.name = $table_name OR __cy_t2__.name = $table_name
                    OR __cy_t1__.id ENDS WITH $table_name OR __cy_t2__.id ENDS WITH $table_name)
                RETURN __cy_t1__.name AS from_table, 
                       __cy_t1__.schema_name AS from_schema,
                       __cy_t1__.description AS from_desc,
                       __cy_t2__.name AS to_table, 
                       __cy_t2__.schema_name AS to_schema,
                       __cy_t2__.description AS to_desc,
                       __cy_r__.sourceColumn AS source_column,
                       __cy_r__.targetColumn AS target_column,
                       COALESCE(__cy_r__.source, 'ddl') AS source,
                       type(__cy_r__) AS rel_type
            """,
            "parameters": {"table_name": table_name}
        }
        
        # 같은 프로시저에서 참조되는 테이블 (CO_REFERENCED)
        proc_query = {
            "query": f"""
                MATCH (__cy_t__:TABLE)
                WHERE __cy_t__.graph_owner = '{ANALYSIS_GRAPH_OWNER}'
                  AND (__cy_t__.name = $table_name OR __cy_t__.id ENDS WITH $table_name)
                
                OPTIONAL MATCH __cy_p1__ = (__cy_t__)<-[:FROM|WRITES]-(__cy_s1__)<-[:PARENT_OF*]-(__cy_proc__)
                WHERE ALL(__cy_n__ IN nodes(__cy_p1__) WHERE __cy_n__.graph_owner = '{ANALYSIS_GRAPH_OWNER}')
                OPTIONAL MATCH __cy_p2__ = (__cy_proc__)-[:PARENT_OF*]->(__cy_s2__)-[:FROM|WRITES]->(__cy_t2__:TABLE)
                WHERE __cy_t2__ <> __cy_t__
                  AND ALL(__cy_n__ IN nodes(__cy_p2__) WHERE __cy_n__.graph_owner = '{ANALYSIS_GRAPH_OWNER}')
                
                WITH __cy_t__, COLLECT(DISTINCT {{
                    name: __cy_t2__.name, 
                    schema: __cy_t2__.schema_name, 
                    description: __cy_t2__.description
                }}) AS proc_related
                
                RETURN __cy_t__.name AS base_table, 
                       __cy_t__.schema_name AS base_schema,
                       proc_related
            """,
            "parameters": {"table_name": table_name}
        }
        
        fk_results = await client.execute_queries([fk_query])
        proc_results = await client.execute_queries([proc_query])
        
        fk_result = fk_results[0] if fk_results else []
        proc_result = proc_results[0] if proc_results else []
        
        tables = []
        relationships = []
        seen_tables = set()
        seen_rels = set()
        
        # 기준 테이블 추가
        seen_tables.add(table_name)
        
        # FK_TO_TABLE 관계 처리
        fk_by_table_pair = {}
        
        for record in fk_result:
            from_table = record.get("from_table")
            to_table = record.get("to_table")
            source_column = record.get("source_column") or ""
            target_column = record.get("target_column") or ""
            source_type = record.get("source") or "ddl"
            
            if from_table and from_table not in seen_tables:
                seen_tables.add(from_table)
                tables.append({
                    "name": from_table,
                    "schema": record.get("from_schema") or "public",
                    "description": record.get("from_desc")
                })
            
            if to_table and to_table not in seen_tables:
                seen_tables.add(to_table)
                tables.append({
                    "name": to_table,
                    "schema": record.get("to_schema") or "public",
                    "description": record.get("to_desc")
                })
            
            pair_key = (from_table, to_table)
            if pair_key not in fk_by_table_pair:
                fk_by_table_pair[pair_key] = {
                    "source": source_type,
                    "column_pairs": []
                }
            
            if source_column or target_column:
                fk_by_table_pair[pair_key]["column_pairs"].append({
                    "source": source_column,
                    "target": target_column
                })
        
        for (from_table, to_table), data in fk_by_table_pair.items():
            rel_key = f"{from_table}->{to_table}"
            if rel_key not in seen_rels:
                seen_rels.add(rel_key)
                relationships.append({
                    "from_table": from_table,
                    "to_table": to_table,
                    "type": "FK_TO_TABLE",
                    "source": data["source"],
                    "column_pairs": data["column_pairs"]
                })
        
        # CO_REFERENCED 관계 처리
        for record in proc_result:
            base_table = record.get("base_table")
            for item in record.get("proc_related", []):
                if item.get("name") and item["name"] not in seen_tables:
                    seen_tables.add(item["name"])
                    tables.append({
                        "name": item["name"],
                        "schema": item.get("schema") or "public",
                        "description": item.get("description")
                    })
                    
                    rel_key = f"{base_table}->{item['name']}"
                    if rel_key not in seen_rels:
                        seen_rels.add(rel_key)
                        relationships.append({
                            "from_table": base_table,
                            "to_table": item["name"],
                            "type": "CO_REFERENCED",
                            "source": "procedure",
                            "column_pairs": []
                        })
        
        return {
            "base_table": table_name,
            "tables": tables,
            "relationships": relationships
        }
    finally:
        await client.close()


# =============================================================================
# 데이터 삭제
# =============================================================================

async def cleanup_all_graph_data(include_files: bool = True) -> None:
    """데이터 전체 삭제 (DataSource 노드 제외)
    
    Args:
        include_files: True면 파일 시스템도 함께 삭제, False면 Neo4j만 삭제
    """
    client = Neo4jClient()
    
    try:
        # 파일 시스템 정리 (옵션)
        if include_files:
            dir_path = _validated_data_dir(settings.path.base_dir)
            if dir_path.exists():
                shutil.rmtree(dir_path)
            dir_path.mkdir(parents=False, exist_ok=True)
            logger.info("Catalog data directory initialized | path=%s", dir_path)
        
        # Analyzer 소유 분석 그래프만 삭제한다. Fabric/Architect 노드는 소유자가 다르다.
        await client.execute_queries([
            f"MATCH (__cy_n__) WHERE {_analysis_node_predicate('__cy_n__')} DETACH DELETE __cy_n__"
        ])
        logging.info("Analyzer 소유 Neo4j 데이터 삭제 완료")
    except Exception as e:
        logger.error("분석 그래프 삭제 실패 | error_type=%s", type(e).__name__)
        raise RuntimeError("분석 그래프 삭제에 실패했습니다") from e
    finally:
        await client.close()


async def delete_graph_data(include_files: bool = False) -> dict:
    """사용자 데이터 삭제
    
    Args:
        include_files: 파일 시스템도 삭제할지 여부
        
    Returns:
        삭제 결과 메시지
    """
    await cleanup_all_graph_data(include_files=include_files)
    if include_files:
        return {"message": "모든 데이터(파일 + Neo4j)가 삭제되었습니다."}
    return {"message": "Neo4j 그래프 데이터가 삭제되었습니다. (파일은 유지됨)"}


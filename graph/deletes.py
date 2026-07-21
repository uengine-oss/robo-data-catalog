"""Deletion lifecycle for analyzer-owned Catalog graph data."""
from __future__ import annotations

import logging
import shutil

from graph.database import CatalogGraphDatabase
from graph.scope import owner_predicate as _analysis_node_predicate
from graph.queries import _validated_data_dir
from shared.config.settings import CATALOG_SETTINGS

logger = logging.getLogger(__name__)

async def cleanup_all_graph_data(include_files: bool = True) -> None:
    """데이터 전체 삭제 (DataSource 노드 제외)

    Args:
        include_files: True면 파일 시스템도 함께 삭제, False면 Neo4j만 삭제
    """
    client = CatalogGraphDatabase()

    try:
        # 파일 시스템 정리 (옵션)
        if include_files:
            dir_path = _validated_data_dir(CATALOG_SETTINGS.storage.base_dir)
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

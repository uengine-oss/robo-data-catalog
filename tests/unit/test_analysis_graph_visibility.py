import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import graph.queries as graph_queries
import graph.deletes as graph_deletes


def test_system_nodes_are_excluded_from_user_graph_predicate() -> None:
    predicate = graph_queries._visible_node_predicate("n")
    assert graph_queries.SYSTEM_GRAPH_NODE_LABELS == frozenset({"GLOSSARY", "EMBED_META"})
    assert "NOT n:GLOSSARY" in predicate
    assert "NOT n:EMBED_META" in predicate

    try:
        graph_queries._visible_node_predicate("n) RETURN n")
    except ValueError:
        pass
    else:
        raise AssertionError("Cypher alias validation must reject non-identifiers")


def test_file_cleanup_target_is_confined_to_catalog_data_directory() -> None:
    with tempfile.TemporaryDirectory() as directory:
        base = Path(directory)
        assert graph_queries._validated_data_dir(str(base)) == (base / "data").resolve()
        with mock.patch.object(Path, "is_symlink", return_value=True):
            with unittest.TestCase().assertRaises(RuntimeError):
                graph_queries._validated_data_dir(str(base))


def test_calls_recursion_is_derived_from_actual_relationship_endpoints() -> None:
    recursive = {"relType": "CALLS", "startId": "42", "endId": "42"}
    normal = {"relType": "CALLS", "startId": "42", "endId": "43"}
    assert graph_queries._normalize_relationship_properties(recursive, {})["recursive"] is True
    assert graph_queries._normalize_relationship_properties(normal, {"recursive": True})["recursive"] is False


class _RecordingCatalogGraphDatabase:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.closed = False

    async def execute_queries(self, queries: list) -> list[list[dict]]:
        self.calls.append(queries)
        first = queries[0]["query"] if isinstance(queries[0], dict) else queries[0]
        if first.startswith("CALL db.propertyKeys"):
            return [[]]
        if "RETURN count(__cy_n__) as count" in first:
            return [[{"count": 0}]]
        return [[] for _ in queries]

    async def close(self) -> None:
        self.closed = True


async def test_all_user_graph_queries_apply_the_system_boundary() -> None:
    clients: list[_RecordingCatalogGraphDatabase] = []

    def client_factory() -> _RecordingCatalogGraphDatabase:
        client = _RecordingCatalogGraphDatabase()
        clients.append(client)
        return client

    original = graph_queries.CatalogGraphDatabase
    delete_original = graph_deletes.CatalogGraphDatabase
    graph_queries.CatalogGraphDatabase = client_factory  # type: ignore[assignment]
    graph_deletes.CatalogGraphDatabase = client_factory  # type: ignore[assignment]
    try:
        exists = await graph_queries.check_graph_data_exists()
        payload = await graph_queries.fetch_graph_data()
    finally:
        graph_queries.CatalogGraphDatabase = original
        graph_deletes.CatalogGraphDatabase = delete_original

    assert exists == {"hasData": False, "nodeCount": 0}
    assert payload == {"Nodes": [], "Relationships": []}
    assert len(clients) == 2 and all(client.closed for client in clients)

    count_query = clients[0].calls[0][0]
    node_query, rel_query = clients[1].calls[1]
    assert "NOT __cy_n__:GLOSSARY" in count_query and "NOT __cy_n__:EMBED_META" in count_query
    assert "NOT __cy_n__:GLOSSARY" in node_query and "NOT __cy_n__:EMBED_META" in node_query
    for alias in ("__cy_a__", "__cy_b__"):
        assert f"NOT {alias}:GLOSSARY" in rel_query
        assert f"NOT {alias}:EMBED_META" in rel_query


async def test_check_and_delete_are_analysis_owner_scoped() -> None:
    clients: list[_RecordingCatalogGraphDatabase] = []

    def client_factory() -> _RecordingCatalogGraphDatabase:
        client = _RecordingCatalogGraphDatabase()
        clients.append(client)
        return client

    original = graph_queries.CatalogGraphDatabase
    delete_original = graph_deletes.CatalogGraphDatabase
    graph_queries.CatalogGraphDatabase = client_factory  # type: ignore[assignment]
    graph_deletes.CatalogGraphDatabase = client_factory  # type: ignore[assignment]
    try:
        await graph_queries.check_graph_data_exists()
        await graph_deletes.cleanup_all_graph_data(include_files=False)
    finally:
        graph_queries.CatalogGraphDatabase = original
        graph_deletes.CatalogGraphDatabase = delete_original

    count_query = clients[0].calls[0][0]
    delete_query = clients[1].calls[0][0]
    for query in (count_query, delete_query):
        assert "graph_owner" in query
        assert "analyzer" in query
    assert "NOT __cy_n__:DataSource" not in delete_query
    assert "DETACH DELETE __cy_n__" in delete_query


async def test_related_table_queries_are_analysis_owner_scoped() -> None:
    clients: list[_RecordingCatalogGraphDatabase] = []

    def client_factory() -> _RecordingCatalogGraphDatabase:
        client = _RecordingCatalogGraphDatabase()
        clients.append(client)
        return client

    original = graph_queries.CatalogGraphDatabase
    graph_queries.CatalogGraphDatabase = client_factory  # type: ignore[assignment]
    try:
        result = await graph_queries.fetch_related_tables("orders")
    finally:
        graph_queries.CatalogGraphDatabase = original

    assert result == {"base_table": "orders", "tables": [], "relationships": []}
    assert len(clients) == 1 and clients[0].closed
    queries = [call[0]["query"] for call in clients[0].calls]
    assert len(queries) == 2
    for call, query in zip(clients[0].calls, queries):
        assert "graph_owner" in query
        assert call[0]["parameters"]["graph_owner"] == "analyzer"


def test_related_table_payload_keeps_distinct_relation_kinds() -> None:
    payload = graph_queries._related_tables_payload(
        "orders",
        [
            {
                "from_table": "orders",
                "to_table": "customers",
                "to_schema": "sales",
                "source_column": "customer_id",
                "target_column": "id",
                "source": "ddl",
            }
        ],
        [
            {
                "base_table": "orders",
                "proc_related": [
                    {"name": "customers", "schema": "sales", "description": "Customer"}
                ],
            }
        ],
    )

    assert [table["name"] for table in payload["tables"]] == ["customers"]
    assert [relation["type"] for relation in payload["relationships"]] == [
        "FK_TO_TABLE",
        "CO_REFERENCED",
    ]


class GraphVisibilityTest(unittest.IsolatedAsyncioTestCase):
    def test_system_node_predicate(self) -> None:
        test_system_nodes_are_excluded_from_user_graph_predicate()

    def test_cleanup_path_boundary(self) -> None:
        test_file_cleanup_target_is_confined_to_catalog_data_directory()

    def test_calls_recursion_normalization(self) -> None:
        test_calls_recursion_is_derived_from_actual_relationship_endpoints()

    def test_related_table_payload_relation_kinds(self) -> None:
        test_related_table_payload_keeps_distinct_relation_kinds()

    async def test_graph_queries_apply_visibility_boundary(self) -> None:
        await test_all_user_graph_queries_apply_the_system_boundary()

    async def test_check_and_delete_apply_owner_boundary(self) -> None:
        await test_check_and_delete_are_analysis_owner_scoped()

    async def test_related_tables_apply_owner_boundary(self) -> None:
        await test_related_table_queries_are_analysis_owner_scoped()


if __name__ == "__main__":
    test_system_nodes_are_excluded_from_user_graph_predicate()
    asyncio.run(test_all_user_graph_queries_apply_the_system_boundary())
    asyncio.run(test_check_and_delete_are_analysis_owner_scoped())
    print("[OK] system graph node boundary")

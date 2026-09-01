import tempfile
import unittest
from pathlib import Path
from unittest import mock

import graph.deletes as graph_deletes
import graph.queries as graph_queries
from graph.scope import owner_predicate


class _RecordingCatalogGraphDatabase:
    def __init__(self) -> None:
        self.calls: list[list] = []
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


class GraphVisibilityTest(unittest.IsolatedAsyncioTestCase):
    def test_owner_predicate_accepts_only_identifiers(self) -> None:
        self.assertEqual(owner_predicate("node"), "node._owner = 'analyzer'")
        with self.assertRaises(ValueError):
            owner_predicate("node) RETURN node")

    def test_cleanup_path_is_confined_to_catalog_data_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            self.assertEqual(
                graph_queries._validated_data_dir(str(base)),
                (base / "data").resolve(),
            )
            with mock.patch.object(Path, "is_symlink", return_value=True):
                with self.assertRaises(RuntimeError):
                    graph_queries._validated_data_dir(str(base))

    def test_calls_recursion_is_derived_from_relationship_endpoints(self) -> None:
        recursive = {"relType": "CALLS", "startId": "42", "endId": "42"}
        normal = {"relType": "CALLS", "startId": "42", "endId": "43"}
        self.assertTrue(
            graph_queries._normalize_relationship_properties(recursive, {})["recursive"]
        )
        self.assertFalse(
            graph_queries._normalize_relationship_properties(
                normal, {"recursive": True}
            )["recursive"]
        )

    async def test_graph_queries_are_analyzer_owner_scoped(self) -> None:
        clients: list[_RecordingCatalogGraphDatabase] = []

        def client_factory() -> _RecordingCatalogGraphDatabase:
            client = _RecordingCatalogGraphDatabase()
            clients.append(client)
            return client

        original = graph_queries.CatalogGraphDatabase
        graph_queries.CatalogGraphDatabase = client_factory
        try:
            exists = await graph_queries.check_graph_data_exists()
            payload = await graph_queries.fetch_graph_data()
        finally:
            graph_queries.CatalogGraphDatabase = original

        self.assertEqual(exists, {"hasData": False, "nodeCount": 0})
        self.assertEqual(payload, {"Nodes": [], "Relationships": []})
        self.assertEqual(len(clients), 2)
        self.assertTrue(all(client.closed for client in clients))

        count_query = clients[0].calls[0][0]
        node_query, rel_query = clients[1].calls[1]
        self.assertIn("__cy_n__._owner = 'analyzer'", count_query)
        self.assertIn("__cy_n__._owner = 'analyzer'", node_query)
        self.assertIn("__cy_a__._owner = 'analyzer'", rel_query)
        self.assertIn("__cy_b__._owner = 'analyzer'", rel_query)

    async def test_delete_is_analyzer_owner_scoped(self) -> None:
        client = _RecordingCatalogGraphDatabase()
        original = graph_deletes.CatalogGraphDatabase
        graph_deletes.CatalogGraphDatabase = lambda: client
        try:
            await graph_deletes.cleanup_all_graph_data(include_files=False)
        finally:
            graph_deletes.CatalogGraphDatabase = original

        query = client.calls[0][0]
        self.assertIn("__cy_n__._owner = 'analyzer'", query)
        self.assertIn("DETACH DELETE __cy_n__", query)
        self.assertTrue(client.closed)

    async def test_related_table_query_uses_only_fk_and_current_owner(self) -> None:
        client = _RecordingCatalogGraphDatabase()
        original = graph_queries.CatalogGraphDatabase
        graph_queries.CatalogGraphDatabase = lambda: client
        try:
            result = await graph_queries.fetch_related_tables("orders")
        finally:
            graph_queries.CatalogGraphDatabase = original

        self.assertEqual(
            result,
            {"base_table": "orders", "tables": [], "relationships": []},
        )
        queries = [call[0]["query"] for call in client.calls]
        self.assertEqual(len(queries), 1)
        self.assertIn("[__cy_r__:FK]", queries[0])
        for call, query in zip(client.calls, queries):
            self.assertIn("_owner", query)
            self.assertEqual(call[0]["parameters"]["owner"], "analyzer")

    def test_related_table_payload_groups_composite_fk_columns(self) -> None:
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
                },
                {
                    "from_table": "orders",
                    "to_table": "customers",
                    "to_schema": "sales",
                    "source_column": "tenant_id",
                    "target_column": "tenant_id",
                    "source": "ddl",
                },
            ],
        )

        self.assertEqual(
            [relation["type"] for relation in payload["relationships"]],
            ["FK"],
        )
        self.assertEqual(
            payload["relationships"][0]["column_pairs"],
            [
                {"source": "customer_id", "target": "id"},
                {"source": "tenant_id", "target": "tenant_id"},
            ],
        )


if __name__ == "__main__":
    unittest.main()

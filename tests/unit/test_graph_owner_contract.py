import inspect
import unittest

from fastapi import HTTPException

from graph import queries as analysis_graph_queries
from graph import schema_commands as schema_metadata_commands
from graph import schema_queries as schema_metadata_queries
from graph.connection import RequestGraphConnection
from lineage import queries as lineage_graph_queries
from lineage import sql_extract as sql_lineage_extraction
from enrichment import foreign_keys as foreign_key_inference
from enrichment import description as table_description_enrichment
from search import semantic as metadata_semantic_search
from samples import context as table_sample_context


class _RecordingClient:
    def __init__(self, response=None):
        self.calls = []
        self.closed = False
        self.response = response if response is not None else [[]]

    async def execute_queries(self, queries, params=None):
        self.calls.append((queries, params))
        return self.response

    async def close(self):
        self.closed = True


class GraphOwnerContractTest(unittest.IsolatedAsyncioTestCase):
    def test_neo4j_override_accepts_only_neo4j_uri_without_embedded_credentials(self):
        override = RequestGraphConnection.from_headers({
            "x-neo4j-uri": "bolt://127.0.0.1:7687",
            "x-neo4j-user": "neo4j",
            "x-neo4j-password": "secret",
        })
        self.assertIsNotNone(override)
        for uri in ("http://127.0.0.1:7687", "bolt://user:pass@127.0.0.1:7687", "bolt:///missing"):
            with self.subTest(uri=uri), self.assertRaises(ValueError):
                RequestGraphConnection.from_headers({"x-neo4j-uri": uri})
        with self.assertRaises(ValueError):
            RequestGraphConnection.from_headers({
                "x-neo4j-uri": "bolt://127.0.0.1:7687",
                "x-neo4j-database": "system",
            })

    def test_every_graph_domain_module_declares_owner_boundary(self):
        modules = (
            analysis_graph_queries,
            schema_metadata_commands,
            schema_metadata_queries,
            lineage_graph_queries,
            sql_lineage_extraction,
            table_description_enrichment,
            foreign_key_inference,
            table_sample_context,
            metadata_semantic_search,
        )
        for module in modules:
            with self.subTest(module=module.__name__):
                self.assertIn("graph_owner", inspect.getsource(module))

    async def test_schema_read_carries_owner_parameter(self):
        client = _RecordingClient()
        original = schema_metadata_queries.CatalogGraphDatabase
        schema_metadata_queries.CatalogGraphDatabase = lambda: client
        try:
            self.assertEqual(await schema_metadata_queries.fetch_schema_tables(limit=5000), [])
        finally:
            schema_metadata_queries.CatalogGraphDatabase = original

        query = client.calls[0][0][0]
        self.assertEqual(query["parameters"]["graph_owner"], "analyzer")
        self.assertEqual(query["parameters"]["limit"], 1000)
        self.assertIn("t.graph_owner = $graph_owner", query["query"])
        self.assertTrue(client.closed)

    def test_metadata_enrichment_targets_are_owner_and_datasource_scoped(self):
        query = schema_metadata_queries.metadata_enrichment_targets_query("shopmall")

        self.assertEqual(
            query["parameters"],
            {"datasource": "shopmall", "graph_owner": "analyzer"},
        )
        self.assertIn("t.graph_owner = $graph_owner", query["query"])
        self.assertIn("coalesce(t.db, t.datasource) = $datasource", query["query"])
        self.assertIn("c:COLUMN {graph_owner: $graph_owner}", query["query"])
        self.assertNotIn("shopmall", query["query"])

    async def test_schema_write_carries_owner_and_type_allowlist(self):
        with self.assertRaises(HTTPException):
            await schema_metadata_commands.create_schema_relationship(
                "a", "public", "id", "b", "public", "id", "X]->(n) DETACH DELETE n //",
            )

        client = _RecordingClient([[{"from_table": "a", "to_table": "b"}]])
        original = schema_metadata_commands.CatalogGraphDatabase
        schema_metadata_commands.CatalogGraphDatabase = lambda: client
        try:
            result = await schema_metadata_commands.create_schema_relationship(
                "a", "public", "id", "b", "public", "id", "FK_TO_TABLE",
            )
        finally:
            schema_metadata_commands.CatalogGraphDatabase = original

        self.assertTrue(result["created"])
        query = client.calls[0][0][0]
        self.assertEqual(query["parameters"]["graph_owner"], "analyzer")
        self.assertIn("t1.graph_owner = $graph_owner", query["query"])

    async def test_lineage_read_is_owner_scoped(self):
        client = _RecordingClient([[]])
        original = lineage_graph_queries.CatalogGraphDatabase
        lineage_graph_queries.CatalogGraphDatabase = lambda: client
        try:
            result = await lineage_graph_queries.fetch_lineage_graph()
        finally:
            lineage_graph_queries.CatalogGraphDatabase = original

        self.assertEqual(result["stats"]["flowCount"], 0)
        self.assertIn("graph_owner = 'analyzer'", client.calls[0][0][0])
        self.assertTrue(client.closed)


if __name__ == "__main__":
    unittest.main()

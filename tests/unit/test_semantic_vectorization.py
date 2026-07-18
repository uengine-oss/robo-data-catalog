import unittest

from search.semantic import _embedding_text, _target_query, _vectorize_kind


class _VectorClient:
    def __init__(self):
        self.calls = []

    async def execute_queries(self, queries):
        self.calls.append(queries)
        if len(self.calls) == 1:
            return [[
                {
                    "node_id": "column-1",
                    "name": "customer_id",
                    "description": "Customer identifier",
                    "table_name": "orders",
                },
                {
                    "node_id": "column-2",
                    "name": "amount",
                    "description": "Order total",
                    "table_name": "orders",
                },
            ]]
        return [[{"persisted": 2}]]


class _EmbeddingGateway:
    def __init__(self):
        self.inputs = []

    async def embed_texts(self, texts):
        self.inputs = texts
        return [[float(index), 1.0] for index, _ in enumerate(texts)]


class SemanticVectorizationTest(unittest.IsolatedAsyncioTestCase):
    async def test_column_vectorization_is_batched_and_reports_persisted_count(self):
        client = _VectorClient()
        gateway = _EmbeddingGateway()
        params = {
            "graph_owner": "analyzer",
            "schema": "public",
            "db_name": "shopmall",
            "reembed_existing": False,
            "batch_size": 100,
        }

        persisted = await _vectorize_kind(
            client=client,
            embedding_gateway=gateway,
            kind="columns",
            params=params,
        )

        self.assertEqual(persisted, 2)
        self.assertEqual(len(gateway.inputs), 2)
        self.assertIn("Column: customer_id", gateway.inputs[0])
        self.assertEqual(len(client.calls), 2)
        update = client.calls[1][0]
        self.assertEqual(update["parameters"]["graph_owner"], "analyzer")
        self.assertEqual(len(update["parameters"]["items"]), 2)

    def test_target_query_rejects_unknown_kind(self):
        with self.assertRaises(ValueError):
            _target_query("procedures")

    def test_column_text_does_not_emit_empty_description_field(self):
        text = _embedding_text(
            "columns", {"name": "id", "table_name": "orders", "description": ""}
        )
        self.assertEqual(text, "Table: orders | Column: id")


if __name__ == "__main__":
    unittest.main()

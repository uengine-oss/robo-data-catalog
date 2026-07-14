import unittest

from service.data_fabric_client import DataFabricClient


class DataFabricClientTest(unittest.TestCase):
    def test_sample_sql_quotes_schema_and_table_separately(self):
        self.assertEqual(
            DataFabricClient.sample_sql("public.order_items", 5),
            'SELECT * FROM "public"."order_items" LIMIT 5',
        )

    def test_native_query_uses_request_datasource_and_strips_semicolon(self):
        client = DataFabricClient("http://127.0.0.1:8004/", "shopmall")
        self.assertEqual(
            client._as_native_query("SELECT 1;"),
            "SELECT * FROM shopmall (SELECT 1)",
        )

    def test_parse_rows_maps_columns_and_preserves_order(self):
        self.assertEqual(
            DataFabricClient._parse_rows(
                {"columns": ["id", "name"], "data": [[1, "one"], [2, "two"]]}
            ),
            [{"id": 1, "name": "one"}, {"id": 2, "name": "two"}],
        )

    def test_parse_rows_returns_none_for_empty_result(self):
        self.assertIsNone(DataFabricClient._parse_rows({"columns": ["id"], "data": []}))


if __name__ == "__main__":
    unittest.main()

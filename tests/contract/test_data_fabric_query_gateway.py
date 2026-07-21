import unittest

from integrations.data_fabric import (
    DataFabricQueryError,
    DataFabricQueryGateway,
    DataFabricUnavailableError,
)


class _Response:
    status = 200

    async def json(self):
        return {"type": "table", "columns": ["value"], "data": [[1]]}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _RecordingSession:
    def __init__(self):
        self.payload = None

    def post(self, url, *, json, timeout):
        self.payload = json
        return _Response()


class DataFabricQueryGatewayTest(unittest.TestCase):
    def test_sample_sql_quotes_schema_and_table_separately(self):
        self.assertEqual(
            DataFabricQueryGateway.sample_sql("public.order_items", 5),
            'SELECT * FROM "public"."order_items" LIMIT 5',
        )

    def test_datasource_rejects_query_syntax(self):
        with self.assertRaises(ValueError):
            DataFabricQueryGateway("http://127.0.0.1:8404", "shopmall) (DROP DATABASE x")

    def test_datasource_accepts_connector_hyphen(self):
        gateway = DataFabricQueryGateway("http://127.0.0.1:8404", "shop-mall")
        self.assertEqual(gateway.datasource, "shop-mall")

    def test_sample_sql_escapes_identifiers_and_bounds_limit(self):
        self.assertEqual(
            DataFabricQueryGateway.sample_sql('public.order"items', 5000),
            'SELECT * FROM "public"."order""items" LIMIT 1000',
        )

    def test_parse_rows_maps_columns_and_preserves_order(self):
        self.assertEqual(
            DataFabricQueryGateway._parse_rows(
                {"columns": ["id", "name"], "data": [[1, "one"], [2, "two"]]}
            ),
            [{"id": 1, "name": "one"}, {"id": 2, "name": "two"}],
        )

    def test_parse_rows_returns_empty_list_for_empty_result(self):
        self.assertEqual(DataFabricQueryGateway._parse_rows({"columns": ["id"], "data": []}), [])

    def test_parse_rows_rejects_malformed_shape(self):
        with self.assertRaises(DataFabricQueryError):
            DataFabricQueryGateway._parse_rows({"columns": ["id", "name"], "data": [[1]]})


class DataFabricQueryGatewayAsyncTest(unittest.IsolatedAsyncioTestCase):
    async def test_request_separates_datasource_query_and_row_limit(self):
        gateway = DataFabricQueryGateway("http://127.0.0.1:8404", "shopmall")
        session = _RecordingSession()
        rows = await gateway.fetch_rows(session, "SELECT 1;", max_rows=25)
        self.assertEqual(rows, [{"value": 1}])
        self.assertEqual(
            session.payload,
            {"datasource": "shopmall", "query": "SELECT 1", "max_rows": 25},
        )

    async def test_row_limit_must_match_fabric_contract(self):
        gateway = DataFabricQueryGateway("http://127.0.0.1:8404", "shopmall")
        with self.assertRaises(ValueError):
            await gateway.fetch_rows(_RecordingSession(), "SELECT 1", max_rows=1001)

    async def test_unconfigured_gateway_is_an_explicit_failure(self):
        gateway = DataFabricQueryGateway("", "")
        with self.assertRaises(DataFabricUnavailableError):
            await gateway.fetch_rows(_RecordingSession(), "SELECT 1")

    async def test_retry_count_must_be_positive(self):
        gateway = DataFabricQueryGateway("http://127.0.0.1:8404", "shopmall")
        with self.assertRaises(ValueError):
            await gateway.fetch_rows(_RecordingSession(), "SELECT 1", max_retries=0)


if __name__ == "__main__":
    unittest.main()

import unittest

from lineage.sql_extract import LineageInfo, SqlLineageExtractor


class _LineageClient:
    def __init__(self) -> None:
        self.queries = []

    async def execute_queries(self, queries):
        self.queries = queries
        return [
            [] if "ETL_WRITES" in item["query"] else [{"matched": 1}]
            for item in queries
        ]


class SqlLineageExtractorTest(unittest.TestCase):
    def test_plan_is_parameterized_and_counts_every_relation(self):
        extractor = SqlLineageExtractor()
        lineage = LineageInfo(
            etl_name="load_orders",
            source_tables=["sales.orders", "sales.customers"],
            target_tables=["mart.order_summary"],
            is_etl=True,
        )

        queries, planned = extractor._build_persistence_plan(
            [lineage], file_name="load.sql", name_case="lowercase"
        )

        self.assertEqual(
            planned,
            {"etl_nodes": 1, "etl_reads": 2, "etl_writes": 1, "data_flows": 2},
        )
        self.assertEqual(len(queries), 6)
        self.assertTrue(all("graph_owner" in item["parameters"] for item in queries))
        self.assertFalse(any("load_orders" in item["query"] for item in queries))

    def test_invalid_name_case_is_rejected(self):
        with self.assertRaises(ValueError):
            SqlLineageExtractor()._build_persistence_plan(
                [], file_name="", name_case="titlecase"
            )


class SqlLineagePersistenceTest(unittest.IsolatedAsyncioTestCase):
    async def test_stats_report_actual_matches_not_attempted_queries(self):
        client = _LineageClient()
        lineage = LineageInfo(
            etl_name="load_orders",
            source_tables=["orders", "customers"],
            target_tables=["order_summary"],
            is_etl=True,
        )

        stats = await SqlLineageExtractor().save_lineage_to_neo4j(
            client, [lineage], file_name="load.sql"
        )

        self.assertEqual(
            stats,
            {"etl_nodes": 1, "etl_reads": 2, "etl_writes": 0, "data_flows": 2},
        )


if __name__ == "__main__":
    unittest.main()

import json
import unittest

from enrichment.orchestrator import _EnrichmentProgress, _description_events
from integrations.data_fabric import DataFabricUnavailableError


class _FailingGateway:
    async def fetch_rows(self, session, sql, max_rows):
        raise DataFabricUnavailableError("offline")


class _UnusedEnricher:
    async def generate(self, *args, **kwargs):
        raise AssertionError("generate must not run after sample failure")


class EnrichmentOrchestratorTest(unittest.IsolatedAsyncioTestCase):
    async def test_table_failure_is_visible_and_phase_is_partial(self):
        progress = _EnrichmentProgress()
        encoded = [
            event
            async for event in _description_events(
                datasource_name="shopmall",
                tables=[
                    {
                        "table_name": "orders",
                        "schema_name": "public",
                        "columns": [],
                    }
                ],
                session=object(),
                gateway=_FailingGateway(),
                enricher=_UnusedEnricher(),
                progress=progress,
            )
        ]
        events = [json.loads(item) for item in encoded]

        self.assertEqual([event["event"] for event in events], [
            "start", "error", "table_done", "phase_done"
        ])
        self.assertEqual(events[1]["error_type"], "DataFabricUnavailableError")
        self.assertFalse(events[2]["description_persisted"])
        self.assertEqual(events[3]["errors"], 1)
        self.assertEqual(progress.error_count, 1)


if __name__ == "__main__":
    unittest.main()

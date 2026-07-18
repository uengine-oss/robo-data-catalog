import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from enrichment.description import MetadataResponseError, TableDescriptionEnricher


class DescriptionEnrichmentTest(unittest.IsolatedAsyncioTestCase):
    def test_prompt_is_project_neutral_and_preserves_quality_rubric(self):
        prompt = TableDescriptionEnricher._build_prompt(
            table_name="asset_record",
            schema_name="public",
            sample_data=[{"status_code": "A", "amount": 42}],
            columns_info=[
                {"name": "status_code", "dtype": "varchar", "description": "처리 상태"},
                {"name": "amount", "dtype": "number"},
            ],
        )

        for forbidden in (
            "BNB_CODE", "LOCGOV", "RDIBONBU", "RDISAUP", "수자원 시스템",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, prompt)

        for required in (
            "DDL 주석이 있는 컬럼", "원본 데이터베이스의 정답",
            "DDL 주석이 없는 컬럼", "[추정]", "추정 근거",
            "가능한 대체 해석", "권장 행동", "약어",
            "처리 상태", "status_code", "'amount': 42",
        ):
            with self.subTest(required=required):
                self.assertIn(required, prompt)

    async def test_provider_failure_is_not_converted_to_empty_success(self):
        create = AsyncMock(side_effect=TypeError("incompatible provider request"))
        openai_client = SimpleNamespace(
            chat=SimpleNamespace(completions=SimpleNamespace(create=create))
        )
        service = TableDescriptionEnricher(client=SimpleNamespace(), openai_client=openai_client)

        with self.assertRaisesRegex(TypeError, "incompatible provider request"):
            await service.generate(
                table_name="comm_code",
                schema_name="public",
                sample_data=[{"code": "A"}],
                columns_info=[{"name": "code", "dtype": "text"}],
            )

    async def test_empty_provider_content_is_an_explicit_contract_failure(self):
        response = SimpleNamespace(
            choices=[SimpleNamespace(
                finish_reason="length",
                message=SimpleNamespace(content=None),
            )]
        )
        create = AsyncMock(return_value=response)
        openai_client = SimpleNamespace(
            chat=SimpleNamespace(completions=SimpleNamespace(create=create))
        )
        service = TableDescriptionEnricher(client=SimpleNamespace(), openai_client=openai_client)

        with self.assertRaisesRegex(MetadataResponseError, "finish_reason=length"):
            await service.generate(
                table_name="comm_code",
                schema_name="public",
                sample_data=[{"code": "A"}],
                columns_info=[{"name": "code", "dtype": "text"}],
            )

    async def test_persist_uses_public_fallback_and_actual_update_counts(self):
        client = SimpleNamespace(
            execute_queries=AsyncMock(side_effect=[
                [[{"updated": 1}]],
                [[{"updated": 0}]],
                [[{"updated": 1}]],
            ])
        )
        service = TableDescriptionEnricher(client=client, openai_client=SimpleNamespace())

        updated = await service.persist(
            datasource="shopmall",
            table_name="comm_code",
            schema_name="public",
            descriptions={
                "table_description": "공통 코드를 저장합니다.",
                "column_descriptions": {"code": "코드", "name": "이름"},
            },
        )

        self.assertEqual(updated, (1, 1))
        queries = [call.args[0][0] for call in client.execute_queries.await_args_list]
        for query in queries:
            self.assertIn("coalesce(t.db, t.datasource) = $datasource", query["query"])
            self.assertEqual(query["params"]["datasource"], "shopmall")
            self.assertIn("$schema_name = 'public'", query["query"])
            self.assertIn("graph_owner", query["params"])


if __name__ == "__main__":
    unittest.main()

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from external.llm import create_metadata_llm_client


class MetadataLlmGatewayTest(unittest.TestCase):
    def test_compatible_provider_base_is_applied_once(self):
        configured = SimpleNamespace(llm=SimpleNamespace(api_base="http://provider.test/v1"))
        with (
            patch("external.llm.CATALOG_SETTINGS", configured),
            patch("external.llm.AsyncOpenAI") as constructor,
        ):
            create_metadata_llm_client("request-key")
        constructor.assert_called_once_with(
            api_key="request-key", base_url="http://provider.test/v1"
        )

    def test_empty_provider_base_uses_sdk_default(self):
        configured = SimpleNamespace(llm=SimpleNamespace(api_base=""))
        with (
            patch("external.llm.CATALOG_SETTINGS", configured),
            patch("external.llm.AsyncOpenAI") as constructor,
        ):
            create_metadata_llm_client("request-key")
        constructor.assert_called_once_with(api_key="request-key")


if __name__ == "__main__":
    unittest.main()

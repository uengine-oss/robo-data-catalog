import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.external.openai_client import create_openai_client


class OpenAIClientTest(unittest.TestCase):
    def test_compatible_provider_base_is_applied_once(self):
        configured = SimpleNamespace(llm=SimpleNamespace(api_base="http://provider.test/v1"))
        with (
            patch("app.external.openai_client.settings", configured),
            patch("app.external.openai_client.AsyncOpenAI") as constructor,
        ):
            create_openai_client("request-key")
        constructor.assert_called_once_with(
            api_key="request-key", base_url="http://provider.test/v1"
        )

    def test_empty_provider_base_uses_sdk_default(self):
        configured = SimpleNamespace(llm=SimpleNamespace(api_base=""))
        with (
            patch("app.external.openai_client.settings", configured),
            patch("app.external.openai_client.AsyncOpenAI") as constructor,
        ):
            create_openai_client("request-key")
        constructor.assert_called_once_with(api_key="request-key")


if __name__ == "__main__":
    unittest.main()

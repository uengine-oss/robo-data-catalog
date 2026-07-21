"""Single construction boundary for OpenAI-compatible clients."""

from openai import AsyncOpenAI

from shared.config.settings import CATALOG_SETTINGS


def create_metadata_llm_client(api_key: str) -> AsyncOpenAI:
    """Create a client from the shared provider base and a request-scoped key."""
    options: dict[str, str] = {"api_key": api_key}
    if CATALOG_SETTINGS.llm.api_base:
        options["base_url"] = CATALOG_SETTINGS.llm.api_base
    return AsyncOpenAI(**options)

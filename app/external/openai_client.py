"""Single construction boundary for OpenAI-compatible clients."""

from openai import AsyncOpenAI

from app.system.settings import settings


def create_openai_client(api_key: str) -> AsyncOpenAI:
    """Create a client from the shared provider base and a request-scoped key."""
    options: dict[str, str] = {"api_key": api_key}
    if settings.llm.api_base:
        options["base_url"] = settings.llm.api_base
    return AsyncOpenAI(**options)

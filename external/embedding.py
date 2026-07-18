"""Embedding client for text vectorization"""
from typing import List, Optional
from openai import AsyncOpenAI

from settings import CATALOG_SETTINGS


class CatalogEmbeddingGateway:
    def __init__(self, client: Optional[AsyncOpenAI] = None):
        self.client = client
        self.model = CATALOG_SETTINGS.llm.embedding_model

    async def embed_text(self, text: str) -> List[float]:
        if not self.client:
            raise RuntimeError("embedding client is not configured")
        embeddings = await self.embed_texts([text])
        return embeddings[0]

    async def embed_texts(self, texts: List[str]) -> List[List[float]]:
        if not self.client:
            raise RuntimeError("embedding client is not configured")
        if not texts:
            return []
        response = await self.client.embeddings.create(
            model=self.model,
            input=texts,
            encoding_format="float"
        )
        embeddings = [item.embedding for item in response.data]
        if len(embeddings) != len(texts):
            raise RuntimeError("embedding response count does not match input count")
        return embeddings

    @staticmethod
    def format_table_text(
        table_name: str,
        description: str = "",
        columns: Optional[List[str]] = None,
    ) -> str:
        parts = [f"Table: {table_name}"]
        if description:
            parts.append(f"Description: {description}")
        if columns:
            parts.append(f"Columns: {', '.join(columns)}")
        return " | ".join(parts)

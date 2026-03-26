"""Embedding client for text vectorization"""
from typing import List, Optional
from openai import AsyncOpenAI

from config.settings import settings


class EmbeddingClient:
    def __init__(self, client: Optional[AsyncOpenAI] = None):
        self.client = client
        self.model = settings.llm.embedding_model

    async def embed_text(self, text: str) -> List[float]:
        if not self.client:
            return []
        response = await self.client.embeddings.create(
            model=self.model,
            input=text,
            encoding_format="float"
        )
        return response.data[0].embedding

    @staticmethod
    def format_table_text(table_name: str, description: str = "", columns: List[str] = None) -> str:
        parts = [f"Table: {table_name}"]
        if description:
            parts.append(f"Description: {description}")
        if columns:
            parts.append(f"Columns: {', '.join(columns)}")
        return " | ".join(parts)

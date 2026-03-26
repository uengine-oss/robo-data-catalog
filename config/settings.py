"""ROBO Data Catalog 환경변수 설정"""

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=project_root / ".env")


def _get_base_dir() -> str:
    if os.getenv("DOCKER_COMPOSE_CONTEXT"):
        return os.getenv("DOCKER_COMPOSE_CONTEXT")
    return str(Path(__file__).resolve().parents[2])


@dataclass(frozen=True)
class Neo4jConfig:
    uri: str = field(default_factory=lambda: os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687"))
    user: str = field(default_factory=lambda: os.getenv("NEO4J_USER", "neo4j"))
    password: str = field(default_factory=lambda: os.getenv("NEO4J_PASSWORD", "neo4j"))
    database: str = "neo4j"


@dataclass(frozen=True)
class LLMConfig:
    api_key: str = field(default_factory=lambda: os.getenv("LLM_API_KEY", ""))
    model: str = field(default_factory=lambda: os.getenv("LLM_MODEL", "gpt-4.1"))
    embedding_model: str = field(default_factory=lambda: os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"))


@dataclass(frozen=True)
class BatchConfig:
    neo4j_query_batch_size: int = field(default_factory=lambda: int(os.getenv("NEO4J_QUERY_BATCH_SIZE", "30")))


@dataclass(frozen=True)
class PathConfig:
    base_dir: str = field(default_factory=_get_base_dir)

    @property
    def data_dir(self) -> str:
        return os.path.join(self.base_dir, "data")


@dataclass(frozen=True)
class MetadataEnrichmentConfig:
    text2sql_api_url: str = field(default_factory=lambda: os.getenv("TEXT2SQL_API_URL", ""))
    fk_inference_enabled: bool = field(default_factory=lambda: os.getenv("FK_INFERENCE_ENABLED", "true").lower() == "true")
    fk_sample_size: int = field(default_factory=lambda: int(os.getenv("FK_SAMPLE_SIZE", "25")))
    fk_similarity_threshold: float = field(default_factory=lambda: float(os.getenv("FK_SIMILARITY_THRESHOLD", "0.8")))
    fk_match_ratio_threshold: float = field(default_factory=lambda: float(os.getenv("FK_MATCH_RATIO_THRESHOLD", "0.8")))
    fk_concurrency: int = field(default_factory=lambda: int(os.getenv("FK_CONCURRENCY", "5")))
    timeout_connect: int = field(default_factory=lambda: int(os.getenv("METADATA_TIMEOUT_CONNECT", "5")))
    timeout_request: int = field(default_factory=lambda: int(os.getenv("METADATA_TIMEOUT_REQUEST", "30")))


@dataclass(frozen=True)
class CatalogConfig:
    neo4j: Neo4jConfig = field(default_factory=Neo4jConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    batch: BatchConfig = field(default_factory=BatchConfig)
    path: PathConfig = field(default_factory=PathConfig)
    metadata_enrichment: MetadataEnrichmentConfig = field(default_factory=MetadataEnrichmentConfig)

    version: str = "2.0.0"
    api_prefix: str = "/robo"
    host: str = field(default_factory=lambda: os.getenv("HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(os.getenv("PORT", "5503")))


@lru_cache(maxsize=1)
def get_settings() -> CatalogConfig:
    return CatalogConfig()


settings = get_settings()

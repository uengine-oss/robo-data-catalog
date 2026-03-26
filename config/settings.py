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
class CatalogConfig:
    neo4j: Neo4jConfig = field(default_factory=Neo4jConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    batch: BatchConfig = field(default_factory=BatchConfig)
    path: PathConfig = field(default_factory=PathConfig)

    version: str = "2.0.0"
    api_prefix: str = "/robo"
    host: str = field(default_factory=lambda: os.getenv("HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(os.getenv("PORT", "5503")))


@lru_cache(maxsize=1)
def get_settings() -> CatalogConfig:
    return CatalogConfig()


settings = get_settings()

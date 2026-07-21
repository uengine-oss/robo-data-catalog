"""ROBO Data Catalog 환경변수 설정"""

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# .env 파일 로드 (프로젝트 내부 → 상위 디렉토리 순으로 탐색)
project_root = Path(__file__).resolve().parents[4]
_env_path = project_root / ".env"
if not _env_path.exists():
    _env_path = find_dotenv(usecwd=True)
load_dotenv(dotenv_path=_env_path)


def _strict_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized not in {"true", "false"}:
        raise ValueError(f"{name} must be true or false")
    return normalized == "true"


def _bounded_int(name: str, default: int, minimum: int, maximum: int) -> int:
    value = int(os.getenv(name, str(default)))
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _bounded_float(name: str, default: float, minimum: float, maximum: float) -> float:
    value = float(os.getenv(name, str(default)))
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _resolve_catalog_base_directory() -> str:
    if os.getenv("DOCKER_COMPOSE_CONTEXT"):
        return os.getenv("DOCKER_COMPOSE_CONTEXT")
    return str(Path(__file__).resolve().parents[4])


def _load_catalog_cors_origins() -> tuple[str, ...]:
    raw = os.getenv(
        "CATALOG_CORS_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000,http://localhost:3003,http://127.0.0.1:3003",
    )
    return tuple(origin.strip() for origin in raw.split(",") if origin.strip())


@dataclass(frozen=True)
class CatalogGraphDatabaseSettings:
    uri: str = field(default_factory=lambda: os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687"))
    user: str = field(default_factory=lambda: os.getenv("NEO4J_USER", "neo4j"))
    password: str = field(default_factory=lambda: os.getenv("NEO4J_PASSWORD", "neo4j"))
    database: str = field(default_factory=lambda: os.getenv("NEO4J_DATABASE", "neo4j"))

    def __post_init__(self) -> None:
        if self.database.lower() == "system":
            raise ValueError("NEO4J_DATABASE cannot be system")


@dataclass(frozen=True)
class CatalogLlmSettings:
    api_key: str = field(default_factory=lambda: os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY", ""))
    api_base: str = field(default_factory=lambda: os.getenv("LLM_API_BASE", "").rstrip("/"))
    model: str = field(default_factory=lambda: os.getenv("LLM_MODEL", "gpt-4.1"))
    max_completion_tokens: int = field(
        default_factory=lambda: _bounded_int("LLM_MAX_COMPLETION_TOKENS", 4096, 1, 1_000_000)
    )
    embedding_model: str = field(default_factory=lambda: os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"))


@dataclass(frozen=True)
class CatalogStorageSettings:
    base_dir: str = field(default_factory=_resolve_catalog_base_directory)

    @property
    def data_dir(self) -> str:
        return os.path.join(self.base_dir, "data")


@dataclass(frozen=True)
class MetadataEnrichmentSettings:
    data_fabric_url: str = field(default_factory=lambda: os.getenv("DATA_FABRIC_URL", ""))
    fk_inference_enabled: bool = field(default_factory=lambda: _strict_bool("FK_INFERENCE_ENABLED", True))
    fk_sample_size: int = field(default_factory=lambda: _bounded_int("FK_SAMPLE_SIZE", 25, 1, 10_000))
    fk_similarity_threshold: float = field(default_factory=lambda: _bounded_float("FK_SIMILARITY_THRESHOLD", 0.8, 0.0, 1.0))
    fk_match_ratio_threshold: float = field(default_factory=lambda: _bounded_float("FK_MATCH_RATIO_THRESHOLD", 0.8, 0.0, 1.0))
    fk_concurrency: int = field(default_factory=lambda: _bounded_int("FK_CONCURRENCY", 5, 1, 100))
    timeout_connect: int = field(default_factory=lambda: _bounded_int("METADATA_TIMEOUT_CONNECT", 5, 1, 300))
    timeout_request: int = field(default_factory=lambda: _bounded_int("METADATA_TIMEOUT_REQUEST", 30, 1, 3600))


@dataclass(frozen=True)
class CatalogSettings:
    graph_database: CatalogGraphDatabaseSettings = field(default_factory=CatalogGraphDatabaseSettings)
    llm: CatalogLlmSettings = field(default_factory=CatalogLlmSettings)
    storage: CatalogStorageSettings = field(default_factory=CatalogStorageSettings)
    metadata_enrichment: MetadataEnrichmentSettings = field(default_factory=MetadataEnrichmentSettings)

    version: str = "2.0.0"
    api_prefix: str = "/robo"
    host: str = field(default_factory=lambda: os.getenv("HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: _bounded_int("PORT", 15503, 1, 65535))
    cors_origins: tuple[str, ...] = field(default_factory=_load_catalog_cors_origins)
    allow_neo4j_header_override: bool = field(
        default_factory=lambda: _strict_bool("CATALOG_ALLOW_NEO4J_HEADER_OVERRIDE", False)
    )


@lru_cache(maxsize=1)
def load_catalog_settings() -> CatalogSettings:
    return CatalogSettings()


CATALOG_SETTINGS = load_catalog_settings()

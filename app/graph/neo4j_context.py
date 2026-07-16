"""요청별 Neo4j 연결 override — Electron 호스트가 헤더로 전달한 연결을 요청 컨텍스트에 싣는다.

설계 원칙:
- 진실의 기본값은 ``settings.neo4j``(.env). 브라우저/테스트/CLI 는 이 기본값을 그대로 쓴다.
- Electron 모드에서는 데스크톱이 고른 연결이 ``X-Neo4j-*`` 헤더로 들어온다.
  라우터 레벨 dependency 가 이를 읽어 ``set_override`` 로 **contextvar** 에 싣고,
  같은 요청 컨텍스트에서 실행되는 핸들러/서비스의 ``Neo4jClient`` 가 그 값을 본다.
- contextvar 는 요청별 격리 — 요청 간 오염 없음. 미설정 시 ``None`` → settings 폴백.
"""
from __future__ import annotations

import contextvars
from dataclasses import dataclass
from typing import Mapping, Optional
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Neo4jOverride:
    """요청이 실어 보낸 Neo4j 연결. ``database`` 미지정이면 호출측이 settings 기본 사용."""

    uri: str
    user: str
    password: str
    database: Optional[str] = None

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> Optional["Neo4jOverride"]:
        """``X-Neo4j-*`` 헤더 → override. URI 없으면(=브라우저/테스트) None → settings 폴백.

        Starlette ``request.headers`` 는 대소문자 무시 — 소문자 키로 조회한다.
        """
        uri = headers.get("x-neo4j-uri")
        if not uri:
            return None
        parsed = urlsplit(uri)
        if parsed.scheme not in {"bolt", "bolt+s", "bolt+ssc", "neo4j", "neo4j+s", "neo4j+ssc"}:
            raise ValueError("unsupported Neo4j URI scheme")
        if not parsed.hostname or parsed.username or parsed.password or len(uri) > 2048:
            raise ValueError("invalid Neo4j URI")
        return cls(
            uri=uri,
            user=headers.get("x-neo4j-user", "neo4j"),
            password=headers.get("x-neo4j-password", ""),
            database=headers.get("x-neo4j-database") or None,
        )


_neo4j_override: contextvars.ContextVar[Optional[Neo4jOverride]] = contextvars.ContextVar(
    "neo4j_override", default=None
)


def set_override(conn: Optional[Neo4jOverride]) -> None:
    """현재 요청 컨텍스트의 Neo4j override 설정 (라우터 dependency 에서 1회)."""
    _neo4j_override.set(conn)


def get_override() -> Optional[Neo4jOverride]:
    """현재 컨텍스트의 Neo4j override 조회 (Neo4jClient 가 사용). 없으면 None → settings."""
    return _neo4j_override.get()

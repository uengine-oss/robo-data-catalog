"""요청별 Neo4j 연결 override — Electron 호스트가 헤더로 전달한 연결을 요청 컨텍스트에 싣는다.

설계 원칙:
- 진실의 기본값은 ``CATALOG_SETTINGS.graph_database``(.env)다.
- Electron 모드에서는 데스크톱이 고른 연결이 ``X-Neo4j-*`` 헤더로 들어온다.
  HTTP dependency가 이를 읽어 ``set_request_graph_connection``으로 contextvar에 싣고,
  같은 요청의 ``CatalogGraphDatabase``가 그 값을 본다.
- contextvar는 요청별로 격리되며, 미설정 시 Catalog 환경 설정을 사용한다.
"""
from __future__ import annotations

import contextvars
from dataclasses import dataclass
from typing import Mapping, Optional
from urllib.parse import urlsplit

from shared.config.settings import CATALOG_SETTINGS


@dataclass(frozen=True)
class RequestGraphConnection:
    """요청이 실어 보낸 Neo4j 연결. ``database`` 미지정이면 호출측이 settings 기본 사용."""

    uri: str
    user: str
    password: str
    database: Optional[str] = None

    def __post_init__(self) -> None:
        parsed = urlsplit(self.uri)
        if parsed.scheme not in {"bolt", "bolt+s", "bolt+ssc", "neo4j", "neo4j+s", "neo4j+ssc"}:
            raise ValueError("unsupported Neo4j URI scheme")
        if not parsed.hostname or parsed.username or parsed.password or len(self.uri) > 2048:
            raise ValueError("invalid Neo4j URI")
        if self.database and self.database.lower() == "system":
            raise ValueError("Neo4j system database is forbidden")

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> Optional["RequestGraphConnection"]:
        """``X-Neo4j-*`` 헤더 → override. URI 없으면(=브라우저/테스트) None → settings 폴백.

        Starlette ``request.headers`` 는 대소문자 무시 — 소문자 키로 조회한다.
        """
        uri = headers.get("x-neo4j-uri")
        if not uri:
            # **대상 graph 만 정해 주는 호출자가 있다.** 브라우저에서 도는 화면은
            # 연결 자격을 클라이언트로 내보내지 않으므로 URI·비밀번호를 실을 수 없고,
            # 프로젝트마다 graph 를 가르기 위해 `X-Neo4j-Database` 만 보낸다
            # (`robo-data-frontend` 의 `stores/session.ts` 가 붙이는 유일한 헤더다).
            #
            # 여기서 `None` 을 돌려주면 그 요청이 **프로세스 환경에 고정된 graph** 로
            # 가고, 남의 분석이 자기 것처럼 나온다. **오류가 안 난다** — 2026-09-17
            # 실측: 3건·7건을 심은 두 graph 를 헤더로 각각 물었는데 둘 다 0 이 왔다
            # (환경 graph 의 값). 403 도 안 났다. 플래그 검사는 `None` 을 안 보기 때문이다.
            database = headers.get("x-neo4j-database") or None
            if not database:
                return None
            if database.lower() == "system":
                raise ValueError("Neo4j system database is forbidden")
            fallback = CATALOG_SETTINGS.graph_database
            return cls(uri=fallback.uri, user=fallback.user,
                       password=fallback.password, database=database)
        parsed = urlsplit(uri)
        if parsed.scheme not in {"bolt", "bolt+s", "bolt+ssc", "neo4j", "neo4j+s", "neo4j+ssc"}:
            raise ValueError("unsupported Neo4j URI scheme")
        if not parsed.hostname or parsed.username or parsed.password or len(uri) > 2048:
            raise ValueError("invalid Neo4j URI")
        database = headers.get("x-neo4j-database") or None
        if database and database.lower() == "system":
            raise ValueError("Neo4j system database is forbidden")
        return cls(
            uri=uri,
            user=headers.get("x-neo4j-user", "neo4j"),
            password=headers.get("x-neo4j-password", ""),
            database=database,
        )


_request_graph_connection: contextvars.ContextVar[Optional[RequestGraphConnection]] = contextvars.ContextVar(
    "catalog_request_graph_connection", default=None
)


def set_request_graph_connection(connection: Optional[RequestGraphConnection]) -> None:
    """현재 요청 컨텍스트의 Neo4j override 설정 (라우터 dependency 에서 1회)."""
    _request_graph_connection.set(connection)


def get_request_graph_connection() -> Optional[RequestGraphConnection]:
    """현재 요청이 선택한 그래프 연결을 반환한다."""
    return _request_graph_connection.get()

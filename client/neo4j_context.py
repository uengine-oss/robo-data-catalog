"""요청이 정한 대상 graph — `X-Neo4j-Database`.

분석 결과는 프로젝트마다 다른 graph 에 산다. 그런데 이 서비스의 대상 graph 는
`NEO4J_DATABASE` 하나로 프로세스에 고정돼 있어, 어느 프로젝트를 보고 있든 늘 같은
곳을 읽었다 — **오류가 아니라 남의 분석이 자기 것처럼 나온다.**

`Neo4jClient` 는 이미 `database` 인자를 받는데 20곳 넘게 인자 없이 만들고 있다.
전부 고치는 대신 **요청 컨텍스트 한 곳**으로 잡는다. `robo-data-fabric` 의
`registry/connection.py` 와 같은 모양이다.

헤더가 없으면 아무 일도 없다 — 단독 실행과 기존 호출자는 그대로 `settings` 를 쓴다.
"""

from __future__ import annotations

import contextvars
from typing import Optional

__all__ = ["set_database", "get_database", "DatabaseScopeMiddleware", "FORBIDDEN"]

# 여기로 향하면 저장소 메타를 건드린다. 실수로도 못 가게 막는다.
FORBIDDEN = {"system"}

_database: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "catalog_neo4j_database", default=None
)


def set_database(name: Optional[str]) -> None:
    _database.set(name or None)


def get_database() -> Optional[str]:
    """이 요청이 봐야 할 graph. 없으면 None → `settings.neo4j.database`."""
    return _database.get()


class DatabaseScopeMiddleware:
    """`X-Neo4j-Database` 를 요청 컨텍스트에 싣는다 (순수 ASGI).

    `BaseHTTPMiddleware` 를 쓰지 않는 이유는 그쪽이 요청을 별도 태스크로 넘겨
    `contextvars` 가 핸들러까지 그대로 이어지지 않는 경우가 있어서다. 여기서는
    이어지는 것이 전부라 그 위험을 지지 않는다.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        value = ""
        for key, raw in scope.get("headers") or []:
            if key == b"x-neo4j-database":
                value = raw.decode("latin-1").strip()
                break
        if value.lower() in FORBIDDEN:
            value = ""
        token = _database.set(value or None)
        try:
            await self.app(scope, receive, send)
        finally:
            _database.reset(token)

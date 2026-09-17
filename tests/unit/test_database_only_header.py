"""`X-Neo4j-Database` 만 실은 요청이 그 graph 를 보는가.

**화면이 보내는 헤더는 그것 하나다.** 브라우저에서 도는 화면은 연결 자격을
클라이언트로 내보내지 않으므로 URI·비밀번호를 실을 수 없다
(`robo-data-frontend` 의 `stores/session.ts` — 붙이는 헤더가 `X-Neo4j-Database`
하나다).

그런데 `from_headers` 가 URI 없으면 `None` 을 돌려줬다. 그러면 그 요청은 **프로세스
환경에 고정된 graph** 로 간다. 남의 분석이 자기 것처럼 나오고, **오류가 안 난다.**

실측(2026-09-17, 설치본 스택):

::

    zz_hdr_a 에 3건 · zz_hdr_b 에 7건을 심고 헤더로 각각 물었다
    X-Neo4j-Database: zz_hdr_a  →  {"hasData":false,"nodeCount":0}
    X-Neo4j-Database: zz_hdr_b  →  {"hasData":false,"nodeCount":0}
    헤더 없음                   →  {"hasData":false,"nodeCount":0}

셋이 같다. **403 도 안 났다** — override 플래그 검사는 `None` 을 안 보기 때문이다.
켜고 끄는 스위치가 있는데 정작 실제로 오는 헤더는 그 스위치를 지나가지 않았다.
"""
import unittest

from graph.connection import RequestGraphConnection
from shared.config.settings import CATALOG_SETTINGS


class DatabaseOnlyHeaderTest(unittest.TestCase):
    def test_database_only_header_selects_that_graph(self):
        override = RequestGraphConnection.from_headers({"x-neo4j-database": "zz_hdr_a"})
        self.assertIsNotNone(override, "database 단독 헤더가 무시된다")
        self.assertEqual("zz_hdr_a", override.database)

    def test_database_only_header_borrows_the_configured_connection(self):
        """자격은 환경에서 온다 — 화면이 비밀번호를 들고 있지 않다."""
        override = RequestGraphConnection.from_headers({"x-neo4j-database": "zz_hdr_a"})
        configured = CATALOG_SETTINGS.graph_database
        self.assertEqual(configured.uri, override.uri)
        self.assertEqual(configured.user, override.user)
        self.assertEqual(configured.password, override.password)

    def test_no_header_still_falls_back_to_settings(self):
        """브라우저·검사·CLI 의 동작이 바뀌면 안 된다."""
        self.assertIsNone(RequestGraphConnection.from_headers({}))
        self.assertIsNone(RequestGraphConnection.from_headers({"x-neo4j-database": ""}))

    def test_system_database_is_refused_without_uri_too(self):
        """URI 가 있을 때만 막으면 반쪽이다 — 실제로 오는 요청에는 URI 가 없다."""
        with self.assertRaises(ValueError):
            RequestGraphConnection.from_headers({"x-neo4j-database": "system"})
        with self.assertRaises(ValueError):
            RequestGraphConnection.from_headers({"x-neo4j-database": "SYSTEM"})


if __name__ == "__main__":
    unittest.main()

import asyncio

import service.graph_query_service as graph_query_service


def test_system_nodes_are_excluded_from_user_graph_predicate() -> None:
    predicate = graph_query_service._visible_node_predicate("n")
    assert graph_query_service.SYSTEM_GRAPH_NODE_LABELS == frozenset({"GLOSSARY", "EMBED_META"})
    assert "NOT n:GLOSSARY" in predicate
    assert "NOT n:EMBED_META" in predicate

    try:
        graph_query_service._visible_node_predicate("n) RETURN n")
    except ValueError:
        pass
    else:
        raise AssertionError("Cypher alias validation must reject non-identifiers")


class _RecordingNeo4jClient:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.closed = False

    async def execute_queries(self, queries: list[str]) -> list[list[dict]]:
        self.calls.append(queries)
        if queries[0].startswith("CALL db.propertyKeys"):
            return [[]]
        if "RETURN count(__cy_n__) as count" in queries[0]:
            return [[{"count": 0}]]
        return [[] for _ in queries]

    async def close(self) -> None:
        self.closed = True


async def test_all_user_graph_queries_apply_the_system_boundary() -> None:
    clients: list[_RecordingNeo4jClient] = []

    def client_factory() -> _RecordingNeo4jClient:
        client = _RecordingNeo4jClient()
        clients.append(client)
        return client

    original = graph_query_service.Neo4jClient
    graph_query_service.Neo4jClient = client_factory  # type: ignore[assignment]
    try:
        exists = await graph_query_service.check_graph_data_exists()
        payload = await graph_query_service.fetch_graph_data()
    finally:
        graph_query_service.Neo4jClient = original

    assert exists == {"hasData": False, "nodeCount": 0}
    assert payload == {"Nodes": [], "Relationships": []}
    assert len(clients) == 2 and all(client.closed for client in clients)

    count_query = clients[0].calls[0][0]
    node_query, rel_query = clients[1].calls[1]
    assert "NOT __cy_n__:GLOSSARY" in count_query and "NOT __cy_n__:EMBED_META" in count_query
    assert "NOT __cy_n__:GLOSSARY" in node_query and "NOT __cy_n__:EMBED_META" in node_query
    for alias in ("__cy_a__", "__cy_b__"):
        assert f"NOT {alias}:GLOSSARY" in rel_query
        assert f"NOT {alias}:EMBED_META" in rel_query


if __name__ == "__main__":
    test_system_nodes_are_excluded_from_user_graph_predicate()
    asyncio.run(test_all_user_graph_queries_apply_the_system_boundary())
    print("[OK] system graph node boundary")

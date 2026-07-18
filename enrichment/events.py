"""NDJSON event encoding for metadata enrichment."""
import json


def ndjson(payload: dict) -> bytes:
    return (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")

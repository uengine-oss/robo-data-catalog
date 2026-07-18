"""Fail-closed public Catalog error payloads."""


def error_body(error: Exception) -> dict:
    return {"detail": "Catalog operation failed", "error_type": type(error).__name__}

# Implementation Plan: Shared Layout

## Constitution Check

The change affects infrastructure placement only. Catalog remains a read/edit/provider layer over Analyzer-owned graph data, preserves user-edit isolation, and does not change HTTP contracts.

## Target Tree

```text
shared/
├─ config/settings.py
└─ observability/logger.py
samples/{context,resolver}.py
```

## Slices

1. Freeze clean baseline and CodeGraph consumers.
2. Move config and all consumers; focused tests and old import 0.
3. Move observability and all consumers; logging tests/import smoke and old import 0.
4. Move `table_samples` to `samples`; update API/tests/docs; contract tests and old path 0.
5. Full tests, compile, structure ledger, CodeGraph sync, diff audit.

# Implementation Plan: Runtime Boundaries

1. Record all top-level folder roles and consumers.
2. Move `external` to `integrations` and update every consumer in one slice.
3. Run focused and full tests, compile/import, old-path grep, CodeGraph, and startup.

All other runtime roots are retained after consumer-based review.

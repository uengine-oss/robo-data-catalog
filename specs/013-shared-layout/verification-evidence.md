# Verification Evidence

## Baseline

- HEAD: `bad908d7`
- Full suite before the slice: 40 tests passed.
- Consumer audit: settings crossed API, graph, enrichment, and external domains; observability crossed enrichment, lineage, external, samples, and bootstrap domains; samples were a cohesive domain.

## Slice Evidence

- Config/observability/samples focus suite ran 9 tests and passed.
- Moving settings preserved both existing `Path(__file__)`-derived base paths by adjusting the parent depth.
- Imports of `main`, `samples.context`, `shared.config.settings`, and `shared.observability.logger` passed.
- Active root settings, root observability, and `table_samples` import/path grep returned 0.
- Final suite: `python -m unittest discover -s tests -t . -p "test_*.py"` ran 40 tests and passed.
- Compileall over `main.py api contracts enrichment external graph lineage samples search shared tests` passed.
- Hygiene: `git diff --check` passed.
- CodeGraph: sync completed with 66 files, 660 nodes, 991 edges; index reported up to date.

The repository has no separate structure-verifier command; the final-tree ledger below plus import, compile, old-path, and full-suite checks cover this slice.

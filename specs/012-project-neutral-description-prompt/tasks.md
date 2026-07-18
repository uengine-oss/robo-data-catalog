# Tasks

- [x] T001 Add red neutrality and retained-quality prompt tests.
- [x] T002 Generalize the shared prompt without weakening its evidence rubric.
- [x] T003 Run focused/full/compile/diff and forbidden-hint gates.
- [x] T004 Record audit evidence before the RWIS run.

## Evidence

- Red: the new contract reported all five known customer/project hints in the
  shared prompt.
- Focused: `python -m unittest tests.unit.test_table_description_enrichment -v`
  passed 4/4.
- Full: `python -m unittest discover -s tests -t . -p "test*.py"` passed
  25/25.
- `python -m compileall -q app tests` and `git diff --check` passed.
- Product-code search for `BNB_CODE|LOCGOV|RDIBONBU|RDISAUP|수자원 시스템`
  returned zero matches (the explicit negative-test fixture was excluded).
- The Catalog inventory generator was intentionally not persisted: it exposes
  30 pre-existing v10 rename-WIP paths plus `.codegraph` as unclassified or
  pending. The generated ledger delta was reversed immediately, leaving the
  user-owned v10 audit work unchanged.

# Feature Specification: Project-neutral Description Prompt

**Created**: 2026-07-18
**Status**: Complete

## Problem

The shared table-description prompt contains customer-specific column names,
abbreviation expansions, and a water-domain assumption. Those hints can make
unrelated schemas look plausible while contaminating RWIS evaluation.

## Requirements

- The prompt contains no customer/project identifiers or preselected domain.
- DDL comments remain authoritative and may be paraphrased without inventing
  meaning.
- Missing-comment descriptions start with `[추정]` and state uncertainty,
  input-derived evidence, alternatives, and a verification action.
- Cross-column evidence is used only when it is present in the supplied DDL
  comments or sample values; abbreviations remain ambiguous unless evidenced.
- Table descriptions follow the same evidence hierarchy and uncertainty rule.

## Acceptance

Automated prompt tests reject every known customer hint and prove that the
general quality rubric, supplied DDL comments, samples, uncertainty,
alternatives, and verification actions remain present. Catalog full tests and
compile/diff gates pass.

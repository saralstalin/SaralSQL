# Parser Improvements Tracker

Keep this list current as we discover parser gaps or remove workarounds.
Each item should describe the parser behavior we want, not the temporary fix in the LSP.

## Current Parser Improvements

1. Improve derived-table lineage for outer alias column definitions.
   - Example: `SELECT a.SomeName FROM (SELECT e.FirstName AS SomeName ...) a`
   - Goal: expose the projected column range and definition cleanly from the parser scope/lineage.

2. Improve APPLY alias classification for non-table expressions.
   - Example: `CROSS APPLY STRING_SPLIT(...) ss`
   - Goal: make parser scope identify APPLY aliases as derived sources with clear lineage metadata so the LSP does not need schema-validation workarounds.

3. Improve resolution metadata for correlated subquery and APPLY flows.
   - Example: outer references such as `e.FirstName` inside APPLY and subquery blocks.
   - Goal: provide reliable `columns.resolutions` entries for correlated expressions.

4. Improve output-column exposure for derived tables, CTEs, PIVOT, and UNPIVOT.
   - Goal: ensure projected column names are available consistently for hover, go to definition, references, and completions.

5. Improve ambiguity reporting for bare columns.
   - Example: `SELECT Id FROM Employee e JOIN Department d ...`
   - Goal: surface explicit parser-side ambiguity metadata when multiple candidate owners exist.

6. Improve target-table metadata for UPDATE and DELETE aliases.
   - Goal: expose a stable source table for mutation targets so consumers do not need fallback alias resolution.

7. Improve parser completeness for schema-validation-friendly table references.
   - Goal: distinguish real table references from function-derived aliases and derived-table aliases in the semantic model.

## Workaround Policy

- If a parser gap affects editor behavior, keep the workaround local and add a regression test for the exact SQL snippet.
- When the parser is fixed, remove the workaround and update this list.
- If a new issue is found, add it here before or alongside the fix so we keep one source of truth.


Changes from Parser

What Changed
Added richer lineage/source metadata in lineage.ts:LineageSourceKind
Source exposure model (sources) with projection columns
Ambiguity metadata (ambiguities)
Mutation target metadata (mutations)
Resolution quality on lineage nodes (resolved/ambiguous/unresolved + candidates)

Upgraded source registration and derived handling in lineageBuilder.ts:Derived subquery projection exposure
APPLY alias classification for non-table expressions (derived_apply)
Alias-column remapping support for derived sources
PIVOT/UNPIVOT projection exposure path
Bare-column ambiguity candidate capture
Stable UPDATE/DELETE mutation target metadata

Improved column resolution output in columnAnalyzer.ts:ambiguityCandidates
isCorrelated
Resolution now uses lineage-captured context (DerivedColumn.inputs) so it stays in-scope aware

Exported new public types in index.ts.
Tests Added
lineage.test.ts:Derived subquery projection exposure
APPLY alias classification
Bare-column ambiguity metadata
UPDATE/DELETE mutation target metadata

columnAnalyzer.test.ts:Correlated APPLY resolution marker
Ambiguity candidates propagation

Validation
npm.cmd test passed (47/47 suites, 1474 tests)
npm.cmd run build passed
Coverage vs your 7 items
1: Covered (derived-table output/projection metadata)
2: Covered (APPLY alias as derived source in semantic lineage)
3: Covered (correlated resolution metadata via lineage-backed column resolutions)
4: Covered (derived/CTE + pivot/unpivot projection exposure in source metadata)
5: Covered (explicit ambiguity candidates metadata)
6: Covered (stable mutation target metadata for UPDATE/DELETE)
7: Covered (semantic distinction between physical vs function/derived/apply sources via source kinds)
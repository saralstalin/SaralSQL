# Parser Improvements Tracker

Keep this list current as parser capabilities evolve and extension workarounds are removed.
Each item should describe parser behavior goals, not temporary LSP logic.

## Current Status

Parser package in this workspace: `@saralsql/tsql-parser@0.3.0`.

The previously tracked 7 improvement areas are now covered by parser output and consumed by the extension:

1. `derived-table` lineage for outer alias columns: covered via lineage sources and column resolutions.
2. `APPLY` alias classification for non-table expressions: covered via `sourceKind` such as `derived_apply`.
3. Correlated subquery/APPLY resolution metadata: covered via lineage-backed `columns.resolutions` with correlation markers.
4. Output-column exposure for derived/CTE/PIVOT/UNPIVOT: covered via source projection metadata.
5. Bare-column ambiguity reporting metadata: covered via lineage ambiguity entries and column analyzer ambiguity candidates.
6. Stable update/delete target metadata: covered via lineage mutation target metadata.
7. Schema-validation-friendly source typing: covered via source kind distinctions (`table`, `derived_subquery`, `derived_apply`, `function`, etc.).

## Recently Confirmed

- `GO` batch boundaries are now represented in AST as `BatchSeparatorStatement`.
- Scope builder creates statement-isolated `batch` child scopes when `GO` is present.
- Cross-batch variable leakage false positives (such as duplicate declaration across `GO`) are no longer produced.
- `CREATE VIEW ... AS WITH ...` bodies are visited/scoped correctly.
- CTE symbols inside `CREATE VIEW` bodies are available in semantic scope for diagnostics/hover/definition/references.
- `OUTPUT` clause semantics now include direct qualified pseudo-table references such as `inserted.<col>` and `deleted.<col>`.
- `INSERT` target-table reference identity/range is hardened for reliable diagnostics anchoring on the target table token.
- Bare-column semantics now expose probable lineage candidates and single-source promotion when exactly one viable owner exists in scope.

## Workaround Policy

- If a parser gap affects editor behavior, keep any workaround local and add a regression test for the exact SQL snippet.
- When parser behavior is fixed and verified in SaralSQL, remove the workaround and update:
  - [PARSER_ISSUES_AND_WORKAROUNDS.md](C:\Users\Nimmy\source\repos\SaralSQL\PARSER_ISSUES_AND_WORKAROUNDS.md)
  - this tracker
- Add new parser gaps here before or alongside fixes so this stays the single source of truth.

## Next Candidate Improvements

No active parser-blocking items are tracked right now. Add new items here only when a concrete failing SQL snippet is identified.

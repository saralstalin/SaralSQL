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

1. Ensure CTE scope symbols are emitted consistently inside function-return query bodies.
   - Example shape:
     - `CREATE FUNCTION ... RETURNS TABLE AS RETURN (WITH cteX AS (...) SELECT ... FROM cteX)`
   - Current gap:
     - parser may emit `FROM cteX` references without surfacing corresponding `CTE` symbol metadata in scope for this function-return form.
   - Desired behavior:
     - CTE definitions inside function-return query bodies should appear in semantic scope the same way they do in top-level/view/procedure query contexts.
   - Why this matters:
     - avoids LSP fallback text-pattern CTE suppression in schema validation and keeps CTE ownership/parser lineage parser-native.

2. Add SQLCMD-aware preprocessing with source mapping.
   - Scope:
     - directives such as `:r`, `:setvar`, and `$(Var)` substitution semantics (with batch/preprocess behavior compatible enough for editor workflows).
   - Desired parser contract:
     - preprocess SQLCMD input into parseable SQL,
     - preserve a source map from preprocessed text back to original files/offsets (including `:r` includes),
     - expose directive/expansion diagnostics in a parser-consumable form.
   - Why this matters:
     - SQLCMD changes the effective token stream before SQL parsing; parser-native preprocessing avoids LSP-only hacks across diagnostics/hover/definition/reference.

3. Correctly classify unquoted special keywords in built-in functions.
   - Scope:
     - Built-in date functions like `DATEDIFF`, `DATEADD`, `DATEPART`, and `DATENAME` which take unquoted interval keywords (`day`, `month`, `yy`, etc.) as their first argument.
     - Data type arguments in `CAST`, `TRY_CAST`, `CONVERT`, `TRY_CONVERT`, `PARSE`, and `TRY_PARSE`.
     - ODBC canonical date/time functions like `{fn TIMESTAMPADD(...) }` and `{fn EXTRACT(...) }`.
     - Special string/analytical function keywords like `BOTH`/`LEADING`/`TRAILING` in `TRIM` and structural keywords in `WITHIN GROUP`.
   - Current gap:
     - The parser's lexer currently classifies many of these unquoted special arguments (especially date parts and ODBC intervals) as standard `Identifier` nodes (columns). The semantic engine then reports them as missing columns because they naturally don't exist in the query's tables.
   - Desired parser contract:
     - The parser should recognize these specific function signatures grammatically and classify the special arguments as distinct node types (e.g., `DatePartKeyword`, `DataType`, or `BuiltInArgument`), explicitly excluding them from column reference extraction and semantic validation.
   - Why this matters:
     - Removes the need for fragile, text-based regex lookbehinds (e.g., `isDatePartArgument`) in the LSP extension.
     - Suppresses false-positive "Unknown column" diagnostics organically.
     - Prevents these keywords from improperly polluting the LSP symbol references index.

4. Prevent outer-scope column leakage when inner scope contains unresolved table variables.
   - Scope:
     - Queries containing table variables (`@TempTable`) or unresolved temporary tables inside subqueries or derived tables.
   - Current gap:
     - When the parser's semantic engine encounters a bare column in a subquery reading from a table variable, it cannot verify the column locally. It then falls back to searching outer scopes, mistakenly binding the column to an outer table (e.g., `TableB`) and emitting a false-positive `Unknown column '...' in 'OuterTable'` diagnostic.
   - Desired parser contract:
     - The parser's semantic engine should halt outer-scope fallback for bare columns if the current scope contains unresolved schema sources (like table variables), or it should flag them in a lower-confidence "unverifiable" state rather than emitting a strict outer-table error.
   - Why this matters:
     - Removes the need for the AST-traversal suppression workaround (`hasTableVar`) in the LSP extension's diagnostic collection.
     - Prevents confusing outer-reference error messages for entirely local columns.

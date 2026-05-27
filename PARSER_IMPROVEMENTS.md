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
- CTE scopes inside `RETURN (WITH ... SELECT ...)` inline function bodies are properly pushed into semantic scope.
- SQLCMD preprocessing (`:r`, `:setvar`, `$(Var)`) is supported with perfect offset mapping back to raw text.
- Built-in date function date-parts (e.g. `day` in `DATEDIFF`) are safely parsed as `BuiltInArgumentNode`.
- Bare columns over unverifiable sources (e.g. table variables) are flagged as `isUnverifiable` to prevent outer-scope leakage false positives.
- `extractReferences` and `documentSymbols` deeply traverse into `TRY...CATCH` blocks, `WHILE` loops, and `RETURN` bodies.

## Workaround Policy

- If a parser gap affects editor behavior, keep any workaround local and add a regression test for the exact SQL snippet.
- When parser behavior is fixed and verified in SaralSQL, remove the workaround and update:
  - [PARSER_ISSUES_AND_WORKAROUNDS.md](C:\Users\Nimmy\source\repos\SaralSQL\PARSER_ISSUES_AND_WORKAROUNDS.md)
  - this tracker
- Add new parser gaps here before or alongside fixes so this stays the single source of truth.

## Next Candidate Improvements

1. Property-access semantic shape for typed columns.
   - Example: `SELECT GeoPoint.Lat, GeoPoint.Long FROM dbo.Store`
   - Goal: parser should emit explicit member/property-access semantics (base expression + member) so LSP does not misclassify these as table-qualified column references and can do type-aware member validation.

2. Parser-native local owner/ambiguity decisions for bare columns.
   - Examples: unaliased columns over local temp tables, table variables, TVPs, nested derived scopes, and `ORDER BY` alias interactions.
   - Goal: parser should expose nearest-scope owner decisions and ambiguity outcomes directly in semantic output so LSP does not re-implement scope-walk ownership inference and can rely on parser truth first.

3. Structured local-column metadata for table variables and TVP-backed aliases.
   - Example: `DECLARE @Emp TABLE (...)` with references like `te.FirstName2` and bare `FirstName2` in join predicates.
   - Goal: parser scope symbols should expose local columns as structured entries (raw name + normalized name + data type + location), not string-only arrays, so hover/definition/references/diagnostics can resolve consistently without LSP-side shape normalization and can always show data type when available.

4. DML read-scope source exposure for INSERT/UPDATE/DELETE statement bodies.
   - Examples: `INSERT ... SELECT`, `UPDATE ... FROM`, `DELETE ... FROM` with bare columns.
   - Goal: parser scope/lineage should expose read-side source ownership at token offsets without mutation-target leakage, so LSP ambiguity diagnostics do not need statement-type filtering to avoid false ambiguous-column reports.

5. Schema-agnostic parser diagnostics contract for unknown columns/tables.
   - Problem: parser currently emits unknown-column style diagnostics even when schema is not available, which can conflict with LSP schema validation and local scope ownership decisions.
   - Goal: parser semantic output should distinguish scope-resolvable issues from schema-dependent issues, and avoid emitting hard unknown-table/unknown-column diagnostics unless schema context is explicitly provided (or mark them as advisory/non-blocking with a clear flag).

6. Wildcard qualifier token shape for alias star projections.
   - Example: `SELECT d.* FROM dbo.Department d`.
   - Problem: parser currently emits an extra `unknown` token for the qualifier (`d`) in `d.*`, which consumers can misinterpret as a bare/qualified column reference.
   - Goal: parser reference extraction should represent `alias.*` as wildcard projection metadata only (or clearly typed wildcard token nodes), without emitting a standalone unknown-column token for the alias qualifier.

7. Derived-alias projected-column contract for qualified references.
   - Example: `SELECT s.UnknownColumn FROM (SELECT d.*, e.Address FROM ... ) s`.
   - Problem: parser can resolve qualified references to derived aliases (`owner: s`) without exposing a strict projected-column existence decision for that alias result shape.
   - Goal: parser semantic output should provide explicit projected-column membership decisions for derived aliases (exists/missing), so consumers can flag `s.UnknownColumn` directly without LSP-side alias projection reconstruction.

8. Alias-reference kind for table tokens (avoid schema-validating alias names).
   - Example: `CREATE VIEW ... LEFT JOIN dbo.BusinessUnits bu ON ...` where `bu` can be surfaced as a table-like token.
   - Problem: consumers currently infer alias-vs-table using scope at offset; if missed, aliases can be schema-validated and produce false `Unknown table 'bu'`.
   - Goal: parser `extractReferences` should emit explicit alias-reference kind/context (or a `validateSchema=false` contract for alias tokens), so LSP does not need scope-based alias suppression.

9. Set-operator projection contract for derived aliases.
   - Example: `FROM (SELECT ... UNION SELECT ... ) s` then `SELECT s.availableInventory`.
   - Problem: derived alias projection can be incomplete/placeholder-based for `SetOperator` query shapes, forcing LSP to reconstruct projection from branch AST.
   - Goal: parser should expose finalized projected output columns for set-operator derived queries (raw + normalized names, stable membership), so qualified validation is direct and case-insensitive without LSP rebuild logic.

10. Schema diagnostics lifecycle contract.
   - Problem: transient schema diagnostics can appear before workspace schema/index is ready; this is currently gated in LSP as a timing workaround.
   - Goal: parser diagnostics payload should clearly separate parser-only semantic diagnostics from schema-dependent diagnostics (or include readiness intent/flag), so clients can consume parser diagnostics immediately without early false schema noise.

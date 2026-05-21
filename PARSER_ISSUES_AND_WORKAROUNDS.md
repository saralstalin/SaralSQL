# Parser Issues and Extension Workarounds

Purpose: track parser defects that currently require extension-side workarounds, so we can remove the workarounds after parser fixes are released.

Status legend:
- `open`: parser issue exists, workaround active
- `ready-to-remove`: parser fix available, validate and remove workaround
- `removed`: workaround deleted from extension

## 1) Qualified alias column resolves to wrong source across statements
- Status: `open`
- First seen:
  - Query pattern with multiple statements where alias `e` in a later `SELECT` was resolved to `#t` from an earlier statement.
  - Example symptom: `e.DepartmentId` in `SELECT ... FROM DepartmentSalaryInfo e` resolved as source `#t`.
- Parser behavior:
  - `parsed.columns.resolutions` can return a `source` that does not match the alias binding in the current scope for `alias.column`.
- Extension workaround:
  - In [server/src/definitions.ts](C:/Users/Nimmy/source/repos/SaralSQL/server/src/definitions.ts), we validate `matchedResolution.inputs[*].source` against current-scope alias binding before indexing a qualified column reference.
  - If source mismatches scope alias target, we skip that resolution input and fallback to scope-based resolution.
- Removal condition:
  - Parser resolution guarantees that `alias.column` always maps to the alias target in the relevant statement scope.

## 2) Derived table alias can be treated as an object instead of a table identifier
- Status: `open`
- First seen:
  - Derived tables like `FROM (SELECT ...) d` produced alias-related table data that could be interpreted as non-identifier objects.
- Parser/AST behavior:
  - Alias table location may be `SubqueryExpression` (not a real table identifier), so naive `String(node.table)` logic creates invalid values.
- Extension workaround:
  - In [server/src/definitions.ts](C:/Users/Nimmy/source/repos/SaralSQL/server/src/definitions.ts), helper-based resolution (`resolveTableNameFromNode`, `resolveAliasTableName`) only accepts real identifier table names.
  - Derived alias columns are handled via projected-column checks (`hasDerivedAliasColumn`) and indexed as `alias.column`.
  - In [server/src/server.ts](C:/Users/Nimmy/source/repos/SaralSQL/server/src/server.ts), diagnostics skip schema validation for derived alias qualifiers and hover reads derived projected columns.
- Removal condition:
  - Parser exposes stable alias metadata for derived tables with reliable projected-column symbol mapping, removing need for extension-side derived alias special-casing.

## 3) Execute-target context typing mismatch (extension compile-time guard)
- Status: `open`
- First seen:
  - Parser reference context union did not include `"execute-target"` while extension needed to guard execution targets from table-schema validation.
- Parser/type behavior:
  - Extracted reference context type and emitted contexts are not aligned for this case.
- Extension workaround:
  - In [server/src/definitions.ts](C:/Users/Nimmy/source/repos/SaralSQL/server/src/definitions.ts), context checks use string-based guard (`String(ref.context ?? "")`) instead of strict union literal checks.
- Removal condition:
  - Parser types include all emitted contexts (including execute target when applicable), and extension can use strict typed comparisons again.

## Cleanup Protocol (when parser fixes arrive)
1. Mark relevant issue(s) as `ready-to-remove`.
2. Validate behavior on the failing SQL snippets.
3. Remove workaround code paths and run:
   - `npm.cmd run check-types`
   - `npm.cmd run compile`
4. Mark issue as `removed` with date and commit reference.

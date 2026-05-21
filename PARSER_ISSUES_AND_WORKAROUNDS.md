# Parser Issues and Extension Workarounds

Purpose: track parser defects that currently require extension-side workarounds, so we can remove the workarounds after parser fixes are released.

Status legend:
- `open`: parser issue exists, workaround active
- `ready-to-remove`: parser fix available, validate and remove workaround
- `removed`: workaround deleted from extension

## 1) Qualified alias column resolves to wrong source across statements
- Status: `removed`
- First seen:
  - Query pattern with multiple statements where alias `e` in a later `SELECT` was resolved to `#t` from an earlier statement.
  - Example symptom: `e.DepartmentId` in `SELECT ... FROM DepartmentSalaryInfo e` resolved as source `#t`.
- Parser behavior:
  - `parsed.columns.resolutions` can return a `source` that does not match the alias binding in the current scope for `alias.column`.
- Removed:
  - The extension-side alias/source mismatch guard was removed after parser `columns.resolutions` became statement-scoped.

## 2) Derived table alias can be treated as an object instead of a table identifier
- Status: `removed`
- First seen:
  - Derived tables like `FROM (SELECT ...) d` produced alias-related table data that could be interpreted as non-identifier objects.
- Parser/AST behavior:
  - Alias table location may be `SubqueryExpression` (not a real table identifier), so naive `String(node.table)` logic creates invalid values.
- Removed:
  - The extension-side derived alias column indexing helper was removed.
  - Schema validation no longer builds an AST-inferred derived alias skip set.
  - The extension now relies on parser `sourceKind`, lineage sources, and column resolutions.

## 3) Execute-target context typing mismatch (extension compile-time guard)
- Status: `removed`
- First seen:
  - Parser reference context union did not include `"execute-target"` while extension needed to guard execution targets from table-schema validation.
- Parser/type behavior:
  - Extracted reference context type and emitted contexts are not aligned for this case.
- Removed:
  - The string-based `String(ref.context ?? "")` guard was replaced with a typed `ref.context !== "execute-target"` check.

## Cleanup Protocol (when parser fixes arrive)
1. Mark relevant issue(s) as `ready-to-remove`.
2. Validate behavior on the failing SQL snippets.
3. Remove workaround code paths and run:
   - `npm.cmd run check-types`
   - `npm.cmd run compile`
4. Mark issue as `removed` with date and commit reference.

## Consumed Parser Fixes

The extension is now using parser-side fixes for:
- statement-scoped alias column resolution
- derived/apply/function source classification via `sourceKind`
- derived source projection metadata via lineage sources
- typed `execute-target` reference contexts
- `GO` batch scopes through `BatchSeparatorStatement` and parser-created `batch` child scopes

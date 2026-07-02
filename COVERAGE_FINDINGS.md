# Findings from the coverage-closure pass

Discovered while writing tests to close coverage gaps to 100% (master branch). These are
real code issues found by tracing *why* certain lines were unreachable through the only
exported entry points — not just "untested," but structurally dead or behaviorally broken.

---

## 1. `scope-column-resolver.ts` — `hasColumnBearingLocalSource` has dead branches

**File:** `server/src/scope-column-resolver.ts`
**Kind:** Dead code

`hasColumnBearingLocalSource`'s kind-specific checks (CTE / Alias / Table+TempTable /
Variable+Parameter, roughly lines 14-53) can never execute. Its only caller,
`collectNearestScopeColumnOwners`, always checks `hasPotentialLocalSourceSymbol(symbols)`
**first** — and that function returns `true` for the exact same set of kinds
(Alias/Table/TempTable/CTE/Variable/Parameter), short-circuiting before
`hasColumnBearingLocalSource` is ever reached for those kinds.

The only branch of `hasColumnBearingLocalSource` that can fire is the kind-agnostic first
check (`Array.isArray(sym.columns) && sym.columns.length > 0`), which fires for a symbol of
an *unrecognized* kind that happens to carry an inline `.columns` array — an edge case, not
the intended design.

**Impact:** None currently (the preempting check produces the same `return []` outcome the
dead code would have produced). But if `hasPotentialLocalSourceSymbol`'s kind list is ever
narrowed in a refactor, this dead code would silently come back to life with whatever
(possibly stale) logic it has — worth resolving one way or the other before the resolver
unification work.

**Fix options:** (a) delete the dead branches and rely on `hasPotentialLocalSourceSymbol`
alone, or (b) reorder the checks so `hasColumnBearingLocalSource` runs first if the
column-bearing distinction is actually meant to matter.

---

## 2. `scope-column-resolver.ts` — `resolveSymbolColumns`'s Alias-derived success branch is dead

**File:** `server/src/scope-column-resolver.ts`
**Kind:** Dead code

Inside `resolveSymbolColumns`, the generic check `if (Array.isArray(sym.columns)) { const c
= findColumn(sym.columns, colNorm); if (c) return {...}; }` runs **before** the kind
dispatch. Later, the `sym.kind === "Alias"` branch's own `isDerivedAlias` handling does the
exact same `findColumn(sym.columns, colNorm)` search on the same `sym.columns`. Since the
generic check already searched and returned early on any match, the Alias-specific success
path (`if (projected) { return {...} }`) can never be the one that resolves — by the time
we reach it, we already know `sym.columns` does NOT contain `colNorm`, because if it did,
the function would have returned already.

**Impact:** Functionally harmless (same outcome, same kindLabel "derived table" either way)
but indicates the two checks should be consolidated — confusing during a refactor since it
looks like derived-alias resolution has special handling when it doesn't.

---

## 3. `diagnostic-helpers.ts` — SELECT * expansion silently fails for CTE and subquery-alias sources

**File:** `server/src/diagnostic-helpers.ts`, function `getSourceColumns` (helper for
`buildSelectStarExpansionCodeActions`)
**Kind:** Functional bug (not just untested — actually broken)

`getSourceColumns` resolves a wildcard source's columns by checking `sym.columns` directly
on the scope symbol. But CTE symbols and subquery-derived alias symbols do **not** carry a
flat `.columns` array — their columns live under `sym.location.query` and are only
accessible via the `getCteColumns()` helper in `ast-utils.ts` (used correctly elsewhere,
e.g. in hover and hover's derived-alias-projection logic). `getSourceColumns` never calls
`getCteColumns()`.

**Confirmed repro:**
```sql
WITH cte AS (SELECT EmployeeId FROM Employee)
SELECT cte.* FROM cte;          -- "Expand cte.* to explicit columns" never appears

SELECT sub.*
FROM (SELECT EmployeeId, Name FROM Employee) sub;   -- same -- no quick fix offered
```
Verified via direct calls to `buildSelectStarExpansionCodeActions` — both return `[]`
(no code actions), even though the same SQL successfully resolves columns through hover
and diagnostics paths elsewhere (which DO use `getCteColumns`/lineage projection).

**User-visible impact:** The "Expand `*` to explicit columns" quick fix works for `SELECT *`
and `SELECT alias.*` against real tables, but is silently a no-op for the same gesture
against a CTE or a derived (subquery) alias — no error, the lightbulb just doesn't offer
the action, which looks like "it doesn't work here" with no explanation.

**Fix:** In `getSourceColumns`, when the scope-walk reaches a CTE or derived-alias symbol
with no flat `.columns`, fall back to `getCteColumns(sym)` (already imported in this file)
before giving up.

---

---

## 4. `diagnostic-helpers.ts` — `expandWildcardForSelect` empty-sources branch is dead

**File:** `server/src/diagnostic-helpers.ts`, `expandWildcardForSelect` helper (called from `buildSelectStarExpansionCodeActions`)
**Kind:** Dead code

`expandWildcardForSelect` has an early `if (sources.length === 0) return null` guard. This branch is never reachable because the caller only invokes this function after the outer `visit` function passes an `Array.isArray(node.from)` check — and the parser never emits a `SelectStatement` with `from: []` (empty array). When a SELECT has no FROM clause, the parser sets `from: undefined`, which fails the `Array.isArray` check before `expandWildcardForSelect` is ever called.

**Impact:** None. The `?? []` fallback in `collectSelectSources(selectNode.from ?? [])` correctly handles the undefined case, but since we never reach that function call with an empty-from SelectStatement, the path is structural dead code. Minor: there is no `SELECT * FROM ()` SQL construct that would produce a SelectStatement with an empty `from` array.

---

## 5. `diagnostic-helpers.ts` — Multiple null-guard branches are unreachable via public API

**File:** `server/src/diagnostic-helpers.ts`
**Kind:** Defensive dead guards (confirmed unreachable via public entry points)

The following guard branches are structurally unreachable through the exported function signatures, because earlier function-entry guards already ensure the values are valid:

- `isBareColumnInMutationStatementAtOffset`: `!ast || typeof offset !== "number"` guard (called only from `collectAmbiguousColumnDiagnostics` which already validates `parsed.ast`)
- `collectAmbiguousColumnDiagnostics` / `collectReadableBareColumnDiagnostics`: `!ref.location || typeof ref.location.start !== "number"` (parser always emits `location` for refs)
- `hasSingleSelectSourceAtOffset`: `!Array.isArray(best.from) || best.from.length === 0` (already screened by caller's own from-check)
- `hasSingleSelectVariableSourceAtOffset`: `best.from.length !== 1` for from.length=0 (already screened upstream)
- `collectReadScopeRanges`: non-finite scope location (parser always emits valid numbers for scopes)
- `getReadScopeSourceCountAtOffset`: returning null due to empty ranges (common in simple queries — actually this IS likely reachable; further investigation needed)
- `getContainingStatementNode`: `!ast` guard (called only when `parsed.ast` is already validated)

**Impact:** These are defensive patterns — none incorrect, just never exercised. During the resolver-unification refactor, if these helpers are extracted and called with different callers, the guards become meaningful.

---

## 6. `diagnostic-helpers.ts` — `getSourceColumns` scope-walk bodies extend Finding #3

**File:** `server/src/diagnostic-helpers.ts`, `getSourceColumns` helper
**Kind:** Extension of Finding #3 (SELECT * expansion doesn't work for CTE/derived sources)

The scope-walk body of `getSourceColumns` (the entire while-loop that checks sym.columns, sym.kind==="Alias" aliasedTable resolution, sym.kind==="CTE"/"Table"/"TempTable" matching, and the `sym.dataType` fallback) is unreachable for the same structural reason as Finding #3: the parser never populates inline `.columns` on Alias or CTE scope symbols (their columns live in `.location.query` accessed via `getCteColumns()`). The scope walk runs but its body never finds matching columns → always returns `[]`.

The `getSymbolColumns` helper (which reads `sym.localColumns` or falls back to `sym.columns`) is equally unreachable — no parser-emitted scope symbol carries a `.localColumns` array.

**Remediation:** Same fix as Finding #3 — call `getCteColumns(sym)` for CTE-kind symbols inside `getSourceColumns` to allow SELECT * expansion to work for CTE sources.

---

*(Log continues as more findings are discovered during the remaining coverage pass.)*

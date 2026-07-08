# Parser Issues — @saralsql/tsql-parser

Validated against v0.4.7. Two sections: **Active Bugs** (wrong or missing output, repro confirmed) and **Capability Gaps** (valid patterns where richer parser output would remove LSP-side reconstruction).

---

## Active Bugs

### Bug 7 — `matchedResolution.decisionReason` vocabulary is too narrow

`decisionReason` still only takes three values: `"single_scope_owner"`, `"qualified_reference"`, and `"ambiguous_candidates"`. Several resolution paths produce no distinct signal.

**Known missing reasons:**
- Resolution via type inference / datatype propagation
- Fallback from scope to schema-only lookup
- Mutation target as sole source (UPDATE/DELETE without FROM alias)
- Bare column inside a CTE body resolved from the CTE's own projection

**Impact:** Consumers cannot distinguish trusted resolutions from weak schema fallbacks, making edge-case handling guesswork.

---

### Bug 8 — Undeclared alias qualifier produces no diagnostic

`SELECT e.Col FROM Table` where `e` was never declared as an alias emits no issue; the expression is treated as a valid reference.

**Repro:**
```sql
SELECT e.EmployeeId FROM Employee;   -- no alias e declared
```
**Observed:** `result.issues // []`  
**Expected:** An error indicating `e` is not a declared alias or table name in scope.  
**Impact:** T-SQL would reject this query; the LSP cannot surface the binding error without parser support.

---

## Capability Gaps

Patterns where the parser produces no wrong output but richer metadata would eliminate LSP-side reconstruction.

### Gap A — Property-access semantics for typed column members

`SELECT GeoPoint.Lat FROM dbo.Store` — the parser produces a column resolution but emits no column ref and provides no member/property-access node. The qualification `GeoPoint.Lat` is indistinguishable from a normal `alias.column` pattern.  
**Goal:** Emit explicit member-access semantics (base expression + member) for typed column access so consumers can do type-aware member validation.

---

### Gap B — Parser-native bare-column ownership decisions

Bare column attribution is reconstructed by the LSP from scope symbols and schema. `readScopes` is now populated (v0.4.7), but `columns.resolutions` for ambiguous bare columns exposes `reason: "ambiguous_candidates"` with no ownership decision — the LSP must re-derive ownership from schema for disambiguation.  
**Goal:** Expose per-token scope-level ownership decisions for all bare columns (including ambiguous ones with a probable-owner list), so the LSP can consume resolution truth rather than re-implementing scope-walk inference.

---

### Gap C — Structured local-column metadata for table variables and TVPs

`DECLARE @Emp TABLE (Id INT, Name NVARCHAR(100))` — scope symbol columns are raw strings (`["Id","Name"]`). No data type, no location, no normalized name.  
**Goal:** Expose local columns as structured entries (raw name + normalized name + data type + location) so hover, definition, and diagnostics resolve consistently without LSP-side shape normalization.

---

### Gap G — Projected-column existence decisions for derived alias references

`SELECT s.UnknownColumn FROM (SELECT Id, Name FROM Employee) s` — the parser resolves `s.UnknownColumn` with `owner: "s"`, `reason: "qualified_reference"` and emits no diagnostic, even though `UnknownColumn` is not in `s`'s projected output.  
**Goal:** Provide explicit projected-column membership decisions (exists/missing) for derived alias references so consumers can flag invalid column references without reconstructing the derived alias projection.

---

### Gap H — Alias-reference context for table tokens in extractReferences

Alias names can appear as table-kind tokens in extracted references, requiring consumers to suppress them via scope lookup. The LSP currently applies scope-based suppression to avoid false unknown-table diagnostics on alias tokens.  
**Goal:** Emit an alias-reference kind or `validateSchema: false` for alias tokens in reference extraction so consumers do not need scope-based alias suppression.

---

### Gap I — Set-operator projection contract for derived aliases

`SELECT s.Name FROM (SELECT Name FROM Employee UNION ALL SELECT Name FROM Department) s` — `s.Name` produces no `columns.resolutions` entry; the derived alias projection over a set-operator query is not exposed.  
**Goal:** Expose finalized projected output columns (raw + normalized names) for set-operator derived queries so qualified validation is direct without LSP branch-AST reconstruction.

---

## LSP Workarounds to Remove

The COL001 suppression in `server.ts` for CTE header column aliases (Bug 9) is now dead code — the parser no longer emits COL001 for `WITH cte(col) AS (...)` patterns. It should be removed.

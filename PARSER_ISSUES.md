# Parser Issues — @saralsql/tsql-parser

Verified against v0.4.3. Each issue includes a minimal repro SQL and the observed vs expected output.

---

## Issue 1 — `lineage.readScopes` never populated

**Description:**  
`result.lineage.readScopes` is always an empty array regardless of query structure. This field is intended to describe the read-scope regions of a statement with their contributing source tables, enabling consumers to know which tables are "in scope" at any given offset.

**Repro:**
```sql
SELECT Name FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId;
```

**Observed:**
```js
result.lineage.readScopes  // []
```

**Expected:**  
At minimum one read-scope entry covering the SELECT's range, listing `Employee` (alias `e`) and `Department` (alias `d`) as sources.

```js
result.lineage.readScopes  // [{
//   location: { start: 0, end: <end> },
//   sources: [
//     { alias: "e", name: "Employee" },
//     { alias: "d", name: "Department" }
//   ]
// }]
```

**Why it matters:**  
Without read-scope data, a bare ambiguous column like `Name` (present in both tables) cannot be narrowed based on which sources are actually in scope at the reference position. Disambiguation relies entirely on the scope symbol tree, which does not always have enough information.

---

## Issue 2 — Scope is empty at UPDATE SET and WHERE clause positions

**Description:**  
For an UPDATE statement with a FROM clause, the FROM aliases are not visible in scope at the SET clause or WHERE clause offset positions. Only the SET target is in scope. This makes it impossible to resolve bare column references in those clause positions using the scope tree.

**Repro:**
```sql
UPDATE e
SET    Name = 'x'
FROM   Employee e
WHERE  e.EmployeeId = 1;
```

**Observed:**
```js
// At the offset of "Name" in SET:
scope.root.findInnermost(offset).getVisibleSymbols()  // []
```

**Expected:**  
The alias `e` (pointing to `Employee`) should be visible in scope at both the SET and WHERE clause positions, just as it is inside a SELECT's WHERE clause.

**Why it matters:**  
A bare column in `SET Col = value` cannot be resolved to its source table. Consumers must fall back to a pure schema lookup using the target table name, which fails when the target is specified by alias (`UPDATE e SET …`) rather than table name (`UPDATE Employee SET …`).

---

## Issue 3 — MERGE USING subquery columns have no `matchedResolution`

**Description:**  
Bare column references inside the USING subquery of a MERGE statement have no entry in `columns.resolutions`. The offset falls within the USING subquery range but is not covered by any resolution object.

**Repro:**
```sql
MERGE Employee AS t
USING (SELECT EmployeeId, Name FROM Staging) AS s
ON    t.EmployeeId = s.EmployeeId
WHEN MATCHED THEN UPDATE SET t.Name = s.Name;
```

**Observed:**
```js
// At the offset of "Name" inside the USING subquery:
result.columns.resolutions.find(r => offset >= r.location.start && offset <= r.location.end)
// undefined
```

**Expected:**  
A resolution entry for each bare column inside the USING subquery, with `owner` and `inputs` identifying the source (`Staging`).

**Why it matters:**  
Column references inside MERGE USING subqueries cannot be resolved or validated — they appear as unresolvable references to consumers even when the source table is known.

---

## Issue 4 — `lineage.mutations[].predicateInputs` never populated

**Description:**  
For UPDATE and DELETE statements, `result.lineage.mutations` is populated with the mutation entries, but `predicateInputs` is always absent (or `undefined`) even when the WHERE clause contains column references with identifiable sources.

**Repro:**
```sql
UPDATE Employee
SET    Name = 'x'
WHERE  EmployeeId = 1
AND    Name = 'old';
```

**Observed:**
```js
result.lineage.mutations[0].predicateInputs  // undefined
```

**Expected:**
```js
result.lineage.mutations[0].predicateInputs  // [
//   { kind: "column", name: "Employee.EmployeeId", source: "Employee", location: {...} },
//   { kind: "column", name: "Employee.Name",       source: "Employee", location: {...} }
// ]
```

**Why it matters:**  
Without predicate inputs, bare column references in mutation WHERE clauses (which often have empty scope — see Issue 2) cannot be attributed to their source table. Consumers cannot validate that the WHERE column belongs to the mutation target.

---

## Issue 5 — Anonymous subquery inner positions have no `matchedResolution`

**Description:**  
Bare column references inside anonymous inline subqueries (not CTEs, not named derived tables at the top level) have no entry in `columns.resolutions`, even when the source is unambiguous.

**Repro:**
```sql
SELECT sub.Name
FROM   (SELECT Name FROM Employee) sub;
```

**Observed:**
```js
// At the offset of "Name" inside the inline SELECT:
result.columns.resolutions.find(r => offset >= r.location.start && offset <= r.location.end)
// undefined
```

**Expected:**  
A resolution with `owner: "Employee"` and `decisionReason: "single_scope_owner"` for the `Name` reference inside the subquery, matching the same resolution that would be emitted for `SELECT Name FROM Employee` at the top level.

**Why it matters:**  
Column references inside arbitrary nested subqueries appear unresolvable to consumers, even in simple unambiguous cases. This breaks validation and navigation inside derived table expressions.

---

## Issue 6 — CTE self-reference visible in anchor branch scope

**Description:**  
Inside a recursive CTE's anchor SELECT, the CTE name itself is already present in scope. This means a bare column reference in the anchor branch resolves to the CTE rather than to the concrete base table.

**Repro:**
```sql
WITH rcte AS (
    SELECT Name FROM Employee WHERE EmployeeId = 1   -- anchor
    UNION ALL
    SELECT e.Name FROM Employee e
    JOIN rcte r ON e.EmployeeId = r.EmployeeId       -- recursive
)
SELECT Name FROM rcte;
```

**Observed:**
```js
// At "Name" in the anchor SELECT:
result.columns.resolutions[...].owner  // "rcte"  (the CTE itself)
```

**Expected:**  
In the anchor branch, the CTE should not be visible in its own scope. `Name` in the anchor branch should resolve to `Employee` (the concrete base table), since `rcte` does not yet exist at anchor evaluation time.

The recursive branch correctly resolves `Name` to `Employee` (via the explicit alias `e`). The anchor branch should match.

**Why it matters:**  
Consumers cannot distinguish "this column comes from the base table" vs "this column comes from the CTE's own projection" in the anchor branch. Validation of anchor-branch column types against the base table schema is broken.

---

## Issue 7 — `matchedResolution.decisionReason` has limited vocabulary

**Description:**  
`decisionReason` (on `columns.resolutions[]`) currently only takes values `"single_scope_owner"`, `"qualified_reference"`, and `"ambiguous_candidates"` for common queries. Several other resolution paths appear to exist in the resolver but are never signalled back to consumers.

**Known missing reasons:**  
- When a column is resolved via type inference or datatype propagation (not scope owner count)
- When resolution falls back from scope to a schema-only lookup
- When a mutation target is the sole source (UPDATE/DELETE without a FROM alias)
- When a bare column is inside a CTE body and the CTE's own projection is the source

**Why it matters:**  
Consumers use `decisionReason` to decide how much to trust the resolution. Without richer signal, edge cases cannot be handled differently from the common case. Specifically, the distinction between "resolved by scope" and "resolved by schema fallback" is currently invisible to consumers.

---

## Issue 8 — `SELECT e.Col FROM Table` with undeclared alias `e` produces no diagnostic

**Description:**  
When a qualified identifier uses an alias that was never declared in the FROM clause, the parser emits no issue and the expression is treated as a valid property-access rather than an unresolved reference.

**Repro:**
```sql
SELECT e.EmployeeId FROM Employee;
```
(No alias `e` is declared — FROM has `Employee` with no `AS e`)

**Observed:**
```js
result.issues  // []   — no errors or warnings
```

**Expected:**  
An issue of severity `"error"` or `"warning"` indicating that `e` is not a declared alias or table name in scope.

**Why it matters:**  
This is a legitimate T-SQL error — SQL Server would reject this query ("The multi-part identifier 'e.EmployeeId' could not be bound."). Consumers cannot surface this error to users without parser support, and the expression being silently valid means the column reference appears legitimate when it is not.

---

*Verified against @saralsql/tsql-parser v0.4.3*

---

## Issue 9 — COL001 false positives for CTEs with explicit column alias lists

**Verified against:** v0.4.5

**Description:**  
When a CTE is declared with an explicit column alias list in the header — `WITH cteTally(N) AS (...)` — the parser does not recognise that `N` is a valid column name for that CTE. Any reference to the CTE column via an alias (e.g. `t.N` where `t` aliases `cteTally`) produces a COL001 "Unknown column" diagnostic.

**Repro:**
```sql
CREATE FUNCTION [dbo].[FastSplitter](@List NVARCHAR(MAX), @Delimiter NVARCHAR(255))
RETURNS TABLE WITH SCHEMABINDING AS
RETURN
  WITH E1(N) AS (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1),
       cteTally(N) AS (
           SELECT 0 UNION ALL
           SELECT TOP (DATALENGTH(ISNULL(@List,1)))
               ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM E1
       ),
       cteStart(N1) AS (
           SELECT t.N+1 FROM cteTally t
           WHERE SUBSTRING(@List, t.N, 1) = @Delimiter OR t.N = 0
       )
  SELECT SUBSTRING(@List, s.N1, ISNULL(NULLIF(CHARINDEX(@Delimiter,@List,s.N1),0)-s.N1,8000))
  FROM cteStart s;
```

**Observed:**
```
COL001: Unknown column 'N' on 't'   (t.N in WHERE clause — t aliases cteTally)
COL001: Unknown column 'N1' on 's'  (s.N1 in SELECT — s aliases cteStart)
```

**Expected:**  
No COL001 diagnostics. The CTE header `cteTally(N)` explicitly declares that the CTE has a column named `N`. References to `t.N` (where `t` aliases `cteTally`) and `s.N1` (where `s` aliases `cteStart`) are valid.

**Root cause:**  
The parser validates CTE column access against the CTE body's SELECT list, ignoring the column names declared in the CTE header. For `cteTally(N)`, the body is `SELECT 0 UNION ALL SELECT ROW_NUMBER()...` — neither expression carries the name `N` in the SELECT list; the name is assigned only by the header alias. The parser does not propagate header aliases into the CTE's column schema.

**Why it matters:**  
Recursive CTEs and tally/numbers CTEs commonly use this header-alias form. Every `WITH cte(col1, col2) AS (...)` pattern in production SQL produces false-positive COL001 errors. This is one of the most common T-SQL patterns for set-based iterative operations.

**Fix:**  
When building the column schema for a CTE symbol, check whether the CTE was declared with an explicit column alias list (the `(N)` in `cteTally(N)`). If so, use those header names as the authoritative column list instead of (or in addition to) inferring names from the SELECT list.

---

*Verified against @saralsql/tsql-parser v0.4.5*

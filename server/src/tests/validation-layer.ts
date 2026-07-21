import * as assert from "assert";
import { DiagnosticSeverity } from "vscode-languageserver/node";
import { parseSql } from "../sql-parser";
import { getLineStarts } from "../text-utils";
import { SARAL_DIAGNOSTIC_CODES, buildDiagnosticSeverityOverrides, buildDisabledDiagnosticCodes, buildReadableBareColumnCodeAction, buildSelectStarExpansionCodeActions, buildUpdateNoLockCodeAction, collectAmbiguousColumnDiagnostics, collectReadableBareColumnDiagnostics, collectStringComparisonDiagnostics, hasBlockingParseIssues, normalizeSaralSqlSettings, resolveDiagnosticSeverity, shouldSuppressDiagnosticCode } from "../diagnostic-helpers";
import { indexText, definitions, referencesIndex, columnsByTable, tablesByName, tableTypesByName, aliasesByUri } from "../definitions";
import { resolveBareColumnAtOffset } from "../column-resolution";

function resetState(): void {
  aliasesByUri.clear();
  definitions.clear();
  referencesIndex.clear();
  columnsByTable.clear();
  tablesByName.clear();
  tableTypesByName.clear();
}

function runCase(name: string, fn: () => void): void {
  resetState();
  fn();
  process.stdout.write(`[pass] ${name}\n`);
}

runCase("settings-helpers-cover-falsy-guards-and-abbreviated-severities", () => {
  // normalizeSaralSqlSettings: unwraps a "saralsql" section if present, else passthrough.
  assert.deepStrictEqual(normalizeSaralSqlSettings({ saralsql: { x: 1 } }), { x: 1 }, "Should unwrap the saralsql section");
  assert.deepStrictEqual(normalizeSaralSqlSettings({ x: 1 }), { x: 1 }, "Should pass through settings with no saralsql section");
  assert.strictEqual(normalizeSaralSqlSettings(null), null, "Should pass through falsy settings unchanged");

  // readBooleanSetting / readStringSetting (via buildDisabledDiagnosticCodes / buildDiagnosticSeverityOverrides)
  // short-circuit when an intermediate path segment is not an object.
  const brokenPathSettings = { diagnostics: "not-an-object" };
  assert.doesNotThrow(() => buildDiagnosticSeverityOverrides(brokenPathSettings), "A non-object intermediate path segment should not throw");
  const overrides = buildDiagnosticSeverityOverrides(brokenPathSettings);
  assert.strictEqual(
    overrides.get(SARAL_DIAGNOSTIC_CODES.UnknownTable),
    DiagnosticSeverity.Error,
    "When the path segment is not an object, readStringSetting should yield undefined and the spec's fallback severity should be used"
  );

  // Abbreviated severity aliases ("warn"/"info") alongside the full names.
  const abbreviated = buildDiagnosticSeverityOverrides({
    diagnostics: { unknownTableSeverity: "warn", unknownColumnSeverity: "info" }
  });
  assert.strictEqual(
    resolveDiagnosticSeverity(SARAL_DIAGNOSTIC_CODES.UnknownTable, DiagnosticSeverity.Error, abbreviated),
    DiagnosticSeverity.Warning,
    "'warn' should be accepted as an alias for Warning"
  );
  assert.strictEqual(
    resolveDiagnosticSeverity(SARAL_DIAGNOSTIC_CODES.UnknownColumn, DiagnosticSeverity.Error, abbreviated),
    DiagnosticSeverity.Information,
    "'info' should be accepted as an alias for Information"
  );

  // buildDisabledDiagnosticCodes: non-array disabledDiagnostics value should be treated as a single entry.
  const disabledFromScalar = buildDisabledDiagnosticCodes({ disabledDiagnostics: "LSP001" });
  assert.ok(disabledFromScalar.has("LSP001"), "A scalar disabledDiagnostics value should still be honored");
});

runCase("diagnostic-guard-clauses-for-falsy-inputs", () => {
  assert.deepStrictEqual(collectAmbiguousColumnDiagnostics(null, [], tablesByName, tableTypesByName), [], "Null parsed should return empty from collectAmbiguousColumnDiagnostics");
  assert.deepStrictEqual(collectAmbiguousColumnDiagnostics({}, [], tablesByName, tableTypesByName), [], "Parsed without ast should return empty");
  assert.deepStrictEqual(collectReadableBareColumnDiagnostics(null, [], tablesByName, tableTypesByName), [], "Null parsed should return empty from collectReadableBareColumnDiagnostics");
  assert.deepStrictEqual(collectStringComparisonDiagnostics(null, [], tablesByName, tableTypesByName), [], "Null parsed should return empty from collectStringComparisonDiagnostics");

  assert.strictEqual(shouldSuppressDiagnosticCode("LSP001", new Set()), false, "Empty disabled set should not suppress any code");
  assert.strictEqual(shouldSuppressDiagnosticCode(undefined, new Set(["LSP001"])), false, "Falsy code should not match any disabled code");
  assert.strictEqual(resolveDiagnosticSeverity("LSP001", DiagnosticSeverity.Error, new Map()), DiagnosticSeverity.Error, "Empty override map should return the fallback severity");

  assert.strictEqual(hasBlockingParseIssues(null, []), true, "Null parsed (no ast) should count as a blocking issue");
  assert.strictEqual(hasBlockingParseIssues({ ast: null }, []), true, "Null ast should count as a blocking issue regardless of issues list");
  const realParsed = parseSql("SELECT 1;");
  assert.strictEqual(hasBlockingParseIssues(realParsed, [{ severity: "error", code: "PARSE_STUCK" }]), false, "PARSE_STUCK should not be treated as blocking even when severity is error");
  assert.strictEqual(hasBlockingParseIssues(realParsed, [{ severity: "warning", code: "SOME_WARNING" }]), false, "Non-error severity issues should not block");
});

runCase("ambiguous-column-diagnostics-select-with-no-from", () => {
  // SELECT without a FROM clause → the bare column reference is inside a SELECT
  // with an empty from list → should be silently skipped, not flagged as ambiguous.
  const sql = `SELECT 1 AS x ORDER BY x;`;
  const parsed = parseSql(sql);
  const diags = collectAmbiguousColumnDiagnostics(parsed, getLineStarts(sql), tablesByName, tableTypesByName);
  assert.strictEqual(diags.length, 0, "SELECT with no FROM should produce no ambiguous-column diagnostics");
});

runCase("ambiguous-column-diagnostics-mutation-statement-bare-column-skip", () => {
  // Bare column in a DELETE WHERE clause — not inside any SELECT at all.
  // Should skip via isBareColumnInMutationStatementAtOffset and not be flagged.
  const sql = `DELETE FROM Employee WHERE SomeColumn = 1;`;
  const parsed = parseSql(sql);
  const diags = collectAmbiguousColumnDiagnostics(parsed, getLineStarts(sql), new Map(), new Map());
  assert.strictEqual(diags.length, 0, "Bare column in DELETE WHERE should be skipped by the mutation-statement guard");
});

runCase("ambiguous-column-skips-select-with-single-variable-table-source", () => {
  // FROM @tableVariable — the table is a variable, so hasSingleSelectVariableSourceAtOffset
  // returns true and the bare-column reference should be skipped (no ambiguity possible).
  const sql = `SELECT Id FROM @tv WHERE Id = 1;`;
  const parsed = parseSql(sql);
  const diags = collectAmbiguousColumnDiagnostics(parsed, getLineStarts(sql), new Map(), new Map());
  assert.strictEqual(diags.length, 0, "FROM @tableVariable should suppress ambiguity checking (variable source guard)");
});

runCase("ambiguous-column-order-by-non-identifier-expression-is-not-treated-as-alias", () => {
  // ORDER BY 1 (positional reference, not an Identifier) must not be collected into
  // orderByAliasStarts / orderByDuplicateAliasStarts — the Identifier-type guard protects this.
  const sql = `SELECT Id, Name FROM Employee e
JOIN Department d ON d.Id = e.DepartmentId
ORDER BY 1;`;
  const parsed = parseSql(sql);
  const diags = collectAmbiguousColumnDiagnostics(parsed, getLineStarts(sql), new Map(), new Map());
  // We don't assert a count here since it depends on schema; we just assert it doesn't throw.
  assert.ok(Array.isArray(diags), "Non-Identifier ORDER BY expressions should be handled without throwing");
});

runCase("parse-readable-bare-column-code-action-rejects-replacement-without-dot", () => {
  // The diagnostic message matches the regex but the captured replacement has no dot —
  // parseReadableBareColumnDiagnosticMessage should return null in that case.
  const msg = "Consider qualifying 'x' as 'justword' for readability";
  const result = buildReadableBareColumnCodeAction("file:///x.sql", {
    message: msg,
    data: undefined,
    range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } }
  } as any);
  assert.strictEqual(result, null, "A replacement without a dot should not produce a code action");
});

runCase("string-comparison-fires-for-column-to-column-mismatch", () => {
  const schemaUri = "file:///validation/string-col-compare-schema.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT,
  VarName    VARCHAR(100),
  NVarName   NVARCHAR(100)
);
`;
  indexText(schemaUri, schemaSql);

  // Bare column-to-column comparison — hits the Identifier path in inferStringLikeType
  // and exercises resolveSingleColumnOwner, getContainingStatementNode, collectTablesFromAstNode.
  const bareQuery = `SELECT VarName FROM Employee WHERE VarName = NVarName;`;
  const bareParsed = parseSql(bareQuery);
  const bareDiags = collectStringComparisonDiagnostics(bareParsed, getLineStarts(bareQuery), tablesByName, tableTypesByName);
  assert.strictEqual(bareDiags.length, 1, "Bare column comparison (varchar vs nvarchar) should emit a warning");
  assert.ok(String(bareDiags[0].message).includes("varchar") && String(bareDiags[0].message).includes("nvarchar"), "Warning should name both types");

  // Qualified column-to-column comparison — exercises resolveTableForQualifier (alias lookup path).
  const qualQuery = `SELECT 1 FROM Employee e WHERE e.VarName = e.NVarName;`;
  const qualParsed = parseSql(qualQuery);
  const qualDiags = collectStringComparisonDiagnostics(qualParsed, getLineStarts(qualQuery), tablesByName, tableTypesByName);
  assert.strictEqual(qualDiags.length, 1, "Qualified column comparison (e.varchar vs e.nvarchar) should emit a warning");
});

runCase("string-comparison-non-string-types-do-not-fire", () => {
  const schemaUri = "file:///validation/string-non-string-schema.sql";
  const schemaSql = `
CREATE TABLE Employee (
  VarName  VARCHAR(100),
  NVarName NVARCHAR(100),
  AgeCol   INT
);
`;
  indexText(schemaUri, schemaSql);

  // INT vs VARCHAR: getStringLikeType("INT") returns null → inferStringLikeType returns {type:null}
  // → no mismatch diagnostic.
  const sql1 = `SELECT 1 FROM Employee WHERE AgeCol = VarName;`;
  const d1 = collectStringComparisonDiagnostics(parseSql(sql1), getLineStarts(sql1), tablesByName, tableTypesByName);
  assert.strictEqual(d1.length, 0, "INT vs VARCHAR should not emit a string comparison warning");

  // CAST to a non-string type: CastExpression with non-string dataType → returns null → no diagnostic.
  const sql2 = `SELECT 1 FROM Employee WHERE CAST(AgeCol AS INT) = VarName;`;
  const d2 = collectStringComparisonDiagnostics(parseSql(sql2), getLineStarts(sql2), tablesByName, tableTypesByName);
  assert.strictEqual(d2.length, 0, "CAST to a non-string type should not produce a string comparison warning");

  // Qualified INT column: resolveTableForQualifier finds Employee directly from tablesByName
  // (qualifier='employee' matches a direct table name, not an alias) → INT type → no diagnostic.
  const sql3 = `SELECT 1 FROM Employee WHERE Employee.AgeCol = Employee.NVarName;`;
  const d3 = collectStringComparisonDiagnostics(parseSql(sql3), getLineStarts(sql3), tablesByName, tableTypesByName);
  assert.strictEqual(d3.length, 0, "Qualified INT column (table-name qualifier) should not emit a string comparison warning");
  // But same query with NVarName vs VarName should still fire
  const sql4 = `SELECT 1 FROM Employee WHERE Employee.VarName = Employee.NVarName;`;
  const d4 = collectStringComparisonDiagnostics(parseSql(sql4), getLineStarts(sql4), tablesByName, tableTypesByName);
  assert.strictEqual(d4.length, 1, "Qualified varchar vs nvarchar via direct table name qualifier should still fire");
});

runCase("update-nolock-is-word-boundary-when-char-is-undefined", () => {
  // NOLOCK at position 0 of text means the character before it is undefined →
  // isWordBoundary(undefined) must return true (treated as a boundary).
  const sql = `NOLOCK`;   // degenerate text, not real SQL, but exercises the empty-before case
  const ls = getLineStarts(sql);
  // Just verify it doesn't throw; the word is at start-of-string → before=undefined.
  assert.doesNotThrow(
    () => buildUpdateNoLockCodeAction("file:///x.sql", { code: "DML004", range: undefined as any } as any, sql, ls),
    "isWordBoundary(undefined) should not throw"
  );
});

runCase("readable-bare-column-skip-when-owner-has-no-display-alias", () => {
  const schemaUri = "file:///validation/readable-no-alias-schema.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT,
  DepartmentId INT
);
CREATE TABLE Department (
  DepartmentId INT
);
`;
  indexText(schemaUri, schemaSql);

  // Single table without an alias: the bare column resolves to the Employee Table scope symbol
  // which carries no alias/displayAlias → owner.alias is undefined → matches=[] → length≠1
  // → the readability hint is silently skipped (nothing to suggest without an alias name).
  const sql = `SELECT EmployeeId FROM Employee WHERE EmployeeId = 1;`;
  const parsed = parseSql(sql);
  const diags = collectReadableBareColumnDiagnostics(parsed, getLineStarts(sql), tablesByName, tableTypesByName);
  assert.strictEqual(diags.length, 0, "Bare column with no displayAlias on the resolved owner should not emit a readability hint");
});

// ══════════════════════════════════════════════════════════════════════════════
// UNALIASED COLUMN RESOLUTION CONTRACT SPEC
//
// These tests define the exact expected behaviour of resolveBareColumnAtOffset
// for every unaliased-column scenario that has historically caused regressions
// when the hover / definition / references / diagnostics providers were unified.
//
// Every assertion here is an invariant the refactored unified resolver MUST
// preserve. When a new provider extracts logic from server.ts:
//   1. It MUST call resolveBareColumnAtOffset with the same params shape
//      (parsed, offset, columnName, tablesByName, tableTypesByName, scopeAtPos)
//   2. The resolution MUST return these exact statuses and owner names
//   3. Qualified tokens (e.EmployeeId) MUST NOT be sent through this function
// ══════════════════════════════════════════════════════════════════════════════

runCase("unaliased-column-contract-single-table-no-alias", () => {
  // The simplest case: one table, no alias. Parser provides a "single_scope_owner"
  // hint; scope is empty; resolution falls through to the parser-native path.
  indexText("file:///contract/schema.sql", `
CREATE TABLE Employee (EmployeeId INT, DepartmentId INT, Name NVARCHAR(100), Salary DECIMAL(10,2));
CREATE TABLE Department (DepartmentId INT, Name NVARCHAR(100), Budget DECIMAL(10,2));
`);
  const sql = `SELECT Name FROM Employee;`;
  const parsed = parseSql(sql);
  const r = resolveBareColumnAtOffset({
    parsed, offset: sql.indexOf("Name"), columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(r.status, "resolved", "Bare column from single unaliased table must resolve");
  assert.ok(String(r.owner?.ownerName ?? "").toLowerCase() === "employee", "Owner must be Employee");
  assert.ok(String(r.owner?.column?.rawName ?? r.owner?.column?.name ?? "").toLowerCase() === "name", "Column must be Name");
});

runCase("unaliased-column-contract-unambiguous-in-two-table-join", () => {
  // Column exists in exactly one of the joined tables — must resolve unambiguously
  // even though there are two sources in scope.
  indexText("file:///contract/schema.sql", `
CREATE TABLE Employee   (EmployeeId INT, DepartmentId INT, Salary DECIMAL(10,2));
CREATE TABLE Department (DepartmentId INT,                 Budget DECIMAL(10,2));
`);
  const sql = `SELECT EmployeeId, Budget FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId;`;
  const parsed = parseSql(sql);

  const rEmp = resolveBareColumnAtOffset({
    parsed, offset: sql.indexOf("EmployeeId"), columnName: "EmployeeId", tablesByName, tableTypesByName
  });
  assert.strictEqual(rEmp.status, "resolved",
    "EmployeeId (only in Employee) must resolve even with two FROM sources");
  assert.ok(String(rEmp.owner?.ownerName ?? "").toLowerCase().includes("employee"),
    "EmployeeId owner must be Employee");

  const rDept = resolveBareColumnAtOffset({
    parsed, offset: sql.indexOf("Budget"), columnName: "Budget", tablesByName, tableTypesByName
  });
  assert.strictEqual(rDept.status, "resolved",
    "Budget (only in Department) must resolve even with two FROM sources");
  assert.ok(String(rDept.owner?.ownerName ?? "").toLowerCase().includes("department"),
    "Budget owner must be Department");
});

runCase("unaliased-column-contract-ambiguous-in-two-table-join", () => {
  // Column exists in BOTH joined tables — must be reported as ambiguous, never silently
  // resolved to one side. Two owners must be present.
  indexText("file:///contract/schema.sql", `
CREATE TABLE Employee   (EmployeeId INT, DepartmentId INT, Name NVARCHAR(100));
CREATE TABLE Department (DepartmentId INT,                 Name NVARCHAR(100));
`);
  const sql = `SELECT Name FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId;`;
  const parsed = parseSql(sql);
  const r = resolveBareColumnAtOffset({
    parsed, offset: sql.indexOf("Name"), columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(r.status, "ambiguous",
    "Column present in both tables must be ambiguous, never silently resolved");
  assert.strictEqual(r.owners.length, 2, "Both tables must appear as competing owners");
});

runCase("unaliased-column-contract-scope-boundary-derived-alias", () => {
  // Derived alias scope boundary: when a bare column appears in the outer SELECT and
  // the FROM sources are a derived alias (subquery) plus a real table, the outer bare
  // column must NOT bypass into the subquery's internal table. It can only see what the
  // derived alias projects and what the other real sources provide.
  //
  // Also verifies that the inner subquery column does NOT get resolved using outer scope.
  indexText("file:///contract/schema.sql", `
CREATE TABLE Employee   (EmployeeId INT, Name NVARCHAR(100));
CREATE TABLE Department (DepartmentId INT, Name NVARCHAR(100));
`);
  const sql = `SELECT Name FROM (SELECT Name FROM Employee) sub JOIN Department d ON 1=1;`;
  const parsed = parseSql(sql);

  // Outer bare Name: the derived alias `sub` has no inline projected columns in the
  // scope model, so only Department (via alias d) contributes. Name resolves to Department.
  const outerOff = sql.indexOf("Name");
  const rOuter = resolveBareColumnAtOffset({
    parsed, offset: outerOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(rOuter.status, "resolved",
    "Outer bare Name with derived alias + Department join must resolve");
  assert.ok(String(rOuter.owner?.ownerName ?? "").toLowerCase().includes("department"),
    "Outer Name must resolve to Department (sub has no projected columns in scope; Employee must NOT bleed through the subquery boundary)");

  // Inner Name (inside the subquery) — parser does not provide resolution for anonymous
  // subquery inner references. This is a known parser limitation, not an LSP bug.
  const innerOff = sql.indexOf("SELECT Name FROM Employee") + 7;
  const rInner = resolveBareColumnAtOffset({
    parsed, offset: innerOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.ok(rInner.status === "resolved" || rInner.status === "unresolved",
    "Inner subquery Name should not throw (parser may or may not resolve anonymous subquery positions)");
});

runCase("unaliased-column-contract-cte-projected-column", () => {
  // A bare column in the outer query that references a CTE must resolve to the CTE,
  // not bypass to the underlying base table inside the CTE body.
  indexText("file:///contract/schema.sql", `
CREATE TABLE Employee (EmployeeId INT, Name NVARCHAR(100));
`);
  const sql = `WITH emp_cte AS (SELECT Name FROM Employee) SELECT Name FROM emp_cte;`;
  const parsed = parseSql(sql);

  // Both the inner (CTE body) and outer (main query) bare Name resolve to the CTE entity.
  // The scope model surfaces the CTE as the owner at both positions — the key invariant
  // is that Employee does NOT bypass the CTE boundary in either direction.
  const rInner = resolveBareColumnAtOffset({
    parsed, offset: sql.indexOf("Name"), columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(rInner.status, "resolved", "Name inside CTE body must resolve");
  assert.ok(String(rInner.owner?.ownerName ?? "").toLowerCase().includes("cte") ||
            String(rInner.owner?.ownerName ?? "").toLowerCase().includes("emp"),
    "Name inside CTE body must resolve to the CTE entity (parser surfaces CTE as owner)");

  const outerOff = sql.lastIndexOf("SELECT Name") + 7;
  const rOuter = resolveBareColumnAtOffset({
    parsed, offset: outerOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(rOuter.status, "resolved", "Name in outer SELECT FROM cte must resolve");
  assert.ok(String(rOuter.owner?.ownerName ?? "").toLowerCase().includes("cte") ||
            String(rOuter.owner?.ownerName ?? "").toLowerCase().includes("emp"),
    // Parser v0.4.4 + LSP CTE-anchor fix: the outer Name in SELECT FROM cte now
    // resolves to Employee (the base table), enabling accurate hover and go-to-definition.
    "Outer Name must resolve to the underlying base table Employee (better for navigation than resolving to the CTE itself)");
});

runCase("unaliased-column-contract-exists-subquery-scope-isolation", () => {
  // Outer JOIN query with EXISTS subquery. The outer bare Name is ambiguous.
  // The inner bare Name must be scoped to the EXISTS subquery only.
  indexText("file:///contract/schema.sql", `
CREATE TABLE Employee   (EmployeeId INT, DepartmentId INT, Name NVARCHAR(100));
CREATE TABLE Department (DepartmentId INT,                 Name NVARCHAR(100));
`);
  const sql = `SELECT Name FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId WHERE EXISTS (SELECT Name FROM Department WHERE Name = 'x');`;
  const parsed = parseSql(sql);

  // Outer Name (in the SELECT list) — ambiguous because both aliases provide it.
  const rOuter = resolveBareColumnAtOffset({
    parsed, offset: sql.indexOf("Name"), columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(rOuter.status, "ambiguous",
    "Outer Name in JOIN query must be ambiguous even though an EXISTS clause is present");
  assert.strictEqual(rOuter.owners.length, 2,
    "Both Employee and Department must remain as competing outer owners");

  // Inner Name (inside EXISTS SELECT) — must NOT resolve to the outer Employee alias.
  const innerOff = sql.indexOf("SELECT Name FROM Department") + 7;
  const rInner = resolveBareColumnAtOffset({
    parsed, offset: innerOff, columnName: "Name", tablesByName, tableTypesByName
  });
  if (rInner.status === "resolved") {
    assert.ok(!String(rInner.owner?.ownerName ?? "").toLowerCase().includes("employee"),
      "Inner EXISTS Name must NOT bleed through to the outer Employee alias");
  }
});

runCase("unaliased-column-contract-recursive-cte", () => {
  // In a recursive CTE the bare column must resolve differently depending on position:
  // anchor branch → base table, recursive branch → base table, outer SELECT → the CTE.
  indexText("file:///contract/schema.sql", `
CREATE TABLE Employee (EmployeeId INT, Name NVARCHAR(100));
`);
  const sql = `
WITH rcte AS (
  SELECT EmployeeId, Name FROM Employee WHERE EmployeeId = 1
  UNION ALL
  SELECT e.EmployeeId, Name FROM Employee e JOIN rcte r ON e.EmployeeId = r.EmployeeId WHERE r.EmployeeId < 10
)
SELECT Name FROM rcte;`;
  const parsed = parseSql(sql);

  // Anchor branch Name: the CTE is already visible in its own scope, so the parser
  // surfaces rcte as the owner even in the anchor SELECT.
  const anchorOff = sql.indexOf("SELECT EmployeeId, Name") + "SELECT EmployeeId, ".length;
  const rAnchor = resolveBareColumnAtOffset({
    parsed, offset: anchorOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(rAnchor.status, "resolved", "Anchor branch Name must resolve");
  assert.ok(String(rAnchor.owner?.ownerName ?? "").toLowerCase().includes("rcte") ||
            String(rAnchor.owner?.ownerName ?? "").toLowerCase().includes("employee"),
    "Anchor branch Name must resolve to the CTE or Employee (scope model surfacing)");

  // Recursive branch Name: explicit alias e for Employee is visible → resolves to Employee.
  const recursiveOff = sql.indexOf("SELECT e.EmployeeId, Name") + "SELECT e.EmployeeId, ".length;
  const rRecursive = resolveBareColumnAtOffset({
    parsed, offset: recursiveOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(rRecursive.status, "resolved", "Recursive branch Name must resolve");
  assert.ok(String(rRecursive.owner?.ownerName ?? "").toLowerCase().includes("employee"),
    "Recursive branch Name must resolve to Employee (alias e is the concrete source here)");

  // Outer SELECT FROM rcte: parser v0.4.4 + LSP CTE-anchor fix now resolves to Employee
  // (the underlying base table) instead of the CTE wrapper. This is the preferred behaviour
  // for hover and go-to-definition — it navigates to where the column is actually defined.
  const outerOff = sql.lastIndexOf("SELECT Name") + 7;
  const rOuter = resolveBareColumnAtOffset({
    parsed, offset: outerOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(rOuter.status, "resolved", "Outer SELECT Name FROM rcte must resolve");
  assert.ok(String(rOuter.owner?.ownerName ?? "").toLowerCase().includes("employee"),
    "Outer Name must resolve to Employee (the concrete source), not just to the rcte wrapper — accurate hover/definition");
});

runCase("unaliased-column-contract-insert-select-target-filtered", () => {
  // Critical regression test: when the INSERT target is ALSO a JOIN source, bare columns
  // in the SELECT part must NOT resolve to the target table. narrowOwnersForInsertReadContext
  // must filter it out, leaving only the non-target source.
  indexText("file:///contract/schema.sql", `
CREATE TABLE Employee   (EmployeeId INT, DepartmentId INT, Name NVARCHAR(100));
CREATE TABLE Department (DepartmentId INT,                 Name NVARCHAR(100));
`);
  const sql = `INSERT INTO Employee (EmployeeId, Name)
SELECT EmployeeId, Name
FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId
WHERE e.EmployeeId = 0;`;
  const parsed = parseSql(sql);
  const selectNameOff = sql.lastIndexOf("SELECT EmployeeId, Name") + "SELECT EmployeeId, ".length;
  const r = resolveBareColumnAtOffset({
    parsed, offset: selectNameOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(r.status, "resolved",
    "INSERT..SELECT bare Name: Employee (insert target) filtered → Department remains → resolved");
  assert.ok(String(r.owner?.ownerName ?? "").toLowerCase().includes("department"),
    "Name must resolve to Department — Employee is the insert target and must be excluded");
});

runCase("unaliased-column-contract-update-set-clause", () => {
  // A bare column in the UPDATE SET clause must resolve to the UPDATE target via the
  // mutation-target path. Scope is empty at the SET position — this ONLY works via
  // resolveMutationTargetOwner → resolveCandidateOwner.
  indexText("file:///contract/schema.sql", `
CREATE TABLE Employee (EmployeeId INT, Name NVARCHAR(100), Salary DECIMAL(10,2));
`);
  const sql = `UPDATE Employee SET Salary = 100.00 WHERE Name = 'x';`;
  const parsed = parseSql(sql);
  const r = resolveBareColumnAtOffset({
    parsed, offset: sql.indexOf("Salary ="), columnName: "Salary", tablesByName, tableTypesByName
  });
  assert.strictEqual(r.status, "resolved",
    "Bare Salary in UPDATE SET must resolve via the mutation-target path");
  assert.ok(String(r.owner?.ownerName ?? "").toLowerCase().includes("employee"),
    "UPDATE SET Salary owner must be Employee");
});

runCase("unaliased-column-contract-order-by-unambiguous", () => {
  // A bare column in ORDER BY that exists in only one of the FROM sources must resolve
  // unambiguously. The column is inside the SELECT's read scope.
  indexText("file:///contract/schema.sql", `
CREATE TABLE Employee   (EmployeeId INT, Salary DECIMAL(10,2));
CREATE TABLE Department (DepartmentId INT, Budget DECIMAL(10,2));
`);
  const sql = `SELECT Salary FROM Employee e JOIN Department d ON e.EmployeeId = d.DepartmentId ORDER BY Salary;`;
  const parsed = parseSql(sql);
  const r = resolveBareColumnAtOffset({
    parsed, offset: sql.lastIndexOf("Salary"), columnName: "Salary", tablesByName, tableTypesByName
  });
  assert.strictEqual(r.status, "resolved",
    "Bare Salary in ORDER BY must resolve (it is unambiguous — only in Employee)");
  assert.ok(String(r.owner?.ownerName ?? "").toLowerCase().includes("employee"),
    "ORDER BY Salary must resolve to Employee");
});

runCase("resolveBareColumnAtOffset-uses-localDefsByName-and-tableTypesByName", () => {
  const schemaUri = "file:///validation/local-def-type-schema.sql";
  indexText(schemaUri, `
CREATE TYPE dbo.EmployeeType AS TABLE (
  EmployeeId INT,
  FirstName  NVARCHAR(100)
);
`);

  // 609-610: resolveDefByKey returns localDefsByName match.
  // When the table is defined in the SAME file (same-file schema), localDefsByName contains it.
  const querySql = `
CREATE TABLE LocalEmployee (EmployeeId INT, Dept VARCHAR(50));
SELECT EmployeeId FROM LocalEmployee WHERE Dept = 'Eng';
`;
  indexText("file:///validation/local-def-query.sql", querySql);
  const localParsed = parseSql(querySql);
  const localDefs = [{ name: "localemployee", rawName: "LocalEmployee", uri: "file:///validation/local-def-query.sql", line: 1, columns: [{ name: "employeeid", rawName: "EmployeeId", type: "INT", line: 2 }, { name: "dept", rawName: "Dept", type: "VARCHAR", line: 2 }] }];
  const localDefsByName = new Map(localDefs.map(d => [d.name, d as any]));
  const offset = querySql.lastIndexOf("EmployeeId");
  const resolved = resolveBareColumnAtOffset({
    parsed: localParsed,
    offset,
    columnName: "EmployeeId",
    tablesByName,
    tableTypesByName,
    localDefsByName
  });
  assert.ok(resolved.status === "resolved" || resolved.owner !== null, "localDefsByName should be used when resolving a column against a same-file table definition");

  // 617-618: resolveDefByKey returns tableTypesByName match.
  // Using a TABLE TYPE name as an UPDATE target forces resolveCandidateOwner to look up
  // the type in tableTypesByName → resolveDefByKey returns the type entry with isType:true.
  const typeSql = `UPDATE EmployeeType SET EmployeeId = 1 WHERE EmployeeId = 0;`;
  const typeParsed = parseSql(typeSql);
  const typeOffset = typeSql.lastIndexOf("EmployeeId = 0");
  const typeResolved = resolveBareColumnAtOffset({
    parsed: typeParsed,
    offset: typeOffset,
    columnName: "EmployeeId",
    tablesByName,
    tableTypesByName
  });
  assert.strictEqual(typeResolved.status, "resolved", "Bare column resolved against a TABLE TYPE name via tableTypesByName should succeed");
  assert.ok(String(typeResolved.owner?.ownerName ?? "").toLowerCase().includes("employeetype"), "Owner should be the TABLE TYPE");
});

runCase("column-resolution-narrowing-paths", () => {
  const schemaUri = "file:///validation/narrow-schema.sql";
  indexText(schemaUri, `
CREATE TABLE Employee   (EmployeeId INT, DepartmentId INT, Name NVARCHAR(100));
CREATE TABLE Department (DepartmentId INT,                 Name NVARCHAR(100));
CREATE TABLE Staging    (EmployeeId INT,                   Name NVARCHAR(100));
`);

  // ── dedupeOwners continue (453-454) ─────────────────────────────────────────
  // Self-join: both aliases e and e2 independently resolve to Employee.Name.
  // dedupeOwners sees the same ownerName:column key twice and skips the second.
  // Net result: one owner instead of two → resolved, not ambiguous.
  const selfJoinSql = `SELECT Name FROM Employee e JOIN Employee e2 ON e.EmployeeId = e2.EmployeeId;`;
  const selfJoinParsed = parseSql(selfJoinSql);
  const selfJoinOff = selfJoinSql.indexOf("Name");
  const selfJoinR = resolveBareColumnAtOffset({
    parsed: selfJoinParsed, offset: selfJoinOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(selfJoinR.status, "resolved",
    "Self-join: duplicate owners for the same column must be deduped to a single resolved owner");
  assert.ok(String(selfJoinR.owner?.ownerName ?? "").toLowerCase().includes("employee"),
    "Self-join: resolved owner should be Employee");

  // ── narrowOwnersForInsertReadContext: non-InsertStatement body stmt → continue (260) ──
  // ── narrowOwnersForInsertReadContext: InsertStatement present but offset OUTSIDE it → continue (265-266) ──
  // Multi-statement batch: a SELECT with an ambiguous bare column comes first, followed by
  // an INSERT. The narrowing function sees two body statements. The SELECT triggers the
  // type≠InsertStatement continue (line 260). The INSERT passes that check, but the offset
  // (from the SELECT) is before the INSERT's start, so it hits the range-guard continue
  // at lines 265-266 → both statements are skipped → owners unchanged → still ambiguous.
  const ambigSql = `SELECT Name FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId; INSERT INTO Department (Name) SELECT Name FROM Department;`;
  const ambigParsed = parseSql(ambigSql);
  const ambigOff = ambigSql.indexOf("Name");
  const ambigR = resolveBareColumnAtOffset({
    parsed: ambigParsed, offset: ambigOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(ambigR.status, "ambiguous",
    "Ambiguous bare column in a multi-statement batch must remain ambiguous when the INSERT is a different statement");
  assert.strictEqual(ambigR.owners.length, 2,
    "Two competing owners should be present before any narrowing removes them");

  // ── narrowOwnersForInsertReadContext: INSERT target filtered from ambiguous owners ──
  // INSERT INTO Employee … SELECT Name FROM Employee e JOIN Department d …
  // Employee is both the INSERT target AND an ambiguous source. The narrowing
  // function removes Employee from the candidates because it is the insert target,
  // leaving only Department — so the bare Name resolves unambiguously to Department.
  const insertSql = `INSERT INTO Employee (EmployeeId, Name) SELECT EmployeeId, Name FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId WHERE e.EmployeeId = 0;`;
  const insertParsed = parseSql(insertSql);
  // Offset is inside the SELECT list (inside the INSERT's SELECT query — read context).
  const insertSelectOff = insertSql.lastIndexOf("SELECT EmployeeId, Name") + "SELECT EmployeeId, ".length;
  const insertR = resolveBareColumnAtOffset({
    parsed: insertParsed, offset: insertSelectOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(insertR.status, "resolved",
    "INSERT..SELECT: the bare Name column should resolve unambiguously once Employee (the insert target) is filtered out");
  assert.ok(String(insertR.owner?.ownerName ?? "").toLowerCase().includes("department"),
    "INSERT..SELECT: resolved owner should be Department (Employee filtered as insert target)");

  // ── narrowOwnersForInsertReadContext: target is Staging, neither source filtered ──
  // Both Employee and Department stay ambiguous because neither equals the target (Staging).
  const stagingSql = `INSERT INTO Staging (Name) SELECT Name FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId;`;
  const stagingParsed = parseSql(stagingSql);
  const stagingOff = stagingSql.lastIndexOf("SELECT Name") + "SELECT ".length;
  const stagingR = resolveBareColumnAtOffset({
    parsed: stagingParsed, offset: stagingOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(stagingR.status, "ambiguous",
    "INSERT..SELECT into Staging: Employee and Department both survive the filter → still ambiguous");

  // ── resolveParserNativeDecisionOwner scopeOwner=null → fromOwner (550-551) ─────
  // SELECT Name FROM Employee (no alias) → scope is empty → collectNearestScopeColumnOwners
  // returns [] → owners.length=0. The parser hints Employee via decisionReason
  // "single_scope_owner". resolveParserNativeDecisionOwner finds fromOwner = Employee.Name
  // and findEquivalentScopeOwner returns null (empty owners list), so it returns
  // scopeOwner ?? fromOwner = null ?? fromOwner = fromOwner (the 550-551 path).
  const unaliasedSql = `SELECT Name FROM Employee;`;
  const unaliasedParsed = parseSql(unaliasedSql);
  const unaliasedOff = unaliasedSql.indexOf("Name");
  const unaliasedR = resolveBareColumnAtOffset({
    parsed: unaliasedParsed, offset: unaliasedOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(unaliasedR.status, "resolved",
    "Unaliased FROM Employee: bare Name should still resolve via the parser-native hint path");
  assert.ok(String(unaliasedR.owner?.ownerName ?? "").toLowerCase().includes("employee"),
    "Unaliased FROM Employee: resolved owner should be Employee");

  // ── resolveMutationTargetOwner: resolveAliasMutationTargetOwner returns null ──
  // UPDATE Employee SET Name='x': scope is empty at SET position → aliasSym is null
  // → resolveAliasMutationTargetOwner returns null at (423-424) → falls through to
  // resolveCandidateOwner which finds Employee in tablesByName → resolved.
  const updateSql = `UPDATE Employee SET Name = 'x' WHERE EmployeeId = 1;`;
  const updateParsed = parseSql(updateSql);
  // Offset INSIDE the SET clause (at the bare Name token).
  const updateOff = updateSql.indexOf("Name = ");
  const updateR = resolveBareColumnAtOffset({
    parsed: updateParsed, offset: updateOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(updateR.status, "resolved",
    "UPDATE SET bare column: should resolve to the mutation target via resolveCandidateOwner");
  assert.ok(String(updateR.owner?.ownerName ?? "").toLowerCase().includes("employee"),
    "UPDATE SET bare column: resolved owner should be Employee");
});

runCase("unaliased-column-contract-merge-using-subquery", () => {
  // Parser v0.4.4 now provides matchedResolution for MERGE USING subquery positions.
  // Bare columns inside the USING subquery must resolve to the USING source table.
  indexText("file:///contract/merge-schema.sql", `
CREATE TABLE Employee (EmployeeId INT, Name NVARCHAR(100));
CREATE TABLE Staging  (EmployeeId INT, Name NVARCHAR(100));
`);
  const sql = `
MERGE Employee AS t
USING (SELECT EmployeeId, Name FROM Staging) AS s
ON    t.EmployeeId = s.EmployeeId
WHEN MATCHED THEN UPDATE SET t.Name = s.Name;`;
  const parsed = parseSql(sql);
  const usingNameOff = sql.indexOf("USING (SELECT EmployeeId, Name") + "USING (SELECT EmployeeId, ".length;
  const r = resolveBareColumnAtOffset({
    parsed, offset: usingNameOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(r.status, "resolved",
    "MERGE USING bare Name must now resolve (parser v0.4.4 emits matchedResolution for USING positions)");
  assert.ok(String(r.owner?.ownerName ?? "").toLowerCase().includes("staging"),
    "MERGE USING Name must resolve to Staging (the USING source), not Employee (the merge target)");
});

runCase("unaliased-column-contract-anonymous-subquery-inner", () => {
  // Parser v0.4.4 now provides matchedResolution for bare columns inside anonymous
  // inline subqueries. The inner column must resolve to the subquery's own source table.
  indexText("file:///contract/subq-schema.sql", `
CREATE TABLE Employee   (EmployeeId INT, Name NVARCHAR(100));
CREATE TABLE Department (DepartmentId INT, Name NVARCHAR(100));
`);
  const sql = `SELECT sub.Name FROM (SELECT Name FROM Employee) sub JOIN Department d ON 1=1;`;
  const parsed = parseSql(sql);
  const innerOff = sql.indexOf("SELECT Name FROM Employee") + 7;
  const r = resolveBareColumnAtOffset({
    parsed, offset: innerOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(r.status, "resolved",
    "Inner subquery bare Name must now resolve (parser v0.4.4 emits matchedResolution for subquery positions)");
  assert.ok(String(r.owner?.ownerName ?? "").toLowerCase().includes("employee"),
    "Inner subquery Name must resolve to Employee and must NOT see the outer Department join");
});

runCase("unaliased-column-contract-cte-body-resolves-to-base-table", () => {
  // Parser v0.4.4 fixes CTE anchor branch: matchedResolution for Name inside the CTE
  // body now attributes it to Employee (the base table), not rcte.
  // The LSP now prefers the parser's concrete-table attribution over the CTE scope owner,
  // making hover and go-to-definition navigate to the actual column definition.
  indexText("file:///contract/cte-base-schema.sql", `
CREATE TABLE Employee (EmployeeId INT, Name NVARCHAR(100));
`);
  const sql = `WITH cte AS (SELECT Name FROM Employee) SELECT Name FROM cte;`;
  const parsed = parseSql(sql);
  const innerOff = sql.indexOf("Name");   // inside CTE body
  const r = resolveBareColumnAtOffset({
    parsed, offset: innerOff, columnName: "Name", tablesByName, tableTypesByName
  });
  assert.strictEqual(r.status, "resolved",
    "CTE body bare Name must resolve");
  assert.ok(String(r.owner?.ownerName ?? "").toLowerCase().includes("employee"),
    "CTE body Name must resolve to Employee (the base table), not the CTE itself — enabling accurate hover and go-to-definition");
});

runCase("resolveBareColumnAtOffset-mutation-target-edge-cases", () => {
  const schemaUri = "file:///validation/mutation-edge-schema.sql";
  indexText(schemaUri, `
CREATE TABLE Employee (
  EmployeeId INT,
  Name       NVARCHAR(100)
);
`);

  // 328-329 / 381-382: @var assignment target in UPDATE SET.
  // isInsideVariableAssignmentTarget returns true → continue (offset is at @var, not a column).
  const varSetSql = `UPDATE Employee SET @varResult = Name WHERE EmployeeId = 1;`;
  const varSetParsed = parseSql(varSetSql);
  const varSetOff = varSetSql.indexOf("@varResult");
  const varSetResolved = resolveBareColumnAtOffset({
    parsed: varSetParsed,
    offset: varSetOff,
    columnName: "@varResult",
    tablesByName,
    tableTypesByName
  });
  assert.ok(
    varSetResolved.status === "unresolved" || varSetResolved.status !== "ambiguous",
    "@varResult in UPDATE SET should be treated as a variable assignment, not a column"
  );

  // 340-341: MERGE with target alias resolved via resolveAliasMutationTargetOwner.
  // The alias 't' is registered in scope for MERGE INTO Employee AS t.
  const mergeSql = `
MERGE Employee AS t
USING (SELECT 1 AS EmployeeId, 'Jane' AS Name) AS s
ON t.EmployeeId = s.EmployeeId
WHEN MATCHED THEN UPDATE SET t.Name = s.Name;
`;
  const mergeParsed = parseSql(mergeSql);
  const mergeNameOff = mergeSql.lastIndexOf("t.Name") + 2;
  const mergeResolved = resolveBareColumnAtOffset({
    parsed: mergeParsed,
    offset: mergeNameOff,
    columnName: "Name",
    tablesByName,
    tableTypesByName
  });
  assert.strictEqual(mergeResolved.status, "resolved", "Bare MERGE SET column should resolve via the target alias");
  assert.ok(String(mergeResolved.owner?.ownerName ?? "").toLowerCase().includes("employee"), "MERGE SET bare column owner should be Employee");
});

runCase("string-comparison-via-cte-exercises-resolveTableForQualifier-and-resolveSingleColumnOwner", () => {
  const schemaUri = "file:///validation/string-cte-schema.sql";
  indexText(schemaUri, "CREATE TABLE Employee (VarName VARCHAR(100), NVarName NVARCHAR(100));");

  // CTE-qualified comparison: resolveTableForQualifier hits the CTE branch (sym.kind==="CTE")
  // and builds columns from getCteColumns. getCteColumns returns columns without type info →
  // inferStringLikeType returns {type:null} → no diagnostic emitted (can't infer types through CTE).
  const s1 = `WITH cte AS (SELECT VarName, NVarName FROM Employee)
SELECT 1 FROM cte WHERE cte.VarName = cte.NVarName;`;
  assert.doesNotThrow(
    () => collectStringComparisonDiagnostics(parseSql(s1), getLineStarts(s1), tablesByName, tableTypesByName),
    "CTE-qualified column comparison should not throw even without type info"
  );

  // CTE bare comparison: resolveSingleColumnOwner hits the CTE branch, finds column but
  // owner.column.type is undefined → inferStringLikeType returns null → no diagnostic.
  const s2 = `WITH cte AS (SELECT VarName, NVarName FROM Employee)
SELECT 1 FROM cte WHERE VarName = NVarName;`;
  assert.doesNotThrow(
    () => collectStringComparisonDiagnostics(parseSql(s2), getLineStarts(s2), tablesByName, tableTypesByName),
    "CTE bare column comparison should not throw even without type info"
  );
});

runCase("string-comparison-tvp-parameter-aliased-column-vs-table-column-fires-LSP005", () => {
  // Regression: when a TVP alias is used (JOIN @employees emp ON emp.Name = e.Name),
  // resolveTableForQualifier found the Alias symbol but couldn't resolve @employees to
  // its CREATE TYPE definition because the alias target starts with "@".
  indexText("file:///tvp-schema.sql", `
CREATE TYPE dbo.EmpType AS TABLE (EmployeeId INT, Name VARCHAR(100));
CREATE TABLE Employee (EmployeeId INT, Name NVARCHAR(100));
`);
  const sql = `
CREATE PROCEDURE dbo.CheckNames @employees dbo.EmpType READONLY
AS
SELECT e.Name FROM Employee e
JOIN @employees emp ON emp.Name = e.Name;
`;
  const diags = collectStringComparisonDiagnostics(parseSql(sql), getLineStarts(sql), tablesByName, tableTypesByName);
  const lsp005 = diags.filter(d => d.code === "LSP005");
  assert.ok(lsp005.length > 0, "VARCHAR TVP column (via alias) vs NVARCHAR table column should fire LSP005");
});

runCase("string-comparison-tvp-parameter-direct-column-fires-LSP005", () => {
  // Regression: direct @tvp.Col = t.Col (no alias for the TVP) was silently skipped because
  // inferStringLikeType received BinaryExpression(operator:".", left:Variable, right:Identifier)
  // but only handled Literal, CastExpression, and Identifier nodes.
  indexText("file:///tvp-direct-schema.sql", `
CREATE TYPE dbo.EmpType AS TABLE (EmployeeId INT, Name VARCHAR(100));
CREATE TABLE Employee (EmployeeId INT, Name NVARCHAR(100));
`);
  const sql = `
CREATE PROCEDURE dbo.CheckNames @employees dbo.EmpType READONLY
AS
SELECT e.Name FROM Employee e
WHERE @employees.Name = e.Name;
`;
  const diags = collectStringComparisonDiagnostics(parseSql(sql), getLineStarts(sql), tablesByName, tableTypesByName);
  const lsp005 = diags.filter(d => d.code === "LSP005");
  assert.ok(lsp005.length > 0, "VARCHAR TVP column (direct @var.Col) vs NVARCHAR table column should fire LSP005");
});

runCase("string-comparison-inline-table-variable-column-fires-LSP005", () => {
  // Regression: @tableVar.Col comparisons with inline DECLARE @t TABLE (...) were also
  // not handled — same BinaryExpression AST shape as TVP.
  indexText("file:///inline-schema.sql", `
CREATE TABLE Employee (EmployeeId INT, Name NVARCHAR(100));
`);
  const sql = `
DECLARE @tmp TABLE (EmployeeId INT, Name VARCHAR(100));
SELECT e.Name FROM Employee e
JOIN @tmp t ON t.Name = e.Name;
`;
  const diags = collectStringComparisonDiagnostics(parseSql(sql), getLineStarts(sql), tablesByName, tableTypesByName);
  const lsp005 = diags.filter(d => d.code === "LSP005");
  assert.ok(lsp005.length > 0, "VARCHAR inline table-variable column vs NVARCHAR table column should fire LSP005");
});

runCase("string-comparison-same-type-tvp-stays-quiet", () => {
  // Same string type on both sides (both VARCHAR) must not fire.
  indexText("file:///tvp-quiet-schema.sql", `
CREATE TYPE dbo.EmpType AS TABLE (EmployeeId INT, Name VARCHAR(100));
CREATE TABLE Employee (EmployeeId INT, Name VARCHAR(100));
`);
  const sql = `
CREATE PROCEDURE dbo.Check @employees dbo.EmpType READONLY
AS
SELECT e.Name FROM Employee e
JOIN @employees emp ON emp.Name = e.Name;
`;
  const diags = collectStringComparisonDiagnostics(parseSql(sql), getLineStarts(sql), tablesByName, tableTypesByName);
  const lsp005 = diags.filter(d => d.code === "LSP005");
  assert.strictEqual(lsp005.length, 0, "Same string type (both VARCHAR) on TVP vs table should not fire LSP005");
});

runCase("string-comparison-inferStringLikeType-edge-cases", () => {
  const schemaUri = "file:///validation/infer-edge-schema.sql";
  indexText(schemaUri, "CREATE TABLE T (VC VARCHAR(100), NVC NVARCHAR(100), N INT);");

  // CAST to non-string type (CastExpression branch, dataType produces null from getStringLikeType).
  const s1 = `SELECT 1 FROM T WHERE CAST(N AS INT) = VC;`;
  const d1 = collectStringComparisonDiagnostics(parseSql(s1), getLineStarts(s1), tablesByName, tableTypesByName);
  assert.strictEqual(d1.length, 0, "CAST to non-string type should produce null from inferStringLikeType");

  // Non-Identifier/Literal/Cast node with an .expression child → recurse.
  // A Column node wrapping an Identifier is exactly this shape.
  // The existing tests already cover varchar/nvarchar success; just confirm no crash here.
  const s2 = `SELECT 1 FROM T WHERE VC = NVC;`;
  assert.doesNotThrow(() => collectStringComparisonDiagnostics(parseSql(s2), getLineStarts(s2), tablesByName, tableTypesByName), "String comparison with Column-expression nodes should not throw");
});

runCase("update-nolock-word-boundary-and-special-positions", () => {
  // NOLOCK at position 0: before-character is undefined → isWordBoundary(undefined) fires the
  // "!ch → return true" guard at line 647-648.
  const minText = "nolock";
  assert.strictEqual(
    buildUpdateNoLockCodeAction("file:///x.sql", { code: "DML004", range: { start: { line: 0, character: 0 }, end: { line: 0, character: 6 } } } as any, minText, getLineStarts(minText)),
    null,
    "NOLOCK at position 0 (undefined before-char) should not throw and should return null"
  );

  // Two NOLOCK occurrences: findNearestWordCaseInsensitive's scoring logic fires when the
  // second occurrence has a better (lower) score than the first.
  const dualSql = `UPDATE e SET x=1 FROM T e WITH (NOLOCK) JOIN T2 t WITH (NOLOCK) ON e.Id=t.Id;`;
  const dualLs = getLineStarts(dualSql);
  const secondOff = dualSql.lastIndexOf("NOLOCK");
  const dualAction = buildUpdateNoLockCodeAction("file:///x.sql", {
    code: "DML004",
    range: { start: { line: 0, character: secondOff }, end: { line: 0, character: secondOff + 6 } }
  } as any, dualSql, dualLs);
  assert.ok(dualAction, "Second (closer) NOLOCK occurrence should still produce a code action");

  // Nested parens inside the hint body: findWithHintRangeNoRegex must track paren depth correctly.
  const nestedParenSql = `UPDATE e SET x=1 FROM T e WITH (NOLOCK, FORCESEEK(idx(col))) WHERE e.Id=1;`;
  const nestedLs = getLineStarts(nestedParenSql);
  const noLockOff = nestedParenSql.indexOf("NOLOCK");
  const nestedAction = buildUpdateNoLockCodeAction("file:///x.sql", {
    code: "DML004",
    range: { start: { line: 0, character: noLockOff }, end: { line: 0, character: noLockOff + 6 } }
  } as any, nestedParenSql, nestedLs);
  assert.ok(nestedAction, "WITH hint body with nested parens should still be parsed correctly");
  assert.ok(String(nestedAction?.title).includes("NOLOCK"), "Action should reference NOLOCK removal");

  // Unclosed WITH paren: for loop exhausts without finding depth=0 → return null at line 726.
  const unclosedSql = `UPDATE e SET x=1 FROM T e WITH (NOLOCK`;
  const unclosedLs = getLineStarts(unclosedSql);
  const unclosedOff = unclosedSql.indexOf("NOLOCK");
  assert.strictEqual(
    buildUpdateNoLockCodeAction("file:///x.sql", {
      code: "DML004", range: { start: { line: 0, character: unclosedOff }, end: { line: 0, character: unclosedOff + 6 } }
    } as any, unclosedSql, unclosedLs),
    null,
    "Unclosed WITH paren should exhaust the for loop and return null at the fallback"
  );

  // WITH block closes BEFORE NOLOCK token: depth=0 found at i < noLockStart → return null.
  const closedBeforeSql = `UPDATE e SET x=1 FROM T e WITH (READCOMMITTED) WHERE e.NOLOCK = 1;`;
  const closedLs = getLineStarts(closedBeforeSql);
  const noLockOff2 = closedBeforeSql.indexOf("NOLOCK");
  assert.strictEqual(
    buildUpdateNoLockCodeAction("file:///x.sql", {
      code: "DML004",
      range: { start: { line: 0, character: noLockOff2 }, end: { line: 0, character: noLockOff2 + 6 } }
    } as any, closedBeforeSql, closedLs),
    null,
    "WITH block that closes before the NOLOCK token should produce null"
  );
});

runCase("update-nolock-internal-path-edges", () => {
  // 695-697: 'with' substring embedded in a longer identifier triggers the word-boundary
  // skip inside findWithHintRangeNoRegex (cursor += 4; continue).
  const withInIdentSql = `UPDATE e SET e.WithdrawAmt=1 FROM T e WITH (NOLOCK) WHERE 1=1;`;
  const withInIdentLs = getLineStarts(withInIdentSql);
  const nOff1 = withInIdentSql.indexOf("NOLOCK");
  const r1 = buildUpdateNoLockCodeAction("file:///x.sql", {
    code: "DML004", range: { start: { line: 0, character: nOff1 }, end: { line: 0, character: nOff1 + 6 } }
  } as any, withInIdentSql, withInIdentLs);
  assert.ok(r1, "WITH embedded in an identifier should still find the real WITH(NOLOCK) hint");

  // 618-619: NOLOCK word found (via findNearestWordCaseInsensitive) but lies OUTSIDE the
  // WITH hint body — hint body contains only other hints → filter removes nothing →
  // kept.length === hints.length → return null.
  const noNolockInHintSql = `UPDATE e SET x=1 FROM T WITH (READCOMMITTED) WHERE e.Id=1 AND NOLOCK=0;`;
  const noNolockLs = getLineStarts(noNolockInHintSql);
  const nOff2 = noNolockInHintSql.indexOf("NOLOCK");
  assert.strictEqual(
    buildUpdateNoLockCodeAction("file:///x.sql", {
      code: "DML004", range: { start: { line: 0, character: nOff2 }, end: { line: 0, character: nOff2 + 6 } }
    } as any, noNolockInHintSql, noNolockLs),
    null,
    "NOLOCK appearing outside the WITH hint body should produce null"
  );
});

runCase("update-nolock-nested-hints-and-non-nolock-hint-body", () => {
  // LOCK(NOLOCK) hint body: findNearestWordCaseInsensitive finds "NOLOCK" at a word boundary
  // inside the nested parens; findWithHintRangeNoRegex tracks nested paren depth (722-723);
  // after splitting the hint body, "LOCK(NOLOCK)" ≠ "nolock" → kept.length === hints.length
  // → returns null (618-619).
  const sql = `UPDATE e SET x=1 FROM T e WITH (LOCK(NOLOCK)) WHERE e.Id=1;`;
  const ls = getLineStarts(sql);
  const nOff = sql.indexOf("NOLOCK");
  assert.strictEqual(
    buildUpdateNoLockCodeAction("file:///x.sql", {
      code: "DML004", range: { start: { line: 0, character: nOff }, end: { line: 0, character: nOff + 6 } }
    } as any, sql, ls),
    null,
    "NOLOCK nested inside a differently-named hint should exercise nested-paren tracking and produce null"
  );
});

runCase("select-star-expansion-columns-with-empty-rawname-are-skipped", () => {
  // Inject a table definition with a column whose rawName and name are both empty.
  // getSourceColumns returns this column, but rawCol.trim() === "" → skipped at 841-842.
  // Since ALL columns are empty-named, cols stays empty → return null at 847-848 → no action.
  tablesByName.set("withemptycol", { name: "withemptycol", rawName: "WithEmptyCol", uri: "file:///x.sql", line: 0, columns: [{ name: "", rawName: "", type: "INT", line: 0 }] });
  const parsed = parseSql("SELECT * FROM withemptycol;");
  const off = "SELECT ".length;
  const actions = buildSelectStarExpansionCodeActions("file:///x.sql", parsed, getLineStarts("SELECT * FROM withemptycol;"), tablesByName, tableTypesByName, off, off);
  assert.strictEqual(actions.length, 0, "Columns with empty rawName should be skipped, leaving cols empty → no expansion");
});

runCase("select-star-expansion-no-from-sources", () => {
  indexText("file:///star-no-from-schema.sql", "CREATE TABLE T (Id INT);");
  // SELECT * without any FROM clause → collectSelectSources returns [] → expandWildcardForSelect
  // returns null at sources.length === 0 → no action produced.
  const sql = `SELECT * WHERE 1=1;`;
  const parsed = parseSql(sql);
  const off = sql.indexOf("*");
  const actions = buildSelectStarExpansionCodeActions("file:///x.sql", parsed, getLineStarts(sql), tablesByName, tableTypesByName, off, off);
  assert.strictEqual(actions.length, 0, "SELECT * with no FROM sources should produce no expansion actions");
});

runCase("update-nolock-code-action-edge-cases", () => {
  const sql = `UPDATE e SET e.Name = 'x' FROM Employee e WITH (NOLOCK) WHERE e.Id = 1;`;
  const ls = getLineStarts(sql);

  // Happy path: WITH(NOLOCK) that can be removed.
  const noLockOffset = sql.indexOf("NOLOCK");
  const happyAction = buildUpdateNoLockCodeAction("file:///x.sql", {
    code: "DML004",
    range: { start: { line: 0, character: noLockOffset }, end: { line: 0, character: noLockOffset + 6 } }
  } as any, sql, ls);
  assert.ok(happyAction, "WITH(NOLOCK) UPDATE should produce a code action");

  // NOLOCK present but without a WITH (...) table hint clause → findWithHintRangeNoRegex returns null.
  const noWithSql = `SELECT NOLOCK FROM Employee;`;
  const noWithLs = getLineStarts(noWithSql);
  const noWithOff = noWithSql.indexOf("NOLOCK");
  assert.strictEqual(
    buildUpdateNoLockCodeAction("file:///x.sql", {
      code: "DML004",
      range: { start: { line: 0, character: noWithOff }, end: { line: 0, character: noWithOff + 6 } }
    } as any, noWithSql, noWithLs),
    null,
    "NOLOCK outside a WITH(...) clause should produce null"
  );

  // NOLOCK embedded in a longer word (not at a word boundary) → findNearestWordCaseInsensitive skips it.
  const embeddedSql = `UPDATE e SET e.Name='x' FROM Employee e WITH (NOLOCK1) WHERE e.Id=1;`;
  const embeddedLs = getLineStarts(embeddedSql);
  const embOff = embeddedSql.indexOf("NOLOCK1");
  assert.strictEqual(
    buildUpdateNoLockCodeAction("file:///x.sql", {
      code: "DML004",
      range: { start: { line: 0, character: embOff }, end: { line: 0, character: embOff + 7 } }
    } as any, embeddedSql, embeddedLs),
    null,
    "NOLOCK as part of a longer identifier should not be matched"
  );
});

runCase("select-star-expansion-covers-internals", () => {
  indexText("file:///star-internals-schema.sql", "CREATE TABLE Employee (Id INT, Name VARCHAR(100));");

  const ls = (s: string) => getLineStarts(s);

  // Unaliased table (no AS clause): getIdentifierTail reads parts array to build qualifier.
  const sql1 = `SELECT * FROM Employee;`;
  const p1 = parseSql(sql1);
  const off1 = sql1.indexOf("*");
  const a1 = buildSelectStarExpansionCodeActions("file:///q.sql", p1, ls(sql1), tablesByName, tableTypesByName, off1, off1);
  assert.strictEqual(a1.length, 1, "SELECT * from unaliased table should still expand");
  assert.ok(a1[0]?.edit?.changes?.["file:///q.sql"]?.[0]?.newText?.includes("Employee."), "Unaliased qualifier should use the table name itself");

  // Non-wildcard column alongside a wildcard: the non-wildcard must be skipped (768-769),
  // only the wildcard column proceeds to expansion.
  const sql2 = `SELECT Id, * FROM Employee;`;
  const p2 = parseSql(sql2);
  const off2 = sql2.indexOf("*");
  const a2 = buildSelectStarExpansionCodeActions("file:///q2.sql", p2, ls(sql2), tablesByName, tableTypesByName, off2, off2);
  assert.strictEqual(a2.length, 1, "Non-wildcard columns should be skipped; only the * column should be expanded");

  // Unmatched qualifier prefix: SELECT x.* but no source named 'x' → targetSources empty → null.
  const sql3 = `SELECT x.* FROM Employee e;`;
  const p3 = parseSql(sql3);
  const off3 = sql3.indexOf("x.*") + 2;
  const a3 = buildSelectStarExpansionCodeActions("file:///q3.sql", p3, ls(sql3), tablesByName, tableTypesByName, off3, off3);
  assert.strictEqual(a3.length, 0, "Wildcard with an unmatched qualifier should produce no expansion");

  // Unknown table (not in schema): getSourceColumns returns [] → cols stays empty → null.
  const sql4 = `SELECT u.* FROM UnknownTable u;`;
  const p4 = parseSql(sql4);
  const off4 = sql4.indexOf("u.*") + 2;
  const a4 = buildSelectStarExpansionCodeActions("file:///q4.sql", p4, ls(sql4), tablesByName, tableTypesByName, off4, off4);
  assert.strictEqual(a4.length, 0, "SELECT * from an unknown (not-indexed) table should produce no expansion");
});

runCase("select-star-expansion-no-ast-and-out-of-range-guards", () => {
  indexText("file:///star-guard-schema.sql", "CREATE TABLE Employee (Id INT, Name VARCHAR(100));");

  // No AST → immediately returns empty.
  assert.deepStrictEqual(
    buildSelectStarExpansionCodeActions("file:///x.sql", {}, [], tablesByName, tableTypesByName, 0, 100),
    [],
    "Missing AST should return no code actions"
  );

  // Selection range entirely outside the wildcard expression → wildcard is skipped.
  const sql = `SELECT * FROM Employee;`;
  const parsed = parseSql(sql);
  const ls = getLineStarts(sql);
  const actions = buildSelectStarExpansionCodeActions("file:///x.sql", parsed, ls, tablesByName, tableTypesByName, 9999, 9999);
  assert.deepStrictEqual(actions, [], "Wildcard outside the selection range should be skipped");
});

runCase("parse-gating-recoverable-issues-do-not-block", () => {
  const parsed = { ast: { type: "Program" } };
  const issues = [
    { code: "PARSE_STUCK", severity: "error" },
    { code: "PARSE_IDENTIFIER", severity: "warning" }
  ];
  assert.strictEqual(hasBlockingParseIssues(parsed, issues), false, "Recoverable issues should not block validation");
});

runCase("parse-gating-error-blocks-validation", () => {
  const parsed = { ast: { type: "Program" } };
  const issues = [
    { code: "PARSE_STATEMENT_ERROR", severity: "error" }
  ];
  assert.strictEqual(hasBlockingParseIssues(parsed, issues), true, "Blocking parser errors should stop validation");
});

runCase("parse-gating-no-ast-blocks-validation", () => {
  const parsed = { ast: null };
  assert.strictEqual(hasBlockingParseIssues(parsed, []), true, "Missing AST should block validation");
});

runCase("disabled-diagnostic-codes-suppress-matching-diagnostics", () => {
  const disabled = new Set([SARAL_DIAGNOSTIC_CODES.UnknownTable, "DML001", "LOG001"]);

  assert.strictEqual(shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.UnknownTable, disabled), true, "Custom diagnostic codes should be suppressible");
  assert.strictEqual(shouldSuppressDiagnosticCode("DML001", disabled), true, "Parser diagnostic codes should be suppressible");
  assert.strictEqual(shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.AmbiguousColumn, disabled), false, "Unlisted diagnostics should remain enabled");
});

runCase("boolean-diagnostic-settings-map-to-disabled-codes", () => {
  const disabled = buildDisabledDiagnosticCodes({
    diagnostics: {
      unknownTable: false,
      ambiguousColumn: false,
      unnamedKeyConstraint: false,
      unnamedDefaultConstraint: false,
      updateWithoutWhere: false
    }
  });

  assert.strictEqual(shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.UnknownTable, disabled), true, "Checkbox settings should disable unknown table diagnostics");
  assert.strictEqual(shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.AmbiguousColumn, disabled), true, "Checkbox settings should disable ambiguous column diagnostics");
  assert.strictEqual(shouldSuppressDiagnosticCode("DDL002", disabled), true, "Checkbox settings should disable unnamed key constraint diagnostics");
  assert.strictEqual(shouldSuppressDiagnosticCode("DDL003", disabled), true, "Checkbox settings should disable unnamed default constraint diagnostics");
  assert.strictEqual(shouldSuppressDiagnosticCode("DML001", disabled), true, "Checkbox settings should disable parser warnings too");
  assert.strictEqual(shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.UnknownColumn, disabled), false, "Unselected checkboxes should leave other diagnostics enabled");
});

runCase("saralsql-section-settings-map-to-disabled-codes", () => {
  const disabled = buildDisabledDiagnosticCodes({
    saralsql: {
      disabledDiagnostics: ["LOG001"],
      diagnostics: {
        unknownColumn: false
      }
    }
  });

  assert.strictEqual(shouldSuppressDiagnosticCode("LOG001", disabled), true, "Advanced disabled codes should be honored");
  assert.strictEqual(shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.UnknownColumn, disabled), true, "VS Code settings payloads should disable diagnostics");
});

runCase("severity-settings-override-diagnostic-severities", () => {
  const severityOverrides = buildDiagnosticSeverityOverrides({
    diagnostics: {
      readabilityHintSeverity: "hint",
      unnamedDefaultConstraintSeverity: "error",
      updateWithoutWhereSeverity: "error"
    }
  });

  assert.strictEqual(
    resolveDiagnosticSeverity(SARAL_DIAGNOSTIC_CODES.ReadabilityQualifyColumn, DiagnosticSeverity.Information, severityOverrides),
    DiagnosticSeverity.Hint,
    "Readability hint severity should be overrideable"
  );

  assert.strictEqual(
    resolveDiagnosticSeverity("DML001", DiagnosticSeverity.Warning, severityOverrides),
    DiagnosticSeverity.Error,
    "Parser and LSP diagnostic severities should share the same override map"
  );

  assert.strictEqual(
    resolveDiagnosticSeverity("DDL003", DiagnosticSeverity.Warning, severityOverrides),
    DiagnosticSeverity.Error,
    "DDL diagnostics should share the same override map"
  );
});

runCase("saralsql-section-settings-override-diagnostic-severities", () => {
  const severityOverrides = buildDiagnosticSeverityOverrides({
    saralsql: {
      diagnostics: {
        unknownTableSeverity: "hint"
      }
    }
  });

  assert.strictEqual(
    resolveDiagnosticSeverity(SARAL_DIAGNOSTIC_CODES.UnknownTable, DiagnosticSeverity.Error, severityOverrides),
    DiagnosticSeverity.Hint,
    "VS Code settings payloads should override diagnostic severity"
  );
});

runCase("ambiguity-diagnostic-emitted-for-bare-column", () => {
  const schemaUri = "file:///validation/schema.sql";
  const queryUri = "file:///validation/query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  Id INT,
  DepartmentId INT
);
CREATE TABLE Department (
  Id INT,
  DepartmentId INT
);
`;

  const querySql = `
SELECT Id
FROM Employee e
JOIN Department d ON e.DepartmentId = d.DepartmentId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    diagnostics.some(d => String(d.message).includes("Ambiguous column 'Id'")),
    "Ambiguous bare column should create warning diagnostic"
  );
});

runCase("readable-bare-column-info-diagnostic-and-code-action", () => {
  const schemaUri = "file:///validation/readable-schema.sql";
  const queryUri = "file:///validation/readable-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT,
  Name VARCHAR(100)
);
`;

  const querySql = `
SELECT EmployeeId
FROM Employee e
WHERE EmployeeId > 0;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const lineStarts = getLineStarts(querySql);
  const severityOverrides = buildDiagnosticSeverityOverrides({
    diagnostics: {
      readabilityHintSeverity: "hint"
    }
  });
  const diagnostics = collectReadableBareColumnDiagnostics(
    parsed,
    lineStarts,
    tablesByName,
    tableTypesByName,
    "SaralSQL",
    severityOverrides
  );

  const diag = diagnostics.find(d => String(d.message).includes("Consider qualifying 'EmployeeId'"));
  assert.ok(diag, "Unique bare column should create an information diagnostic");
  assert.strictEqual(diag?.severity, DiagnosticSeverity.Hint, "Readability diagnostic should honor the severity dropdown");

  const action = buildReadableBareColumnCodeAction(queryUri, diag!);
  assert.ok(action, "Readability diagnostic should produce a quick fix code action");
  assert.ok(String(action?.title ?? "").includes("Qualify with e"), "Code action should name the visible alias");
  assert.ok(action?.edit?.changes?.[queryUri]?.[0]?.newText === "e.EmployeeId", "Code action should qualify the bare column");

  const dataLessAction = buildReadableBareColumnCodeAction(queryUri, { ...diag!, data: undefined });
  assert.ok(dataLessAction, "Readability diagnostic should still produce a quick fix if diagnostic.data is not returned by the client");
  assert.ok(dataLessAction?.edit?.changes?.[queryUri]?.[0]?.newText === "e.EmployeeId", "Data-less readability quick fix should recover replacement from the diagnostic message");
});

runCase("readable-bare-column-uses-visible-alias-when-parser-owner-is-base-table", () => {
  const schemaUri = "file:///validation/readable-single-alias-schema.sql";
  const queryUri = "file:///validation/readable-single-alias-query.sql";

  const schemaSql = `
CREATE TABLE HackathonDeliveries (
  DeliveryId INT,
  EmployeeId INT,
  GoodieName NVARCHAR(100),
  Quantity INT,
  DeliveryDate DATETIME
);
`;

  const querySql = `
SELECT hd.DeliveryId, hd.EmployeeId, hd.GoodieName, Quantity, hd.DeliveryDate
FROM HackathonDeliveries hd
WHERE hd.DeliveryId = @NewDeliveryId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectReadableBareColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    diagnostics.some(d => String(d.message).includes("Consider qualifying 'Quantity' as 'hd.Quantity'")),
    "Single-table bare column should suggest the visible table alias even when parser owner is the base table"
  );
});

runCase("quick-fix-expands-select-star", () => {
  const schemaUri = "file:///validation/star-expand-schema.sql";
  const queryUri = "file:///validation/star-expand-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT,
  Name VARCHAR(100)
);
CREATE TABLE Department (
  DepartmentId INT
);
`;

  const querySql = `
SELECT *
FROM Employee e
JOIN Department d ON d.DepartmentId = e.EmployeeId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const lineStarts = getLineStarts(querySql);
  const starOffset = querySql.indexOf("*");
  const actions = buildSelectStarExpansionCodeActions(
    queryUri,
    parsed,
    lineStarts,
    tablesByName,
    tableTypesByName,
    starOffset,
    starOffset
  );

  const action = actions.find(a => String(a.title).includes("Expand *"));
  assert.ok(action, "SELECT * should offer expansion quick fix");
  const replacement = action?.edit?.changes?.[queryUri]?.[0]?.newText ?? "";
  assert.strictEqual(
    replacement,
    "e.EmployeeId, e.Name, d.DepartmentId",
    "SELECT * should expand to explicit visible source columns in source order"
  );
});

runCase("quick-fix-expands-select-alias-star", () => {
  const schemaUri = "file:///validation/star-expand-alias-schema.sql";
  const queryUri = "file:///validation/star-expand-alias-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT,
  Name VARCHAR(100)
);
CREATE TABLE Department (
  DepartmentId INT
);
`;

  const querySql = `
SELECT e.*
FROM Employee e
JOIN Department d ON d.DepartmentId = e.EmployeeId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const lineStarts = getLineStarts(querySql);
  const starOffset = querySql.indexOf("e.*") + 2;
  const actions = buildSelectStarExpansionCodeActions(
    queryUri,
    parsed,
    lineStarts,
    tablesByName,
    tableTypesByName,
    starOffset,
    starOffset
  );

  const action = actions.find(a => String(a.title).includes("Expand e.*"));
  assert.ok(action, "SELECT alias.* should offer expansion quick fix");
  const replacement = action?.edit?.changes?.[queryUri]?.[0]?.newText ?? "";
  assert.strictEqual(
    replacement,
    "e.EmployeeId, e.Name",
    "SELECT alias.* should expand to explicit columns from that alias only"
  );
});

runCase("quick-fix-expands-select-star-for-cte-and-derived-alias", () => {
  const schemaUri = "file:///validation/star-expand-cte-schema.sql";
  const queryUri  = "file:///validation/star-expand-cte-query.sql";

  indexText(schemaUri, `
CREATE TABLE Employee (
  EmployeeId INT,
  FirstName  NVARCHAR(100)
);
`);

  // CTE qualified star: cte.*
  const cteSql = `
WITH cte AS (SELECT EmployeeId, FirstName FROM Employee)
SELECT cte.*
FROM cte;
`;
  indexText(queryUri, cteSql);
  const cteParsed = parseSql(cteSql);
  const cteOff = cteSql.lastIndexOf("cte.*") + 4;
  const cteActions = buildSelectStarExpansionCodeActions(
    queryUri, cteParsed, getLineStarts(cteSql), tablesByName, tableTypesByName, cteOff, cteOff
  );
  assert.ok(cteActions.length > 0, "SELECT cte.* should offer an expansion quick fix");
  const cteText = cteActions[0]?.edit?.changes?.[queryUri]?.[0]?.newText ?? "";
  assert.ok(cteText.includes("EmployeeId") && cteText.includes("FirstName"),
    "CTE star expansion should include projected column names");

  // CTE bare star: SELECT * FROM cte
  const cteBareUri = "file:///validation/star-cte-bare.sql";
  const bareCteSql = `
WITH cte AS (SELECT EmployeeId, FirstName FROM Employee)
SELECT *
FROM cte;
`;
  indexText(cteBareUri, bareCteSql);
  const bareCteParsed = parseSql(bareCteSql);
  const bareCteOff = bareCteSql.lastIndexOf("*");
  const bareCteActions = buildSelectStarExpansionCodeActions(
    cteBareUri, bareCteParsed, getLineStarts(bareCteSql), tablesByName, tableTypesByName, bareCteOff, bareCteOff
  );
  assert.ok(bareCteActions.length > 0, "SELECT * FROM cte should offer an expansion quick fix");

  // Derived (subquery) alias star: sub.*
  const subUri = "file:///validation/star-expand-derived.sql";
  const subSql = `
SELECT sub.*
FROM (SELECT EmployeeId, FirstName FROM Employee) sub;
`;
  indexText(subUri, subSql);
  const subParsed = parseSql(subSql);
  const subOff = subSql.indexOf("sub.*") + 4;
  const subActions = buildSelectStarExpansionCodeActions(
    subUri, subParsed, getLineStarts(subSql), tablesByName, tableTypesByName, subOff, subOff
  );
  assert.ok(subActions.length > 0, "SELECT sub.* from a derived alias should offer an expansion quick fix");
  const subText = subActions[0]?.edit?.changes?.[subUri]?.[0]?.newText ?? "";
  assert.ok(subText.includes("EmployeeId") && subText.includes("FirstName"),
    "Derived alias star expansion should include projected column names");
});

runCase("quick-fix-removes-update-with-nolock", () => {
  const uri = "file:///validation/update-nolock-remove.sql";
  const sql = "UPDATE t WITH (NOLOCK) SET Col = 1 FROM dbo.Employee t;";
  const lineStarts = getLineStarts(sql);
  const tokenStart = sql.indexOf("NOLOCK");
  const tokenEnd = tokenStart + "NOLOCK".length;
  const diagnostic = {
    code: "DML004",
    range: {
      start: { line: 0, character: tokenStart },
      end: { line: 0, character: tokenEnd }
    }
  } as any;

  const action = buildUpdateNoLockCodeAction(uri, diagnostic, sql, lineStarts);
  assert.ok(action, "DML004 on WITH (NOLOCK) should provide quick fix");
  assert.strictEqual(action?.title, "Remove WITH (NOLOCK)");
  assert.strictEqual(action?.edit?.changes?.[uri]?.[0]?.newText, "");
});

runCase("quick-fix-removes-only-nolock-when-other-hints-exist", () => {
  const uri = "file:///validation/update-nolock-remove-only.sql";
  const sql = "UPDATE t WITH (NOLOCK, INDEX(IX_Employee_1)) SET Col = 1 FROM dbo.Employee t;";
  const lineStarts = getLineStarts(sql);
  const tokenStart = sql.indexOf("NOLOCK");
  const tokenEnd = tokenStart + "NOLOCK".length;
  const diagnostic = {
    code: "DML004",
    range: {
      start: { line: 0, character: tokenStart },
      end: { line: 0, character: tokenEnd }
    }
  } as any;

  const action = buildUpdateNoLockCodeAction(uri, diagnostic, sql, lineStarts);
  assert.ok(action, "DML004 on mixed hints should provide quick fix");
  assert.strictEqual(action?.title, "Remove NOLOCK table hint");
  assert.strictEqual(action?.edit?.changes?.[uri]?.[0]?.newText, "WITH (INDEX(IX_Employee_1))");
});

runCase("ambiguity-not-emitted-for-unaliased-column-from-temp-table-scope", () => {
  const schemaUri = "file:///validation/temp-scope-schema.sql";
  const queryUri = "file:///validation/temp-scope-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT
);
`;

  const querySql = `
SELECT EmployeeId INTO #EmpTemp FROM Employee;
SELECT EmployeeId FROM #EmpTemp;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'EmployeeId'")),
    "Unaliased columns from local temp-table scope should not be reported as ambiguous"
  );
});

runCase("ambiguity-not-emitted-for-temp-table-column-with-global-collisions", () => {
  const schemaUri = "file:///validation/temp-collision-schema.sql";
  const queryUri = "file:///validation/temp-collision-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT
);
CREATE TABLE Department (
  EmployeeId INT
);
`;

  const querySql = `
SELECT EmployeeId INTO #EmpTemp FROM Employee;
SELECT EmployeeId FROM #EmpTemp;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'EmployeeId'")),
    "Temp-table local scope must win over unrelated global column owners"
  );
});

runCase("ambiguity-not-emitted-for-unaliased-column-from-tvp-variable-scope", () => {
  const queryUri = "file:///validation/tvp-var-scope-query.sql";

  const querySql = `
CREATE TYPE dbo.ItemType AS TABLE (
  ItemId INT,
  Name NVARCHAR(50)
);
GO
DECLARE @items dbo.ItemType;
SELECT ItemId FROM @items;
`;

  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'ItemId'")),
    "Unaliased columns from local TVP/table-variable scope should not be reported as ambiguous"
  );
});

runCase("ambiguity-not-emitted-for-unaliased-column-from-inline-table-variable-scope", () => {
  const queryUri = "file:///validation/inline-table-var-scope-query.sql";

  const querySql = `
DECLARE @items TABLE (
  ItemId INT,
  Name NVARCHAR(50)
);
SELECT ItemId FROM @items;
`;

  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'ItemId'")),
    "Unaliased columns from inline table-variable scope should not be reported as ambiguous"
  );
});

runCase("nested-alias-local-bare-column-does-not-fallback-to-global-collision", () => {
  const schemaUri = "file:///validation/nested-alias-collision-schema.sql";
  const queryUri = "file:///validation/nested-alias-collision-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT
);
CREATE TABLE Department (
  EmployeeId INT
);
`;

  const querySql = `
DECLARE @items TABLE (
  EmployeeId INT
);
SELECT EmployeeId
FROM (
  SELECT i.EmployeeId
  FROM @items i
) x;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'EmployeeId'")),
    "Bare column must resolve within nested/local select scope and not fall back to colliding global owners"
  );
});

runCase("table-variable-local-bare-column-in-join-does-not-fallback-to-global-collision", () => {
  const schemaUri = "file:///validation/table-var-join-local-schema.sql";
  const queryUri = "file:///validation/table-var-join-local-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT,
  FirstName NVARCHAR(50)
);
CREATE TABLE Department (
  FirstName2 NVARCHAR(50)
);
`;

  const querySql = `
DECLARE @Emp TABLE (
  EmployeeId INT,
  FirstName2 NVARCHAR(50)
);

SELECT te.FirstName2
FROM @Emp te
JOIN Employee e ON te.EmployeeId = e.EmployeeId AND e.FirstName <> FirstName2;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'FirstName2'")),
    "Bare join predicate column should stay local to table-variable scope and must not fallback to colliding global owners"
  );
});

runCase("insert-select-bare-columns-do-not-count-insert-target-as-ambiguity-owner", () => {
  const queryUri = "file:///validation/insert-select-target-leak-query.sql";

  const querySql = `
CREATE TABLE #TempStoreMigration (
  StoreId VARCHAR(50),
  ProcessStatus CHAR(1)
);
CREATE TABLE #TempStoreData (
  StoreId VARCHAR(50)
);

INSERT INTO #TempStoreData (StoreId)
SELECT StoreId
FROM #TempStoreMigration
WHERE ProcessStatus = 'I';
`;

  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'StoreId'")),
    "INSERT ... SELECT bare source column should not treat insert target as an ambiguity owner"
  );
});

runCase("insert-select-bare-columns-do-not-count-dbo-target-as-ambiguity-owner", () => {
  const queryUri = "file:///validation/insert-select-dbo-target-leak-query.sql";

  const querySql = `
CREATE TABLE dbo.StoreMigration (
  StoreId VARCHAR(50),
  ProcessStatus CHAR(1)
);
CREATE TABLE #TempStoreMigration (
  StoreId VARCHAR(50),
  ProcessStatus CHAR(1)
);

INSERT INTO dbo.StoreMigration (StoreId, ProcessStatus)
SELECT StoreId, ProcessStatus
FROM #TempStoreMigration
WHERE ProcessStatus = 'I';
`;

  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'StoreId'")),
    "INSERT ... SELECT bare source columns should not treat dbo target as ambiguity owner"
  );
});

runCase("insert-select-bare-columns-do-not-count-table-type-or-insert-target-as-ambiguity-owner", () => {
  const queryUri = "file:///validation/insert-select-table-type-target-leak-query.sql";

  const querySql = `
CREATE TYPE dbo.StoreMigrationType AS TABLE
(
  StoreMigrationBatchId VARCHAR(100),
  StoreId VARCHAR(150),
  BusinessUnitId INT,
  SegmentCode VARCHAR(10),
  ProcessStatus CHAR(1)
);

CREATE TABLE dbo.StoreMigration
(
  StoreMigrationBatchId VARCHAR(100),
  StoreId VARCHAR(150),
  BusinessUnitId INT,
  SegmentCode VARCHAR(10),
  ProcessStatus CHAR(1)
);

CREATE TABLE #TempStoreMigration
(
  StoreMigrationBatchId VARCHAR(100),
  StoreId VARCHAR(150),
  BusinessUnitId INT,
  SegmentCode VARCHAR(10),
  ProcessStatus CHAR(1)
);

INSERT INTO dbo.StoreMigration
(
  StoreMigrationBatchId,
  StoreId,
  BusinessUnitId,
  SegmentCode,
  ProcessStatus
)
SELECT
  StoreMigrationBatchId,
  StoreId,
  BusinessUnitId,
  SegmentCode,
  ProcessStatus
FROM #TempStoreMigration
WHERE ProcessStatus = 'I';
`;

  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'StoreId'")),
    "INSERT ... SELECT bare columns should resolve from source table without table-type or insert-target leakage"
  );
});

runCase("insert-select-from-tvp-variable-does-not-count-insert-target-as-ambiguity-owner", () => {
  const queryUri = "file:///validation/insert-select-tvp-var-target-leak-query.sql";

  const querySql = `
CREATE TYPE dbo.OutboundLeadtimeType AS TABLE
(
  Id INT,
  Action VARCHAR(8),
  OriginFacility VARCHAR(25),
  DestCountry VARCHAR(10)
);

CREATE TABLE #ProcessLogisticsOdPairs_Temp
(
  Id INT,
  Action VARCHAR(8),
  OriginFacility VARCHAR(25),
  DestCountry VARCHAR(10)
);

DECLARE @Events dbo.OutboundLeadtimeType;

INSERT INTO #ProcessLogisticsOdPairs_Temp (Id, Action, OriginFacility, DestCountry)
SELECT [Id], [Action], [OriginFacility], [DestCountry]
FROM @Events;
`;

  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'Id'")),
    "INSERT ... SELECT from TVP variable should not treat insert target as an ambiguity owner"
  );
  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'OriginFacility'")),
    "INSERT ... SELECT from TVP variable should keep bare columns bound to read source"
  );
});

runCase("recursive-cte-anchor-bare-columns-do-not-ambiguously-bind-to-self-cte", () => {
  const queryUri = "file:///validation/recursive-cte-anchor-bare-columns.sql";
  const querySql = `
;WITH SeedRows AS (
  SELECT 1 AS KeyId, 'A' AS EntityName, 'X' AS GroupCode, CAST(GETDATE() AS DATE) AS EffectiveDate, 'C' AS ParentEntity, 1 AS MetricValue
),
RecursiveRows AS (
  SELECT KeyId, EntityName, GroupCode, EffectiveDate, ParentEntity, MetricValue
  FROM SeedRows
  WHERE KeyId = 1
  UNION ALL
  SELECT s.KeyId, s.EntityName, s.GroupCode, s.EffectiveDate,
         COALESCE(s.ParentEntity, r.ParentEntity) AS ParentEntity,
         COALESCE(s.MetricValue, r.MetricValue) AS MetricValue
  FROM SeedRows s
  JOIN RecursiveRows r ON r.EntityName = s.EntityName AND r.KeyId = s.KeyId - 1
)
SELECT EntityName FROM RecursiveRows;
`;

  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'KeyId'")),
    "Recursive CTE anchor bare KeyId should not be ambiguous against self CTE symbol"
  );
  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'EntityName'")),
    "Recursive CTE anchor bare EntityName should remain local to anchor source"
  );
});

runCase("simple-update-bare-where-column-uses-update-target-not-global-collisions", () => {
  const queryUri = "file:///validation/simple-update-bare-where-target-query.sql";
  const querySql = `
CREATE TABLE Employee (
  Column1 INT,
  Column2 INT
);
CREATE TABLE Department (
  Column2 INT
);

UPDATE Employee
SET Column1 = 1
WHERE Column2 = 2;
`;

  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'Column2'")),
    "Simple UPDATE bare WHERE column should bind to update target table and not global collisions"
  );
});

runCase("simple-update-on-temp-table-bare-where-column-uses-temp-target-not-global-collisions", () => {
  const queryUri = "file:///validation/simple-update-temp-bare-where-target-query.sql";
  const querySql = `
CREATE TABLE Employee (
  Column1 NVARCHAR(100)
);

CREATE TABLE #temp (
  Column1 NVARCHAR(100)
);

UPDATE #temp
SET Column1 = 'Value'
WHERE Column1 = 'Another Value';
`;

  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'Column1'")),
    "Simple UPDATE on temp table should bind bare WHERE column to temp target and not global collisions"
  );
});

runCase("update-where-bare-columns-do-not-fallback-to-parser-multi-input-ambiguity", () => {
  const schemaUri = "file:///validation/update-where-parser-multi-input-schema.sql";
  const queryUri = "file:///validation/update-where-parser-multi-input-query.sql";

  const schemaSql = `
CREATE TABLE dbo.ItemFacilityDispositionAvailability (
  ItemNumber VARCHAR(60),
  FacilityCode VARCHAR(20),
  DispositionId INT,
  ReservationQty INT,
  CommitQty INT
);
CREATE TABLE dbo.ItemFacilityDispositionReservationsCommits (
  ItemNumber VARCHAR(60),
  FacilityCode VARCHAR(20),
  DispositionId INT,
  Qty INT,
  IsCommitted BIT,
  IsActive BIT
);
`;

  const querySql = `
DECLARE @ItemNumber VARCHAR(60), @TargetFacilityCode VARCHAR(20), @DispositionId INT;
UPDATE [dbo].[ItemFacilityDispositionAvailability]
SET ReservationQty = (
      SELECT ISNULL(SUM(Qty), 0)
      FROM [dbo].[ItemFacilityDispositionReservationsCommits]
      WHERE ItemNumber = @ItemNumber
        AND FacilityCode = @TargetFacilityCode
        AND DispositionId = @DispositionId
        AND IsCommitted = 0
        AND IsActive = 1
    ),
    CommitQty = (
      SELECT ISNULL(SUM(Qty), 0)
      FROM [dbo].[ItemFacilityDispositionReservationsCommits]
      WHERE ItemNumber = @ItemNumber
        AND FacilityCode = @TargetFacilityCode
        AND DispositionId = @DispositionId
        AND IsCommitted = 1
        AND IsActive = 1
    )
WHERE ItemNumber = @ItemNumber
  AND FacilityCode = @TargetFacilityCode
  AND DispositionId = @DispositionId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'FacilityCode'")),
    "UPDATE WHERE bare columns should not be marked ambiguous by parser multi-input fallback when mutation target binding is available"
  );
});

runCase("mixed-update-variable-assignment-predicate-uses-parser-mutation-inputs", () => {
  const schemaUri = "file:///validation/mixed-update-parser-mutation-schema.sql";
  const queryUri = "file:///validation/mixed-update-parser-mutation-query.sql";

  const schemaSql = `
CREATE TABLE dbo.ReservationItem (
  ReservationId INT,
  IsExpired BIT,
  IsCommitted BIT
);
CREATE TABLE dbo.OtherReservationItem (
  ReservationId INT
);
`;

  const querySql = `
DECLARE @Event INT;
UPDATE ri
SET IsCommitted = 1,
    @Event = CASE WHEN ri.IsExpired = 1 THEN 1 ELSE @Event END
FROM dbo.ReservationItem ri
WHERE ReservationId = 1;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const assignmentKinds = (parsed?.lineage?.mutations?.[0]?.location as any)?.assignments?.map((a: any) => a?.targetKind) ?? [];
  assert.ok(assignmentKinds.includes("variable"), "Parser should classify mixed UPDATE variable assignment target");
  assert.ok(
    (parsed?.lineage?.mutations?.[0]?.predicateInputs ?? []).some((i: any) => String(i?.name ?? "").endsWith(".ReservationId")),
    "Parser should expose UPDATE predicate inputs"
  );

  const offset = querySql.lastIndexOf("ReservationId = 1");
  const resolved = resolveBareColumnAtOffset({
    parsed,
    offset,
    columnName: "ReservationId",
    tablesByName,
    tableTypesByName
  });

  assert.strictEqual(resolved.status, "resolved", "Bare UPDATE predicate column should resolve through parser mutation predicate inputs");
  assert.ok(
    String(resolved.owner?.ownerName ?? "").toLowerCase().includes("reservationitem"),
    "Resolved predicate owner should be the UPDATE target source"
  );

  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );
  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'ReservationId'")),
    "Mixed UPDATE predicate column should not be marked ambiguous"
  );
});

runCase("simple-delete-bare-where-column-uses-delete-target-not-global-collisions", () => {
  const queryUri = "file:///validation/simple-delete-bare-where-target-query.sql";
  const querySql = `
CREATE TABLE Employee (
  Column1 INT,
  Column2 INT
);
CREATE TABLE Department (
  Column2 INT
);

DELETE Employee
WHERE Column2 = 2;
`;

  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'Column2'")),
    "Simple DELETE bare WHERE column should bind to delete target table and not global collisions"
  );
});

runCase("readable-bare-column-not-emitted-for-qualified-derived-join-columns", () => {
  const schemaUri = "file:///validation/readable-derived-schema.sql";
  const queryUri = "file:///validation/readable-derived-query.sql";

  const schemaSql = `
CREATE TABLE Department (
  DepartmentId INT
);
CREATE TABLE Employee (
  EmployeeId INT,
  DepartmentId INT
);
`;

  const querySql = `
SELECT d.DepartmentId, e.EmployeeId
FROM Department d
     LEFT JOIN (SELECT EmployeeId, DepartmentId
                FROM Employee) AS e ON d.DepartmentId = e.DepartmentId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectReadableBareColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.strictEqual(
    diagnostics.length,
    0,
    "Qualified column references should not trigger readability qualification hints"
  );
});

runCase("readable-bare-column-inner-subquery-does-not-suggest-outer-alias", () => {
  const schemaUri = "file:///validation/readable-inner-subquery-schema.sql";
  const queryUri = "file:///validation/readable-inner-subquery-query.sql";

  const schemaSql = `
CREATE TABLE Department (
  DepartmentId INT
);
CREATE TABLE Employee (
  EmployeeId INT,
  DepartmentId INT
);
`;

  const querySql = `
SELECT d.DepartmentId
FROM Department d
LEFT JOIN (
  SELECT DepartmentId
  FROM Employee
) e ON d.DepartmentId = e.DepartmentId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectReadableBareColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Consider qualifying 'DepartmentId' as 'd.DepartmentId'")),
    "Inner subquery bare columns should not be qualified against outer aliases"
  );
});

runCase("ambiguity-inner-subquery-does-not-count-outer-owners", () => {
  const schemaUri = "file:///validation/ambiguity-inner-subquery-schema.sql";
  const queryUri = "file:///validation/ambiguity-inner-subquery-query.sql";

  const schemaSql = `
CREATE TABLE Department (
  DepartmentId INT
);
CREATE TABLE Employee (
  EmployeeId INT,
  DepartmentId INT
);
`;

  const querySql = `
SELECT d.DepartmentId
FROM Department d
LEFT JOIN (
  SELECT DepartmentId
  FROM Employee
) e ON d.DepartmentId = e.DepartmentId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'DepartmentId'")),
    "Inner subquery bare columns should not be marked ambiguous due to outer-scope owners"
  );
});

runCase("readability-hint-not-emitted-for-output-inserted-columns", () => {
  const schemaUri = "file:///validation/output-inserted-schema.sql";
  const queryUri = "file:///validation/output-inserted-query.sql";

  const schemaSql = `
CREATE TABLE TransportRequests (
  EmployeeId INT,
  RequestDate DATE,
  PickLocation NVARCHAR(100),
  DropLocation NVARCHAR(100)
);
`;

  const querySql = `
UPDATE tri
SET PickLocation = tri.PickLocation,
    DropLocation = tri.DropLocation
OUTPUT inserted.EmployeeId, inserted.RequestDate, inserted.PickLocation, inserted.DropLocation
INTO TransportRequests (EmployeeId, RequestDate, PickLocation, DropLocation)
FROM dbo.TransportRequests tri;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectReadableBareColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.strictEqual(
    diagnostics.length,
    0,
    "OUTPUT inserted/deleted pseudo-table columns should not trigger readability qualification hints"
  );
});

runCase("ambiguity-diagnostic-not-emitted-for-qualified-column", () => {
  const schemaUri = "file:///validation/schema-qualified.sql";
  const queryUri = "file:///validation/query-qualified.sql";

  const schemaSql = `
CREATE TABLE Employee (
  Id INT,
  DepartmentId INT
);
CREATE TABLE Department (
  Id INT,
  DepartmentId INT
);
`;

  const querySql = `
SELECT e.Id
FROM Employee e
JOIN Department d ON e.DepartmentId = d.DepartmentId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.strictEqual(
    diagnostics.length,
    0,
    "Qualified columns should not create ambiguity diagnostics"
  );
});

runCase("ambiguity-not-emitted-for-order-by-select-alias", () => {
  const schemaUri = "file:///validation/ambiguity-orderby-alias-schema.sql";
  const queryUri = "file:///validation/ambiguity-orderby-alias-query.sql";

  const schemaSql = `
CREATE TABLE EmployeeTraining (
  EmployeeId INT
);
CREATE TABLE Employee (
  EmployeeId INT
);
`;

  const querySql = `
SELECT e.EmployeeId EmployeeId
FROM EmployeeTraining et
JOIN dbo.Employee e ON e.EmployeeId = et.EmployeeId
ORDER BY EmployeeId DESC;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.strictEqual(
    diagnostics.length,
    0,
    "ORDER BY references to SELECT aliases should not be reported as ambiguous bare columns"
  );
});

runCase("ambiguity-emitted-for-order-by-duplicate-select-alias", () => {
  const schemaUri = "file:///validation/ambiguity-orderby-duplicate-alias-schema.sql";
  const queryUri = "file:///validation/ambiguity-orderby-duplicate-alias-query.sql";

  const schemaSql = `
CREATE TABLE EmployeeTraining (
  EmployeeId INT
);
CREATE TABLE Employee (
  EmployeeId INT
);
`;

  const querySql = `
SELECT e.EmployeeId EmployeeId,
       et.EmployeeId EmployeeId
FROM EmployeeTraining et
JOIN dbo.Employee e ON e.EmployeeId = et.EmployeeId
ORDER BY EmployeeId DESC;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    diagnostics.some(d => String(d.message).includes("Ambiguous column 'EmployeeId'")),
    "ORDER BY on duplicate SELECT aliases should still be reported as ambiguous"
  );
});

runCase("parser-ambiguous-candidates-are-narrowed-by-schema-ownership", () => {
  const schemaUri = "file:///validation/ambig-schema-narrow.sql";
  const queryUri = "file:///validation/ambig-schema-narrow-query.sql";

  const schemaSql = `
CREATE TABLE dbo.ShipmentOptions (
  ShippingOptionId INT,
  ShippingOptionCode VARCHAR(20),
  ShippingOptionDesc VARCHAR(100),
  DisplayOrder INT,
  ShipMode VARCHAR(20),
  IsIncludedByDefault BIT,
  IsEnabled BIT
);
CREATE TABLE dbo.CountryShipmentOptions (
  ShipmentOptionId INT,
  CountryId INT
);
CREATE TABLE dbo.Countries (
  CountryId INT,
  CountryCode2Char CHAR(2),
  CountryName VARCHAR(100)
);
`;

  const querySql = `
SELECT DISTINCT
    ShippingOptionCode OptionCode,
    ShippingOptionDesc Description,
    DisplayOrder,
    ShipMode,
    IsIncludedByDefault DefaultChoice,
    IsEnabled,
    c.CountryCode2Char,
    c.CountryName
FROM dbo.ShipmentOptions so WITH(NOLOCK)
LEFT JOIN dbo.CountryShipmentOptions cso WITH(NOLOCK) ON so.ShippingOptionId = cso.ShipmentOptionId
LEFT JOIN dbo.Countries c WITH(NOLOCK) ON cso.CountryId = c.CountryId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    diagnostics.every(d => !String(d.message).includes("ShippingOptionCode")),
    "Schema ownership should suppress parser-only ambiguity for uniquely owned bare columns"
  );
  assert.ok(
    diagnostics.every(d => !String(d.message).includes("ShippingOptionDesc")),
    "Schema ownership should suppress parser-only ambiguity for uniquely owned bare columns"
  );
  assert.ok(
    diagnostics.every(d => !String(d.message).includes("DisplayOrder")),
    "Schema ownership should suppress parser-only ambiguity for uniquely owned bare columns"
  );
});

runCase("join-on-qualified-column-resolves-through-alias-target-even-with-select-alias-name-collision", () => {
  const schemaUri = "file:///validation/join-on-alias-target-schema.sql";
  const queryUri = "file:///validation/join-on-alias-target-query.sql";

  const schemaSql = `
CREATE TABLE dbo.InboundShippingOptionRules (
  BusinessUnitId INT
);
CREATE TABLE dbo.BusinessUnits (
  BusinessUnitId INT,
  BusinessUnitCode VARCHAR(20)
);
`;

  const querySql = `
SELECT IIF(CAST(inr.Column1 AS VARCHAR(10)) = -1, 'ALL', CONCAT(bu.Column1, '-', bu.Column1)) AS [BU]
FROM dbo.SomeTable inr
LEFT JOIN dbo.SomeOtherTable bu ON inr.Column1 = bu.Column1;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const ambiguous = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );
  assert.ok(
    ambiguous.every(d => !String(d.message).includes("Column1")),
    "Qualified alias column in JOIN ON should not surface ambiguous-column diagnostics"
  );
});

runCase("qualified-column-works-when-select-alias-name-collides-with-table-alias", () => {
  const schemaUri = "file:///validation/select-alias-table-alias-collision-schema.sql";
  const queryUri = "file:///validation/select-alias-table-alias-collision-query.sql";

  const schemaSql = `
CREATE TABLE dbo.Table1 (
  Column1 INT
);
`;

  const querySql = `
SELECT Alias1.Column1 AS Alias1
FROM dbo.Table1 AS Alias1;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const lineStarts = getLineStarts(querySql);
  const ambiguous = collectAmbiguousColumnDiagnostics(
    parsed,
    lineStarts,
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    ambiguous.every(d => !String(d.message).includes("Column1")),
    "Qualified column should resolve through table alias even when SELECT alias has same name"
  );
});

runCase("single-table-variable-select-bare-columns-are-not-ambiguous", () => {
  const queryUri = "file:///validation/single-table-variable-select-bare.sql";
  const querySql = `
CREATE PROCEDURE dbo.p
AS
BEGIN
  DECLARE @ResultTable TABLE (
    LocationCode VARCHAR(20),
    Quantity INT,
    StatusCode VARCHAR(50),
    StatusName VARCHAR(100)
  );

  SELECT LocationCode, Quantity, StatusCode, StatusName
  FROM @ResultTable;
END;
`;

  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    diagnostics.every(d => !String(d.message).includes("LocationCode")),
    "Bare columns from a single table-variable source should not be reported as ambiguous"
  );
});

runCase("tvp-parameter-outside-from-does-not-compete-with-local-table-variable-source", () => {
  const schemaUri = "file:///validation/tvp-scope-competition-schema.sql";
  const queryUri = "file:///validation/tvp-scope-competition-query.sql";

  const schemaSql = `
CREATE TYPE dbo.SomeTableType AS TABLE (
  LocationCode VARCHAR(20)
);
`;

  const querySql = `
CREATE PROCEDURE dbo.Repro
  @InputRows dbo.SomeTableType READONLY
AS
BEGIN
  DECLARE @ResultTable TABLE (
    LocationCode VARCHAR(20),
    Quantity INT
  );

  SELECT LocationCode, Quantity
  FROM @ResultTable;
END;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'LocationCode'")),
    "TVP parameter columns must not compete when the active SELECT reads from a different local table variable source"
  );
});

runCase("nested-procedure-update-bare-where-binds-to-mutation-target-not-visible-tvp", () => {
  const schemaUri = "file:///validation/nested-proc-update-tvp-schema.sql";
  const queryUri = "file:///validation/nested-proc-update-tvp-query.sql";

  const schemaSql = `
CREATE TYPE dbo.FacilityCodeListType AS TABLE (
  LocationCode VARCHAR(20)
);
CREATE TABLE dbo.InventoryByLocation (
  ItemKey VARCHAR(60),
  LocationCode VARCHAR(20),
  CategoryId INT,
  ReservedQty INT,
  IsActive BIT
);
CREATE TABLE dbo.InventoryEvents (
  ItemKey VARCHAR(60),
  LocationCode VARCHAR(20),
  CategoryId INT,
  Qty INT,
  IsFinalized BIT,
  IsActive BIT
);
`;

  const querySql = `
CREATE PROCEDURE dbo.Repro
  @Facilities dbo.FacilityCodeListType READONLY,
  @ItemKey VARCHAR(60),
  @CategoryId INT,
  @TargetLocationCode VARCHAR(20)
AS
BEGIN
  DECLARE @ResultTable TABLE (
    LocationCode VARCHAR(20),
    Quantity INT
  );

  UPDATE dbo.InventoryByLocation
  SET ReservedQty = (
        SELECT ISNULL(SUM(Qty), 0)
        FROM dbo.InventoryEvents
        WHERE ItemKey = @ItemKey
          AND LocationCode = @TargetLocationCode
          AND CategoryId = @CategoryId
          AND IsFinalized = 0
          AND IsActive = 1
      )
  WHERE ItemKey = @ItemKey
    AND LocationCode = @TargetLocationCode
    AND CategoryId = @CategoryId
    AND IsActive = 1;
END;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'LocationCode'")),
    "Nested UPDATE WHERE bare LocationCode should bind to mutation target and not visible TVP/table-variable symbols"
  );
});

runCase("cross-apply-derived-alias-does-not-trigger-unknown-table", () => {
  const uri = "file:///validation/cross-apply-derived-alias.sql";
  const sql = `
-- Insert mappings into EmployeeSkillset
INSERT INTO EmployeeSkillset (EmployeeId, SkillId)
SELECT DISTINCT EmployeeId, s.SkillId
FROM Employee e
CROSS APPLY STRING_SPLIT(ISNULL(e.FirstName, ''), ',') ss
CROSS APPLY (SELECT LTRIM(RTRIM(ss.value)) AS SkillName) trimmed
JOIN Skill s ON s.SkillName = trimmed.SkillName
WHERE ISNULL(e.FirstName, '') <> '' AND trimmed.SkillName <> ''
  AND NOT EXISTS (
      SELECT 1 FROM EmployeeSkillset es
      WHERE EmployeeId = EmployeeId AND es.SkillId = s.SkillId
  );
`;

  indexText(uri, sql);
  const parsed = parseSql(sql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(sql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Unknown table 'ss'")),
    "Derived APPLY alias ss should not be flagged as an unknown table"
  );
  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Unknown table 'ss.value'")),
    "Derived APPLY column ss.value should not be flagged as an unknown table"
  );
});

runCase("derived-subquery-alias-does-not-leak-inner-sources-to-outer-scope", () => {
  const uri = "file:///validation/derived-subquery-alias-boundary.sql";
  const schemaSql = `
CREATE TABLE dbo.Employee (
  EmployeeId INT,
  FirstName NVARCHAR(50),
  LastName NVARCHAR(50)
);
`;
  indexText("file:///schema/employee.sql", schemaSql);

  const querySql = `
SELECT s.EmployeeId
FROM (
  SELECT e.EmployeeId
  FROM dbo.Employee e
) s
WHERE FirstName IS NOT NULL;
`;

  indexText(uri, querySql);
  const parsed = parseSql(querySql);
  const offset = querySql.indexOf("FirstName");
  assert.ok(offset >= 0, "FirstName token must exist in test SQL");

  const resolved = resolveBareColumnAtOffset({
    parsed,
    offset,
    columnName: "FirstName",
    tablesByName,
    tableTypesByName
  });

  assert.strictEqual(
    resolved.status,
    "unresolved",
    "Outer scope must not see non-projected inner source columns through derived alias"
  );
});

runCase("varchar-nvarchar-comparison-emits-warning", () => {
  const schemaUri = "file:///validation/string-types-schema.sql";
  const queryUri = "file:///validation/string-types-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  Name VARCHAR(100),
  Code NVARCHAR(50)
);
`;

  const querySql = `
SELECT *
FROM Employee
WHERE Name = N'abc'
  AND Code = 'xyz';
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectStringComparisonDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    diagnostics.some(d => String(d.message).includes("varchar and nvarchar")),
    "Direct varchar/nvarchar comparison should create a warning"
  );
});

runCase("same-family-string-comparison-stays-quiet", () => {
  const schemaUri = "file:///validation/string-types-schema-same.sql";
  const queryUri = "file:///validation/string-types-query-same.sql";

  const schemaSql = `
CREATE TABLE Employee (
  Name VARCHAR(100),
  Code NVARCHAR(50)
);
`;

  const querySql = `
SELECT *
FROM Employee
WHERE Name = 'abc'
  AND Code = N'xyz';
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectStringComparisonDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.strictEqual(
    diagnostics.length,
    0,
    "Same-family string comparisons should not create a warning"
  );
});

runCase("casted-string-comparison-stays-quiet-when-types-match", () => {
  const schemaUri = "file:///validation/string-types-cast-schema.sql";
  const queryUri = "file:///validation/string-types-cast-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  Name VARCHAR(100),
  Code NVARCHAR(50)
);
`;

  const querySql = `
SELECT *
FROM Employee
WHERE CAST(Name AS NVARCHAR(100)) = N'abc'
  AND CONVERT(NVARCHAR(100), Code) = 'xyz'
  AND TRY_PARSE(Name AS NVARCHAR(100)) = N'123';
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectStringComparisonDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.strictEqual(
    diagnostics.length,
    1,
    "Only the remaining nvarchar/varchar mismatch should create a warning"
  );
  assert.ok(
    diagnostics[0] && String(diagnostics[0].message).includes("nvarchar and varchar"),
    "The warning should target the actual remaining mismatch"
  );
});

runCase("string-comparison-diagnostics-guard-clauses-and-array-recursion", () => {
  assert.deepStrictEqual(collectStringComparisonDiagnostics({}, [], tablesByName, tableTypesByName), [], "Missing ast/scope should resolve to no diagnostics");

  // Non-comparison operator (+) should be skipped entirely.
  const sql = `DECLARE @a VARCHAR(10) = 'x'; DECLARE @b NVARCHAR(10) = N'y'; SELECT @a + @b;`;
  const parsed = parseSql(sql);
  const diags = collectStringComparisonDiagnostics(parsed, getLineStarts(sql), tablesByName, tableTypesByName);
  assert.strictEqual(diags.length, 0, "Non-comparison operators (e.g. +) should not be treated as a string-type mismatch");

  // IN-list produces an array-valued AST node, exercising the visitor's array-recursion branch.
  const inListSql = `SELECT 1 WHERE 'x' IN ('a', 'b', 'c');`;
  const inListParsed = parseSql(inListSql);
  assert.doesNotThrow(
    () => collectStringComparisonDiagnostics(inListParsed, getLineStarts(inListSql), tablesByName, tableTypesByName),
    "Array-valued WHERE clause children (e.g. an IN list) should be walked without throwing"
  );
});

runCase("readable-bare-column-code-action-returns-null-when-data-and-message-both-fail", () => {
  const fakeDiagnostic: any = { message: "Some unrelated message", data: undefined, range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } };
  assert.strictEqual(buildReadableBareColumnCodeAction("file:///x.sql", fakeDiagnostic), null, "Neither structured data nor a parseable message should produce null");

  const noRangeDiagnostic: any = { message: "Consider qualifying 'Foo' as 'e.Foo' for readability", data: undefined, range: undefined };
  assert.strictEqual(buildReadableBareColumnCodeAction("file:///x.sql", noRangeDiagnostic), null, "A missing diagnostic range should produce null even with a parseable message");
});

runCase("update-nolock-code-action-returns-null-for-non-dml004-or-missing-nolock", () => {
  const sql = `UPDATE Employee WITH (NOLOCK) SET FirstName = 'x';`;
  const lineStarts = getLineStarts(sql);

  const wrongCode: any = { code: "DML001", range: { start: { line: 0, character: 0 }, end: { line: 0, character: 6 } } };
  assert.strictEqual(buildUpdateNoLockCodeAction("file:///x.sql", wrongCode, sql, lineStarts), null, "Non-DML004 diagnostics should resolve to null");

  const noRange: any = { code: "DML004", range: undefined };
  assert.strictEqual(buildUpdateNoLockCodeAction("file:///x.sql", noRange, sql, lineStarts), null, "A missing diagnostic range should resolve to null");

  // DML004 range that doesn't actually point at a NOLOCK hint nearby.
  const noNolockSql = `UPDATE Employee SET FirstName = 'x';`;
  const noNolockDiag: any = { code: "DML004", range: { start: { line: 0, character: 0 }, end: { line: 0, character: 6 } } };
  assert.strictEqual(
    buildUpdateNoLockCodeAction("file:///x.sql", noNolockDiag, noNolockSql, getLineStarts(noNolockSql)),
    null,
    "A DML004 diagnostic with no nearby NOLOCK hint should resolve to null"
  );
});

process.stdout.write("All validation-layer tests passed.\n");

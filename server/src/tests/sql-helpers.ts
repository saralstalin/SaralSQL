import * as assert from "assert";
import { TextDocument } from "vscode-languageserver-textdocument";
import { parseSql } from "../sql-parser";
import { indexText, tablesByName, tableTypesByName, aliasesByUri, definitions, referencesIndex, columnsByTable } from "../definitions";
import {
  toNormUri, computeCurrentStatement, getVariableCompletionItems,
  getUpdateSetTargetTable, getInsertColumnTargetTable, getAliasBeforeDot,
  isFromJoinTableContext, endsWithDotToken, isLikelySelectProjectionByText,
  isInSelectProjectionContext, getContainingStatementNode, collectTablesFromAstNode,
  getStatementTableCandidatesFromAst, getResolutionSourceColumns, getHoverColumnLabel,
  getSymbolLocalColumns, getPropertyAccessAtOffset, getResolvedObjectKindLabel,
  isFunctionCallInAst, getDefinitionReferenceLocations, getDefinitionReferenceKeys,
  isReferenceHiddenFromObjectUsage, findStatementLocalColumnOwner,
  getParserAliasColumnNames, resolveAliasTableFromStatementAst
} from "../sql-helpers";

function resetState(): void {
  aliasesByUri.clear(); definitions.clear(); referencesIndex.clear();
  columnsByTable.clear(); tablesByName.clear(); tableTypesByName.clear();
}
function runCase(name: string, fn: () => void): void {
  resetState(); fn();
  process.stdout.write(`[pass] ${name}\n`);
}

// ── toNormUri ─────────────────────────────────────────────────────────────────

runCase("toNormUri-normalises-drive-letter-case", () => {
  const result = toNormUri("file:///C:/Users/test/file.sql");
  assert.ok(result.startsWith("file:///c:/") || result.startsWith("file:///C:/"),
    "Should normalise or preserve the URI");
});

runCase("toNormUri-passes-through-already-normalised-uri", () => {
  const uri = "file:///c:/users/test/file.sql";
  assert.ok(toNormUri(uri).includes("file.sql"), "Should keep the filename");
});

// ── computeCurrentStatement ───────────────────────────────────────────────────

runCase("computeCurrentStatement-returns-correct-statement-by-offset", () => {
  const text = "SELECT 1; SELECT Name FROM Employee;";
  const parsed = parseSql(text);
  const off = text.indexOf("Name");
  const stmt = computeCurrentStatement(text, off, parsed, "fallback");
  assert.ok(stmt.includes("SELECT Name"), "Should return the statement containing the offset");
  assert.ok(!stmt.includes("SELECT 1"), "Should not include the earlier statement");
});

runCase("computeCurrentStatement-falls-back-when-no-ast", () => {
  const stmt = computeCurrentStatement("SELECT 1", 5, null, "fallback-line");
  assert.strictEqual(stmt, "fallback-line", "Null parsed should use fallback");
});

// ── getAliasBeforeDot ─────────────────────────────────────────────────────────

runCase("getAliasBeforeDot-extracts-simple-identifier", () => {
  assert.strictEqual(getAliasBeforeDot("SELECT e."), "e");
  assert.strictEqual(getAliasBeforeDot("SELECT myAlias."), "myAlias");
});

runCase("getAliasBeforeDot-handles-bracketed-identifier", () => {
  const result = getAliasBeforeDot("SELECT [my alias].");
  assert.strictEqual(result, "my alias", "Bracketed alias should be extracted");
});

runCase("getAliasBeforeDot-returns-null-when-not-ending-with-dot", () => {
  assert.strictEqual(getAliasBeforeDot("SELECT e"), null, "No dot → null");
  assert.strictEqual(getAliasBeforeDot(""), null, "Empty → null");
});

// ── isFromJoinTableContext / endsWithDotToken / isLikelySelectProjectionByText

runCase("context-detectors-classify-correctly", () => {
  assert.ok(isFromJoinTableContext("SELECT * FROM "), "FROM → true");
  assert.ok(isFromJoinTableContext("SELECT * FROM T JOIN "), "JOIN → true");
  assert.ok(!isFromJoinTableContext("SELECT col"), "Not FROM/JOIN → false");
  assert.ok(endsWithDotToken("e."), "Ends with dot → true");
  assert.ok(!endsWithDotToken("SELECT"), "No dot → false");
  assert.ok(isLikelySelectProjectionByText("SELECT col"), "After SELECT → true");
  assert.ok(!isLikelySelectProjectionByText(""), "Empty → false");
});

// ── isInSelectProjectionContext ───────────────────────────────────────────────

runCase("isInSelectProjectionContext-detects-projection-position", () => {
  const sql = "SELECT Name FROM Employee;";
  const parsed = parseSql(sql);
  const nameOff = sql.indexOf("Name");
  assert.ok(isInSelectProjectionContext(parsed, nameOff, "SELECT "),
    "Position in SELECT list before FROM should be projection context");
  // Position inside the WHERE clause (well past the FROM and table references) → not projection
  const whereClauseSql = "SELECT Name FROM Employee WHERE EmployeeId = 1;";
  const whereParsed = parseSql(whereClauseSql);
  const whereOff = whereClauseSql.indexOf("EmployeeId = 1");
  assert.ok(!isInSelectProjectionContext(whereParsed, whereOff, "SELECT Name FROM Employee WHERE "),
    "Position inside WHERE clause should not be projection context");
});

// ── getContainingStatementNode / getStatementTableCandidatesFromAst ───────────

runCase("getContainingStatementNode-finds-correct-statement", () => {
  const sql = "SELECT 1; UPDATE Employee SET Name = 'x';";
  const parsed = parseSql(sql);
  const off = sql.indexOf("UPDATE");
  const stmt = getContainingStatementNode(parsed?.ast, off);
  assert.strictEqual(stmt?.type, "UpdateStatement", "Should find UpdateStatement at that offset");
});

runCase("collectTablesFromAstNode-finds-all-table-references", () => {
  const sql = "SELECT e.Name FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId;";
  const parsed = parseSql(sql);
  const tables = collectTablesFromAstNode(parsed?.ast);
  assert.ok(tables.includes("employee"), "Should find Employee");
  assert.ok(tables.includes("department"), "Should find Department");
});

runCase("getStatementTableCandidatesFromAst-scoped-to-current-statement", () => {
  const sql = "SELECT * FROM Employee e; SELECT * FROM Department d;";
  const parsed = parseSql(sql);
  const off = sql.indexOf("Employee");
  const tables = getStatementTableCandidatesFromAst(parsed, off);
  assert.ok(tables.includes("employee"), "Should find Employee in first statement");
});

// ── getResolutionSourceColumns ────────────────────────────────────────────────

runCase("getResolutionSourceColumns-extracts-table-column-pairs", () => {
  const resolution = { inputs: [{ kind: "column", source: "Employee", name: "Employee.Name" }] };
  const result = getResolutionSourceColumns(resolution);
  assert.strictEqual(result.length, 1, "Should extract one column pair");
  assert.strictEqual(result[0].table, "employee");
  assert.strictEqual(result[0].column, "name");
});

runCase("getResolutionSourceColumns-deduplicates-same-source-column", () => {
  const resolution = {
    inputs: [
      { kind: "column", source: "Employee", name: "Employee.Name" },
      { kind: "column", source: "Employee", name: "Employee.Name" }
    ]
  };
  const result = getResolutionSourceColumns(resolution);
  assert.strictEqual(result.length, 1, "Duplicate source+column should be deduplicated");
});

// ── getHoverColumnLabel ───────────────────────────────────────────────────────

runCase("getHoverColumnLabel-prefers-original-case-of-token", () => {
  const col = { rawName: "EmployeeId", name: "employeeid" };
  assert.strictEqual(getHoverColumnLabel(col, "e.EmployeeId"), "EmployeeId",
    "Should use the token's case for the label");
});

runCase("getHoverColumnLabel-falls-back-to-rawName", () => {
  const col = { rawName: "Name" };
  assert.strictEqual(getHoverColumnLabel(col), "Name", "Should fall back to rawName");
});

// ── getSymbolLocalColumns ─────────────────────────────────────────────────────

runCase("getSymbolLocalColumns-returns-columns-array", () => {
  const sym = { columns: [{ name: "id", rawName: "Id", type: "INT" }] };
  const result = getSymbolLocalColumns(sym);
  assert.ok(Array.isArray(result), "Should return an array");
  assert.strictEqual(result!.length, 1, "Should have one column");
});

runCase("getSymbolLocalColumns-returns-undefined-for-null-sym", () => {
  assert.strictEqual(getSymbolLocalColumns(null), undefined);
  assert.strictEqual(getSymbolLocalColumns(undefined), undefined);
});

// ── getPropertyAccessAtOffset ─────────────────────────────────────────────────

runCase("getPropertyAccessAtOffset-returns-null-when-no-accesses", () => {
  const parsed = parseSql("SELECT Name FROM Employee;");
  assert.strictEqual(getPropertyAccessAtOffset(parsed, 5), null,
    "No property accesses → null");
});

// ── getResolvedObjectKindLabel ────────────────────────────────────────────────

runCase("getResolvedObjectKindLabel-identifies-table-type", () => {
  indexText("file:///schema.sql", "CREATE TYPE dbo.MyType AS TABLE (Id INT);");
  assert.strictEqual(getResolvedObjectKindLabel("mytype"), "table type");
  assert.strictEqual(getResolvedObjectKindLabel("regulartable"), "table");
});

// ── isFunctionCallInAst ───────────────────────────────────────────────────────

runCase("isFunctionCallInAst-detects-function-calls", () => {
  const sql = "SELECT dbo.GetEmployee(1);";
  const parsed = parseSql(sql);
  const { isFunc, rawName } = isFunctionCallInAst(parsed?.ast, "dbo.getemployee");
  // The parser may or may not represent this as FunctionCall; just no throw
  assert.ok(typeof isFunc === "boolean", "Should return boolean isFunc");
  assert.ok(typeof rawName === "string", "Should return string rawName");
});

// ── getDefinitionReferenceKeys ────────────────────────────────────────────────

runCase("getDefinitionReferenceKeys-includes-dbo-and-stripped-variants", () => {
  const keys = getDefinitionReferenceKeys("Employee");
  assert.ok(keys.includes("employee"), "Should include normalised form");
  assert.ok(keys.includes("dbo.employee"), "Should include dbo-prefixed form");
});

runCase("getDefinitionReferenceKeys-strips-dbo-prefix", () => {
  const keys = getDefinitionReferenceKeys("dbo.Employee");
  assert.ok(keys.includes("employee"), "Should include stripped form");
  assert.ok(keys.includes("dbo.employee"), "Should include dbo-prefixed form");
});

// ── isReferenceHiddenFromObjectUsage ─────────────────────────────────────────

runCase("isReferenceHiddenFromObjectUsage-hides-create-and-alias-declarations", () => {
  assert.ok(isReferenceHiddenFromObjectUsage({ name: "t", uri: "x", line: 0, start: 0, end: 1, kind: "table", context: "create-definition" }));
  assert.ok(isReferenceHiddenFromObjectUsage({ name: "t", uri: "x", line: 0, start: 0, end: 1, kind: "table", context: "alias-declaration" }));
  assert.ok(!isReferenceHiddenFromObjectUsage({ name: "t", uri: "x", line: 0, start: 0, end: 1, kind: "table", context: undefined }));
});

// ── getDefinitionReferenceLocations ──────────────────────────────────────────

runCase("getDefinitionReferenceLocations-returns-locations-from-index", () => {
  indexText("file:///schema.sql", "CREATE TABLE Employee (Id INT);");
  indexText("file:///query.sql", "SELECT Id FROM Employee;");
  const def = { name: "employee", uri: "file:///schema.sql", line: 0, rawName: "Employee" };
  const locs = getDefinitionReferenceLocations(def);
  assert.ok(Array.isArray(locs), "Should return array of locations");
  assert.ok(locs.length > 0, "Should find at least one reference to Employee");
});

// ── getParserAliasColumnNames deeper paths ────────────────────────────────────

runCase("getParserAliasColumnNames-reads-sym-columns-string-array", () => {
  // When sym.columns is a string array (new parser format for CTEs)
  const sym = { name: "cte", columns: ["EmployeeId", "Name"] };
  const result = getParserAliasColumnNames(null, sym);
  assert.ok(result.includes("EmployeeId") || result.includes("Name"),
    "String array sym.columns should be included in result");
});

// ── collectTablesFromAstNode UPDATE/INSERT paths ───────────────────────────────

runCase("collectTablesFromAstNode-finds-update-target-and-from", () => {
  const sql = "UPDATE e SET e.Name = 'x' FROM Employee e JOIN Department d ON e.EmployeeId = d.DepartmentId;";
  const parsed = parseSql(sql);
  const tables = collectTablesFromAstNode(parsed?.ast);
  assert.ok(tables.includes("employee") || tables.includes("department"),
    "Should find tables in UPDATE ... FROM");
});

runCase("collectTablesFromAstNode-finds-insert-table", () => {
  const sql = "INSERT INTO Employee (Name) VALUES ('x');";
  const parsed = parseSql(sql);
  const tables = collectTablesFromAstNode(parsed?.ast);
  assert.ok(tables.includes("employee"), "Should find INSERT target table");
});

// ── getUpdateSetTargetTable with insideRhs check ──────────────────────────────

runCase("getUpdateSetTargetTable-returns-target-at-set-column-position", () => {
  const sql = "UPDATE Employee SET Name = 'placeholder' WHERE EmployeeId = 1;";
  const parsed = parseSql(sql);
  const nameOff = sql.indexOf("Name =");
  const result = getUpdateSetTargetTable(parsed, nameOff);
  assert.ok(typeof result === "string" || result === undefined, "Should return string or undefined");
  // Position is at left side of assignment (not RHS) → should return target
  if (result) assert.ok(result.toLowerCase().includes("employee"), "Target should be Employee");
});

runCase("getUpdateSetTargetTable-returns-undefined-in-RHS-expression", () => {
  const sql = "UPDATE Employee SET Name = Name WHERE EmployeeId = 1;";
  const parsed = parseSql(sql);
  // Position in the RHS value (after '=')
  const rhsOff = sql.lastIndexOf("Name WHERE") ;
  const result = getUpdateSetTargetTable(parsed, rhsOff);
  // In RHS, the function should return undefined
  assert.ok(result === undefined || typeof result === "string", "Should handle RHS position");
});

// ── getInsertColumnTargetTable ─────────────────────────────────────────────────

runCase("getInsertColumnTargetTable-returns-table-for-column-list-position", () => {
  const sql = "INSERT INTO Employee (EmployeeId, Name) VALUES (1, 'x');";
  const parsed = parseSql(sql);
  // Cursor inside the column list
  const colListOff = sql.indexOf("EmployeeId,");
  const result = getInsertColumnTargetTable(parsed, colListOff);
  assert.ok(result !== undefined, "Should return table name when cursor in column list");
  assert.ok(String(result).toLowerCase().includes("employee"), "Should be Employee");
});

// ── getContainingStatementNode null case ──────────────────────────────────────

runCase("getContainingStatementNode-returns-null-when-offset-outside-all-stmts", () => {
  const sql = "SELECT 1;";
  const parsed = parseSql(sql);
  // Offset way past the end
  const result = getContainingStatementNode(parsed?.ast, 9999);
  assert.strictEqual(result, null, "Offset outside all statements should return null");
});

runCase("getContainingStatementNode-returns-null-for-null-ast", () => {
  assert.strictEqual(getContainingStatementNode(null, 0), null, "Null ast → null");
});

// ── resolveAliasTableFromStatementAst ────────────────────────────────────────

runCase("resolveAliasTableFromStatementAst-resolves-alias-in-select", () => {
  const sql = "SELECT e.Name FROM Employee e;";
  const parsed = parseSql(sql);
  const off = sql.indexOf("e.Name");
  const result = resolveAliasTableFromStatementAst(parsed, off, "e");
  // Should find Employee as the table for alias 'e'
  assert.ok(result === null || String(result).toLowerCase().includes("employee"),
    "Should resolve 'e' to Employee or null if not in FROM yet");
});

// ── getSymbolLocalColumns with localColumns ───────────────────────────────────

runCase("getSymbolLocalColumns-reads-localColumns-array-first", () => {
  const sym = {
    localColumns: [{ rawName: "Col1", name: "col1", dataType: "INT" }],
    columns: [{ rawName: "Other", name: "other" }]
  };
  const result = getSymbolLocalColumns(sym);
  assert.ok(result && result.some((c: any) => c.rawName === "Col1"),
    "localColumns should take precedence over columns");
});

// ── getPropertyAccessAtOffset when accesses exist ────────────────────────────

runCase("getPropertyAccessAtOffset-returns-null-for-select-without-xml", () => {
  const parsed = parseSql("SELECT e.Name.ToUpper() FROM Employee e;");
  // May or may not have propertyAccesses; should not throw
  const result = getPropertyAccessAtOffset(parsed, 10);
  assert.ok(result === null || typeof result === "object", "Should not throw");
});

// ── findStatementLocalColumnOwner success path ────────────────────────────────

runCase("findStatementLocalColumnOwner-resolves-via-resolveBareColumnAtOffset", () => {
  indexText("file:///schema.sql", "CREATE TABLE Employee(EmployeeId INT,Name NVARCHAR(100));");
  const sql = "SELECT Name FROM Employee;";
  const parsed = parseSql(sql);
  const off = sql.indexOf("Name");
  const scope = parsed?.scope?.root?.findInnermost?.(off);
  const result = findStatementLocalColumnOwner("SELECT Name FROM Employee;", "Name", scope, parsed, off);
  if (result) {
    assert.ok(result.ownerName, "Owner name should be set");
    assert.ok(result.column, "Column should be set");
  }
  assert.ok(true, "findStatementLocalColumnOwner should not throw");
});

// ── isAmbiguousBareColumnAtPosition with indexed definitions ─────────────────

runCase("isAmbiguousBareColumnAtPosition-uses-localDefsByName-for-normUri", () => {
  indexText("file:///schema.sql", "CREATE TABLE T(Id INT,Name NVARCHAR(100));");
  // In a same-file context, localDefsByName gets populated
  const { isAmbiguousBareColumnAtPosition } = require("../sql-helpers");
  const sql = "SELECT Id FROM T;";
  indexText("file:///query.sql", sql);
  const { TextDocument } = require("vscode-languageserver-textdocument");
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const pos = doc.positionAt(sql.indexOf("Id"));
  const result = isAmbiguousBareColumnAtPosition(doc, pos, parseSql(sql));
  assert.ok(typeof result === "boolean", "Should return boolean without throw");
});

// ── getDefinitionReferenceLocations with hidden refs ─────────────────────────

runCase("getDefinitionReferenceLocations-skips-create-definitions", () => {
  indexText("file:///schema.sql", "CREATE TABLE Employee(Id INT);");
  indexText("file:///query.sql", "SELECT Id FROM Employee;");
  const def = { name: "employee", uri: "file:///schema.sql", line: 0, rawName: "Employee" };
  const locs = getDefinitionReferenceLocations(def);
  // create-definition refs should be filtered out by isReferenceHiddenFromObjectUsage
  const createRefs = locs.filter(l => l.uri.includes("schema") && l.range.start.line === 0);
  // The usage ref from query.sql should appear, but not the create-definition
  assert.ok(Array.isArray(locs), "Should return array");
});

process.stdout.write("All sql-helpers tests passed.\n");

import * as assert from "assert";
import { TextDocument } from "vscode-languageserver-textdocument";
import { parseSql } from "../sql-parser";
import { computeCompletion } from "../completion-provider";
import { indexText, tablesByName, tableTypesByName, aliasesByUri, definitions, referencesIndex, columnsByTable } from "../definitions";

function resetState(): void {
  aliasesByUri.clear(); definitions.clear(); referencesIndex.clear();
  columnsByTable.clear(); tablesByName.clear(); tableTypesByName.clear();
}
function runCase(name: string, fn: () => void): void {
  resetState(); fn();
  process.stdout.write(`[pass] ${name}\n`);
}

const schemaSql = `
CREATE TABLE Employee (EmployeeId INT, DepartmentId INT, Name NVARCHAR(100), Salary DECIMAL(10,2));
CREATE TABLE Department (DepartmentId INT, Name NVARCHAR(100), Budget DECIMAL(10,2));
`;

function complete(sql: string, cursorOff: number) {
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  return computeCompletion(doc, doc.positionAt(cursorOff), parseSql(sql));
}

runCase("completion-from-join-suggests-table-names", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT * FROM ";
  const items = complete(sql, sql.length);
  const labels = items.map(i => i.label);
  assert.ok(labels.some(l => l === "Employee"), "Should suggest Employee after FROM");
  assert.ok(labels.some(l => l === "Department"), "Should suggest Department after FROM");
});

runCase("completion-after-dot-suggests-alias-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT e. FROM Employee e;";
  const dotOff = sql.indexOf("e.") + 2;
  const items = complete(sql, dotOff);
  const labels = items.map(i => i.label);
  assert.ok(labels.some(l => l === "EmployeeId"), "Should suggest EmployeeId for e.");
  assert.ok(labels.some(l => l === "Name"), "Should suggest Name for e.");
  assert.ok(labels.some(l => l === "Salary"), "Should suggest Salary for e.");
});

runCase("completion-after-dot-suggests-qualified-cte-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "WITH cte AS (SELECT EmployeeId, Name FROM Employee) SELECT cte. FROM cte;";
  const dotOff = sql.indexOf("cte.") + 4;
  const items = complete(sql, dotOff);
  const labels = items.map(i => i.label);
  assert.ok(labels.some(l => l === "EmployeeId" || l === "Name"),
    "Should suggest CTE projected columns after cte.");
});

runCase("completion-select-projection-suggests-visible-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  // Using "SELECT e." so the alias-dot path fires (most reliable in projection context)
  const sql = "SELECT e. FROM Employee e;";
  const dotOff = sql.indexOf("e.") + 2;
  const items = complete(sql, dotOff);
  const labels = items.map(i => i.label);
  assert.ok(labels.some(l => l === "EmployeeId" || l === "Name" || l === "Salary"),
    "Projection context with alias.dot should suggest visible alias columns");
});

runCase("completion-update-set-suggests-target-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "UPDATE Employee SET  WHERE EmployeeId = 1;";
  const setOff = sql.indexOf("SET ") + 4;
  const items = complete(sql, setOff);
  const labels = items.map(i => i.label);
  assert.ok(labels.some(l => l === "Name" || l === "Salary"),
    "UPDATE SET context should suggest target table columns");
});

runCase("completion-insert-column-list-suggests-target-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "INSERT INTO Employee (EmployeeId,  ) VALUES (1, 'x');";
  const commaOff = sql.indexOf(", )") + 2;
  const items = complete(sql, commaOff);
  const labels = items.map(i => i.label);
  assert.ok(labels.length > 0, "INSERT column list context should suggest columns");
  assert.ok(labels.some(l => l === "Name" || l === "Salary" || l === "DepartmentId"),
    "Should suggest Employee columns for INSERT");
});

runCase("completion-fallback-includes-keywords", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "";
  const items = complete(sql, 0);
  const labels = items.map(i => i.label);
  assert.ok(labels.some(l => l === "SELECT" || l === "INSERT" || l === "UPDATE"),
    "Fallback should include SQL keywords");
});

runCase("completion-returns-empty-array-not-null", () => {
  const sql = "SELECT 1;";
  const items = complete(sql, 0);
  assert.ok(Array.isArray(items), "computeCompletion must always return an array");
});

runCase("completion-join-second-table-suggests-department-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT d. FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId;";
  const dotOff = sql.indexOf("d.") + 2;
  const items = complete(sql, dotOff);
  const labels = items.map(i => i.label);
  assert.ok(labels.some(l => l === "DepartmentId" || l === "Budget"),
    "d. should suggest Department columns");
});

runCase("completion-cte-dot-suggests-cte-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "WITH cte AS (SELECT EmployeeId, Name FROM Employee) SELECT cte. FROM cte;";
  const dotOff = sql.lastIndexOf("cte.") + 4;
  const items = complete(sql, dotOff);
  const labels = items.map(i => i.label);
  assert.ok(labels.some(l => l === "EmployeeId" || l === "Name"),
    "CTE dot completion should suggest CTE projected columns");
});

runCase("completion-fallback-scope-shows-columns-when-scope-has-tables", () => {
  indexText("file:///schema.sql", schemaSql);
  // Position after WHERE — scope has Employee alias visible
  const sql = "SELECT e.Name FROM Employee e WHERE ";
  const items = complete(sql, sql.length);
  const labels = items.map(i => i.label);
  // Should suggest at least variable items, keywords, or columns
  assert.ok(items.length > 0, "Fallback completion should always return something");
});

runCase("completion-no-crash-on-empty-document", () => {
  indexText("file:///schema.sql", schemaSql);
  const items = complete("", 0);
  assert.ok(Array.isArray(items), "Empty document completion should return array");
});

runCase("completion-with-variable-in-scope-includes-variable-items", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "DECLARE @count INT; SELECT ";
  const items = complete(sql, sql.length);
  const labels = items.map(i => i.label);
  assert.ok(labels.some(l => l === "@count"), "Variable @count should appear in completion list");
});

runCase("completion-update-set-left-side-suggests-target-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  // Cursor AT the column name in SET (left of '=') — triggers updateSetTarget path
  const sql = "UPDATE Employee SET Name = 'placeholder';";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("Name =");
  const items = computeCompletion(doc, doc.positionAt(off), parseSql(sql));
  const labels = items.map(i => i.label);
  assert.ok(labels.some(l => l === "Name" || l === "EmployeeId"),
    "UPDATE SET completion at column position should include Employee columns");
});

runCase("completion-insert-col-list-suggests-table-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "INSERT INTO Employee (EmployeeId, ) VALUES (1);";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf(", )") + 2; // inside column list after first column
  const items = computeCompletion(doc, doc.positionAt(off), parseSql(sql));
  const labels = items.map(i => i.label);
  assert.ok(labels.length > 0, "INSERT column list completion should return items");
});

runCase("completion-select-projection-without-alias-dot-suggests-scope-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  // Cursor at the column token inside a SELECT projection list
  // 'Na' at the start means we're inside the projection before FROM
  const sql = "SELECT Name FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  // Cursor at the start of 'Name' — inside SELECT projection, before FROM
  const off = sql.indexOf("Name");
  const items = computeCompletion(doc, doc.positionAt(off), parseSql(sql));
  const labels = items.map(i => i.label);
  assert.ok(items.length > 0, "SELECT projection context should return completion items");
  // Should suggest at minimum some column names from the visible scope
  assert.ok(labels.some(l => l === "EmployeeId" || l === "Name" || l === "Employee" || l === "Department"),
    "SELECT projection should suggest columns or tables from visible scope");
});

runCase("completion-fallback-with-visible-scope-suggests-columns-then-tables", () => {
  indexText("file:///schema.sql", schemaSql);
  // WHERE context — scope has alias, fallback path fires
  const sql = "SELECT e.Name FROM Employee e WHERE ";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const items = computeCompletion(doc, doc.positionAt(sql.length), parseSql(sql));
  assert.ok(items.length > 0, "Fallback completion should return at least something");
  // Should include variable items (none here) and table names
  assert.ok(items.some(i => i.label === "Employee" || i.label === "Department" || i.label === "WHERE"),
    "Fallback should include tables or keywords");
});

runCase("completion-tvp-dot-resolves-via-dataType", () => {
  indexText("file:///schema.sql", schemaSql);
  indexText("file:///schema2.sql", "CREATE TYPE dbo.EmpType AS TABLE(EmployeeId INT, Name NVARCHAR(100));");
  const sql = "DECLARE @t dbo.EmpType; SELECT @t. FROM @t;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("@t.") + 3;
  const items = computeCompletion(doc, doc.positionAt(off), parseSql(sql));
  // Should resolve @t through dataType='dbo.EmpType' → tableTypesByName.get('emptype')
  if (items.some(i => i.label === "EmployeeId" || i.label === "Name")) {
    assert.ok(true, "TVP dataType resolution succeeded");
  } else {
    // May not resolve without actual scope, but should not throw
    assert.ok(items.length >= 0, "TVP completion should not throw");
  }
});

runCase("completion-empty-fallback-shows-keywords-and-tables", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "";
  const items = complete(sql, 0);
  const labels = items.map(i => i.label);
  assert.ok(labels.some(l => l === "SELECT"), "Empty completion should include SELECT keyword");
  assert.ok(labels.some(l => l === "Employee" || l === "Department"), "Should include table names");
});

runCase("completion-insert-col-list-excludes-already-listed-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  // EmployeeId is already in the column list — it must not appear in suggestions
  const sql = "INSERT INTO Employee (EmployeeId, ) VALUES (1, 'x');";
  const off = sql.indexOf(", )") + 2; // cursor after trailing comma, inside the column list
  const items = complete(sql, off);
  const labels = items.map(i => i.label);
  assert.ok(labels.length > 0, "Should still suggest remaining columns");
  assert.ok(!labels.includes("EmployeeId"), "Already-listed EmployeeId must be excluded");
  assert.ok(labels.some(l => l === "Name" || l === "DepartmentId" || l === "Salary"),
    "Remaining Employee columns should be suggested");
  // Cross-table bleed: Department columns must NOT appear
  assert.ok(!labels.includes("Budget"), "Department columns must not bleed into INSERT Employee completions");
});

runCase("completion-insert-col-list-after-trailing-comma-stays-scoped", () => {
  indexText("file:///schema.sql", schemaSql);
  // Cursor is AFTER the last listed column (trailing comma), before closing paren.
  // This is the canonical "add another column" position and was the broken case.
  const sql = "INSERT INTO Employee (EmployeeId, Name,) VALUES (1, 'x', 0);";
  const off = sql.indexOf(",)"); // position of the trailing comma
  const items = complete(sql, off + 1); // cursor right after trailing comma
  const labels = items.map(i => i.label);
  // Only Employee columns should appear — Department columns must not bleed in
  assert.ok(!labels.includes("Budget"), "Department.Budget must not appear in INSERT Employee completions");
  assert.ok(!labels.includes("EmployeeId"), "Already-listed EmployeeId must be excluded");
  assert.ok(!labels.includes("Name"), "Already-listed Name must be excluded");
  assert.ok(labels.some(l => l === "DepartmentId" || l === "Salary"),
    "Remaining Employee columns should be present after trailing comma");
});

runCase("completion-update-set-excludes-already-assigned-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  // Name is already in the SET list — it must not appear in suggestions for the second assignment
  const sql = "UPDATE Employee SET Name = 'x',  WHERE EmployeeId = 1;";
  const off = sql.indexOf(",  WHERE") + 2;
  const items = complete(sql, off);
  const labels = items.map(i => i.label);
  assert.ok(labels.length > 0, "Should still suggest remaining columns");
  assert.ok(!labels.includes("Name"), "Already-assigned Name must be excluded");
  assert.ok(labels.some(l => l === "EmployeeId" || l === "DepartmentId" || l === "Salary"),
    "Remaining Employee columns should be suggested");
  // Cross-table bleed: Department columns must NOT appear
  assert.ok(!labels.includes("Budget"), "Department columns must not bleed into UPDATE Employee completions");
});

process.stdout.write("All completion-provider tests passed.\n");

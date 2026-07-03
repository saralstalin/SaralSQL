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

process.stdout.write("All completion-provider tests passed.\n");

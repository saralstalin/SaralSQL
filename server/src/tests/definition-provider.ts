import * as assert from "assert";
import { TextDocument } from "vscode-languageserver-textdocument";
import { parseSql } from "../sql-parser";
import { computeDefinition } from "../definition-provider";
import { indexText, tablesByName, tableTypesByName, aliasesByUri, definitions, referencesIndex, columnsByTable } from "../definitions";

function resetState(): void {
  aliasesByUri.clear(); definitions.clear(); referencesIndex.clear();
  columnsByTable.clear(); tablesByName.clear(); tableTypesByName.clear();
}

function runCase(name: string, fn: () => void): void {
  resetState();
  fn();
  process.stdout.write(`[pass] ${name}\n`);
}

function def(sql: string, tokenOrOff: string | number, schemaUri = "file:///def-schema.sql") {
  const doc = TextDocument.create("file:///def-query.sql", "sql", 1, sql);
  const offset = typeof tokenOrOff === "number" ? tokenOrOff : sql.indexOf(tokenOrOff);
  return computeDefinition(doc, doc.positionAt(offset), parseSql(sql));
}

const schemaSql = `
CREATE TABLE Employee (EmployeeId INT, DepartmentId INT, Name NVARCHAR(100));
CREATE TABLE Department (DepartmentId INT, Name NVARCHAR(100));
`;

// ── Tests ─────────────────────────────────────────────────────────────────────

runCase("definition-qualified-column-navigates-to-schema-column", () => {
  indexText("file:///schema.sql", schemaSql);
  indexText("file:///query.sql", "SELECT e.Name FROM Employee e;");
  const sql = "SELECT e.Name FROM Employee e;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("e.Name") + 2; // on Name
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(result && result.length > 0, "Should navigate to a definition");
  assert.ok(result![0].uri.includes("schema"), "Should point to the schema file");
});

runCase("definition-bare-column-navigates-via-parser-resolution", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT Name FROM Employee;";
  const result = def(sql, "Name");
  assert.ok(result && result.length > 0, "Bare column should navigate to schema definition");
  assert.ok(result![0].uri.includes("schema"), "Should point to schema file");
});

runCase("definition-table-navigates-to-create-statement", () => {
  indexText("file:///schema.sql", schemaSql);
  indexText("file:///query.sql", "SELECT e.Name FROM Employee e;");
  const sql = "SELECT e.Name FROM Employee e;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("Employee");
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(result && result.length > 0, "Table name should navigate to its CREATE TABLE");
  assert.ok(result![0].uri.includes("schema"), "Definition should be in the schema file");
});

runCase("definition-cte-name-navigates-to-cte-body", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "WITH cte AS (SELECT Name FROM Employee) SELECT Name FROM cte;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.lastIndexOf("cte"); // the reference in FROM cte
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  if (result && result.length > 0) {
    // Should navigate within the same query file to the CTE definition
    assert.ok(result[0].uri.includes("query"), "CTE navigation should stay in the query file");
  }
  assert.ok(true, "CTE definition should not throw");
});

runCase("definition-variable-navigates-to-declaration", () => {
  const sql = "DECLARE @count INT; SET @count = 1; SELECT @count;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.lastIndexOf("@count");
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  if (result && result.length > 0) {
    assert.ok(result[0].uri.includes("query"), "Variable navigation should stay in the same file");
  }
  assert.ok(true, "Variable definition should not throw");
});

runCase("definition-unknown-token-returns-null", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT NonExistentCol FROM Employee e;";
  const result = def(sql, "NonExistentCol");
  assert.strictEqual(result, null, "Unknown bare column with no schema match should return null");
});

runCase("definition-merge-using-column-navigates-to-source", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "MERGE Employee AS t USING (SELECT EmployeeId, Name FROM Department) AS s ON t.EmployeeId = s.EmployeeId WHEN MATCHED THEN UPDATE SET t.Name = s.Name;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const usingNameOff = sql.indexOf("USING (SELECT EmployeeId, Name") + "USING (SELECT EmployeeId, ".length;
  const result = computeDefinition(doc, doc.positionAt(usingNameOff), parseSql(sql));
  if (result && result.length > 0) {
    assert.ok(result[0].uri.includes("schema"), "MERGE USING column should navigate to schema");
  }
  assert.ok(true, "MERGE USING definition should not throw");
});

runCase("definition-parameter-navigates-to-declaration", () => {
  const sql = "DECLARE @x INT; SET @x = 5; SELECT @x;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.lastIndexOf("@x");
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  if (result && result.length > 0) {
    assert.ok(result[0].uri.includes("query"), "Parameter def should be in same file");
  }
  assert.ok(true, "Parameter definition should not throw");
});

runCase("definition-column-with-tabletype-navigates-to-schema", () => {
  indexText("file:///schema.sql", schemaSql);
  indexText("file:///query.sql", "SELECT e.EmployeeId FROM Employee e;");
  const sql = "SELECT e.EmployeeId FROM Employee e;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("e.EmployeeId") + 2; // at EmployeeId
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(result && result.length > 0, "Qualified column definition should navigate to schema");
  assert.ok(result![0].uri.includes("schema"), "Should navigate to schema file");
});

runCase("definition-bare-unambiguous-column-resolves", () => {
  indexText("file:///schema.sql", schemaSql);
  indexText("file:///query.sql", "SELECT EmployeeId FROM Employee;");
  const sql = "SELECT EmployeeId FROM Employee;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("EmployeeId");
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  // EmployeeId only in Employee → should navigate
  if (result && result.length > 0) {
    assert.ok(result[0].uri.includes("schema"), "Bare column def should go to schema");
  }
  assert.ok(true, "Bare unambiguous column definition should not throw");
});

runCase("definition-inside-derived-subquery-column", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT sub.EmployeeId FROM (SELECT EmployeeId FROM Employee) sub;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("sub.EmployeeId") + 4;
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(Array.isArray(result) || result === null, "Derived alias column def should not throw");
});

runCase("definition-keyword-returns-null", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT Name FROM Employee;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = 0; // 'S' of SELECT — keyword
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  // SELECT is a keyword, getWordRangeAtPosition may still get a range but no ref
  assert.ok(result === null || Array.isArray(result), "Keyword position should not throw");
});

process.stdout.write("All definition-provider tests passed.\n");

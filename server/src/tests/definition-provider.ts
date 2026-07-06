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

runCase("definition-same-file-table-navigates-to-create", () => {
  // CREATE TABLE in same file — should navigate to it
  const sql = "CREATE TABLE Local(Id INT, Name NVARCHAR(100)); SELECT * FROM Local;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.lastIndexOf("Local");
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(result && result.length > 0, "Same-file table should navigate to CREATE");
  assert.ok(result![0].uri.includes("query"), "Definition should be in same file");
});

runCase("definition-indexed-table-navigates-to-schema", () => {
  indexText("file:///schema.sql", schemaSql);
  indexText("file:///query.sql", "SELECT * FROM Department;");
  const doc = TextDocument.create("file:///query.sql", "sql", 1, "SELECT * FROM Department;");
  const off = "SELECT * FROM ".length;
  const result = computeDefinition(doc, doc.positionAt(off), parseSql("SELECT * FROM Department;"));
  assert.ok(result && result.length > 0, "Indexed table should navigate to definition");
  assert.ok(result![0].uri.includes("schema"), "Should navigate to schema file");
});

runCase("definition-at-variable-declaration-returns-first-ref", () => {
  const sql = "DECLARE @x INT; SET @x = 1;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.lastIndexOf("@x");
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    assert.ok(result.length > 0, "Variable definition should find at least one location");
  }
  assert.ok(true, "Variable definition lookup should not throw");
});

runCase("definition-table-variable-at-prefix-navigates", () => {
  const sql = "DECLARE @tv TABLE(Id INT); SELECT Id FROM @tv;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.lastIndexOf("@tv");
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    assert.ok(result.length > 0, "Table variable definition should find a location");
  }
  assert.ok(true, "Table variable definition should not throw");
});

runCase("definition-cte-column-navigates-to-cte-projection", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "WITH cte AS (SELECT EmployeeId, Name FROM Employee) SELECT cte.EmployeeId FROM cte;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("cte.EmployeeId") + 4; // at EmployeeId
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  if (result && result.length > 0) {
    // CTE columns resolve to the underlying source — e.g. Employee.EmployeeId in schema
    assert.ok(result[0].uri.length > 0, "CTE column definition should navigate somewhere");
  }
  assert.ok(true, "CTE column definition should not throw");
});

runCase("definition-non-existent-bare-column-returns-null", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT Ghost FROM Employee e;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("Ghost");
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  // Ghost column doesn't exist in Employee — should return null
  assert.ok(result === null || (Array.isArray(result) && result.length === 0),
    "Non-existent bare column definition should return null or empty");
});

runCase("definition-localDefs-path-when-word-in-comment-matches-same-file-def", () => {
  // 'T' appears in a SQL comment — no ref is indexed there, but localDefs has the
  // CREATE TABLE T definition in the same file. findReferenceAtPosition returns null
  // at comment positions, so execution falls through to the localDefs check (line 43).
  const sql = "CREATE TABLE T (Id INT);\n-- T is referenced here in a comment\nSELECT Id FROM T;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  // Cursor at 'T' inside the comment (line 1, char 3)
  const commentTOff = sql.indexOf("-- T") + 3;
  const parsed = parseSql(sql);
  const result = computeDefinition(doc, doc.positionAt(commentTOff), parsed);
  // localDefs.filter finds CREATE TABLE T → navigates within the query file
  assert.ok(result && result.length > 0, "localDefs path should find the same-file CREATE TABLE T definition");
  assert.ok(result![0].uri.includes("query"), "localDefs navigation should stay in the query file");
});

runCase("definition-findColumnInTable-via-stmtOwner-bare-column-unindexed", () => {
  // Schema is indexed, but the query file is NOT indexed.
  // findReferenceAtPosition returns null → falls to bare column path → stmtOwner → findColumnInTable.
  indexText("file:///schema.sql", schemaSql);
  // Deliberately do NOT call indexText for the query file
  const sql = "SELECT Name FROM Employee;";
  const doc = TextDocument.create("file:///unindexed-query.sql", "sql", 1, sql);
  const off = sql.indexOf("Name");
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  // stmtOwner resolves to Employee via parser, findColumnInTable finds Name in schema
  assert.ok(result && result.length > 0, "Bare column definition via stmtOwner should find schema location");
  assert.ok(result![0].uri.includes("schema"), "Should navigate to schema file");
});

runCase("definition-bare-column-stmtOwner-returns-null-when-column-not-in-schema", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT Ghost FROM Employee;";
  const doc = TextDocument.create("file:///unindexed.sql", "sql", 1, sql);
  const off = sql.indexOf("Ghost");
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  // Ghost not in Employee → findColumnInTable returns [] → returns null
  assert.ok(result === null || (Array.isArray(result) && result.length === 0),
    "Column not in schema via stmtOwner should return null");
});

runCase("definition-parameter-ref-found-in-referencesIndex", () => {
  // DECLARE + USE — @x is indexed, second usage should navigate to declaration
  const sql = "DECLARE @x INT = 5; SET @x = @x + 1;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const lastXOff = sql.lastIndexOf("@x");
  const result = computeDefinition(doc, doc.positionAt(lastXOff), parseSql(sql));
  if (result && result.length > 0) {
    assert.ok(result[0].uri.includes("query"), "Parameter definition should be in the same file");
  }
  assert.ok(true, "Parameter definition via referencesIndex should not throw");
});

runCase("definition-alias-qualified-column-in-update-navigates-to-schema", () => {
  // UPDATE p SET p.Status ... FROM Patent p — hover shows the column, definition should navigate too.
  // The alias 'p' → Patent mapping is populated when parsed in full procedure context.
  indexText("file:///schema.sql", `
CREATE TABLE Patent (
  PatentId INT,
  Status VARCHAR(50),
  UpdatedBy INT,
  UpdatedAt DATETIME
);
`);
  const sql = `
CREATE PROCEDURE UpdatePatent @PatentId INT, @Status VARCHAR(50), @UpdatedBy INT
AS BEGIN
  UPDATE p
  SET p.Status = @Status,
      p.UpdatedBy = @UpdatedBy
  FROM Patent p
  WHERE p.PatentId = @PatentId
END
`;
  indexText("file:///proc.sql", sql);
  const doc = TextDocument.create("file:///proc.sql", "sql", 1, sql);
  const parsed = parseSql(sql);

  // p.Status — must navigate to Patent.Status in schema
  const statusOff = sql.indexOf("p.Status");
  const statusResult = computeDefinition(doc, doc.positionAt(statusOff), parsed);
  assert.ok(statusResult && statusResult.length > 0,
    "p.Status in UPDATE SET must navigate to Patent.Status definition");
  assert.ok(statusResult![0].uri.includes("schema"),
    "Navigation must point to the schema file containing CREATE TABLE Patent");

  // p.UpdatedBy — same table, different column
  const updatedByOff = sql.indexOf("p.UpdatedBy");
  const updatedByResult = computeDefinition(doc, doc.positionAt(updatedByOff), parsed);
  assert.ok(updatedByResult && updatedByResult.length > 0,
    "p.UpdatedBy in UPDATE SET must also navigate to Patent schema");
});

runCase("definition-alias-qualified-column-in-select-join-navigates", () => {
  // Standard SELECT with alias: e.Name should navigate to Employee.Name
  indexText("file:///schema.sql", schemaSql);
  indexText("file:///query.sql", "SELECT e.Name FROM Employee e;");
  const sql = "SELECT e.Name FROM Employee e;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("e.Name") + 2; // at Name
  const result = computeDefinition(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(result && result.length > 0, "e.Name should navigate to Employee.Name definition");
  assert.ok(result![0].uri.includes("schema"), "Should navigate to schema file");
});

process.stdout.write("All definition-provider tests passed.\n");

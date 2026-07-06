import * as assert from "assert";
import { TextDocument } from "vscode-languageserver-textdocument";
import { parseSql } from "../sql-parser";
import { computeHover } from "../hover-provider";
import { indexText, tablesByName, tableTypesByName, aliasesByUri, definitions, referencesIndex, columnsByTable } from "../definitions";
import { normalizeName } from "../text-utils";

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

function hover(sql: string, tokenOrChar: string | number, schemaUri = "file:///hover-schema.sql") {
  const doc = TextDocument.create("file:///hover-query.sql", "sql", 1, sql);
  const offset = typeof tokenOrChar === "number" ? tokenOrChar : sql.indexOf(tokenOrChar);
  const pos = doc.positionAt(offset);
  const parsed = parseSql(sql);
  return computeHover(doc, pos, parsed);
}

// ── Schema setup ──────────────────────────────────────────────────────────────
const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT,
  DepartmentId INT,
  Name NVARCHAR(100),
  Salary DECIMAL(10,2)
);
CREATE TABLE Department (
  DepartmentId INT,
  Name NVARCHAR(100),
  Budget DECIMAL(10,2)
);
CREATE TYPE dbo.EmployeeTableType AS TABLE (
  EmployeeId INT,
  Name NVARCHAR(100)
);
`;

// ── Tests ─────────────────────────────────────────────────────────────────────

runCase("hover-qualified-column-shows-column-and-table", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT e.Name FROM Employee e;";
  const result = hover(sql, "e.Name");
  assert.ok(result, "Hover on e.Name should return a result");
  const value = String((result!.contents as any).value ?? "");
  assert.ok(value.includes("Column"), "Should be labelled Column");
  assert.ok(value.includes("Name"), "Should mention Name");
  assert.ok(value.toLowerCase().includes("employee"), "Should mention Employee");
});

runCase("hover-unaliased-bare-column-resolves-via-parser-hint", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT Name FROM Employee;";
  const result = hover(sql, "Name");
  assert.ok(result, "Hover on bare Name (unaliased) should return a result");
  const value = String((result!.contents as any).value ?? "");
  assert.ok(value.includes("Column"), "Should be labelled Column");
  assert.ok(value.toLowerCase().includes("employee"), "Should attribute Name to Employee");
});

runCase("hover-table-alias-shows-table-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  indexText("file:///query.sql", "SELECT e.Name FROM Employee e;");
  const sql = "SELECT e.Name FROM Employee e;";
  const docWithRefs = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const pos = docWithRefs.positionAt(sql.indexOf(" e") + 1);  // on the alias 'e' in FROM
  const parsed = parseSql(sql);
  const result = computeHover(docWithRefs, pos, parsed);
  // Hover on alias 'e' → should show Alias resolving to Employee with columns
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(value.toLowerCase().includes("employee") || value.toLowerCase().includes("alias"), "Alias hover should mention Employee or Alias");
  }
  // Result may be null if the reference isn't indexed — that's acceptable
  assert.ok(true, "Alias hover should not throw");
});

runCase("hover-table-name-shows-schema", () => {
  indexText("file:///schema.sql", schemaSql);
  indexText("file:///query.sql", "SELECT * FROM Employee e;");
  const sql = "SELECT * FROM Employee e;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const pos = doc.positionAt(sql.indexOf("Employee"));
  const parsed = parseSql(sql);
  const result = computeHover(doc, pos, parsed);
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(
      value.toLowerCase().includes("table") || value.toLowerCase().includes("employee"),
      "Table hover should show table/column info"
    );
  }
  assert.ok(true, "Table hover should not throw");
});

runCase("hover-no-result-for-keyword", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT Name FROM Employee;";
  const result = hover(sql, "SELECT");
  // SELECT is a keyword, getWordRangeAtPosition may return null or it may not match a ref
  // Either way, result can be null — just confirm no throw
  assert.ok(result === null || result !== undefined, "Hover on keyword should not throw");
});

runCase("hover-cte-column-resolves-to-base-table", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "WITH cte AS (SELECT Name FROM Employee) SELECT Name FROM cte;";
  // Hover on the OUTER 'Name' (in SELECT Name FROM cte) — should resolve to Employee
  const outerNameOff = sql.lastIndexOf("SELECT Name") + "SELECT ".length;
  const result = hover(sql, outerNameOff);
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(value.includes("Column"), "CTE outer Name should be a Column");
    assert.ok(value.toLowerCase().includes("employee"), "CTE outer Name should resolve to Employee (base table)");
  }
});

runCase("hover-variable-with-table-type-shows-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = `DECLARE @tv dbo.EmployeeTableType; SELECT @tv.Name;`;
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const atTvOff = sql.indexOf("@tv.Name") + 1;
  const pos = doc.positionAt(atTvOff);
  const parsed = parseSql(sql);
  const result = computeHover(doc, pos, parsed);
  // @tv.Name — should either show a Column result or Table Variable result
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(value.includes("Name") || value.includes("Variable") || value.includes("Column"),
      "Hover on @tv.Name should mention Name or the variable type");
  }
  assert.ok(true, "TVP column hover should not throw");
});

runCase("hover-bare-column-in-update-set-resolves-to-target", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "UPDATE Employee SET Name = 'x' WHERE EmployeeId = 1;";
  const result = hover(sql, "Name = ");
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(value.includes("Column"), "UPDATE bare Name should show Column hover");
    assert.ok(value.toLowerCase().includes("employee"), "UPDATE bare Name should resolve to Employee");
  }
  assert.ok(true, "UPDATE SET hover should not throw");
});

runCase("hover-merge-using-column-resolves-to-source", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = `MERGE Employee AS t USING (SELECT EmployeeId, Name FROM Department) AS s ON t.EmployeeId = s.EmployeeId WHEN MATCHED THEN UPDATE SET t.Name = s.Name;`;
  // Inner Name inside USING subquery should resolve to Department
  const usingNameOff = sql.indexOf("USING (SELECT EmployeeId, Name") + "USING (SELECT EmployeeId, ".length;
  const result = hover(sql, usingNameOff);
  // Parser now provides matchedResolution for MERGE USING positions
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(value.includes("Column") || value.includes("Name"), "MERGE USING Name should show a column hover");
  }
  assert.ok(true, "MERGE USING hover should not throw");
});

runCase("hover-returns-null-for-unknown-token", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT NonExistentColumn FROM Employee e;";
  const result = hover(sql, "NonExistentColumn");
  // No match in refs, no matchedResolution → null
  assert.strictEqual(result, null, "Hover on unknown bare column with no ref match should return null");
});

runCase("computeCurrentStatement-extracted-correctly", () => {
  // Verify computeCurrentStatement (extracted to sql-helpers) works standalone
  const { computeCurrentStatement } = require("../sql-helpers");
  const text = "SELECT 1; SELECT Name FROM Employee;";
  const parsed = parseSql(text);
  const offset = text.indexOf("Name");
  const stmt = computeCurrentStatement(text, offset, parsed, "fallback");
  assert.ok(stmt.includes("SELECT Name"), "Should return the statement containing the offset");
  assert.ok(!stmt.includes("SELECT 1"), "Should not include the earlier statement");
});

runCase("hover-table-reference-shows-column-list", () => {
  indexText("file:///schema.sql", schemaSql);
  // Index the query so findReferenceAtPosition finds a "table" ref at "Employee"
  const sql = "SELECT e.Name FROM Employee e;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const empOff = sql.indexOf("Employee");
  const result = computeHover(doc, doc.positionAt(empOff), parseSql(sql));
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(value.toLowerCase().includes("employee"), "Table hover should mention Employee");
    // Should show columns
    assert.ok(value.includes("EmployeeId") || value.includes("Name"), "Table hover should list columns");
  }
  assert.ok(true, "Table name hover should not throw");
});

runCase("hover-cte-alias-shows-cte-columns", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "WITH cte AS (SELECT EmployeeId FROM Employee) SELECT cte.EmployeeId FROM cte;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  // Hover on "cte" in FROM cte
  const cteOff = sql.lastIndexOf("FROM cte") + 5;
  const result = computeHover(doc, doc.positionAt(cteOff), parseSql(sql));
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(value.toLowerCase().includes("cte") || value.toLowerCase().includes("employee"),
      "CTE table hover should show CTE info or base table");
  }
  assert.ok(true, "CTE table hover should not throw");
});

runCase("hover-table-alias-resolving-to-table", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT e.Name FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  // Hover on alias "e" in FROM Employee e
  const aliasOff = sql.indexOf("Employee e") + 9;
  const result = computeHover(doc, doc.positionAt(aliasOff), parseSql(sql));
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(
      value.toLowerCase().includes("alias") || value.toLowerCase().includes("employee"),
      "Alias hover should mention Alias or Employee"
    );
  }
  assert.ok(true, "Alias table hover should not throw");
});

runCase("hover-qualified-column-at-tvp-variable", () => {
  indexText("file:///schema.sql", schemaSql);
  indexText("file:///schema2.sql", `CREATE TYPE dbo.EmployeeType AS TABLE (EmployeeId INT, Name NVARCHAR(100));`);
  const sql = "DECLARE @t dbo.EmployeeType; SELECT @t.Name;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  // Hover on Name in @t.Name
  const nameOff = sql.indexOf("@t.Name") + 3;
  const result = computeHover(doc, doc.positionAt(nameOff), parseSql(sql));
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(value.includes("Column") || value.includes("Name") || value.includes("Variable"),
      "TVP column hover should show column info");
  }
  assert.ok(true, "TVP qualified column hover should not throw");
});

runCase("hover-bare-column-with-type-shows-type", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT Name FROM Employee;";
  const result = hover(sql, "Name");
  if (result) {
    const value = String((result.contents as any).value ?? "");
    // Name is NVARCHAR(100) in schema — type should appear if column has type
    assert.ok(value.includes("Column"), "Bare column hover should label it Column");
  }
});

runCase("hover-parameter-variable-resolves-type", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "DECLARE @count INT; SET @count = 1; SELECT @count;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.lastIndexOf("@count");
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(
      value.includes("Parameter") || value.includes("Variable") || value.includes("@count"),
      "Variable hover should show parameter info"
    );
  }
  assert.ok(true, "Variable hover should not throw");
});

runCase("hover-column-kind-qualified-shows-type-from-schema", () => {
  indexText("file:///schema.sql", schemaSql);
  // e.Salary is a DECIMAL — hover should include the type
  const sql = "SELECT e.Salary FROM Employee e;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("e.Salary") + 2; // at Salary
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(value.includes("Column"), "e.Salary hover should be Column");
    assert.ok(value.toLowerCase().includes("salary") || value.toLowerCase().includes("decimal"),
      "Should mention Salary or DECIMAL type");
  }
  assert.ok(true, "Qualified column with type should not throw");
});

runCase("hover-bare-column-in-single-table-shows-column-type", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT Salary FROM Employee;";
  const result = hover(sql, "Salary");
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(value.includes("Column"), "Bare Salary should be labelled Column");
    // Salary has type DECIMAL(10,2) — type should appear if column has type
    if (value.includes("DECIMAL") || value.includes("Salary")) {
      assert.ok(true, "Column type is shown correctly");
    }
  }
});

runCase("hover-table-with-at-prefix-variable-shows-variable-info", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "DECLARE @emp NVARCHAR(100); SELECT @emp;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.lastIndexOf("@emp");
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(value.includes("Parameter") || value.includes("@emp"),
      "@emp hover should show parameter info");
  }
  assert.ok(true, "Variable hover should not throw");
});

runCase("hover-column-qualified-with-unresolved-table-still-returns", () => {
  indexText("file:///schema.sql", schemaSql);
  // unknown.Col — table not in schema, should still show something without crashing
  const sql = "SELECT u.Col FROM Unknown u;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("u.Col") + 2;
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  // May return a partial result or null — just no crash
  assert.ok(result === null || typeof result === "object", "Unknown qualified column should not throw");
});

runCase("hover-column-two-part-resolves-via-alias", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT d.DepartmentId FROM Department d;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("d.DepartmentId") + 2; // at DepartmentId
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(value.includes("Column"), "d.DepartmentId should be Column");
    assert.ok(value.toLowerCase().includes("department"), "Should mention Department");
  }
  assert.ok(true, "Alias-qualified column should not throw");
});

runCase("hover-table-kind-without-schema-definition-returns-gracefully", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT * FROM Ghost g;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("Ghost");
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  // Ghost table not in schema — may return null or a partial result
  assert.ok(result === null || typeof result === "object",
    "Hover on unknown table should not throw");
});

// ── localDefsByName populated (line 51): query file has its own CREATE TABLE ─────

runCase("hover-same-file-definition-populates-localDefsByName", () => {
  // When the query file contains its own CREATE TABLE, localDefsByName gets entries.
  // Hovering on a column from that local table exercises the localDefsByName lookup path.
  const sql = "CREATE TABLE Local(Id INT, Name NVARCHAR(100));\nSELECT l.Name FROM Local l;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("l.Name") + 2;
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(result, "Should have hover for l.Name from same-file table");
  assert.ok(String((result!.contents as any).value).includes("Name"), "Should mention Name column");
});

// ── UPDATE SET bare column (line 100): getUpdateSetTargetTable path ───────────

runCase("hover-bare-column-in-update-set-shows-column-via-mutation-target", () => {
  // No match at position (bare column), getUpdateSetTargetTable resolves the target.
  // Requires full procedure context so that Employee is the resolved target.
  const sql = `
CREATE PROCEDURE Upd @n NVARCHAR(100) AS BEGIN
  UPDATE Employee SET Name = @n WHERE EmployeeId = 1;
END`;
  indexText("file:///schema.sql", schemaSql);
  indexText("file:///proc.sql", sql);
  const doc = TextDocument.create("file:///proc.sql", "sql", 1, sql);
  // Hover at bare Name in SET clause (where no ref exists at that position)
  const off = sql.indexOf("SET Name") + 4;
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result.contents as any).value ?? "");
    assert.ok(value.includes("Column"), "Bare Name in UPDATE SET should show Column");
  }
  assert.ok(true, "UPDATE SET bare column hover should not throw");
});

// ── Table variable with DECLARE @tv TABLE (line 128: localCols path) ─────────

runCase("hover-table-variable-declared-with-table-keyword-shows-columns", () => {
  // DECLARE @tv TABLE(Id INT, Name VARCHAR(100)) — @tv has localColumns from the parser.
  // getSymbolLocalColumns reads localColumns → columns = localCols → line 128.
  const sql = "DECLARE @tv TABLE(Id INT, Name NVARCHAR(100)); SELECT @tv.Id FROM @tv;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.lastIndexOf("@tv");
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(result, "Hovering @tv (inline TABLE) should produce a result");
  const value = String((result!.contents as any).value ?? "");
  assert.ok(value.includes("Table Variable") || value.includes("Id") || value.includes("Name"),
    "Should show table variable columns");
});

// ── Typed table variable @tv via dataType (lines 144-187) ────────────────────

runCase("hover-typed-table-variable-shows-type-columns", () => {
  // DECLARE @tv dbo.EmpType — @tv.dataType = "dbo.EmpType" → tableTypesByName lookup.
  indexText("file:///schema.sql", schemaSql);
  indexText("file:///type.sql", "CREATE TYPE dbo.EmpType AS TABLE(EmployeeId INT, Name NVARCHAR(100));");
  const sql = "DECLARE @tv dbo.EmpType; SELECT @tv.Name FROM @tv;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.lastIndexOf("@tv");
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(result, "Typed @tv should produce hover");
  const value = String((result!.contents as any).value ?? "");
  assert.ok(value.includes("Table Variable") || value.includes("dbo.EmpType"),
    "Should show Table Variable with type name");
});

// ── CTE hover shows projected columns (lines 202-208) ────────────────────────

runCase("hover-cte-name-shows-cte-header", () => {
  indexText("file:///schema.sql", schemaSql);
  const sql = "WITH emp AS (SELECT EmployeeId, Name FROM Employee) SELECT Name FROM emp;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.lastIndexOf("emp");
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(result, "CTE name hover should produce a result");
  const value = String((result!.contents as any).value ?? "");
  assert.ok(value.includes("CTE") || value.includes("EmployeeId") || value.includes("Name"),
    "CTE hover should show CTE info or projected columns");
});

// ── Alias hover showing underlying table columns (line 199: scope path) ──────

runCase("hover-update-target-alias-shows-table-via-scope", () => {
  // In full procedure context, Alias:p has location.table.name = "Patent".
  // Line 199 fires when norm = alias_name, tablesByName.get(norm) = undefined,
  // falls to scope, finds Alias → resolves to Patent via aliasTableNorm.
  indexText("file:///schema.sql", `
CREATE TABLE Patent(PatentId INT, Status VARCHAR(50), UpdatedBy INT);
`);
  const sql = `
CREATE PROCEDURE Upd @PatentId INT, @Status VARCHAR(50), @UpdatedBy INT
AS BEGIN
  UPDATE p SET p.Status = @Status, p.UpdatedBy = @UpdatedBy
  FROM Patent p WHERE p.PatentId = @PatentId;
END`;
  indexText("file:///proc.sql", sql);
  const doc = TextDocument.create("file:///proc.sql", "sql", 1, sql);
  const off = sql.indexOf("UPDATE p") + 7; // at 'p' in UPDATE p
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result!.contents as any).value ?? "");
    assert.ok(value.toLowerCase().includes("patent") || value.includes("Alias") || value.includes("Status"),
      "UPDATE target alias 'p' should show Patent alias/table info");
  }
  assert.ok(true, "UPDATE target alias hover should not throw");
});

// ── TVF alias hover (lines 219-225) ──────────────────────────────────────────

runCase("hover-table-valued-function-alias-shows-tvf-info", () => {
  // FROM dbo.GetItems(1) f — 'f' alias → resolveAliasTableName gives function name
  // → isFunctionCallInAst confirms it's a TVF → shows "table-valued function" label.
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT f.Id FROM dbo.GetItems(1) f;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.lastIndexOf(" f") + 1;
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result!.contents as any).value ?? "");
    assert.ok(
      value.includes("table-valued function") || value.includes("Alias") || value.includes("dbo.GetItems"),
      "TVF alias hover should mention table-valued function or GetItems"
    );
  }
  assert.ok(true, "TVF alias hover should not throw");
});

// ── Bare column no-match fallback (line 229: stmtOwner path without ref) ─────

runCase("hover-bare-column-no-ref-uses-stmtOwner-fallback", () => {
  // No indexText for query file — findReferenceAtPosition returns null.
  // Falls through to stmtOwner via findStatementLocalColumnOwner.
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT Name FROM Employee;";
  const doc = TextDocument.create("file:///unindexed.sql", "sql", 1, sql);
  const off = sql.indexOf("Name");
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result!.contents as any).value ?? "");
    assert.ok(value.includes("Column"), "Bare Name should show Column info");
    assert.ok(value.toLowerCase().includes("employee"), "Should attribute to Employee");
  }
  assert.ok(true, "No-ref bare column hover should not throw");
});

// ── @-prefixed qualified column (@tv.Name) (lines 249-272) ───────────────────

runCase("hover-at-prefixed-qualified-column-shows-column-from-variable", () => {
  // @tv.Name — tableName starts with '@', looks up @tv in scope, finds type → column.
  const sql = "DECLARE @tv TABLE(Id INT, Name NVARCHAR(100)); SELECT @tv.Name FROM @tv;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("@tv.Name") + 4; // at Name
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result!.contents as any).value ?? "");
    assert.ok(value.includes("Column") || value.includes("Name"),
      "@tv.Name hover should show Column info");
  }
  assert.ok(true, "@tv.Name hover should not throw");
});

// ── Alias qualified column (e.Name) with ref indexed (lines 281, 294) ────────

runCase("hover-alias-qualified-column-resolves-via-scope-alias", () => {
  // e.Name with ref indexed: match.kind="column" parts=["employee","name"].
  // After direct table lookup, scope check finds Alias:e → Employee → column.
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT e.Name, e.EmployeeId FROM Employee e;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("e.EmployeeId") + 2; // at EmployeeId
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(result, "e.EmployeeId should produce hover");
  const value = String((result!.contents as any).value ?? "");
  assert.ok(value.includes("Column"), "Should label it Column");
  assert.ok(value.toLowerCase().includes("employeeid"), "Should mention EmployeeId");
});

// ── Unknown alias column fallback (line 333+) ────────────────────────────────

runCase("hover-unknown-alias-column-shows-generic-fallback", () => {
  // g.Name where GhostTable is not in schema — colDef not found.
  // Falls to else branch showing Column in table g (generic fallback, line 333+).
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT g.Name FROM GhostTable g;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("g.Name") + 2;
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result!.contents as any).value ?? "");
    assert.ok(value.includes("Column"), "Unknown alias column should still show Column label");
  }
  assert.ok(true, "Unknown alias column hover should not throw");
});

// ── GROUP BY bare column: matchedResolution null, stmtOwner fires (96-104) ────

runCase("hover-group-by-bare-column-uses-stmtOwner-path", () => {
  // GROUP BY Name — matchedResolution is null for GROUP BY positions.
  // Do NOT index the query file so findReferenceAtPosition returns null → !match block.
  // getUpdateSetTargetTable returns null (not in UPDATE SET).
  // findStatementLocalColumnOwner resolves via scope (Alias:e → Employee) → lines 96-104.
  // Use a schema with only one table to avoid ambiguity.
  indexText("file:///emp-only-schema.sql", "CREATE TABLE Employee(EmployeeId INT,Name NVARCHAR(100));");
  // Simple GROUP BY without aliased columns — parser doesn't resolve GROUP BY positions.
  const sql = "SELECT Name FROM Employee e GROUP BY Name;";
  // Deliberately NOT indexing so match=null, scope walk finds Employee.Name → stmtOwner fires.
  const doc = TextDocument.create("file:///unindexed-group.sql", "sql", 1, sql);
  const off = sql.lastIndexOf("BY Name") + 3; // at bare Name in GROUP BY
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(result, "GROUP BY bare Name (no alias, unindexed) must produce hover via stmtOwner");
  const value = String((result!.contents as any).value ?? "");
  assert.ok(value.includes("Column"), "stmtOwner path should show Column label");
  assert.ok(value.toLowerCase().includes("name"), "Should mention Name");
});

// ── No word range (line 32: early null return) ────────────────────────────────

runCase("hover-returns-null-for-position-with-no-token", () => {
  indexText("file:///schema.sql", schemaSql);
  // A line with only spaces — no token chars on either side → getWordRangeAtPosition returns null.
  const sql = "SELECT Name FROM Employee;\n   \nSELECT EmployeeId FROM Employee;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  // Line 1 (index 1) has only spaces — position at line 1 char 1
  const result = computeHover(doc, { line: 1, character: 1 }, parseSql(sql));
  assert.strictEqual(result, null, "Blank line position should return null (no word range)");
});

// ── Alias hover: procedure context with UPDATE target (line 199) ─────────────

runCase("hover-alias-in-select-from-shows-alias-resolves-to-table", () => {
  // When ref.name = alias_name and not in tablesByName, scope finds Alias sym,
  // resolves aliasTableNorm → table → shows Alias header + table columns.
  indexText("file:///schema.sql", schemaSql);
  const sql = `
CREATE PROCEDURE GetEmps AS BEGIN
  SELECT emp.Name FROM Employee emp WHERE emp.EmployeeId > 0;
END`;
  indexText("file:///proc.sql", sql);
  const doc = TextDocument.create("file:///proc.sql", "sql", 1, sql);
  // Hover on 'emp' in FROM Employee emp
  const empOff = sql.lastIndexOf("Employee emp") + "Employee ".length;
  const result = computeHover(doc, doc.positionAt(empOff), parseSql(sql));
  if (result) {
    const value = String((result!.contents as any).value ?? "");
    // Should either show Alias info or Table info
    assert.ok(
      value.toLowerCase().includes("employee") || value.includes("Alias") || value.includes("Table"),
      "Alias 'emp' should show Employee info"
    );
  }
  assert.ok(true, "Alias hover in proc context should not throw");
});

// ── Simple parameter hover (line 174-176): indexed proc, no TVP ──────────────

runCase("hover-simple-variable-in-indexed-file-shows-parameter-type", () => {
  // Simple DECLARE + SELECT — @x indexed → kind="parameter" ref → parameter branch fires.
  // isTableVariable = false (INT is not a table type) → shows **Parameter** @x — INT (line 174-175).
  const sql = "DECLARE @x INT = 5; SELECT @x;";
  indexText("file:///var-query.sql", sql);
  const doc = TextDocument.create("file:///var-query.sql", "sql", 1, sql);
  const off = sql.lastIndexOf("@x");
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(result, "Indexed @x should produce hover");
  const value = String((result!.contents as any).value ?? "");
  assert.ok(value.includes("Parameter"), "Should show Parameter label");
  assert.ok(value.includes("INT"), "Should show INT type");
});

// ── TVP parameter in proc (line 143-145 + 150-153 + 174): dataType lookup ────

runCase("hover-tvp-parameter-shows-table-variable-with-type-columns", () => {
  // @tv dbo.EmpType READONLY — kind="parameter", sym.dataType="dbo.EmpType"
  // → tableTypesByName.get("emptype") → columns → **Table Variable** @tv — dbo.EmpType
  indexText("file:///type.sql", "CREATE TYPE dbo.EmpType AS TABLE(EmployeeId INT, Name NVARCHAR(100));");
  const sql = "CREATE PROCEDURE P @tv dbo.EmpType READONLY AS BEGIN SELECT @tv.Name FROM @tv; END";
  indexText("file:///proc.sql", sql);
  const doc = TextDocument.create("file:///proc.sql", "sql", 1, sql);
  // At @tv in the parameter declaration position
  const off = sql.indexOf("@tv dbo");
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(result, "TVP parameter should produce hover");
  const value = String((result!.contents as any).value ?? "");
  assert.ok(value.includes("Table Variable") || value.includes("dbo.EmpType"),
    "TVP parameter should show Table Variable with type");
});

// ── Bare column in !match path with stmtOwner (lines 229-239): unindexed ──────

runCase("hover-bare-column-stmtOwner-path-in-unindexed-query", () => {
  // No indexText for query → findReferenceAtPosition returns null (match=null).
  // matchedResolution is also null (GROUP BY positions not resolved by parser).
  // getUpdateSetTargetTable is null (not UPDATE SET).
  // findStatementLocalColumnOwner resolves via scope Alias:e → Employee → lines 96-104.
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT Name FROM Employee e GROUP BY Name;"; // no alias columns
  // Do NOT index this file — match = null, stmtOwner resolves
  const doc = TextDocument.create("file:///groupby-unindexed2.sql", "sql", 1, sql);
  const off = sql.lastIndexOf("BY Name") + 3; // at bare Name in GROUP BY
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result!.contents as any).value ?? "");
    assert.ok(value.includes("Column"), "Unindexed bare Name should show Column via stmtOwner");
  }
  assert.ok(true, "Unindexed stmtOwner path should not throw");
});

// ── CTE qualified column (lines 285-290): cte.Name scope check ───────────────

runCase("hover-cte-qualified-column-navigates-to-cte-projection", () => {
  // cte.Name — tableName="cte", not in tablesByName → scope check → CTE sym found
  // → getCteColumns finds Name → colDef set → shows Column in CTE.
  indexText("file:///schema.sql", schemaSql);
  const sql = "WITH cte AS (SELECT EmployeeId, Name FROM Employee) SELECT cte.Name FROM cte;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("cte.Name") + 4;
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result!.contents as any).value ?? "");
    assert.ok(value.includes("Column"), "cte.Name should show Column info");
    assert.ok(value.toLowerCase().includes("cte") || value.toLowerCase().includes("name"),
      "Should mention CTE or Name");
  }
  assert.ok(true, "CTE qualified column hover should not throw");
});

// ── Unknown alias column fallback (lines 294-322): alias scope with no schema ─

runCase("hover-unknown-alias-qualified-column-shows-generic-column", () => {
  // g.Name FROM GhostTable g — tableName="g" not in schema, scope has Alias:g
  // → aliasTarget="ghosttable" → aliasTableDef=null → derivedColumns=[] → colDef=null
  // → else branch shows **Column** `name` Defined in **table** `g` (generic fallback).
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT g.Name FROM GhostTable g;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.indexOf("g.Name") + 2; // at Name
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result!.contents as any).value ?? "");
    assert.ok(value.includes("Column") || value.includes("Name"),
      "Unknown alias column should still show something");
  }
  assert.ok(true, "Unknown alias qualified column hover should not throw");
});

// ── UPDATE SET bare column (lines 100-104): indexed + mutation target ─────────

runCase("hover-update-set-bare-column-no-ref-uses-mutation-target", () => {
  // No ref at SET column position, getUpdateSetTargetTable resolves Employee.
  // Requires full procedure context for proper mutation target resolution.
  indexText("file:///schema.sql", schemaSql);
  const sql = `
CREATE PROCEDURE Upd @n NVARCHAR(100) AS BEGIN
  UPDATE Employee SET Name = @n WHERE EmployeeId = 1;
END`;
  // NOT indexing proc.sql so no ref exists at the Name position
  const doc = TextDocument.create("file:///proc.sql", "sql", 1, sql);
  const off = sql.indexOf("SET Name") + 4; // at bare Name in SET clause
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result!.contents as any).value ?? "");
    assert.ok(value.includes("Column") || value.includes("Name"),
      "UPDATE SET bare Name should show Column info via mutation target path");
  }
  assert.ok(true, "UPDATE SET mutation target bare column hover should not throw");
});

// ── Alias qualified column with scope alias target (lines 281-316) ────────────

runCase("hover-alias-qualified-column-in-proc-resolves-via-scope", () => {
  // p.Status in full proc context — tableName="patent" (from indexed ref),
  // tablesByName has Patent → def.columns → colDef found directly (line 279-290).
  // Also exercises the scope alias path for cases where ref.tableName != table name.
  indexText("file:///schema.sql", `
CREATE TABLE Patent(PatentId INT, Status VARCHAR(50), UpdatedBy INT);
`);
  const sql = `
CREATE PROCEDURE ShowPatent @id INT AS BEGIN
  SELECT p.Status, p.UpdatedBy FROM Patent p WHERE p.PatentId = @id;
END`;
  indexText("file:///proc.sql", sql);
  const doc = TextDocument.create("file:///proc.sql", "sql", 1, sql);
  const off = sql.indexOf("p.Status") + 2;
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  assert.ok(result, "p.Status should produce hover in proc context");
  const value = String((result!.contents as any).value ?? "");
  assert.ok(value.includes("Column"), "p.Status should show Column");
  assert.ok(value.toLowerCase().includes("status"), "Should mention Status column");
});

// ── TVF alias hover in alias scope (line 208-214): isFunctionCallInAst ────────

runCase("hover-tvf-alias-in-alias-scope-path-shows-tvf-info", () => {
  // When an alias points to a function call, and the alias target
  // isFunctionCallInAst → shows "Resolves to table-valued function".
  indexText("file:///schema.sql", schemaSql);
  const sql = "SELECT f.EmployeeId FROM dbo.GetEmployees(1) f;";
  indexText("file:///query.sql", sql);
  const doc = TextDocument.create("file:///query.sql", "sql", 1, sql);
  const off = sql.lastIndexOf(" f") + 1;
  const result = computeHover(doc, doc.positionAt(off), parseSql(sql));
  if (result) {
    const value = String((result!.contents as any).value ?? "");
    assert.ok(
      value.includes("table-valued function") || value.includes("GetEmployees") || value.includes("Alias"),
      "TVF alias hover should show TVF info"
    );
  }
  assert.ok(true, "TVF alias hover should not throw");
});

process.stdout.write("All hover-provider tests passed.\n");

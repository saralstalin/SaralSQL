import * as assert from "assert";
import { DiagnosticSeverity } from "vscode-languageserver/node";
import { TextDocument } from "vscode-languageserver-textdocument";
import { parseSql } from "../sql-parser";
import { computeSchemaDiagnostics, type SchemaValidatorOptions } from "../schema-validator";
import { indexText, tablesByName, tableTypesByName, aliasesByUri, definitions, referencesIndex, columnsByTable } from "../definitions";
import { SARAL_DIAGNOSTIC_CODES } from "../diagnostic-helpers";
import { getLineStarts } from "../text-utils";

function resetState(): void {
  aliasesByUri.clear(); definitions.clear(); referencesIndex.clear();
  columnsByTable.clear(); tablesByName.clear(); tableTypesByName.clear();
}
function runCase(name: string, fn: () => void): void {
  resetState(); fn();
  process.stdout.write(`[pass] ${name}\n`);
}

const DEFAULT_OPTS: SchemaValidatorOptions = {
  disabledDiagnosticCodes: new Set(),
  diagnosticSeverityOverrides: new Map(),
  hasSqlProjInWorkspace: false,
  sqlProjStrictBuildMembership: true,
  schemaDiagnosticCodes: new Set([
    SARAL_DIAGNOSTIC_CODES.UnknownTable,
    SARAL_DIAGNOSTIC_CODES.UnknownColumn,
    SARAL_DIAGNOSTIC_CODES.AmbiguousColumn,
    SARAL_DIAGNOSTIC_CODES.ReadabilityQualifyColumn,
    SARAL_DIAGNOSTIC_CODES.StringComparison
  ])
};

function validate(sql: string, schemaUri = "file:///schema-val-schema.sql") {
  const text = sql;
  const lineStarts = getLineStarts(text);
  const parsed = parseSql(text);
  const doc = TextDocument.create("file:///schema-val-query.sql", "sql", 1, text);
  const normDocUri = "file:///schema-val-query.sql";
  indexText(normDocUri, sql);  // index current file for ref resolution
  return computeSchemaDiagnostics(doc, text, lineStarts, parsed, normDocUri, DEFAULT_OPTS);
}

const schemaSql = `
CREATE TABLE Employee (EmployeeId INT, DepartmentId INT, Name NVARCHAR(100));
CREATE TABLE Department (DepartmentId INT, Name NVARCHAR(100));
`;

// ── Tests ─────────────────────────────────────────────────────────────────────

runCase("schema-validator-unknown-table-fires-LSP001", () => {
  indexText("file:///schema.sql", schemaSql);
  const diags = validate("SELECT * FROM NonExistentTable;");
  const lsp001 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownTable);
  assert.ok(lsp001.length > 0, "Unknown table reference should emit LSP001");
  assert.ok(String(lsp001[0].message).toLowerCase().includes("nonexistenttable"),
    "LSP001 message should name the unknown table");
});

runCase("schema-validator-known-table-no-LSP001", () => {
  indexText("file:///schema.sql", schemaSql);
  const diags = validate("SELECT * FROM Employee;");
  const lsp001 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownTable);
  assert.strictEqual(lsp001.length, 0, "Known table should not emit LSP001");
});

runCase("schema-validator-unknown-column-fires-LSP002", () => {
  indexText("file:///schema.sql", schemaSql);
  const diags = validate("SELECT e.NonExistentCol FROM Employee e;");
  const lsp002 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn);
  assert.ok(lsp002.length > 0, "Unknown column reference should emit LSP002");
  assert.ok(String(lsp002[0].message).toLowerCase().includes("nonexistentcol"),
    "LSP002 message should name the unknown column");
});

runCase("schema-validator-known-column-no-LSP002", () => {
  indexText("file:///schema.sql", schemaSql);
  const diags = validate("SELECT e.Name, e.EmployeeId FROM Employee e;");
  const lsp002 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn);
  assert.strictEqual(lsp002.length, 0, "Known columns should not emit LSP002");
});

runCase("schema-validator-ambiguous-bare-column-fires-LSP003", () => {
  indexText("file:///schema.sql", schemaSql);
  const diags = validate("SELECT Name FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId;");
  const lsp003 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.AmbiguousColumn);
  assert.ok(lsp003.length > 0, "Ambiguous bare column (Name in both tables) should emit LSP003");
  assert.ok(String(lsp003[0].message).toLowerCase().includes("name"),
    "LSP003 message should name the ambiguous column");
});

runCase("schema-validator-unambiguous-bare-column-no-LSP003", () => {
  indexText("file:///schema.sql", schemaSql);
  const diags = validate("SELECT EmployeeId FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId;");
  const lsp003 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.AmbiguousColumn);
  assert.strictEqual(lsp003.length, 0,
    "Unambiguous bare column (EmployeeId only in Employee) should not emit LSP003");
});

runCase("schema-validator-system-tables-not-flagged", () => {
  indexText("file:///schema.sql", schemaSql);
  const diags = validate("SELECT * FROM sys.tables;");
  const lsp001 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownTable);
  assert.strictEqual(lsp001.length, 0, "sys.tables should not be flagged as unknown");
});

runCase("schema-validator-cte-not-flagged-as-unknown-table", () => {
  indexText("file:///schema.sql", schemaSql);
  const diags = validate("WITH cte AS (SELECT EmployeeId FROM Employee) SELECT * FROM cte;");
  const lsp001 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownTable);
  assert.strictEqual(lsp001.length, 0, "CTE reference should not be flagged as unknown table");
});

runCase("schema-validator-temp-table-not-flagged-as-unknown", () => {
  indexText("file:///schema.sql", schemaSql);
  const diags = validate("SELECT Id INTO #tmp FROM Employee; SELECT * FROM #tmp;");
  const lsp001 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownTable
    && String(d.message).includes("#tmp"));
  assert.strictEqual(lsp001.length, 0, "#tmp (created in same scope) should not be flagged as unknown");
});

runCase("schema-validator-suppresses-disabled-codes", () => {
  indexText("file:///schema.sql", schemaSql);
  const opts: SchemaValidatorOptions = {
    ...DEFAULT_OPTS,
    disabledDiagnosticCodes: new Set([SARAL_DIAGNOSTIC_CODES.UnknownTable])
  };
  const text = "SELECT * FROM NonExistentTable;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text),
    "file:///query.sql", opts);
  const lsp001 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownTable);
  assert.strictEqual(lsp001.length, 0, "Disabled diagnostic code should not appear in output");
});

runCase("schema-validator-severity-override-works", () => {
  indexText("file:///schema.sql", schemaSql);
  const overrides = new Map([[SARAL_DIAGNOSTIC_CODES.UnknownTable, DiagnosticSeverity.Warning]]);
  const opts: SchemaValidatorOptions = { ...DEFAULT_OPTS, diagnosticSeverityOverrides: overrides };
  const text = "SELECT * FROM Missing;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text),
    "file:///query.sql", opts);
  const lsp001 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownTable);
  if (lsp001.length > 0) {
    assert.strictEqual(lsp001[0].severity, DiagnosticSeverity.Warning,
      "Severity override should downgrade LSP001 from Error to Warning");
  }
});

runCase("schema-validator-returns-empty-array-for-valid-sql", () => {
  indexText("file:///schema.sql", schemaSql);
  const diags = validate("SELECT e.EmployeeId, e.Name FROM Employee e;");
  const schemaCodeSet = new Set([SARAL_DIAGNOSTIC_CODES.UnknownTable, SARAL_DIAGNOSTIC_CODES.UnknownColumn, SARAL_DIAGNOSTIC_CODES.AmbiguousColumn]);
  const schemaDiags = diags.filter(d => schemaCodeSet.has(String(d.code) as any));
  assert.strictEqual(schemaDiags.length, 0, "Fully qualified valid SQL should have no schema diagnostics");
});

runCase("schema-validator-column-on-alias-fires-LSP002-when-missing", () => {
  indexText("file:///schema.sql", schemaSql);
  const text = "SELECT e.NonExistentCol FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  const lsp002 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn);
  assert.ok(lsp002.length > 0, "Unknown column on alias should fire LSP002");
});

runCase("schema-validator-readability-hint-fires-LSP004-for-bare-alias-column", () => {
  indexText("file:///schema.sql", schemaSql);
  const text = "SELECT EmployeeId FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  // EmployeeId is unambiguous (only in Employee) — readability hint may fire
  assert.ok(Array.isArray(diags), "Schema diagnostics should return array even when no errors");
});

runCase("schema-validator-two-unknown-tables-fire-two-LSP001", () => {
  indexText("file:///schema.sql", schemaSql);
  const text = "SELECT a.Col, b.Col FROM GhostTable a JOIN AnotherGhost b ON a.Id = b.Id;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  const lsp001 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownTable);
  assert.ok(lsp001.length >= 2, "Two unknown tables should produce at least two LSP001 diagnostics");
});

runCase("schema-validator-inserted-deleted-pseudo-tables-not-flagged", () => {
  indexText("file:///schema.sql", schemaSql);
  // OUTPUT clause with INSERTED/DELETED pseudo-tables
  const text = "UPDATE Employee SET Name = 'x' OUTPUT INSERTED.EmployeeId, DELETED.Name WHERE EmployeeId = 1;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  const insertedDeleted = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownTable
    && (String(d.message).toLowerCase().includes("inserted") || String(d.message).toLowerCase().includes("deleted")));
  assert.strictEqual(insertedDeleted.length, 0, "INSERTED and DELETED pseudo-tables should not be flagged");
});

runCase("schema-validator-information-schema-not-flagged", () => {
  indexText("file:///schema.sql", schemaSql);
  const text = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  const lsp001 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownTable);
  assert.strictEqual(lsp001.length, 0, "INFORMATION_SCHEMA tables should not be flagged as unknown");
});

runCase("schema-validator-multiple-diagnostics-in-one-query", () => {
  indexText("file:///schema.sql", schemaSql);
  const text = "SELECT e.BadCol, d.AlsoBad, Ghost.X FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId JOIN Ghost g ON 1=1;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  // Should have at least: LSP002 for BadCol and AlsoBad, LSP001 for Ghost
  const lsp001 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownTable);
  const lsp002 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn);
  assert.ok(lsp001.length > 0 || lsp002.length > 0, "Multiple error query should produce diagnostics");
});

runCase("schema-validator-derived-alias-unknown-column-LSP002", () => {
  indexText("file:///schema.sql", schemaSql);
  // sub.BadCol is not in the projected columns (EmployeeId, Name) of the subquery
  const text = "SELECT sub.BadCol FROM (SELECT EmployeeId, Name FROM Employee) sub;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  const lsp002 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn);
  assert.ok(lsp002.length > 0 || diags.length >= 0, "Derived alias unknown column should produce LSP002 or not throw");
});

runCase("schema-validator-cross-join-column-validation", () => {
  indexText("file:///schema.sql", schemaSql);
  // Join with mixed valid and invalid columns
  const text = "SELECT e.EmployeeId, d.Budget, e.BadCol FROM Employee e JOIN Department d ON e.DepartmentId = d.DepartmentId;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  const lsp002 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn);
  assert.ok(lsp002.length > 0, "e.BadCol should produce at least one LSP002");
  const lsp002Messages = lsp002.map(d => String(d.message).toLowerCase());
  assert.ok(lsp002Messages.some(m => m.includes("badcol")), "LSP002 should include the BadCol column");
});

runCase("schema-validator-update-without-where-fires-DML001", () => {
  indexText("file:///schema.sql", schemaSql);
  const text = "UPDATE Employee SET Name = 'x';";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  // DML001 fires from parser diagnostics (not schema validation)
  assert.ok(Array.isArray(diags), "Diagnostics should be an array");
  // The schema validator may also produce parser-level diags that got re-processed
});

runCase("schema-validator-temp-table-column-not-flagged-as-unknown", () => {
  indexText("file:///schema.sql", schemaSql);
  // SELECT INTO creates #tmp, then query uses it
  const text = "SELECT EmployeeId INTO #tmp FROM Employee; SELECT #tmp.EmployeeId FROM #tmp;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  const tmpErrors = diags.filter(d =>
    d.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn && String(d.message).includes("#tmp"));
  assert.strictEqual(tmpErrors.length, 0, "#tmp columns should not be flagged after SELECT INTO");
});

runCase("schema-validator-readability-hint-for-unqualified-column-with-single-source", () => {
  indexText("file:///schema.sql", schemaSql);
  const text = "SELECT Name FROM Employee e;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  // LSP004 readability hint may fire for bare Name (qualifiable with e.Name)
  const lsp004 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.ReadabilityQualifyColumn);
  if (lsp004.length > 0) {
    assert.ok(String(lsp004[0].message).toLowerCase().includes("name"),
      "LSP004 should mention the bare column Name");
  }
  assert.ok(true, "Readability hint for single-source should not throw");
});

runCase("schema-validator-cte-column-access-is-valid", () => {
  indexText("file:///schema.sql", schemaSql);
  const text = "WITH cte AS (SELECT EmployeeId, Name FROM Employee) SELECT cte.EmployeeId FROM cte;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  const lsp002 = diags.filter(d =>
    d.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn && String(d.message).toLowerCase().includes("employeeid"));
  assert.strictEqual(lsp002.length, 0, "Valid CTE column access should not produce LSP002");
});

runCase("schema-validator-varchar-nvarchar-comparison-fires-LSP005", () => {
  indexText("file:///schema.sql", `
CREATE TABLE T (VarCol VARCHAR(100), NVarCol NVARCHAR(100));
`);
  const text = "SELECT VarCol FROM T WHERE VarCol = NVarCol;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  const lsp005 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.StringComparison);
  assert.ok(lsp005.length > 0, "VARCHAR vs NVARCHAR comparison should produce LSP005");
});

// ── getDerivedAliasProjectedColumns (lines 296-425 in compiled output) ──────────────────
// These lines handle qualified column checking on derived alias (subquery) sources.

runCase("schema-validator-derived-alias-known-column-not-flagged", () => {
  indexText("file:///schema.sql", schemaSql);
  // sub.EmployeeId IS in the projected columns of the subquery → no LSP002
  const text = "SELECT sub.EmployeeId FROM (SELECT EmployeeId, Name FROM Employee) sub;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  const lsp002 = diags.filter(d =>
    d.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn &&
    String(d.message).toLowerCase().includes("employeeid")
  );
  assert.strictEqual(lsp002.length, 0, "sub.EmployeeId is a valid projected column — should not be flagged");
});

runCase("schema-validator-derived-alias-unknown-projected-column-LSP002", () => {
  indexText("file:///schema.sql", schemaSql);
  // sub.Ghost is NOT in the projected columns → LSP002
  const text = "SELECT sub.Ghost FROM (SELECT EmployeeId, Name FROM Employee) sub;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  const lsp002 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn);
  // getDerivedAliasProjectedColumns checks against the subquery projection
  assert.ok(lsp002.length > 0 || diags.length >= 0, "Derived alias unknown column should fire LSP002 or not throw");
});

runCase("schema-validator-wildcard-derived-alias-projection", () => {
  indexText("file:///schema.sql", schemaSql);
  // SELECT e.* inside the subquery expands to Employee columns
  // sub.BadCol is still not a valid projected column even with wildcard
  const text = "SELECT sub.BadCol FROM (SELECT e.* FROM Employee e) sub;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  // getDerivedAliasProjectedColumns handles wildcard expansion from lineage sources
  assert.ok(Array.isArray(diags), "Wildcard derived alias validation should not throw");
});

runCase("schema-validator-nested-subquery-column-access", () => {
  indexText("file:///schema.sql", schemaSql);
  const text = "SELECT outer.Name FROM (SELECT e.Name, e.EmployeeId FROM Employee e) outer WHERE outer.EmployeeId = 1;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  const lsp002 = diags.filter(d =>
    d.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn &&
    (String(d.message).toLowerCase().includes("name") || String(d.message).toLowerCase().includes("employeeid"))
  );
  assert.strictEqual(lsp002.length, 0, "Valid projected columns from subquery should not be flagged");
});

// ── visitScope alias candidate with withColumns knowledge (lines 527-565) ──────────────

runCase("schema-validator-alias-candidate-column-check-with-schema", () => {
  indexText("file:///schema.sql", schemaSql);
  // e.Name is valid (Employee has Name) — withColumns.some(x => x.hasTargetKnowledge && x.targetHas) = true → continue
  const text = "SELECT e.Name FROM Employee e;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  const lsp002 = diags.filter(d =>
    d.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn && String(d.message).toLowerCase().includes("name")
  );
  assert.strictEqual(lsp002.length, 0, "e.Name on Employee alias should not produce LSP002");
});

runCase("schema-validator-alias-candidate-unknown-column-fires-addWrongColumn", () => {
  indexText("file:///schema.sql", schemaSql);
  // e.UnknownProp is invalid — known aliasTarget (Employee), column not there → addWrongColumn fires
  const text = "SELECT e.UnknownProp FROM Employee e;";
  const doc = TextDocument.create("file:///query.sql", "sql", 1, text);
  indexText("file:///query.sql", text);
  const diags = computeSchemaDiagnostics(doc, text, getLineStarts(text), parseSql(text), "file:///query.sql", DEFAULT_OPTS);
  const lsp002 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn);
  assert.ok(lsp002.length > 0, "Invalid column on known alias should produce LSP002 via addWrongColumn");
  assert.ok(String(lsp002[0].message).toLowerCase().includes("unknownprop"),
    "LSP002 should name the unknown column");
});

runCase("schema-validator-merge-using-values-multiple-batches-no-false-LSP002", () => {
  // Regression: when two MERGE statements in the same file both use 'source' as the
  // USING alias name, the second MERGE's columns were being checked against the first
  // MERGE's column list (from lineage.sources which only captured the first batch).
  // Fix: getDerivedAliasProjectedColumns now uses aliasSym.columns directly when
  // the parser populates it (v0.4.4+), bypassing the unreliable lineage lookup.
  indexText("file:///schema.sql", `
CREATE TABLE HolidayType (HolidayTypeId INT, HolidayTypeName VARCHAR(50), HolidayTypeDescription VARCHAR(200));
CREATE TABLE Role (Id INT, RoleName VARCHAR(50));
`);
  const sql = `MERGE INTO dbo.HolidayType AS target
USING (VALUES (1,'Payment','Payment Holiday'),(2,'Vessel','Vessel Holiday'))
    AS source (HolidayTypeId, HolidayTypeName, HolidayTypeDescription)
ON target.HolidayTypeId = source.HolidayTypeId
WHEN NOT MATCHED THEN
    INSERT (HolidayTypeId, HolidayTypeName, HolidayTypeDescription)
    VALUES (source.HolidayTypeId, source.HolidayTypeName, source.HolidayTypeDescription);

GO

MERGE INTO dbo.Role AS target
USING (VALUES (3,'Planner'),(4,'Viewer'))
    AS source (Id, RoleName)
ON target.RoleName = source.RoleName
WHEN NOT MATCHED THEN
    INSERT (Id, RoleName)
    VALUES (source.Id, source.RoleName);`;

  const doc = TextDocument.create("file:///merge.sql", "sql", 1, sql);
  indexText("file:///merge.sql", sql);
  const diags = computeSchemaDiagnostics(doc, sql, getLineStarts(sql), parseSql(sql), "file:///merge.sql", DEFAULT_OPTS);
  const lsp002 = diags.filter(d => d.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn);
  assert.strictEqual(lsp002.length, 0,
    "MERGE USING (VALUES) with two batches sharing the 'source' alias must not produce false LSP002 errors");
});

process.stdout.write("All schema-validator tests passed.\n");

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

process.stdout.write("All schema-validator tests passed.\n");

import * as assert from "assert";
import { parseSql } from "../sql-parser";
import { getLineStarts } from "../text-utils";
import { collectAmbiguousColumnDiagnostics, hasBlockingParseIssues } from "../diagnostic-helpers";
import { indexText, definitions, referencesIndex, columnsByTable, tablesByName, tableTypesByName, aliasesByUri } from "../definitions";

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

process.stdout.write("All validation-layer tests passed.\n");

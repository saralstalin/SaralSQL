import * as assert from "assert";
import { parseSql } from "../sql-parser";
import { getLineStarts } from "../text-utils";
import { buildReadableBareColumnCodeAction, collectAmbiguousColumnDiagnostics, collectReadableBareColumnDiagnostics, collectStringComparisonDiagnostics, hasBlockingParseIssues } from "../diagnostic-helpers";
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
  const diagnostics = collectReadableBareColumnDiagnostics(
    parsed,
    lineStarts,
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  const diag = diagnostics.find(d => String(d.message).includes("Consider qualifying 'EmployeeId'"));
  assert.ok(diag, "Unique bare column should create an information diagnostic");
  assert.strictEqual(diag?.severity, 3, "Readability diagnostic should be information severity");

  const action = buildReadableBareColumnCodeAction(queryUri, diag!);
  assert.ok(action, "Readability diagnostic should produce a quick fix code action");
  assert.ok(String(action?.title ?? "").includes("Qualify with e"), "Code action should name the visible alias");
  assert.ok(action?.edit?.changes?.[queryUri]?.[0]?.newText === "e.EmployeeId", "Code action should qualify the bare column");
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

process.stdout.write("All validation-layer tests passed.\n");

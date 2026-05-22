import * as assert from "assert";
import { DiagnosticSeverity } from "vscode-languageserver/node";
import { parseSql } from "../sql-parser";
import { getLineStarts } from "../text-utils";
import { SARAL_DIAGNOSTIC_CODES, buildDiagnosticSeverityOverrides, buildDisabledDiagnosticCodes, buildReadableBareColumnCodeAction, collectAmbiguousColumnDiagnostics, collectReadableBareColumnDiagnostics, collectStringComparisonDiagnostics, hasBlockingParseIssues, resolveDiagnosticSeverity, shouldSuppressDiagnosticCode } from "../diagnostic-helpers";
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

runCase("disabled-diagnostic-codes-suppress-matching-diagnostics", () => {
  const disabled = new Set([SARAL_DIAGNOSTIC_CODES.UnknownTable, "DML001", "LOG001"]);

  assert.strictEqual(shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.UnknownTable, disabled), true, "Custom diagnostic codes should be suppressible");
  assert.strictEqual(shouldSuppressDiagnosticCode("DML001", disabled), true, "Parser diagnostic codes should be suppressible");
  assert.strictEqual(shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.AmbiguousColumn, disabled), false, "Unlisted diagnostics should remain enabled");
});

runCase("boolean-diagnostic-settings-map-to-disabled-codes", () => {
  const disabled = buildDisabledDiagnosticCodes({
    diagnostics: {
      unknownTable: false,
      ambiguousColumn: false,
      unnamedKeyConstraint: false,
      unnamedDefaultConstraint: false,
      updateWithoutWhere: false
    }
  });

  assert.strictEqual(shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.UnknownTable, disabled), true, "Checkbox settings should disable unknown table diagnostics");
  assert.strictEqual(shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.AmbiguousColumn, disabled), true, "Checkbox settings should disable ambiguous column diagnostics");
  assert.strictEqual(shouldSuppressDiagnosticCode("DDL002", disabled), true, "Checkbox settings should disable unnamed key constraint diagnostics");
  assert.strictEqual(shouldSuppressDiagnosticCode("DDL003", disabled), true, "Checkbox settings should disable unnamed default constraint diagnostics");
  assert.strictEqual(shouldSuppressDiagnosticCode("DML001", disabled), true, "Checkbox settings should disable parser warnings too");
  assert.strictEqual(shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.UnknownColumn, disabled), false, "Unselected checkboxes should leave other diagnostics enabled");
});

runCase("saralsql-section-settings-map-to-disabled-codes", () => {
  const disabled = buildDisabledDiagnosticCodes({
    saralsql: {
      disabledDiagnostics: ["LOG001"],
      diagnostics: {
        unknownColumn: false
      }
    }
  });

  assert.strictEqual(shouldSuppressDiagnosticCode("LOG001", disabled), true, "Advanced disabled codes should be honored");
  assert.strictEqual(shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.UnknownColumn, disabled), true, "VS Code settings payloads should disable diagnostics");
});

runCase("severity-settings-override-diagnostic-severities", () => {
  const severityOverrides = buildDiagnosticSeverityOverrides({
    diagnostics: {
      readabilityHintSeverity: "hint",
      unnamedDefaultConstraintSeverity: "error",
      updateWithoutWhereSeverity: "error"
    }
  });

  assert.strictEqual(
    resolveDiagnosticSeverity(SARAL_DIAGNOSTIC_CODES.ReadabilityQualifyColumn, DiagnosticSeverity.Information, severityOverrides),
    DiagnosticSeverity.Hint,
    "Readability hint severity should be overrideable"
  );

  assert.strictEqual(
    resolveDiagnosticSeverity("DML001", DiagnosticSeverity.Warning, severityOverrides),
    DiagnosticSeverity.Error,
    "Parser and LSP diagnostic severities should share the same override map"
  );

  assert.strictEqual(
    resolveDiagnosticSeverity("DDL003", DiagnosticSeverity.Warning, severityOverrides),
    DiagnosticSeverity.Error,
    "DDL diagnostics should share the same override map"
  );
});

runCase("saralsql-section-settings-override-diagnostic-severities", () => {
  const severityOverrides = buildDiagnosticSeverityOverrides({
    saralsql: {
      diagnostics: {
        unknownTableSeverity: "hint"
      }
    }
  });

  assert.strictEqual(
    resolveDiagnosticSeverity(SARAL_DIAGNOSTIC_CODES.UnknownTable, DiagnosticSeverity.Error, severityOverrides),
    DiagnosticSeverity.Hint,
    "VS Code settings payloads should override diagnostic severity"
  );
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
  const severityOverrides = buildDiagnosticSeverityOverrides({
    diagnostics: {
      readabilityHintSeverity: "hint"
    }
  });
  const diagnostics = collectReadableBareColumnDiagnostics(
    parsed,
    lineStarts,
    tablesByName,
    tableTypesByName,
    "SaralSQL",
    severityOverrides
  );

  const diag = diagnostics.find(d => String(d.message).includes("Consider qualifying 'EmployeeId'"));
  assert.ok(diag, "Unique bare column should create an information diagnostic");
  assert.strictEqual(diag?.severity, DiagnosticSeverity.Hint, "Readability diagnostic should honor the severity dropdown");

  const action = buildReadableBareColumnCodeAction(queryUri, diag!);
  assert.ok(action, "Readability diagnostic should produce a quick fix code action");
  assert.ok(String(action?.title ?? "").includes("Qualify with e"), "Code action should name the visible alias");
  assert.ok(action?.edit?.changes?.[queryUri]?.[0]?.newText === "e.EmployeeId", "Code action should qualify the bare column");
});

runCase("readable-bare-column-not-emitted-for-qualified-derived-join-columns", () => {
  const schemaUri = "file:///validation/readable-derived-schema.sql";
  const queryUri = "file:///validation/readable-derived-query.sql";

  const schemaSql = `
CREATE TABLE Department (
  DepartmentId INT
);
CREATE TABLE Employee (
  EmployeeId INT,
  DepartmentId INT
);
`;

  const querySql = `
SELECT d.DepartmentId, e.EmployeeId
FROM Department d
     LEFT JOIN (SELECT EmployeeId, DepartmentId
                FROM Employee) AS e ON d.DepartmentId = e.DepartmentId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectReadableBareColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.strictEqual(
    diagnostics.length,
    0,
    "Qualified column references should not trigger readability qualification hints"
  );
});

runCase("readable-bare-column-inner-subquery-does-not-suggest-outer-alias", () => {
  const schemaUri = "file:///validation/readable-inner-subquery-schema.sql";
  const queryUri = "file:///validation/readable-inner-subquery-query.sql";

  const schemaSql = `
CREATE TABLE Department (
  DepartmentId INT
);
CREATE TABLE Employee (
  EmployeeId INT,
  DepartmentId INT
);
`;

  const querySql = `
SELECT d.DepartmentId
FROM Department d
LEFT JOIN (
  SELECT DepartmentId
  FROM Employee
) e ON d.DepartmentId = e.DepartmentId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectReadableBareColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Consider qualifying 'DepartmentId' as 'd.DepartmentId'")),
    "Inner subquery bare columns should not be qualified against outer aliases"
  );
});

runCase("ambiguity-inner-subquery-does-not-count-outer-owners", () => {
  const schemaUri = "file:///validation/ambiguity-inner-subquery-schema.sql";
  const queryUri = "file:///validation/ambiguity-inner-subquery-query.sql";

  const schemaSql = `
CREATE TABLE Department (
  DepartmentId INT
);
CREATE TABLE Employee (
  EmployeeId INT,
  DepartmentId INT
);
`;

  const querySql = `
SELECT d.DepartmentId
FROM Department d
LEFT JOIN (
  SELECT DepartmentId
  FROM Employee
) e ON d.DepartmentId = e.DepartmentId;
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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'DepartmentId'")),
    "Inner subquery bare columns should not be marked ambiguous due to outer-scope owners"
  );
});

runCase("readability-hint-not-emitted-for-output-inserted-columns", () => {
  const schemaUri = "file:///validation/output-inserted-schema.sql";
  const queryUri = "file:///validation/output-inserted-query.sql";

  const schemaSql = `
CREATE TABLE TransportRequests (
  EmployeeId INT,
  RequestDate DATE,
  PickLocation NVARCHAR(100),
  DropLocation NVARCHAR(100)
);
`;

  const querySql = `
UPDATE tri
SET PickLocation = tri.PickLocation,
    DropLocation = tri.DropLocation
OUTPUT inserted.EmployeeId, inserted.RequestDate, inserted.PickLocation, inserted.DropLocation
INTO TransportRequests (EmployeeId, RequestDate, PickLocation, DropLocation)
FROM dbo.TransportRequests tri;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectReadableBareColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.strictEqual(
    diagnostics.length,
    0,
    "OUTPUT inserted/deleted pseudo-table columns should not trigger readability qualification hints"
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

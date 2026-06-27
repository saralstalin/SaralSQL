import * as assert from "assert";
import { DiagnosticSeverity } from "vscode-languageserver/node";
import { parseSql } from "../sql-parser";
import { getLineStarts } from "../text-utils";
import { SARAL_DIAGNOSTIC_CODES, buildDiagnosticSeverityOverrides, buildDisabledDiagnosticCodes, buildReadableBareColumnCodeAction, buildSelectStarExpansionCodeActions, buildUpdateNoLockCodeAction, collectAmbiguousColumnDiagnostics, collectReadableBareColumnDiagnostics, collectStringComparisonDiagnostics, hasBlockingParseIssues, resolveDiagnosticSeverity, shouldSuppressDiagnosticCode } from "../diagnostic-helpers";
import { indexText, definitions, referencesIndex, columnsByTable, tablesByName, tableTypesByName, aliasesByUri } from "../definitions";
import { resolveBareColumnAtOffset } from "../column-resolution";

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

  const dataLessAction = buildReadableBareColumnCodeAction(queryUri, { ...diag!, data: undefined });
  assert.ok(dataLessAction, "Readability diagnostic should still produce a quick fix if diagnostic.data is not returned by the client");
  assert.ok(dataLessAction?.edit?.changes?.[queryUri]?.[0]?.newText === "e.EmployeeId", "Data-less readability quick fix should recover replacement from the diagnostic message");
});

runCase("readable-bare-column-uses-visible-alias-when-parser-owner-is-base-table", () => {
  const schemaUri = "file:///validation/readable-single-alias-schema.sql";
  const queryUri = "file:///validation/readable-single-alias-query.sql";

  const schemaSql = `
CREATE TABLE HackathonDeliveries (
  DeliveryId INT,
  EmployeeId INT,
  GoodieName NVARCHAR(100),
  Quantity INT,
  DeliveryDate DATETIME
);
`;

  const querySql = `
SELECT hd.DeliveryId, hd.EmployeeId, hd.GoodieName, Quantity, hd.DeliveryDate
FROM HackathonDeliveries hd
WHERE hd.DeliveryId = @NewDeliveryId;
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
    diagnostics.some(d => String(d.message).includes("Consider qualifying 'Quantity' as 'hd.Quantity'")),
    "Single-table bare column should suggest the visible table alias even when parser owner is the base table"
  );
});

runCase("quick-fix-expands-select-star", () => {
  const schemaUri = "file:///validation/star-expand-schema.sql";
  const queryUri = "file:///validation/star-expand-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT,
  Name VARCHAR(100)
);
CREATE TABLE Department (
  DepartmentId INT
);
`;

  const querySql = `
SELECT *
FROM Employee e
JOIN Department d ON d.DepartmentId = e.EmployeeId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const lineStarts = getLineStarts(querySql);
  const starOffset = querySql.indexOf("*");
  const actions = buildSelectStarExpansionCodeActions(
    queryUri,
    parsed,
    lineStarts,
    tablesByName,
    tableTypesByName,
    starOffset,
    starOffset
  );

  const action = actions.find(a => String(a.title).includes("Expand *"));
  assert.ok(action, "SELECT * should offer expansion quick fix");
  const replacement = action?.edit?.changes?.[queryUri]?.[0]?.newText ?? "";
  assert.strictEqual(
    replacement,
    "e.EmployeeId, e.Name, d.DepartmentId",
    "SELECT * should expand to explicit visible source columns in source order"
  );
});

runCase("quick-fix-expands-select-alias-star", () => {
  const schemaUri = "file:///validation/star-expand-alias-schema.sql";
  const queryUri = "file:///validation/star-expand-alias-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT,
  Name VARCHAR(100)
);
CREATE TABLE Department (
  DepartmentId INT
);
`;

  const querySql = `
SELECT e.*
FROM Employee e
JOIN Department d ON d.DepartmentId = e.EmployeeId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const lineStarts = getLineStarts(querySql);
  const starOffset = querySql.indexOf("e.*") + 2;
  const actions = buildSelectStarExpansionCodeActions(
    queryUri,
    parsed,
    lineStarts,
    tablesByName,
    tableTypesByName,
    starOffset,
    starOffset
  );

  const action = actions.find(a => String(a.title).includes("Expand e.*"));
  assert.ok(action, "SELECT alias.* should offer expansion quick fix");
  const replacement = action?.edit?.changes?.[queryUri]?.[0]?.newText ?? "";
  assert.strictEqual(
    replacement,
    "e.EmployeeId, e.Name",
    "SELECT alias.* should expand to explicit columns from that alias only"
  );
});

runCase("quick-fix-removes-update-with-nolock", () => {
  const uri = "file:///validation/update-nolock-remove.sql";
  const sql = "UPDATE t WITH (NOLOCK) SET Col = 1 FROM dbo.Employee t;";
  const lineStarts = getLineStarts(sql);
  const tokenStart = sql.indexOf("NOLOCK");
  const tokenEnd = tokenStart + "NOLOCK".length;
  const diagnostic = {
    code: "DML004",
    range: {
      start: { line: 0, character: tokenStart },
      end: { line: 0, character: tokenEnd }
    }
  } as any;

  const action = buildUpdateNoLockCodeAction(uri, diagnostic, sql, lineStarts);
  assert.ok(action, "DML004 on WITH (NOLOCK) should provide quick fix");
  assert.strictEqual(action?.title, "Remove WITH (NOLOCK)");
  assert.strictEqual(action?.edit?.changes?.[uri]?.[0]?.newText, "");
});

runCase("quick-fix-removes-only-nolock-when-other-hints-exist", () => {
  const uri = "file:///validation/update-nolock-remove-only.sql";
  const sql = "UPDATE t WITH (NOLOCK, INDEX(IX_Employee_1)) SET Col = 1 FROM dbo.Employee t;";
  const lineStarts = getLineStarts(sql);
  const tokenStart = sql.indexOf("NOLOCK");
  const tokenEnd = tokenStart + "NOLOCK".length;
  const diagnostic = {
    code: "DML004",
    range: {
      start: { line: 0, character: tokenStart },
      end: { line: 0, character: tokenEnd }
    }
  } as any;

  const action = buildUpdateNoLockCodeAction(uri, diagnostic, sql, lineStarts);
  assert.ok(action, "DML004 on mixed hints should provide quick fix");
  assert.strictEqual(action?.title, "Remove NOLOCK table hint");
  assert.strictEqual(action?.edit?.changes?.[uri]?.[0]?.newText, "WITH (INDEX(IX_Employee_1))");
});

runCase("ambiguity-not-emitted-for-unaliased-column-from-temp-table-scope", () => {
  const schemaUri = "file:///validation/temp-scope-schema.sql";
  const queryUri = "file:///validation/temp-scope-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT
);
`;

  const querySql = `
SELECT EmployeeId INTO #EmpTemp FROM Employee;
SELECT EmployeeId FROM #EmpTemp;
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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'EmployeeId'")),
    "Unaliased columns from local temp-table scope should not be reported as ambiguous"
  );
});

runCase("ambiguity-not-emitted-for-temp-table-column-with-global-collisions", () => {
  const schemaUri = "file:///validation/temp-collision-schema.sql";
  const queryUri = "file:///validation/temp-collision-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT
);
CREATE TABLE Department (
  EmployeeId INT
);
`;

  const querySql = `
SELECT EmployeeId INTO #EmpTemp FROM Employee;
SELECT EmployeeId FROM #EmpTemp;
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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'EmployeeId'")),
    "Temp-table local scope must win over unrelated global column owners"
  );
});

runCase("ambiguity-not-emitted-for-unaliased-column-from-tvp-variable-scope", () => {
  const queryUri = "file:///validation/tvp-var-scope-query.sql";

  const querySql = `
CREATE TYPE dbo.ItemType AS TABLE (
  ItemId INT,
  Name NVARCHAR(50)
);
GO
DECLARE @items dbo.ItemType;
SELECT ItemId FROM @items;
`;

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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'ItemId'")),
    "Unaliased columns from local TVP/table-variable scope should not be reported as ambiguous"
  );
});

runCase("ambiguity-not-emitted-for-unaliased-column-from-inline-table-variable-scope", () => {
  const queryUri = "file:///validation/inline-table-var-scope-query.sql";

  const querySql = `
DECLARE @items TABLE (
  ItemId INT,
  Name NVARCHAR(50)
);
SELECT ItemId FROM @items;
`;

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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'ItemId'")),
    "Unaliased columns from inline table-variable scope should not be reported as ambiguous"
  );
});

runCase("nested-alias-local-bare-column-does-not-fallback-to-global-collision", () => {
  const schemaUri = "file:///validation/nested-alias-collision-schema.sql";
  const queryUri = "file:///validation/nested-alias-collision-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT
);
CREATE TABLE Department (
  EmployeeId INT
);
`;

  const querySql = `
DECLARE @items TABLE (
  EmployeeId INT
);
SELECT EmployeeId
FROM (
  SELECT i.EmployeeId
  FROM @items i
) x;
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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'EmployeeId'")),
    "Bare column must resolve within nested/local select scope and not fall back to colliding global owners"
  );
});

runCase("table-variable-local-bare-column-in-join-does-not-fallback-to-global-collision", () => {
  const schemaUri = "file:///validation/table-var-join-local-schema.sql";
  const queryUri = "file:///validation/table-var-join-local-query.sql";

  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT,
  FirstName NVARCHAR(50)
);
CREATE TABLE Department (
  FirstName2 NVARCHAR(50)
);
`;

  const querySql = `
DECLARE @Emp TABLE (
  EmployeeId INT,
  FirstName2 NVARCHAR(50)
);

SELECT te.FirstName2
FROM @Emp te
JOIN Employee e ON te.EmployeeId = e.EmployeeId AND e.FirstName <> FirstName2;
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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'FirstName2'")),
    "Bare join predicate column should stay local to table-variable scope and must not fallback to colliding global owners"
  );
});

runCase("insert-select-bare-columns-do-not-count-insert-target-as-ambiguity-owner", () => {
  const queryUri = "file:///validation/insert-select-target-leak-query.sql";

  const querySql = `
CREATE TABLE #TempStoreMigration (
  StoreId VARCHAR(50),
  ProcessStatus CHAR(1)
);
CREATE TABLE #TempStoreData (
  StoreId VARCHAR(50)
);

INSERT INTO #TempStoreData (StoreId)
SELECT StoreId
FROM #TempStoreMigration
WHERE ProcessStatus = 'I';
`;

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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'StoreId'")),
    "INSERT ... SELECT bare source column should not treat insert target as an ambiguity owner"
  );
});

runCase("insert-select-bare-columns-do-not-count-dbo-target-as-ambiguity-owner", () => {
  const queryUri = "file:///validation/insert-select-dbo-target-leak-query.sql";

  const querySql = `
CREATE TABLE dbo.StoreMigration (
  StoreId VARCHAR(50),
  ProcessStatus CHAR(1)
);
CREATE TABLE #TempStoreMigration (
  StoreId VARCHAR(50),
  ProcessStatus CHAR(1)
);

INSERT INTO dbo.StoreMigration (StoreId, ProcessStatus)
SELECT StoreId, ProcessStatus
FROM #TempStoreMigration
WHERE ProcessStatus = 'I';
`;

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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'StoreId'")),
    "INSERT ... SELECT bare source columns should not treat dbo target as ambiguity owner"
  );
});

runCase("insert-select-bare-columns-do-not-count-table-type-or-insert-target-as-ambiguity-owner", () => {
  const queryUri = "file:///validation/insert-select-table-type-target-leak-query.sql";

  const querySql = `
CREATE TYPE dbo.StoreMigrationType AS TABLE
(
  StoreMigrationBatchId VARCHAR(100),
  StoreId VARCHAR(150),
  BusinessUnitId INT,
  SegmentCode VARCHAR(10),
  ProcessStatus CHAR(1)
);

CREATE TABLE dbo.StoreMigration
(
  StoreMigrationBatchId VARCHAR(100),
  StoreId VARCHAR(150),
  BusinessUnitId INT,
  SegmentCode VARCHAR(10),
  ProcessStatus CHAR(1)
);

CREATE TABLE #TempStoreMigration
(
  StoreMigrationBatchId VARCHAR(100),
  StoreId VARCHAR(150),
  BusinessUnitId INT,
  SegmentCode VARCHAR(10),
  ProcessStatus CHAR(1)
);

INSERT INTO dbo.StoreMigration
(
  StoreMigrationBatchId,
  StoreId,
  BusinessUnitId,
  SegmentCode,
  ProcessStatus
)
SELECT
  StoreMigrationBatchId,
  StoreId,
  BusinessUnitId,
  SegmentCode,
  ProcessStatus
FROM #TempStoreMigration
WHERE ProcessStatus = 'I';
`;

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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'StoreId'")),
    "INSERT ... SELECT bare columns should resolve from source table without table-type or insert-target leakage"
  );
});

runCase("insert-select-from-tvp-variable-does-not-count-insert-target-as-ambiguity-owner", () => {
  const queryUri = "file:///validation/insert-select-tvp-var-target-leak-query.sql";

  const querySql = `
CREATE TYPE dbo.OutboundLeadtimeType AS TABLE
(
  Id INT,
  Action VARCHAR(8),
  OriginFacility VARCHAR(25),
  DestCountry VARCHAR(10)
);

CREATE TABLE #ProcessLogisticsOdPairs_Temp
(
  Id INT,
  Action VARCHAR(8),
  OriginFacility VARCHAR(25),
  DestCountry VARCHAR(10)
);

DECLARE @Events dbo.OutboundLeadtimeType;

INSERT INTO #ProcessLogisticsOdPairs_Temp (Id, Action, OriginFacility, DestCountry)
SELECT [Id], [Action], [OriginFacility], [DestCountry]
FROM @Events;
`;

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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'Id'")),
    "INSERT ... SELECT from TVP variable should not treat insert target as an ambiguity owner"
  );
  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'OriginFacility'")),
    "INSERT ... SELECT from TVP variable should keep bare columns bound to read source"
  );
});

runCase("recursive-cte-anchor-bare-columns-do-not-ambiguously-bind-to-self-cte", () => {
  const queryUri = "file:///validation/recursive-cte-anchor-bare-columns.sql";
  const querySql = `
;WITH SeedRows AS (
  SELECT 1 AS KeyId, 'A' AS EntityName, 'X' AS GroupCode, CAST(GETDATE() AS DATE) AS EffectiveDate, 'C' AS ParentEntity, 1 AS MetricValue
),
RecursiveRows AS (
  SELECT KeyId, EntityName, GroupCode, EffectiveDate, ParentEntity, MetricValue
  FROM SeedRows
  WHERE KeyId = 1
  UNION ALL
  SELECT s.KeyId, s.EntityName, s.GroupCode, s.EffectiveDate,
         COALESCE(s.ParentEntity, r.ParentEntity) AS ParentEntity,
         COALESCE(s.MetricValue, r.MetricValue) AS MetricValue
  FROM SeedRows s
  JOIN RecursiveRows r ON r.EntityName = s.EntityName AND r.KeyId = s.KeyId - 1
)
SELECT EntityName FROM RecursiveRows;
`;

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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'KeyId'")),
    "Recursive CTE anchor bare KeyId should not be ambiguous against self CTE symbol"
  );
  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'EntityName'")),
    "Recursive CTE anchor bare EntityName should remain local to anchor source"
  );
});

runCase("simple-update-bare-where-column-uses-update-target-not-global-collisions", () => {
  const queryUri = "file:///validation/simple-update-bare-where-target-query.sql";
  const querySql = `
CREATE TABLE Employee (
  Column1 INT,
  Column2 INT
);
CREATE TABLE Department (
  Column2 INT
);

UPDATE Employee
SET Column1 = 1
WHERE Column2 = 2;
`;

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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'Column2'")),
    "Simple UPDATE bare WHERE column should bind to update target table and not global collisions"
  );
});

runCase("simple-update-on-temp-table-bare-where-column-uses-temp-target-not-global-collisions", () => {
  const queryUri = "file:///validation/simple-update-temp-bare-where-target-query.sql";
  const querySql = `
CREATE TABLE Employee (
  Column1 NVARCHAR(100)
);

CREATE TABLE #temp (
  Column1 NVARCHAR(100)
);

UPDATE #temp
SET Column1 = 'Value'
WHERE Column1 = 'Another Value';
`;

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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'Column1'")),
    "Simple UPDATE on temp table should bind bare WHERE column to temp target and not global collisions"
  );
});

runCase("update-where-bare-columns-do-not-fallback-to-parser-multi-input-ambiguity", () => {
  const schemaUri = "file:///validation/update-where-parser-multi-input-schema.sql";
  const queryUri = "file:///validation/update-where-parser-multi-input-query.sql";

  const schemaSql = `
CREATE TABLE dbo.ItemFacilityDispositionAvailability (
  ItemNumber VARCHAR(60),
  FacilityCode VARCHAR(20),
  DispositionId INT,
  ReservationQty INT,
  CommitQty INT
);
CREATE TABLE dbo.ItemFacilityDispositionReservationsCommits (
  ItemNumber VARCHAR(60),
  FacilityCode VARCHAR(20),
  DispositionId INT,
  Qty INT,
  IsCommitted BIT,
  IsActive BIT
);
`;

  const querySql = `
DECLARE @ItemNumber VARCHAR(60), @TargetFacilityCode VARCHAR(20), @DispositionId INT;
UPDATE [dbo].[ItemFacilityDispositionAvailability]
SET ReservationQty = (
      SELECT ISNULL(SUM(Qty), 0)
      FROM [dbo].[ItemFacilityDispositionReservationsCommits]
      WHERE ItemNumber = @ItemNumber
        AND FacilityCode = @TargetFacilityCode
        AND DispositionId = @DispositionId
        AND IsCommitted = 0
        AND IsActive = 1
    ),
    CommitQty = (
      SELECT ISNULL(SUM(Qty), 0)
      FROM [dbo].[ItemFacilityDispositionReservationsCommits]
      WHERE ItemNumber = @ItemNumber
        AND FacilityCode = @TargetFacilityCode
        AND DispositionId = @DispositionId
        AND IsCommitted = 1
        AND IsActive = 1
    )
WHERE ItemNumber = @ItemNumber
  AND FacilityCode = @TargetFacilityCode
  AND DispositionId = @DispositionId;
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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'FacilityCode'")),
    "UPDATE WHERE bare columns should not be marked ambiguous by parser multi-input fallback when mutation target binding is available"
  );
});

runCase("mixed-update-variable-assignment-predicate-uses-parser-mutation-inputs", () => {
  const schemaUri = "file:///validation/mixed-update-parser-mutation-schema.sql";
  const queryUri = "file:///validation/mixed-update-parser-mutation-query.sql";

  const schemaSql = `
CREATE TABLE dbo.ReservationItem (
  ReservationId INT,
  IsExpired BIT,
  IsCommitted BIT
);
CREATE TABLE dbo.OtherReservationItem (
  ReservationId INT
);
`;

  const querySql = `
DECLARE @Event INT;
UPDATE ri
SET IsCommitted = 1,
    @Event = CASE WHEN ri.IsExpired = 1 THEN 1 ELSE @Event END
FROM dbo.ReservationItem ri
WHERE ReservationId = 1;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const assignmentKinds = (parsed?.lineage?.mutations?.[0]?.location as any)?.assignments?.map((a: any) => a?.targetKind) ?? [];
  assert.ok(assignmentKinds.includes("variable"), "Parser should classify mixed UPDATE variable assignment target");
  assert.ok(
    (parsed?.lineage?.mutations?.[0]?.predicateInputs ?? []).some((i: any) => String(i?.name ?? "").endsWith(".ReservationId")),
    "Parser should expose UPDATE predicate inputs"
  );

  const offset = querySql.lastIndexOf("ReservationId = 1");
  const resolved = resolveBareColumnAtOffset({
    parsed,
    offset,
    columnName: "ReservationId",
    tablesByName,
    tableTypesByName
  });

  assert.strictEqual(resolved.status, "resolved", "Bare UPDATE predicate column should resolve through parser mutation predicate inputs");
  assert.ok(
    String(resolved.owner?.ownerName ?? "").toLowerCase().includes("reservationitem"),
    "Resolved predicate owner should be the UPDATE target source"
  );

  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );
  assert.ok(
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'ReservationId'")),
    "Mixed UPDATE predicate column should not be marked ambiguous"
  );
});

runCase("simple-delete-bare-where-column-uses-delete-target-not-global-collisions", () => {
  const queryUri = "file:///validation/simple-delete-bare-where-target-query.sql";
  const querySql = `
CREATE TABLE Employee (
  Column1 INT,
  Column2 INT
);
CREATE TABLE Department (
  Column2 INT
);

DELETE Employee
WHERE Column2 = 2;
`;

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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'Column2'")),
    "Simple DELETE bare WHERE column should bind to delete target table and not global collisions"
  );
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

runCase("ambiguity-not-emitted-for-order-by-select-alias", () => {
  const schemaUri = "file:///validation/ambiguity-orderby-alias-schema.sql";
  const queryUri = "file:///validation/ambiguity-orderby-alias-query.sql";

  const schemaSql = `
CREATE TABLE EmployeeTraining (
  EmployeeId INT
);
CREATE TABLE Employee (
  EmployeeId INT
);
`;

  const querySql = `
SELECT e.EmployeeId EmployeeId
FROM EmployeeTraining et
JOIN dbo.Employee e ON e.EmployeeId = et.EmployeeId
ORDER BY EmployeeId DESC;
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
    "ORDER BY references to SELECT aliases should not be reported as ambiguous bare columns"
  );
});

runCase("ambiguity-emitted-for-order-by-duplicate-select-alias", () => {
  const schemaUri = "file:///validation/ambiguity-orderby-duplicate-alias-schema.sql";
  const queryUri = "file:///validation/ambiguity-orderby-duplicate-alias-query.sql";

  const schemaSql = `
CREATE TABLE EmployeeTraining (
  EmployeeId INT
);
CREATE TABLE Employee (
  EmployeeId INT
);
`;

  const querySql = `
SELECT e.EmployeeId EmployeeId,
       et.EmployeeId EmployeeId
FROM EmployeeTraining et
JOIN dbo.Employee e ON e.EmployeeId = et.EmployeeId
ORDER BY EmployeeId DESC;
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
    diagnostics.some(d => String(d.message).includes("Ambiguous column 'EmployeeId'")),
    "ORDER BY on duplicate SELECT aliases should still be reported as ambiguous"
  );
});

runCase("parser-ambiguous-candidates-are-narrowed-by-schema-ownership", () => {
  const schemaUri = "file:///validation/ambig-schema-narrow.sql";
  const queryUri = "file:///validation/ambig-schema-narrow-query.sql";

  const schemaSql = `
CREATE TABLE dbo.ShipmentOptions (
  ShippingOptionId INT,
  ShippingOptionCode VARCHAR(20),
  ShippingOptionDesc VARCHAR(100),
  DisplayOrder INT,
  ShipMode VARCHAR(20),
  IsIncludedByDefault BIT,
  IsEnabled BIT
);
CREATE TABLE dbo.CountryShipmentOptions (
  ShipmentOptionId INT,
  CountryId INT
);
CREATE TABLE dbo.Countries (
  CountryId INT,
  CountryCode2Char CHAR(2),
  CountryName VARCHAR(100)
);
`;

  const querySql = `
SELECT DISTINCT
    ShippingOptionCode OptionCode,
    ShippingOptionDesc Description,
    DisplayOrder,
    ShipMode,
    IsIncludedByDefault DefaultChoice,
    IsEnabled,
    c.CountryCode2Char,
    c.CountryName
FROM dbo.ShipmentOptions so WITH(NOLOCK)
LEFT JOIN dbo.CountryShipmentOptions cso WITH(NOLOCK) ON so.ShippingOptionId = cso.ShipmentOptionId
LEFT JOIN dbo.Countries c WITH(NOLOCK) ON cso.CountryId = c.CountryId;
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
    diagnostics.every(d => !String(d.message).includes("ShippingOptionCode")),
    "Schema ownership should suppress parser-only ambiguity for uniquely owned bare columns"
  );
  assert.ok(
    diagnostics.every(d => !String(d.message).includes("ShippingOptionDesc")),
    "Schema ownership should suppress parser-only ambiguity for uniquely owned bare columns"
  );
  assert.ok(
    diagnostics.every(d => !String(d.message).includes("DisplayOrder")),
    "Schema ownership should suppress parser-only ambiguity for uniquely owned bare columns"
  );
});

runCase("join-on-qualified-column-resolves-through-alias-target-even-with-select-alias-name-collision", () => {
  const schemaUri = "file:///validation/join-on-alias-target-schema.sql";
  const queryUri = "file:///validation/join-on-alias-target-query.sql";

  const schemaSql = `
CREATE TABLE dbo.InboundShippingOptionRules (
  BusinessUnitId INT
);
CREATE TABLE dbo.BusinessUnits (
  BusinessUnitId INT,
  BusinessUnitCode VARCHAR(20)
);
`;

  const querySql = `
SELECT IIF(CAST(inr.Column1 AS VARCHAR(10)) = -1, 'ALL', CONCAT(bu.Column1, '-', bu.Column1)) AS [BU]
FROM dbo.SomeTable inr
LEFT JOIN dbo.SomeOtherTable bu ON inr.Column1 = bu.Column1;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const ambiguous = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );
  assert.ok(
    ambiguous.every(d => !String(d.message).includes("Column1")),
    "Qualified alias column in JOIN ON should not surface ambiguous-column diagnostics"
  );
});

runCase("qualified-column-works-when-select-alias-name-collides-with-table-alias", () => {
  const schemaUri = "file:///validation/select-alias-table-alias-collision-schema.sql";
  const queryUri = "file:///validation/select-alias-table-alias-collision-query.sql";

  const schemaSql = `
CREATE TABLE dbo.Table1 (
  Column1 INT
);
`;

  const querySql = `
SELECT Alias1.Column1 AS Alias1
FROM dbo.Table1 AS Alias1;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  const parsed = parseSql(querySql);
  const lineStarts = getLineStarts(querySql);
  const ambiguous = collectAmbiguousColumnDiagnostics(
    parsed,
    lineStarts,
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );

  assert.ok(
    ambiguous.every(d => !String(d.message).includes("Column1")),
    "Qualified column should resolve through table alias even when SELECT alias has same name"
  );
});

runCase("single-table-variable-select-bare-columns-are-not-ambiguous", () => {
  const queryUri = "file:///validation/single-table-variable-select-bare.sql";
  const querySql = `
CREATE PROCEDURE dbo.p
AS
BEGIN
  DECLARE @ResultTable TABLE (
    LocationCode VARCHAR(20),
    Quantity INT,
    StatusCode VARCHAR(50),
    StatusName VARCHAR(100)
  );

  SELECT LocationCode, Quantity, StatusCode, StatusName
  FROM @ResultTable;
END;
`;

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
    diagnostics.every(d => !String(d.message).includes("LocationCode")),
    "Bare columns from a single table-variable source should not be reported as ambiguous"
  );
});

runCase("tvp-parameter-outside-from-does-not-compete-with-local-table-variable-source", () => {
  const schemaUri = "file:///validation/tvp-scope-competition-schema.sql";
  const queryUri = "file:///validation/tvp-scope-competition-query.sql";

  const schemaSql = `
CREATE TYPE dbo.SomeTableType AS TABLE (
  LocationCode VARCHAR(20)
);
`;

  const querySql = `
CREATE PROCEDURE dbo.Repro
  @InputRows dbo.SomeTableType READONLY
AS
BEGIN
  DECLARE @ResultTable TABLE (
    LocationCode VARCHAR(20),
    Quantity INT
  );

  SELECT LocationCode, Quantity
  FROM @ResultTable;
END;
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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'LocationCode'")),
    "TVP parameter columns must not compete when the active SELECT reads from a different local table variable source"
  );
});

runCase("nested-procedure-update-bare-where-binds-to-mutation-target-not-visible-tvp", () => {
  const schemaUri = "file:///validation/nested-proc-update-tvp-schema.sql";
  const queryUri = "file:///validation/nested-proc-update-tvp-query.sql";

  const schemaSql = `
CREATE TYPE dbo.FacilityCodeListType AS TABLE (
  LocationCode VARCHAR(20)
);
CREATE TABLE dbo.InventoryByLocation (
  ItemKey VARCHAR(60),
  LocationCode VARCHAR(20),
  CategoryId INT,
  ReservedQty INT,
  IsActive BIT
);
CREATE TABLE dbo.InventoryEvents (
  ItemKey VARCHAR(60),
  LocationCode VARCHAR(20),
  CategoryId INT,
  Qty INT,
  IsFinalized BIT,
  IsActive BIT
);
`;

  const querySql = `
CREATE PROCEDURE dbo.Repro
  @Facilities dbo.FacilityCodeListType READONLY,
  @ItemKey VARCHAR(60),
  @CategoryId INT,
  @TargetLocationCode VARCHAR(20)
AS
BEGIN
  DECLARE @ResultTable TABLE (
    LocationCode VARCHAR(20),
    Quantity INT
  );

  UPDATE dbo.InventoryByLocation
  SET ReservedQty = (
        SELECT ISNULL(SUM(Qty), 0)
        FROM dbo.InventoryEvents
        WHERE ItemKey = @ItemKey
          AND LocationCode = @TargetLocationCode
          AND CategoryId = @CategoryId
          AND IsFinalized = 0
          AND IsActive = 1
      )
  WHERE ItemKey = @ItemKey
    AND LocationCode = @TargetLocationCode
    AND CategoryId = @CategoryId
    AND IsActive = 1;
END;
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
    !diagnostics.some(d => String(d.message).includes("Ambiguous column 'LocationCode'")),
    "Nested UPDATE WHERE bare LocationCode should bind to mutation target and not visible TVP/table-variable symbols"
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

runCase("derived-subquery-alias-does-not-leak-inner-sources-to-outer-scope", () => {
  const uri = "file:///validation/derived-subquery-alias-boundary.sql";
  const schemaSql = `
CREATE TABLE dbo.Employee (
  EmployeeId INT,
  FirstName NVARCHAR(50),
  LastName NVARCHAR(50)
);
`;
  indexText("file:///schema/employee.sql", schemaSql);

  const querySql = `
SELECT s.EmployeeId
FROM (
  SELECT e.EmployeeId
  FROM dbo.Employee e
) s
WHERE FirstName IS NOT NULL;
`;

  indexText(uri, querySql);
  const parsed = parseSql(querySql);
  const offset = querySql.indexOf("FirstName");
  assert.ok(offset >= 0, "FirstName token must exist in test SQL");

  const resolved = resolveBareColumnAtOffset({
    parsed,
    offset,
    columnName: "FirstName",
    tablesByName,
    tableTypesByName
  });

  assert.strictEqual(
    resolved.status,
    "unresolved",
    "Outer scope must not see non-projected inner source columns through derived alias"
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

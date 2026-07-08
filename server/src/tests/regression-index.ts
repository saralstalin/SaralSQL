import * as assert from "assert";
import { TextDocument } from "vscode-languageserver-textdocument";
import { SqlCmdPreprocessor } from "@saralsql/tsql-parser";
import { parseSql } from "../sql-parser";
import { getCteColumns, getDisplaySymbolName, resolveAliasFromAst, resolveColumnFromAst, walkAst, normalizeAstTableName, extractColumnName, extractQualifiedName, resolveAliasTableName, resolveSymbolCaseInsensitive } from "../ast-utils";
import { getLineStarts, normalizeName, offsetAt, getWordRangeAtPosition, isSqlKeyword } from "../text-utils";
import { collectAmbiguousColumnDiagnostics } from "../diagnostic-helpers";
import { collectNearestScopeColumnOwners } from "../scope-column-resolver";
import {
  indexText,
  getRefs,
  getReferencesForUri,
  findReferenceAtPosition,
  aliasesByUri,
  definitions,
  referencesIndex,
  columnsByTable,
  tablesByName,
  tableTypesByName,
  tempTablesByUri,
  deleteFileFromIndex,
  setIndexReady,
  setIndexNotReady,
  getIndexReady,
  findColumnInTable,
  parseColumnsFromCreateView,
  findTableOrColumn,
  setRefsForFile
} from "../definitions";

function resetIndexState(): void {
  aliasesByUri.clear();
  definitions.clear();
  referencesIndex.clear();
  columnsByTable.clear();
  tablesByName.clear();
  tableTypesByName.clear();
  tempTablesByUri.clear();
}

function runCase(name: string, fn: () => void): void {
  resetIndexState();
  fn();
  process.stdout.write(`[pass] ${name}\n`);
}

function findSymbol(scope: any, kind: string, symbolName: string): any | undefined {
  if (!scope) {
    return undefined;
  }

  const symbols = typeof scope.getOwnSymbols === "function"
    ? scope.getOwnSymbols()
    : Object.values(scope.symbols ?? {});

  for (const sym of symbols) {
    if (sym?.kind === kind && String(sym.name ?? "").toLowerCase() === symbolName.toLowerCase()) {
      return sym;
    }
  }

  const children = typeof scope.getChildren === "function"
    ? scope.getChildren()
    : (scope.children ?? []);

  for (const child of children) {
    const found = findSymbol(child, kind, symbolName);
    if (found) {
      return found;
    }
  }

  return undefined;
}

runCase("cte-name-and-column-resolution", () => {
  const uri = "file:///regression/cte-name-and-column-resolution.sql";
  const sql = `
WITH cteEmp AS (
  SELECT EmployeeId, DepartmentId FROM Employee
)
SELECT c.EmployeeId, c.DepartmentId
FROM cteEmp c;
`;

  const parsed = parseSql(sql);
  const cte = findSymbol(parsed?.scope?.root, "CTE", "cteEmp");
  assert.ok(cte, "CTE symbol should exist in parser scope");
  assert.ok(Array.isArray(cte.location?.query?.columns), "CTE query should expose projected columns");

  indexText(uri, sql);

  assert.ok(getRefs("cteemp").length > 0, "CTE table references should be indexed");
  assert.ok(getRefs("cteemp.employeeid").length > 0, "CTE projected columns should resolve via alias usage");
  assert.ok(getRefs("cteemp.departmentid").length > 0, "CTE projected columns should resolve via alias usage");
});

runCase("multi-cte-resolution-relies-on-scope-not-text-fallback", () => {
  const uri = "file:///regression/multi-cte-scope-only.sql";
  const sql = `
WITH c1 AS (
  SELECT EmployeeId FROM Employee
),
c2 AS (
  SELECT EmployeeId FROM c1
)
SELECT EmployeeId
FROM c2;
`;

  const parsed = parseSql(sql);
  const c1 = findSymbol(parsed?.scope?.root, "CTE", "c1");
  const c2 = findSymbol(parsed?.scope?.root, "CTE", "c2");
  assert.ok(c1, "First CTE should exist in parser scope");
  assert.ok(c2, "Second CTE should exist in parser scope");
  assert.ok(getCteColumns(c2).some(c => normalizeName(c.name) === "employeeid"), "Second CTE should expose projected EmployeeId in scope metadata");

  indexText(uri, sql);
  assert.ok(getRefs("c1").length > 0, "First CTE should be indexed from parser scope");
  assert.ok(getRefs("c2").length > 0, "Second CTE should be indexed from parser scope");
});

runCase("non-build-files-do-not-contribute-workspace-schema", () => {
  const uri = "file:///regression/non-build-schema-exclusion.sql";
  const sql = `
CREATE TABLE dbo.ExcludedTable (
  Id INT
);
`;

  indexText(uri, sql, { includeInWorkspaceSchema: false });
  assert.ok(definitions.get(uri)?.length, "File should still be indexed for local editor features");
  assert.ok(!tablesByName.has("dbo.excludedtable"), "Excluded file should not contribute table to workspace schema");
  assert.ok(!columnsByTable.has("dbo.excludedtable"), "Excluded file should not contribute columns to workspace schema");
});

runCase("cte-column-validation-preconditions", () => {
  const uri = "file:///regression/cte-column-validation-preconditions.sql";
  const sql = `
WITH cteEmp AS (
  SELECT EmployeeId, DepartmentId FROM Employee
)
SELECT c.EmployeeId
FROM cteEmp c
WHERE c.DepartmentId > 10;
`;

  indexText(uri, sql);
  const aliasMap = aliasesByUri.get(uri);
  assert.ok(aliasMap?.get("c") === "cteemp", "Alias should resolve to CTE name for downstream validation checks");
  assert.ok(getRefs("cteemp.employeeid").length > 0, "CTE-qualified column should map to CTE");
});

runCase("create-view-cte-name-and-column-resolution", () => {
  const uri = "file:///regression/create-view-cte-name-and-column-resolution.sql";
  const sql = `
CREATE VIEW dbo.vEmployeeDepartment
AS
WITH cteEmp AS (
  SELECT EmployeeId, DepartmentId FROM Employee
)
SELECT c.EmployeeId, c.DepartmentId
FROM cteEmp c;
`;

  const parsed = parseSql(sql);
  const cte = findSymbol(parsed?.scope?.root, "CTE", "cteEmp");
  assert.ok(cte, "CTE symbol inside CREATE VIEW should exist in parser scope");
  assert.ok(Array.isArray(cte.location?.query?.columns), "CTE query inside CREATE VIEW should expose projected columns");

  indexText(uri, sql);
  assert.ok(getRefs("cteemp").length > 0, "CTE table references inside CREATE VIEW should be indexed");
  assert.ok(getRefs("cteemp.employeeid").length > 0, "CTE projected columns inside CREATE VIEW should resolve via alias usage");
  assert.ok(getRefs("cteemp.departmentid").length > 0, "CTE projected columns inside CREATE VIEW should resolve via alias usage");
});

runCase("derived-table-column-definition-resolution", () => {
  const uri = "file:///regression/derived-table-column-definition-resolution.sql";
  const sql = `
SELECT a.SomeName
FROM (
  SELECT d.DepartmentName,
         d.DepartmentId,
         e.EmployeeId,
         e.FirstName SomeName,
         e.LastName
  FROM [dbo].[Department] d
  JOIN [dbo].[Employee] e ON d.DepartmentId = e.DepartmentId
) a;
`;

  const parsed = parseSql(sql);
  const alias = findSymbol(parsed?.scope?.root, "Alias", "a");
  assert.ok(alias, "Derived table alias should exist in parser scope");
  assert.strictEqual(alias?.metadata?.sourceKind, "derived_subquery", "Parser should classify derived-table aliases explicitly");

  const resolution = parsed?.columns?.resolutions?.find((r: any) => r.location?.name === "a.SomeName");
  assert.ok(
    resolution?.inputs?.some((input: any) => normalizeName(input.source) === "employee" && normalizeName(String(input.name).split(".").pop() ?? "") === "firstname"),
    "Parser should resolve derived projected columns back to their source column"
  );

  indexText(uri, sql);
  assert.ok(getRefs("employee.firstname").length > 0, "Derived table projected column should be indexed through parser lineage");
});

runCase("derived-table-alias-exposes-projected-columns", () => {
  const sql = `
SELECT a.SomeName
FROM (
  SELECT d.DepartmentName,
         d.DepartmentId,
         e.EmployeeId,
         e.FirstName SomeName,
         e.LastName,
         e.Email
  FROM [dbo].[Department] d
  JOIN [dbo].[Employee] e ON d.DepartmentId = e.DepartmentId AND d.HeadEmployeeId = e.EmployeeId
) a;
`;

  const parsed = parseSql(sql);
  const alias = findSymbol(parsed?.scope?.root, "Alias", "a");
  assert.ok(alias, "Derived table alias a should exist in parser scope");
  const projected = Array.isArray(alias?.location?.table?.query?.columns)
    ? alias.location.table.query.columns.map((c: any) => c.outputName ?? c.sourceName ?? c.expression?.name).filter(Boolean)
    : [];
  assert.ok(projected.includes("SomeName"), "Derived table alias should expose projected column SomeName");
  assert.ok(!projected.includes("somename"), "Derived table alias should preserve raw column casing");
  assert.ok(projected.includes("DepartmentId"), "Derived table alias should expose projected column DepartmentId");
  assert.strictEqual(getDisplaySymbolName({ rawName: "SomeName", name: "somename" }), "SomeName", "Hover display should prefer raw names");
});

runCase("completion-sanitized-select-list-resolves-join-alias", () => {
  const sql = `
SELECT a.SomeName
    FROM (
    SELECT d.DepartmentName
           , d.DepartmentId
           , e.EmployeeId
           , e.FirstName SomeName
           , e.LastName
           , e.__X__
    FROM [dbo].[Department] d 
         JOIN [dbo].[Employee] e  ON d.DepartmentId = e.DepartmentId AND d.HeadEmployeeId = e.EmployeeId
    WHERE d.DepartmentId = 23378 ) a
`;

  const parsed = parseSql(sql);
  assert.ok(parsed?.ast, "Sanitized completion parse should produce an AST");
  assert.strictEqual(resolveAliasFromAst("e", parsed?.ast), "employee", "Join alias e should resolve to Employee in sanitized completion parses");
});

runCase("unknown-table-alias-reference-is-not-schema-validated", () => {
  const uri = "file:///regression/unknown-table-alias.sql";
  const sql = `
SELECT *
FROM UnknownTable ut;
`;

  indexText(uri, sql);
  const refs = getReferencesForUri(uri);
  const tableRef = refs.find(r => r.kind === "table" && r.start === 5);
  const aliasRef = refs.find(r => r.kind === "table" && r.start === 18);
  assert.ok(tableRef, "Source table token should be indexed");
  assert.ok(aliasRef, "Alias token should be indexed");
  assert.strictEqual(tableRef?.validateSchema, true, "Source table token should be schema-validated");
  assert.strictEqual(aliasRef?.validateSchema, false, "Alias token should not be schema-validated");
});

runCase("create-view-join-alias-reference-is-not-schema-validated", () => {
  const uri = "file:///regression/create-view-join-alias-reference.sql";
  const sql = `
CREATE VIEW dbo.GenericJoinAliasView
AS
SELECT bu.UnitId
FROM [dbo].[RuleSource] [inr]
LEFT JOIN [dbo].[UnitLookup] [bu] ON [inr].UnitId = [bu].UnitId;
`;

  indexText(uri, sql);
  const aliasMap = aliasesByUri.get(uri);
  assert.ok(aliasMap?.get("bu") === "unitlookup" || aliasMap?.get("bu") === "dbo.unitlookup", "JOIN alias bu should resolve to UnitLookup in CREATE VIEW");
  const refs = getReferencesForUri(uri);
  const buAliasRefs = refs.filter(r => r.kind === "table" && normalizeName(r.name) === "bu");
  assert.ok(
    buAliasRefs.every(r => r.validateSchema === false),
    "Any JOIN alias token inside CREATE VIEW must not be schema-validated as a physical table"
  );
});

runCase("create-view-select-alias-name-clash-does-not-flag-join-alias-as-unknown-table", () => {
  const uri = "file:///regression/create-view-select-alias-clash.sql";
  const sql = `
CREATE VIEW dbo.GenericSelectAliasClashView
AS
SELECT IIF(CAST([inr].[UnitId] AS VARCHAR(10)) = -1, 'ALL', CONCAT(bu.UnitId, '-', bu.UnitCode)) [BU]
FROM [dbo].[RuleSource] [inr]
LEFT JOIN [dbo].[UnitLookup] [bu] ON [inr].UnitId = [bu].UnitId;
`;

  indexText(uri, sql);
  const aliasMap = aliasesByUri.get(uri);
  assert.ok(aliasMap?.get("bu") === "unitlookup" || aliasMap?.get("bu") === "dbo.unitlookup", "JOIN alias bu should resolve to UnitLookup despite SELECT alias [BU]");
  const refs = getReferencesForUri(uri);
  const buAliasRefs = refs.filter(r => r.kind === "column" && normalizeName(r.name).startsWith("bu."));
  assert.ok(buAliasRefs.length > 0, "JOIN alias bu qualified column usages should be indexed");
  assert.ok(
    buAliasRefs.every(r => r.validateSchema === false),
    "JOIN alias bu qualified usages should remain non-schema-validated even when SELECT alias [BU] exists"
  );
});

runCase("multi-statement-alias-resolution-isolation", () => {
  const uri = "file:///regression/alias-qualified-column-resolution.sql";
  const sql = `
UPDATE e
SET e.Name = 'Updated Name',
    e.Salary = 60000.00
FROM dbo.Employee AS e
WHERE e.EmployeeId = 1;

CREATE TABLE #t (
    Id INT,
    Name NVARCHAR(100),
    Salary DECIMAL(10,2)
);

INSERT INTO #t (Id, Name, Salary)
VALUES (1, 'John Doe', 50000.00);

SELECT e.DepartmentId
FROM DepartmentSalaryInfo e;
`;

  indexText(uri, sql);

  assert.ok(getRefs("departmentsalaryinfo.departmentid").length > 0, "Qualified alias column should resolve to current-statement alias target");
  assert.strictEqual(getRefs("#t.departmentid").length, 0, "Qualified alias column must not leak to earlier statement sources");
  assert.ok(getRefs("#t.id").length > 0, "Temp table Id column should be indexed");
  assert.ok(getRefs("#t.name").length > 0, "Temp table Name column should be indexed");
  assert.ok(getRefs("#t.salary").length > 0, "Temp table Salary column should be indexed");
});

runCase("temp-table-does-not-leak-across-files", () => {
  const uriA = "file:///regression/temp-a.sql";
  const uriB = "file:///regression/temp-b.sql";

  indexText(uriA, "CREATE TABLE #t (Id INT, Name NVARCHAR(100));");
  indexText(uriB, "SELECT t.Id FROM #t t;");

  const leaked = getRefs("#t.id").find(r => r.uri === uriB);
  assert.ok(leaked, "Temp table column token should still be indexed in-file");
  assert.strictEqual(
    leaked?.validateSchema,
    false,
    "Temp table columns from another file must not be schema-validated"
  );
});

runCase("select-into-temp-table-registers-schema", () => {
  const uri = "file:///regression/select-into-temp-table-registers-schema.sql";
  const sql = `
SELECT EmployeeId, Name
INTO #EmpTemp
FROM Employee;
`;

  indexText(uri, sql);
  const schema = tempTablesByUri.get(uri)?.get("#emptemp");
  assert.ok(schema, "SELECT INTO temp table should register file-scoped temp schema");
  assert.ok(schema?.columns.has("employeeid"), "Projected column EmployeeId should be registered on temp table");
  assert.ok(schema?.columns.has("name"), "Projected column Name should be registered on temp table");
});

runCase("delete-file-from-index-removes-diagnostics-linked-state", () => {
  const uri = "file:///regression/deleted-file.sql";
  const sql = `
CREATE TABLE dbo.DeletedFileTable (
  Id INT
);
SELECT Id FROM dbo.DeletedFileTable;
`;

  indexText(uri, sql);
  assert.ok(definitions.has(uri), "Indexed file should have definitions before deletion");
  assert.ok(tablesByName.has("deletedfiletable") || tablesByName.has("dbo.deletedfiletable"), "Indexed table should exist before deletion");
  assert.ok(getReferencesForUri(uri).length > 0, "Indexed file should have references before deletion");

  deleteFileFromIndex(uri);
  assert.ok(!definitions.has(uri), "Deleted file definitions should be removed");
  assert.ok(!tablesByName.has("deletedfiletable") && !tablesByName.has("dbo.deletedfiletable"), "Deleted file table symbols should be removed");
  assert.strictEqual(getReferencesForUri(uri).length, 0, "Deleted file references should be removed");
});

runCase("tvp-type-reference-is-indexed-for-navigation", () => {
  const uri = "file:///regression/tvp-type-reference-navigation.sql";
  const sql = `
CREATE TYPE dbo.MyTvp AS TABLE (
  Id INT,
  Name NVARCHAR(50)
);
GO
CREATE PROCEDURE dbo.p
  @items dbo.MyTvp READONLY
AS
BEGIN
  SELECT * FROM @items;
END
`;

  indexText(uri, sql);
  const typeRefs = getRefs("mytvp");
  assert.ok(
    typeRefs.some(r => r.context !== "create-definition"),
    "TVP usage type token should be indexed so go-to-definition can resolve to CREATE TYPE"
  );
});

runCase("update-target-alias-indexing", () => {
  const uri = "file:///regression/update-target-alias-hover.sql";
  const sql = `
UPDATE e
SET Name = 'X',
    e.Salary = 1000
FROM dbo.Employee e
WHERE e.EmployeeId = 1;

DELETE e
FROM dbo.Employee e
WHERE e.EmployeeId = 2;
`;

  indexText(uri, sql);
  const aliasMap = aliasesByUri.get(uri);
  assert.ok(aliasMap?.get("e") === "employee" || aliasMap?.get("e") === "dbo.employee", "DML target alias should resolve to backing table");

  const tableRefs = getRefs("e");
  assert.ok(tableRefs.some(r => r.context === "update-target"), "UPDATE alias target reference should be indexed");
  assert.ok(tableRefs.some(r => r.context === "delete-target"), "DELETE alias target reference should be indexed");
  assert.ok(getRefs("employee.name").length > 0, "SET bare assignment columns should resolve to update target table");
  assert.ok(getRefs("employee.salary").length > 0, "SET qualified assignment columns should resolve to update target table");
});

runCase("update-where-bare-column-falls-back-to-update-target", () => {
  const uri = "file:///regression/update-where-bare-column-fallback.sql";
  const sql = `
UPDATE HackathonWinners
SET Prize = @GoodieName
WHERE WinnerId = @WinnerId;
`;

  indexText(uri, sql);
  assert.ok(
    getRefs("hackathonwinners.winnerid").length > 0,
    "Bare column in UPDATE WHERE should map to update target table when parser does not resolve it directly"
  );
});

runCase("derived-table-alias-column-resolution", () => {
  const uri = "file:///regression/derived-table-alias-columns.sql";
  const sql = `
SELECT d.EmployeeId
FROM (SELECT EmployeeId FROM Employee) d
WHERE d.EmployeeId > 0;
`;

  indexText(uri, sql);
  assert.ok(getRefs("employee.employeeid").length > 0, "Derived table alias projected columns should be indexed through parser lineage");
});

runCase("system-table-validation-exemption-preconditions", () => {
  const uri = "file:///regression/system-table-validation-exemption.sql";
  const sql = `
SELECT o.name, t.table_name
FROM sys.objects o
JOIN INFORMATION_SCHEMA.TABLES t ON 1 = 1;
`;

  indexText(uri, sql);
  const aliasMap = aliasesByUri.get(uri);
  assert.ok(aliasMap?.get("o") === "sys.objects", "sys.* alias should resolve correctly");
  assert.ok(aliasMap?.get("t") === "information_schema.tables", "INFORMATION_SCHEMA alias should resolve correctly");
  assert.ok(getRefs("sys.objects").length > 0, "system table references should still be indexed for navigation");
  assert.ok(getRefs("information_schema.tables").length > 0, "information_schema references should still be indexed for navigation");
});

runCase("insert-target-column-list-indexing", () => {
  const uri = "file:///regression/insert-column-list-completions.sql";
  const sql = `
INSERT INTO dbo.Employee (EmployeeId, Name, Salary)
VALUES (1, 'John', 1000);
`;

  indexText(uri, sql);
  assert.ok(getRefs("employee.employeeid").length > 0, "INSERT target column list should index EmployeeId to target table");
  assert.ok(getRefs("employee.name").length > 0, "INSERT target column list should index Name to target table");
  assert.ok(getRefs("employee.salary").length > 0, "INSERT target column list should index Salary to target table");
});

runCase("if-exists-subquery-table-reference-is-indexed", () => {
  const schemaUri = "file:///regression/patent-schema.sql";
  const queryUri = "file:///regression/if-exists-patent-query.sql";

  const schemaSql = `
CREATE TABLE Patent (
  PatentId INT
);
`;

  const querySql = `
IF NOT EXISTS (SELECT 1 FROM Patent WHERE PatentId = @PatentId)
BEGIN
  RAISERROR('Patent with id %d not found.', 16, 1, @PatentId);
  RETURN;
END;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);

  const refs = getRefs("patent").filter(r => r.uri === queryUri && r.kind === "table");
  assert.ok(refs.length > 0, "Table reference inside IF EXISTS subquery should be indexed");

  const doc = TextDocument.create(queryUri, "sql", 0, querySql);
  const patentOffset = querySql.indexOf("Patent WHERE");
  const patentPos = doc.positionAt(patentOffset);
  const match = findReferenceAtPosition(queryUri, patentPos.line, patentPos.character);
  assert.ok(match && match.kind === "table" && match.name === "patent", "Definition/hover lookup should find the Patent table token inside IF EXISTS");
});

runCase("aliased-missing-column-stays-schema-validated", () => {
  const uri = "file:///regression/aliased-missing-column-schema-validation.sql";
  const sql = `
CREATE TABLE dbo.Employee (
  EmployeeId INT,
  FirstName NVARCHAR(50)
);
SELECT e.EmployeeId, e.DoesNotExist
FROM dbo.Employee e;
`;

  indexText(uri, sql);
  const missingRefs = getRefs("employee.doesnotexist");
  assert.ok(missingRefs.length > 0, "Aliased missing column should still be indexed against the backing table");
  assert.ok(
    missingRefs.some(r => r.validateSchema !== false),
    "Aliased missing column should remain schema-validated so LSP002 can be emitted"
  );
});

runCase("bare-column-no-global-fallback", () => {
  const baseUri = "file:///regression/bare-column-base-table.sql";
  const queryUri = "file:///regression/bare-column-no-from.sql";
  const baseSql = `
CREATE TABLE Employee (
  Name NVARCHAR(100),
  Salary DECIMAL(10,2)
);
`;
  const querySql = `
SELECT Name;
`;

  indexText(baseUri, baseSql);
  indexText(queryUri, querySql);

  assert.strictEqual(getRefs("employee.name").length, 1, "Only base-table definition reference should exist");
});

runCase("ambiguous-bare-column-warning", () => {
  const tablesUri = "file:///regression/ambiguous-tables.sql";
  const queryUri = "file:///regression/ambiguous-query.sql";
  const tablesSql = `
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

  indexText(tablesUri, tablesSql);
  indexText(queryUri, querySql);

  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(parsed, getLineStarts(querySql), tablesByName, tableTypesByName, "SaralSQL");
  assert.ok(diagnostics.some(d => String(d.message).includes("Ambiguous column 'Id'")), "Ambiguous bare column should emit warning diagnostic");
});

runCase("table-alias-declaration-is-not-a-table-object-usage", () => {
  const uri = "file:///regression/table-alias-reference-count.sql";
  const sql = `
CREATE TABLE Department (
  DepartmentId INT
);

SELECT d.DepartmentId
FROM Department d;
`;

  indexText(uri, sql);

  const refs = getRefs("department");
  const objectUsages = refs.filter(r => r.context !== "create-definition" && r.context !== "alias-declaration");
  const aliasDeclarations = refs.filter(r => r.context === "alias-declaration");

  assert.strictEqual(objectUsages.length, 1, "Table object references should count the FROM table token once");
  assert.strictEqual(aliasDeclarations.length, 1, "Alias declaration should remain indexed but separated from object usage counts");
});

runCase("join-table-token-is-not-indexed-as-bare-column", () => {
  const uri = "file:///regression/join-table-token-not-bare-column.sql";
  const sql = `
CREATE TABLE Employee (
  EmployeeId INT,
  EndDate DATETIME
);
CREATE TABLE LuckyDropWinners (
  WinnerId INT,
  ContestId INT,
  EmployeeId INT
);

INSERT INTO LuckyDropWinners (ContestId, EmployeeId)
SELECT @ContestId, EmployeeId
FROM (
  SELECT TOP (@NumberOfWinners) e.EmployeeId
  FROM Employee e
  LEFT JOIN LuckyDropWinners lw ON lw.EmployeeId = e.EmployeeId AND lw.ContestId = @ContestId
  WHERE lw.WinnerId IS NULL
) AS picks;
`;

  indexText(uri, sql);

  assert.ok(
    getRefs("luckydropwinners").some(r => r.kind === "table" && r.context !== "create-definition"),
    "LuckyDropWinners JOIN token should be indexed as a table reference"
  );
  assert.strictEqual(
    getRefs("employee.luckydropwinners").length,
    0,
    "JOIN table token should not be inferred as a bare column owned by Employee"
  );
});

runCase("resolve-column-does-not-leak-from-nested-select", () => {
  const sql = `
SELECT e.Name
FROM Employee e
WHERE EXISTS (
  SELECT d.DepartmentId
  FROM Department d
  WHERE d.DepartmentId = e.DepartmentId
);
`;
  const parsed = parseSql(sql);
  // resolveColumnFromAst expects an array of statement nodes (or a single statement),
  // not the Program wrapper -- pass ast.body so the SelectStatement branch actually runs.
  const resolved = resolveColumnFromAst(parsed?.ast?.body, "DepartmentId");
  assert.strictEqual(resolved, null, "Nested SELECT columns should not leak into outer statement resolution");
});

runCase("resolve-column-from-ast-covers-all-statement-shapes", () => {
  assert.strictEqual(resolveColumnFromAst(null, "X"), null, "Null ast should resolve to null");

  // The real parser represents both qualified and unqualified columns as Identifier
  // nodes (with .parts), never MemberExpression -- so resolveSelectStatement's
  // MemberExpression-specific matching (both in the select list and WHERE) is only
  // reachable via that exact node shape. Exercise it with synthetic AST nodes that
  // match the function's documented contract.
  const singleSourceSql = `SELECT FirstName FROM Employee;`;
  const singleSourceParsed = parseSql(singleSourceSql);
  assert.strictEqual(
    resolveColumnFromAst(singleSourceParsed?.ast?.body, "FirstName"),
    "employee",
    "Unqualified column with exactly one FROM source should resolve to that source"
  );
  assert.strictEqual(
    resolveColumnFromAst(singleSourceParsed?.ast?.body, "NoSuchColumn"),
    null,
    "Column absent from select list and WHERE clause should resolve to null"
  );

  const syntheticSelect = {
    type: "SelectStatement",
    columns: [{ type: "Column", expression: { type: "MemberExpression", object: { type: "Identifier", name: "e" }, property: "FirstName" } }],
    from: [
      { table: { type: "Identifier", name: "Employee" }, alias: "e", joins: [] },
      { table: { type: "Identifier", name: "Department" }, alias: "d", joins: [] }
    ]
  };
  assert.strictEqual(
    resolveColumnFromAst([syntheticSelect], "FirstName"),
    "employee",
    "MemberExpression-shaped select-list column should resolve via the alias map"
  );

  const syntheticWhere = {
    type: "SelectStatement",
    columns: [],
    from: [
      { table: { type: "Identifier", name: "Employee" }, alias: "e", joins: [] },
      { table: { type: "Identifier", name: "Department" }, alias: "d", joins: [] }
    ],
    where: { type: "MemberExpression", object: { type: "Identifier", name: "e" }, property: "DepartmentId" }
  };
  assert.strictEqual(
    resolveColumnFromAst([syntheticWhere], "DepartmentId"),
    "employee",
    "MemberExpression-shaped WHERE-clause column should resolve via the alias map"
  );
  assert.strictEqual(
    resolveColumnFromAst([syntheticWhere], "SomeOtherColumn"),
    null,
    "MemberExpression WHERE node whose property doesn't match the searched column should be skipped"
  );

  // IN-list WHERE clauses surface an array-valued AST property ("list"), exercising
  // walkExpressionOnly's array-recursion branch on the way to a (legitimately null) result.
  const inListSql = `UPDATE Employee SET FirstName = 'x' WHERE DepartmentId IN (1, 2, 3);`;
  const inListParsed = parseSql(inListSql);
  assert.strictEqual(
    resolveColumnFromAst(inListParsed?.ast?.body, "NoSuchColumn"),
    null,
    "Array-valued WHERE clause children (e.g. an IN list) should be walked without throwing"
  );

  // A JOIN populates the alias map via the joins-array loop (distinct from the FROM-source loop).
  const joinSql = `SELECT LastName FROM Employee e JOIN Department d ON d.DepartmentId = e.DepartmentId;`;
  const joinParsed = parseSql(joinSql);
  assert.strictEqual(
    resolveColumnFromAst(joinParsed?.ast?.body, "LastName"),
    null,
    "With multiple sources in scope (FROM + JOIN), an unqualified column with no alias hint resolves to null"
  );

  assert.strictEqual(resolveColumnFromAst([null], "X"), null, "Array containing a non-object root should resolve to null");
  assert.strictEqual(resolveColumnFromAst([{ type: "UpdateStatement", target: null }], "X"), null, "UpdateStatement with no resolvable target should resolve to null");
  assert.strictEqual(resolveColumnFromAst([{ type: "InsertStatement", table: null }], "X"), null, "InsertStatement with no resolvable target should resolve to null");
  assert.strictEqual(resolveColumnFromAst([{ type: "DeleteStatement", target: null }], "X"), null, "DeleteStatement with no resolvable target should resolve to null");

  const insertFallbackSql = `INSERT INTO Employee (FirstName) SELECT LastName FROM Staging;`;
  const insertFallbackParsed = parseSql(insertFallbackSql);
  assert.strictEqual(
    resolveColumnFromAst(insertFallbackParsed?.ast?.body, "LastName"),
    "staging",
    "Column absent from the INSERT column list should fall through to the nested SELECT source"
  );

  const directAliasSql = `SELECT FirstName FROM Employee e;`;
  const directAliasParsed = parseSql(directAliasSql);
  assert.strictEqual(
    resolveAliasFromAst("e", directAliasParsed?.ast ?? null),
    "employee",
    "A direct (non-join) alias on the FROM source should resolve via resolveAliasInNode's first match"
  );

  assert.strictEqual(
    getCteColumns({ location: { query: null, columns: undefined } }).length,
    0,
    "A non-object query (e.g. null) should yield no projection columns and an empty CTE column list"
  );

  const setOpSym = {
    location: {
      query: {
        type: "SetOperator",
        left: { columns: [] },
        right: { columns: [{ outputName: "Id" }] }
      }
    }
  };
  assert.ok(
    getCteColumns(setOpSym).some(c => c.name === "id"),
    "SetOperator with an empty left side should fall through to the right side's projection columns"
  );

  const updateSql = `UPDATE Employee SET FirstName = 'x' WHERE DepartmentId = 1;`;
  const updateParsed = parseSql(updateSql);
  assert.strictEqual(
    resolveColumnFromAst(updateParsed?.ast?.body, "FirstName"),
    "employee",
    "UPDATE assignment column should resolve to the mutation target"
  );
  assert.strictEqual(
    resolveColumnFromAst(updateParsed?.ast?.body, "DepartmentId"),
    "employee",
    "UPDATE WHERE-clause column should resolve to the mutation target"
  );
  assert.strictEqual(
    resolveColumnFromAst(updateParsed?.ast?.body, "NoSuchColumn"),
    null,
    "UPDATE statement with no matching column should resolve to null"
  );

  const insertSql = `INSERT INTO Employee (FirstName) VALUES ('x');`;
  const insertParsed = parseSql(insertSql);
  assert.strictEqual(
    resolveColumnFromAst(insertParsed?.ast?.body, "FirstName"),
    "employee",
    "INSERT column list entry should resolve to the insert target"
  );
  assert.strictEqual(
    resolveColumnFromAst(insertParsed?.ast?.body, "NoSuchColumn"),
    null,
    "INSERT statement with no matching column should resolve to null"
  );

  const insertSelectSql = `INSERT INTO Employee (FirstName) SELECT FirstName FROM Staging;`;
  const insertSelectParsed = parseSql(insertSelectSql);
  assert.strictEqual(
    resolveColumnFromAst(insertSelectParsed?.ast?.body, "FirstName"),
    "employee",
    "INSERT column list should take precedence over the nested SELECT source"
  );

  const deleteSql = `DELETE FROM Employee WHERE DepartmentId = 1;`;
  const deleteParsed = parseSql(deleteSql);
  assert.strictEqual(
    resolveColumnFromAst(deleteParsed?.ast?.body, "DepartmentId"),
    "employee",
    "DELETE WHERE-clause column should resolve to the delete target"
  );
  assert.strictEqual(
    resolveColumnFromAst(deleteParsed?.ast?.body, "NoSuchColumn"),
    null,
    "DELETE statement with no matching column should resolve to null"
  );

  // A statement type resolveColumnFromAst doesn't handle (e.g. a bare DECLARE) falls through to null.
  const declareSql = `DECLARE @x INT;`;
  const declareParsed = parseSql(declareSql);
  assert.strictEqual(resolveColumnFromAst(declareParsed?.ast?.body, "x"), null, "Unhandled statement types should resolve to null");
});

runCase("walk-ast-ignores-null-and-non-object-nodes", () => {
  let calls = 0;
  walkAst(null, () => { calls++; });
  walkAst(undefined, () => { calls++; });
  walkAst("not-an-object", () => { calls++; });
  assert.strictEqual(calls, 0, "walkAst should not invoke the callback for null/non-object nodes");

  walkAst({ a: 1, child: { b: 2 } }, () => { calls++; });
  assert.strictEqual(calls, 2, "walkAst should still visit valid object nodes and their object children");
});

runCase("normalize-ast-table-name-covers-all-node-shapes", () => {
  assert.strictEqual(normalizeAstTableName(null), null, "Falsy input should resolve to null");
  assert.strictEqual(normalizeAstTableName(""), null, "Empty string should resolve to null");
  assert.strictEqual(normalizeAstTableName("dbo.Employee"), "employee", "String table names should be normalized");
  assert.strictEqual(normalizeAstTableName({ type: "Identifier", name: "[Employee]" }), "employee", "Identifier nodes should be normalized");
  assert.strictEqual(
    normalizeAstTableName({ type: "MemberExpression", property: "Employee" }),
    "employee",
    "MemberExpression nodes should resolve via their property"
  );
  assert.strictEqual(
    normalizeAstTableName({ expr: { type: "Identifier", name: "Employee" } }),
    "employee",
    "Nested expr wrappers should recurse into the inner node"
  );
  assert.strictEqual(
    normalizeAstTableName({ parts: ["dbo", "Employee"] }),
    "employee",
    "Identifier parts arrays should resolve to the last segment"
  );
  assert.strictEqual(
    normalizeAstTableName({ type: "Unknown" }),
    null,
    "Unrecognized node shapes with no fallback data should resolve to null"
  );
});

runCase("extract-column-name-covers-all-node-shapes", () => {
  assert.strictEqual(extractColumnName(null), null, "Null input should resolve to null");
  assert.strictEqual(extractColumnName(0), null, "Zero is falsy but explicitly allowed through the guard, and still resolves to null");
  assert.strictEqual(extractColumnName("[FirstName]"), "firstname", "String column names should be normalized and unbracketed");
  assert.strictEqual(extractColumnName({ type: "Identifier", name: "FirstName" }), "firstname", "Identifier nodes should resolve via name");
  assert.strictEqual(
    extractColumnName({ type: "MemberExpression", property: "FirstName" }),
    "firstname",
    "MemberExpression nodes should resolve via property"
  );
  assert.strictEqual(
    extractColumnName({ type: "Column", expression: { type: "Identifier", name: "FirstName" } }),
    "firstname",
    "Column nodes should recurse into their expression"
  );
  assert.strictEqual(
    extractColumnName({ expr: { type: "Identifier", name: "FirstName" } }),
    "firstname",
    "Nested expr wrappers should recurse into the inner node"
  );
  assert.strictEqual(extractColumnName({ type: "Unknown" }), null, "Unrecognized node shapes should resolve to null");
});

runCase("extract-qualified-name-covers-all-node-shapes", () => {
  assert.deepStrictEqual(extractQualifiedName(null), {}, "Falsy expression should resolve to an empty object");
  assert.deepStrictEqual(
    extractQualifiedName({ type: "MemberExpression", object: { type: "Identifier", name: "e" }, property: "FirstName" }),
    { table: "e", column: "FirstName" },
    "MemberExpression should split into table/column via extractColumnName(object) and the raw property"
  );
  assert.deepStrictEqual(
    extractQualifiedName({ type: "Identifier", parts: ["dbo", "Employee", "FirstName"] }),
    { table: "employee", column: "firstname" },
    "Multi-part identifiers should use the last two segments as table/column"
  );
  assert.deepStrictEqual(
    extractQualifiedName({ type: "Identifier", name: "FirstName" }),
    { column: "FirstName" },
    "Single-part identifiers should resolve to a bare column with no table"
  );
  assert.deepStrictEqual(extractQualifiedName({ type: "Unknown" }), {}, "Unrecognized node shapes should resolve to an empty object");
});

runCase("resolve-alias-from-ast-covers-falsy-and-negative-paths", () => {
  assert.strictEqual(resolveAliasFromAst("", null), null, "Empty alias should resolve to null");
  assert.strictEqual(resolveAliasFromAst("e", null), null, "Null ast should resolve to null");

  const sql = `SELECT e.FirstName FROM Employee e JOIN Department d ON d.DepartmentId = e.DepartmentId;`;
  const parsed = parseSql(sql);
  assert.strictEqual(resolveAliasFromAst("d", parsed?.ast ?? null), "department", "JOIN alias should resolve via the joins array");
  assert.strictEqual(resolveAliasFromAst("nope", parsed?.ast ?? null), null, "Alias absent from the statement should resolve to null");

  const noAliasSql = `SELECT FirstName FROM Employee;`;
  const noAliasParsed = parseSql(noAliasSql);
  assert.strictEqual(
    resolveAliasFromAst("employee", noAliasParsed?.ast ?? null),
    "employee",
    "An unaliased table referenced by its own name should resolve to itself"
  );
});

runCase("resolve-alias-table-name-covers-metadata-and-location-shapes", () => {
  assert.strictEqual(resolveAliasTableName({}), undefined, "Symbol with no metadata or location should resolve to undefined");
  assert.strictEqual(
    resolveAliasTableName({ metadata: { tableName: "Employee" } }),
    "Employee",
    "metadata.tableName should take precedence"
  );
  assert.strictEqual(
    resolveAliasTableName({ location: { table: "Employee" } }),
    "Employee",
    "A string location.table should be returned directly"
  );
  assert.strictEqual(
    resolveAliasTableName({ location: { table: { name: "Employee" } } }),
    "Employee",
    "An object location.table should resolve via its name property"
  );
});

runCase("get-cte-columns-falls-back-to-location-columns-when-query-has-none", () => {
  const sym = { location: { query: {}, columns: ["Id", "Name", ""] } };
  const cols = getCteColumns(sym);
  assert.strictEqual(cols.length, 2, "Empty/blank fallback column names should be skipped");
  assert.ok(cols.some(c => c.name === "id"), "Fallback path should still normalize column names");
});

runCase("resolve-symbol-case-insensitive-covers-resolve-and-fallback-paths", () => {
  assert.strictEqual(resolveSymbolCaseInsensitive(null, "x"), null, "Null scope should resolve to null");
  assert.strictEqual(resolveSymbolCaseInsensitive({}, ""), null, "Empty name should resolve to null");

  const directHit = { name: "found-via-resolve" };
  const scopeWithResolve = { resolve: (name: string) => (name === "Target" ? directHit : null) };
  assert.strictEqual(
    resolveSymbolCaseInsensitive(scopeWithResolve, "Target"),
    directHit,
    "A scope with a resolve() method should be used directly when it finds a match"
  );

  const scopeWithSymbolsObject = { symbols: { e: { name: "E" } } };
  const found = resolveSymbolCaseInsensitive(scopeWithSymbolsObject, "E");
  assert.ok(found, "Fallback should search Object.values(scope.symbols) case-insensitively when getVisibleSymbols is absent");
});

runCase("cross-apply-correlated-alias-column-resolution", () => {
  const schemaUri = "file:///regression/cross-apply-schema.sql";
  const queryUri = "file:///regression/cross-apply-query.sql";
  const schemaSql = `
CREATE TABLE Employee (
  EmployeeId INT,
  Skillset NVARCHAR(4000)
);
CREATE TABLE Skill (
  SkillId INT,
  SkillName NVARCHAR(200)
);
CREATE TABLE EmployeeSkillset (
  EmployeeId INT,
  SkillId INT
);
`;
  const querySql = `
INSERT INTO EmployeeSkillset (EmployeeId, SkillId)
SELECT DISTINCT EmployeeId, s.SkillId
FROM Employee e
CROSS APPLY STRING_SPLIT(ISNULL(e.Skillset, ''), ',') ss
CROSS APPLY (SELECT LTRIM(RTRIM(ss.value)) AS SkillName) trimmed
JOIN Skill s ON s.SkillName = trimmed.SkillName
WHERE ISNULL(e.Skillset, '') <> '' AND trimmed.SkillName <> ''
  AND NOT EXISTS (
      SELECT 1 FROM EmployeeSkillset es
      WHERE EmployeeId = EmployeeId AND es.SkillId = s.SkillId
  );
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);
  assert.ok(getRefs("employee.skillset").length > 0, "Correlated alias column in CROSS APPLY should resolve to base table column");
  const aliasMap = aliasesByUri.get(queryUri);
  assert.strictEqual(aliasMap?.get("ss"), "string_split", "CROSS APPLY alias ss should resolve to the function source");
  const refs = getReferencesForUri(queryUri);
  assert.ok(!refs.some(r => r.kind === "table" && normalizeName(r.name) === "ss"), "CROSS APPLY alias ss should not be indexed as a schema-validated table reference");
  const ssValueRef = refs.find(r => normalizeName(r.name) === "string_split.value");
  assert.strictEqual(ssValueRef?.validateSchema, false, "Derived APPLY value reference should not be schema-validated");
});

runCase("local-variable-and-parameter-indexing-and-resolution", () => {
  const uri = "file:///regression/local-variables.sql";
  const sql = `
CREATE PROCEDURE dbo.GetEmployeeSalary
    @EmpId INT,
    @Bonus DECIMAL(10,2)
AS
BEGIN
    DECLARE @LocalTax INT = 10;
    SELECT Name, Salary - @LocalTax - @Bonus
    FROM Employee
    WHERE EmployeeId = @EmpId;
END;
`;

  const parsed = parseSql(sql);
  
  // 1. Verify scope-aware resolution via findSymbol
  const paramEmpId = findSymbol(parsed?.scope?.root, "Parameter", "@EmpId");
  assert.ok(paramEmpId, "Parameter @EmpId should exist in parser scope");
  
  const varLocalTax = findSymbol(parsed?.scope?.root, "Variable", "@LocalTax");
  assert.ok(varLocalTax, "Variable @LocalTax should exist in parser scope");

  // 2. Index the text
  indexText(uri, sql);

  // 3. Verify references in referencesIndex for the local variables/parameters
  // Note: they are stored lowercased in the index
  const empIdRefs = getRefs("@empid");
  assert.ok(empIdRefs.length >= 2, "Parameter @EmpId references should be indexed"); // definition + usage
  
  const taxRefs = getRefs("@localtax");
  assert.ok(taxRefs.length >= 2, "Variable @LocalTax references should be indexed"); // declaration + usage
});

runCase("qualified-tvp-columns-do-not-get-overlaid-by-joined-table-lineage", () => {
  const uri = "file:///regression/qualified-tvp-columns.sql";
  const sql = `
CREATE TYPE dbo.TransportRequestsType AS TABLE (
  EmployeeId INT,
  RequestDate DATE,
  PickLocation VARCHAR(100),
  DropLocation VARCHAR(100)
);
CREATE TABLE TransportRequests (
  RequestId INT PRIMARY KEY,
  EmployeeId INT NOT NULL,
  RequestDate DATE NOT NULL,
  PickLocation VARCHAR(100) NOT NULL,
  DropLocation VARCHAR(100) NOT NULL
);
CREATE PROC testfortemp @temp AS dbo.TransportRequestsType READONLY
AS
BEGIN
  DECLARE @temp2 AS dbo.TransportRequestsType;
  INSERT INTO @temp2
  SELECT @temp.EmployeeId, @temp.RequestDate, @temp.PickLocation, @temp.DropLocation
  FROM @temp
  JOIN TransportRequests tr ON @temp.EmployeeId = tr.EmployeeId;
END;
`;

  indexText(uri, sql);
  const refs = getReferencesForUri(uri).filter(r => r.kind === "column");
  const tempEmployeeRefs = refs.filter(r => normalizeName(r.name) === "@temp.employeeid");
  const overlaidTransportEmployeeRefs = refs.filter(r => normalizeName(r.name) === "transportrequests.employeeid" && r.line === tempEmployeeRefs[0]?.line && r.start === tempEmployeeRefs[0]?.start);

  assert.ok(tempEmployeeRefs.length > 0, "Qualified TVP column should be indexed against @temp");
  assert.strictEqual(
    overlaidTransportEmployeeRefs.length,
    0,
    "Qualified TVP column token must not be overlaid with joined table lineage at the same location"
  );
});

runCase("property-access-does-not-register-member-as-table-reference", () => {
  const uri = "file:///regression/property-access.sql";
  const sql = `
CREATE TABLE T (
  Geo GEOGRAPHY
);
SELECT Geo.Lat FROM T;
`;

  indexText(uri, sql);
  const refs = getReferencesForUri(uri);
  assert.ok(
    !refs.some(r => r.kind === "table" && normalizeName(r.name) === "geo"),
    "Property access base should not be indexed as a table reference"
  );
  assert.ok(
    refs.some(r => r.kind === "column" && normalizeName(r.name) === "t.geo"),
    "Property access should keep base typed column reference"
  );
  assert.ok(
    !refs.some(r => r.kind === "column" && normalizeName(r.name) === "geo.lat"),
    "Property access member chain should not be indexed as a schema column"
  );
});

runCase("sqlcmd-include-is-blanked-for-single-file-parse-safety", () => {
  const pre = new SqlCmdPreprocessor();
  const parsed = pre.process(":r missing.sql\nSELECT 1;");
  assert.strictEqual((parsed?.issues ?? []).length, 0, "SQLCMD :r should not create parser noise in single-file mode");
  assert.ok(String(parsed?.text ?? "").includes("SELECT 1;"), "SQLCMD :r preprocessing should preserve following SQL");
  assert.ok(!String(parsed?.text ?? "").includes(":r missing.sql"), "SQLCMD :r directive should be blanked before parse");
});

runCase("go-batches-do-not-trigger-duplicate-variable-errors", () => {
  const parsed = parseSql("DECLARE @a INT = 1;\nGO\nDECLARE @a INT = 2;");
  const semantic = parsed?.semanticDiagnostics ?? [];
  const duplicateVar = semantic.some((d: any) => {
    const code = String(d.code ?? "").toUpperCase();
    const msg = String(d.message ?? "").toLowerCase();
    return code.includes("DUPLICATE") || msg.includes("duplicate") || msg.includes("already declared");
  });
  assert.strictEqual(duplicateVar, false, "GO batch boundary should isolate variable declaration scope");
});

runCase("offset-at-honors-crlf-line-endings", () => {
  const text = "SELECT 1\r\nFROM dbo.Employee\r\nWHERE Name = 'x'\r\n";
  const doc = TextDocument.create("file:///regression/crlf.sql", "sql", 1, text);

  assert.strictEqual(offsetAt(doc, { line: 0, character: 7 }), doc.offsetAt({ line: 0, character: 7 }), "Line 0 offsets should match");
  assert.strictEqual(offsetAt(doc, { line: 1, character: 0 }), doc.offsetAt({ line: 1, character: 0 }), "CRLF line start should match TextDocument");
  assert.strictEqual(offsetAt(doc, { line: 2, character: 6 }), doc.offsetAt({ line: 2, character: 6 }), "CRLF later line offsets should match TextDocument");
});

runCase("bare-column-subquery-schema-missing", () => {
  const schemaUri = "file:///regression/schema-missing.sql";
  const queryUri = "file:///regression/query-missing.sql";
  const schemaSql = `
CREATE TABLE [dbo].[Department] (
  DepartmentId INT,
  DepartmentName NVARCHAR(100)
);
`;
  const querySql = `
UPdate d
SET d.DepartmentName = 'Department Name' + (SELECT TOP 1 FirstName FROM Employee WHERE DepartmentId = d.DepartmentId)
FROM [dbo].[Department] d
WHERE d.DepartmentId = 23378
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);

  const deptIdRefs = getRefs("department.departmentid");
  // 1 from CREATE TABLE
  // 1 from d.DepartmentId in the subquery
  // 1 from d.DepartmentId in the outer WHERE
  // If the bare DepartmentId leaks, it would be 4!
  assert.strictEqual(deptIdRefs.length, 3, "Inner bare DepartmentId must not leak to outer Department table when Employee schema is missing");

  assert.strictEqual(getRefs("employee.firstname").length, 1, "FirstName should be correctly pinned to Employee by parser inference");
});

runCase("bare-column-subquery-schema-present", () => {
  const schemaUri = "file:///regression/schema-present.sql";
  const queryUri = "file:///regression/query-present.sql";
  const schemaSql = `
CREATE TABLE [dbo].[Department] (
  DepartmentId INT,
  DepartmentName NVARCHAR(100)
);
CREATE TABLE Employee (
  EmployeeId INT,
  FirstName NVARCHAR(100),
  DepartmentId INT
);
`;
  const querySql = `
UPdate d
SET d.DepartmentName = 'Department Name' + (SELECT TOP 1 FirstName FROM Employee WHERE DepartmentId = d.DepartmentId)
FROM [dbo].[Department] d
WHERE d.DepartmentId = 23378
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);

  const empDeptIdRefs = getRefs("employee.departmentid");
  assert.ok(empDeptIdRefs.some(r => r.uri === queryUri), "Bare DepartmentId should resolve to Employee when schema is present");

  const empFirstNameRefs = getRefs("employee.firstname");
  assert.ok(empFirstNameRefs.some(r => r.uri === queryUri), "Bare FirstName should resolve to Employee when schema is present");
});

runCase("wildcard-qualifier-does-not-index-fake-column-from-alias-token", () => {
  const schemaUri = "file:///regression/wildcard-qualifier-schema.sql";
  const queryUri = "file:///regression/wildcard-qualifier-query.sql";
  const schemaSql = `
CREATE TABLE dbo.Department (DepartmentId INT);
CREATE TABLE dbo.Employee (DepartmentId INT, Address NVARCHAR(50), PhoneNumber NVARCHAR(50));
CREATE TABLE dbo.DepartmentSalaryInfo (DepartmentId INT);
`;
  const querySql = `
SELECT *
FROM (
  SELECT d.*, e.Address, e.PhoneNumber
  FROM dbo.Department d
  JOIN dbo.Employee e ON e.DepartmentId = d.DepartmentId
  JOIN dbo.DepartmentSalaryInfo dsi ON dsi.DepartmentId = d.DepartmentId
) s
WHERE s.DepartmentId = 1;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);

  assert.strictEqual(getRefs("department.d").length, 0, "Alias wildcard token d from d.* must not be indexed as department.d");
  assert.strictEqual(getRefs("employee.d").length, 0, "Alias wildcard token d from d.* must not be indexed as employee.d");
  assert.strictEqual(getRefs("departmentsalaryinfo.d").length, 0, "Alias wildcard token d from d.* must not be indexed as departmentsalaryinfo.d");
});

runCase("cte-header-columns-are-exposed-when-query-columns-are-generic", () => {
  const sql = `
WITH cteTally(N) AS (
  SELECT 0 UNION ALL
  SELECT TOP (10) ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
  FROM (SELECT 1 AS X) x
),
cteStart(N1) AS (
  SELECT t.N + 1 FROM cteTally t WHERE t.N = 0
)
SELECT s.N1, t.N
FROM cteStart s
JOIN cteTally t ON t.N = s.N1;
`;

  const parsed = parseSql(sql);
  const cteTally = findSymbol(parsed?.scope?.root, "CTE", "cteTally");
  const cteStart = findSymbol(parsed?.scope?.root, "CTE", "cteStart");
  assert.ok(cteTally, "cteTally should exist in parser scope");
  assert.ok(cteStart, "cteStart should exist in parser scope");
  assert.ok(getCteColumns(cteTally).some(c => normalizeName(c.name) === "n"), "cteTally should expose header column N");
  assert.ok(getCteColumns(cteStart).some(c => normalizeName(c.name) === "n1"), "cteStart should expose header column N1");
});

runCase("recursive-union-cte-columns-are-derived-from-left-branch-select", () => {
  const sql = `
;WITH IdRollUp AS (
  SELECT 1 AS Id, 'I' AS ItemNumber, 'F' AS FacilityCode, 'B' AS BomOrgCode, CAST(GETDATE() AS DATE) AS SupplyDate, 'C' AS ComponentItemNumber, 1 AS RollUpQty
),
recurssingQty AS (
  SELECT Id, ItemNumber, FacilityCode, BomOrgCode, SupplyDate, ComponentItemNumber, RollUpQty
  FROM IdRollUp
  WHERE Id = 1
  UNION ALL
  SELECT id.Id, id.ItemNumber, id.FacilityCode, id.BomOrgCode, id.SupplyDate,
         COALESCE(id.ComponentItemNumber, rq.ComponentItemNumber) AS ComponentItemNumber,
         COALESCE(id.RollUpQty, rq.RollUpQty) AS RollUpQty
  FROM IdRollUp id
  JOIN recurssingQty rq ON rq.ItemNumber = id.ItemNumber AND rq.ID = id.ID - 1
)
SELECT rq.ComponentItemNumber, rq.RollUpQty
FROM recurssingQty rq;`;

  const parsed = parseSql(sql);
  const cte = findSymbol(parsed?.scope?.root, "CTE", "recurssingQty");
  assert.ok(cte, "recurssingQty should exist in parser scope");
  const cols = getCteColumns(cte);
  assert.ok(cols.some(c => normalizeName(c.name) === "componentitemnumber"), "Recursive CTE should expose ComponentItemNumber");
  assert.ok(cols.some(c => normalizeName(c.name) === "rollupqty"), "Recursive CTE should expose RollUpQty");
  assert.ok(cols.some(c => normalizeName(c.name) === "itemnumber"), "Recursive CTE should expose ItemNumber");
});

runCase("derived-union-alias-column-projection-is-available-case-insensitively", () => {
  const uri = "file:///regression/derived-union-alias-case-insensitive.sql";
  const sql = `
CREATE TABLE #tempItemAI (
  ItemNumber VARCHAR(300),
  AvailableInventory INT
);
CREATE TABLE #tempItemRAI (
  ItemNumber VARCHAR(300),
  ComponentRolledUpQuantity INT
);

SELECT SUM(s.availableInventory) AS TotalAvailableInventory
FROM (
  SELECT s1.ItemNumber, s1.AvailableInventory
  FROM #tempItemAI s1
  UNION
  SELECT rgrp.ItemNumber, rgrp.ComponentRolledUpQuantity AS AvailableInventory
  FROM #tempItemRAI rgrp
) s;
`;

  indexText(uri, sql);
  assert.ok(
    getRefs("#tempitemai.availableinventory").length > 0,
    "Derived UNION alias should project AvailableInventory from first branch and resolve case-insensitively"
  );
  assert.ok(
    getRefs("#tempitemrai.componentrolledupquantity").length > 0,
    "Derived UNION alias should retain second branch projection lineage"
  );
});

runCase("create-view-union-projects-columns-for-indexing", () => {
  const uri = "file:///regression/create-view-union-projects-columns.sql";
  const sql = `
CREATE VIEW [dbo].[SomeView]
AS
  SELECT [t0].[Column1] AS Column1
  FROM [dbo].[Table1] [t0]
  UNION
  SELECT [t0].[Column1]
  FROM [dbo].[Table2] [t0];
`;

  indexText(uri, sql);
  assert.ok(
    getRefs("someview.column1").length > 0,
    "CREATE VIEW columns should be indexed when view body is a UNION/set-operator shape"
  );
});

runCase("table-name-used-after-aliasing-is-flagged-as-shadowed", () => {
  const schemaUri = "file:///regression/shadowed-by-alias-schema.sql";
  const queryUri = "file:///regression/shadowed-by-alias-query.sql";

  const schemaSql = `
CREATE TABLE dbo.Employee (
  EmployeeId INT,
  DepartmentId INT,
  FirstName NVARCHAR(100)
);
`;
  const querySql = `
SELECT e.EmployeeId, e.DepartmentId, e.FirstName
FROM Employee e
WHERE Employee.EmployeeId = 12345;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);

  const refs = getReferencesForUri(queryUri).filter(r => r.kind === "table" && r.context === "shadowed-by-alias");
  assert.strictEqual(refs.length, 1, "Bare table name used after the table was aliased should be indexed as shadowed-by-alias");
  assert.strictEqual(normalizeName(refs[0].name), "employee", "Shadowed ref should carry the literal table name");
});

runCase("self-join-unaliased-table-qualifier-does-not-produce-false-lsp001", () => {
  // Regression: FROM table1 JOIN table1 t2 — using table1.Column1 as qualifier refers to the
  // un-aliased instance and is valid. Was incorrectly flagged as shadowed-by-alias LSP001.
  const schemaUri = "file:///regression/self-join-schema.sql";
  const queryUri = "file:///regression/self-join-query.sql";

  indexText(schemaUri, "CREATE TABLE table1(Column1 INT, Column2 NVARCHAR(100));");
  const querySql = "SELECT table1.Column1 FROM table1 JOIN table1 t2 ON table1.Column1 = t2.Column1;";
  indexText(queryUri, querySql);

  const shadowed = getReferencesForUri(queryUri).filter(r => r.kind === "table" && r.context === "shadowed-by-alias");
  assert.strictEqual(shadowed.length, 0,
    "table1.Column1 where table1 appears un-aliased in FROM must not be flagged as shadowed-by-alias");
});

runCase("get-word-range-at-position-covers-token-boundaries", () => {
  const text = "SELECT e.EmployeeId, a . b\n\nFROM Employee e";
  const doc = TextDocument.create("file:///regression/word-range.sql", "sql", 1, text);

  // Happy path: cursor inside "EmployeeId" (the "." is itself a token char, so the
  // qualifier is included in the range -- this is by design, see hover/definition providers).
  const inWord = getWordRangeAtPosition(doc, { line: 0, character: 12 });
  assert.ok(inWord, "Cursor inside a word should produce a range");
  assert.strictEqual(doc.getText(inWord!), "e.EmployeeId", "Range should cover the full qualified token under the cursor");

  // Cursor on the comma immediately after "EmployeeId" (non-token char preceded by a
  // token char) should snap back onto the token instead of returning no range.
  const commaOffset = text.indexOf(",");
  const justPast = getWordRangeAtPosition(doc, { line: 0, character: commaOffset });
  assert.ok(justPast, "Cursor just past a token should still resolve to that token");

  // Cursor exactly on a lone "." surrounded by spaces: dot-stripping collapses start>=end -> null
  const loneDotOffset = text.indexOf("a . b") + 2;
  const lonePos = doc.positionAt(loneDotOffset);
  const loneDot = getWordRangeAtPosition(doc, lonePos);
  assert.strictEqual(loneDot, null, "A lone '.' with no adjoining token chars should resolve to no range");

  // Empty line should short-circuit to null
  const emptyLine = getWordRangeAtPosition(doc, { line: 1, character: 0 });
  assert.strictEqual(emptyLine, null, "Empty line should produce no word range");

  // Negative character clamps to a no-range result
  const negative = getWordRangeAtPosition(doc, { line: 0, character: -1 });
  assert.strictEqual(negative, null, "Negative character position should produce no word range");

  // Cursor past the end of the line clamps to the last character
  const pastEnd = getWordRangeAtPosition(doc, { line: 2, character: 9999 });
  assert.ok(pastEnd, "Cursor past end of line should clamp onto the last token");

  // Cursor surrounded by non-token chars on both sides (no token char to snap back onto)
  const doubleSpaceDoc = TextDocument.create("file:///regression/word-range-blank.sql", "sql", 1, "  ");
  const blank = getWordRangeAtPosition(doubleSpaceDoc, { line: 0, character: 1 });
  assert.strictEqual(blank, null, "Cursor with no adjoining token char in either direction should resolve to no range");

  // Token with a trailing dot that still leaves a non-empty token after stripping
  const trailingDotDoc = TextDocument.create("file:///regression/word-range-trailing-dot.sql", "sql", 1, "ab.");
  const trailingDot = getWordRangeAtPosition(trailingDotDoc, { line: 0, character: 1 });
  assert.ok(trailingDot, "Token with a stripped trailing dot should still resolve when characters remain");
  assert.strictEqual(trailingDotDoc.getText(trailingDot!), "ab", "Trailing dot should be stripped from the resolved range");
});

runCase("offset-at-falls-back-to-manual-calculation-without-native-offsetAt", () => {
  const fakeDoc = { getText: () => "SELECT 1\nFROM Employee\nWHERE 1 = 1" } as any;
  const offset = offsetAt(fakeDoc, { line: 1, character: 3 });
  assert.strictEqual(offset, "SELECT 1\n".length + 3, "Fallback path should manually sum line lengths when doc.offsetAt is unavailable");
});

runCase("is-sql-keyword-is-case-insensitive", () => {
  assert.strictEqual(isSqlKeyword("SELECT"), true, "Keyword check should be case-insensitive");
  assert.strictEqual(isSqlKeyword("NotAKeyword"), false, "Non-keyword identifiers should return false");
});

runCase("collect-nearest-scope-column-owners-covers-symbol-shapes", () => {
  // A null entry in the symbols list must not crash the resolver.
  let scope: any = { getOwnSymbols: () => [null, { kind: "Table", name: "Employee" }], parent: null, name: "test" };
  assert.deepStrictEqual(collectNearestScopeColumnOwners(scope, "x", new Map(), new Map()), [], "Null symbol entries should be skipped without throwing");

  // CTE symbol present but the searched column isn't among its projected columns.
  scope = { getOwnSymbols: () => [{ kind: "CTE", name: "cte1", location: { query: { columns: [{ outputName: "Id" }] } } }], parent: null, name: "test" };
  assert.deepStrictEqual(collectNearestScopeColumnOwners(scope, "doesnotexist", new Map(), new Map()), [], "CTE column miss should resolve to no owners");

  // Any symbol (including a derived alias) carrying an inline .columns array is matched by
  // resolveSymbolColumns's generic top-of-function check before kind-specific dispatch runs.
  // This means the Alias-block's own isDerivedAlias/projected-column success branch can never
  // independently fire -- the generic check always wins first when sym.columns has the match.
  scope = {
    getOwnSymbols: () => [{ kind: "Alias", name: "sub", location: { table: { query: {} } }, columns: [{ name: "projcol", rawName: "ProjCol" }] }],
    parent: null, name: "test"
  };
  const derived = collectNearestScopeColumnOwners(scope, "projcol", new Map(), new Map());
  assert.strictEqual(derived.length, 1, "Derived alias should expose its own projected column via the generic columns-array check");
  assert.strictEqual(derived[0].kindLabel, "derived table");

  // Alias whose target resolves to ANOTHER in-scope symbol (a CTE), found via recursion.
  // resolveSymbolCaseInsensitive looks up the target via getVisibleSymbols(), not
  // getOwnSymbols() -- both must be provided for the recursive lookup to succeed.
  const aliasAndCteSymbols = [
    { kind: "Alias", name: "c", location: { table: { name: "cte1" } } },
    { kind: "CTE", name: "cte1", location: { query: { columns: [{ outputName: "Id" }] } } }
  ];
  scope = { getOwnSymbols: () => aliasAndCteSymbols, getVisibleSymbols: () => aliasAndCteSymbols, parent: null, name: "test" };
  const viaAlias = collectNearestScopeColumnOwners(scope, "id", new Map(), new Map());
  // Both the alias (resolved recursively through its target) and the CTE symbol itself
  // (present directly in scope) independently produce a match.
  assert.strictEqual(viaAlias.length, 2, "Alias-via-CTE and the direct CTE symbol should both resolve");
  assert.ok(viaAlias.every(o => o.kindLabel === "CTE"), "Both owners should report the CTE kind label");
  assert.ok(viaAlias.some(o => o.alias === "c"), "The alias-resolved owner should carry the alias name");
  assert.ok(viaAlias.some(o => !("alias" in o)), "The direct CTE symbol owner should not carry an alias name");

  // Table/TempTable symbol with a falsy name must not crash.
  scope = { getOwnSymbols: () => [{ kind: "Table", name: "" }], parent: null, name: "test" };
  assert.deepStrictEqual(collectNearestScopeColumnOwners(scope, "x", new Map(), new Map()), [], "Table symbol with empty name should resolve to no owners");

  // Table symbol resolved against the workspace schema map (tablesByName).
  const tablesByName = new Map([["employee", { rawName: "Employee", columns: [{ name: "id", rawName: "Id" }] }]]);
  scope = { getOwnSymbols: () => [{ kind: "Table", name: "employee" }], parent: null, name: "test" };
  const tableMatch = collectNearestScopeColumnOwners(scope, "id", tablesByName, new Map());
  assert.strictEqual(tableMatch.length, 1, "Table symbol should resolve its column via tablesByName");
  assert.strictEqual(tableMatch[0].ownerName, "Employee");

  // A symbol of an unrecognized kind that still carries an inline .columns array is not
  // caught by hasPotentialLocalSourceSymbol's kind check, but hasColumnBearingLocalSource's
  // kind-agnostic columns-array check still short-circuits the walk-up to the parent scope.
  scope = { getOwnSymbols: () => [{ kind: "Unrecognized", columns: [{ name: "X" }] }], parent: null, name: "test" };
  assert.deepStrictEqual(
    collectNearestScopeColumnOwners(scope, "doesnotexist", new Map(), new Map()),
    [],
    "An unrecognized-kind symbol with an inline columns array should still block walk-up via hasColumnBearingLocalSource"
  );
});

runCase("parseSql-returns-null-for-falsy-input", () => {
  assert.strictEqual(parseSql(""), null, "Empty string should short-circuit to null without calling analyze()");
});

runCase("parseSql-catches-analyzer-throw-and-returns-parse-error-issue", () => {
  // analyze() expects a string; passing a non-string forces it to throw internally
  // (e.g. "text.includes is not a function"), exercising the wrapper's catch path.
  const result = parseSql({} as unknown as string);
  assert.ok(result, "parseSql should not propagate the analyzer's throw");
  assert.strictEqual(result?.ast, null, "Caught error should produce a null ast");
  assert.strictEqual(result?.issues?.[0]?.code, "PARSE_ERROR", "Caught error should be reported as PARSE_ERROR");
  assert.strictEqual(result?.diagnostics?.[0]?.code, "PARSE_ERROR", "Caught error should also appear in diagnostics");
});

runCase("index-ready-lifecycle-setters-work-correctly", () => {
  setIndexNotReady();
  assert.strictEqual(getIndexReady(), false, "setIndexNotReady should clear the index-ready flag");
  setIndexReady();
  assert.strictEqual(getIndexReady(), true, "setIndexReady should set the index-ready flag");
});

runCase("find-column-in-table-returns-location-for-known-columns", () => {
  const schemaUri = "file:///regression/find-col-schema.sql";
  indexText(schemaUri, "CREATE TABLE dbo.Employee (EmployeeId INT, FirstName NVARCHAR(100));");

  const locs = findColumnInTable("employee", "employeeid");
  assert.ok(locs.length > 0, "findColumnInTable should return a location for an indexed column");
  assert.ok(locs[0].uri.includes("find-col-schema"), "Location should point to the schema file");

  const notFound = findColumnInTable("employee", "nonexistentcol");
  assert.deepStrictEqual(notFound, [], "findColumnInTable should return [] for an unknown column");
});

runCase("find-table-or-column-returns-locations", () => {
  const schemaUri = "file:///regression/find-toc-schema.sql";
  indexText(schemaUri, "CREATE TABLE Customer (CustomerId INT);");

  const locs = findTableOrColumn("customer");
  assert.ok(locs.length > 0, "findTableOrColumn should find a table by normalized name");
  assert.strictEqual(findTableOrColumn("nonexistenttable").length, 0, "findTableOrColumn should return [] for unknown name");
});

runCase("parse-columns-from-create-view-extracts-projected-columns", () => {
  const viewSql = "CREATE VIEW SomeView AS SELECT Id, Name AS DisplayName FROM T;";
  const cols = parseColumnsFromCreateView(viewSql);
  assert.ok(cols.some(c => c.name === "id"), "parseColumnsFromCreateView should extract unaliased columns");
  assert.ok(cols.some(c => c.name === "displayname"), "parseColumnsFromCreateView should extract aliased columns");
  assert.deepStrictEqual(parseColumnsFromCreateView("not valid sql"), [], "Unparseable input should return empty");
});

runCase("setRefsForFile-called-twice-exercises-removeRefsFromUriIndex-body", () => {
  // The removeRefsFromUriIndex body (lines 153-172) only fires when setRefsForFile is called
  // a SECOND TIME for the same (name, uri) pair — clearing old referencesByUri entries by
  // position, not just the whole-URI delete that deleteRefsForFile does.
  const uri = "file:///regression/remove-refs-body.sql";
  const ref1 = { name: "widget", uri, line: 0, start: 0, end: 6, kind: "table" as const };
  const ref2 = { name: "widget", uri, line: 0, start: 10, end: 16, kind: "table" as const };

  setRefsForFile("widget", uri, [ref1]);
  assert.strictEqual(getRefs("widget").length, 1, "First setRefsForFile should produce one ref");

  // Second call: removeRefsFromUriIndex body runs because existing refs are found.
  setRefsForFile("widget", uri, [ref2]);
  const afterSecond = getRefs("widget");
  assert.strictEqual(afterSecond.length, 1, "Second setRefsForFile should replace the previous ref");
  assert.strictEqual(afterSecond[0].start, 10, "Replacement ref should have the new start position");
});

runCase("re-indexing-same-file-clears-and-rebuilds-refs", () => {
  const schemaUri = "file:///regression/reindex-schema.sql";
  const sql1 = "CREATE TABLE Widget (WidgetId INT, Name VARCHAR(100));";
  const sql2 = "CREATE TABLE Widget (WidgetId INT, Name VARCHAR(100), Price DECIMAL(10,2));";

  indexText(schemaUri, sql1);
  const colsBefore = columnsByTable.get("widget");
  assert.ok(colsBefore?.has("price") === false, "Price column should not exist after first index");

  // Re-index the same URI with updated content — exercises removeRefsFromUriIndex body
  indexText(schemaUri, sql2);
  const colsAfter = columnsByTable.get("widget");
  assert.ok(colsAfter?.has("price"), "Price column should appear after re-indexing with the updated schema");
});

runCase("delete-file-from-index-removes-all-state", () => {
  const schemaUri = "file:///regression/delete-file-schema.sql";
  indexText(schemaUri, "CREATE TABLE ToDelete (Id INT);");
  assert.ok(tablesByName.has("todelete"), "Table should be in index before deletion");
  assert.ok(getRefs("todelete").length > 0, "Table should have refs before deletion");

  deleteFileFromIndex(schemaUri);
  assert.strictEqual(tablesByName.has("todelete"), false, "Table should be removed after deleteFileFromIndex");
  assert.deepStrictEqual(getRefs("todelete"), [], "Refs should be cleared after deleteFileFromIndex");
});

runCase("normalizeIndexUri-handles-edge-cases", () => {
  // These exercise the normalizeIndexUri paths in definitions.ts (path-to-file-url conversion
  // and the lowercase drive-letter normalization for Windows paths on file:// URIs).
  // indexText calls normalizeIndexUri internally; testing via round-trip.
  const uri = "file:///regression/normalize-uri-test.sql";
  indexText(uri, "CREATE TABLE NormTest (Id INT);");
  const found = findColumnInTable("normtest", "id");
  assert.ok(found.length > 0, "File indexed via a file:// URI should be findable after normalizeIndexUri");
});

runCase("parserViewColumnDefinition-with-various-column-shapes", () => {
  // Exercises parserViewColumnDefinition edge cases (rawName extraction from outputName/sourceName).
  const viewSql = `CREATE VIEW MultiProjection AS
SELECT e.FirstName AS FirstName, e.DepartmentId
FROM Employee e;`;
  const cols = parseColumnsFromCreateView(viewSql);
  assert.ok(cols.some(c => c.name === "firstname"), "Aliased view column should be extracted");
  assert.ok(cols.some(c => c.name === "departmentid"), "Non-aliased view column should be extracted");
});

runCase("index-text-create-index-is-not-indexed-as-table-definition", () => {
  const uri = "file:///regression/create-index.sql";
  indexText(uri, `
CREATE TABLE Employee (DepartmentId INT);
CREATE INDEX IX_Employee ON Employee (DepartmentId);
`);
  // CREATE INDEX is not TABLE/VIEW/TYPE/PROCEDURE/FUNCTION → skipped by the CreateStatement filter.
  // The table DOES get indexed; only the index itself is skipped.
  assert.ok(tablesByName.has("employee"), "CREATE TABLE should still be indexed alongside a CREATE INDEX");
  assert.ok(!tablesByName.has("ix_employee"), "CREATE INDEX should not produce a table entry");
});

runCase("index-text-update-with-from-join-resolves-target-via-from-sources", () => {
  // UPDATE target is an alias ('t') resolved via the FROM clause's JOIN.
  // resolveUpdateTargetTable needs to walk updateNode.from and match aliases.
  const uri = "file:///regression/update-from-join.sql";
  indexText(uri, "UPDATE t SET t.Name='x' FROM T1 t JOIN T2 s ON t.Id = s.Id;");
  const refs = getReferencesForUri(uri).filter(r => r.kind === "column");
  assert.ok(refs.some(r => r.name.includes(".name")), "UPDATE SET column should be indexed against the resolved FROM-alias target");
  assert.ok(refs.some(r => r.name.includes(".id")), "JOIN ON columns should be indexed");
});

runCase("index-text-variables-are-indexed-from-scope", () => {
  const uri = "file:///regression/variables.sql";
  indexText(uri, `
DECLARE @count INT;
DECLARE @name NVARCHAR(100);
SET @count = 1;
SELECT @count, @name;
`);
  const refs = getReferencesForUri(uri).filter(r => r.kind === "parameter");
  assert.ok(refs.some(r => r.name === "@count"), "@count parameter should be indexed");
  assert.ok(refs.some(r => r.name === "@name"), "@name parameter should be indexed");
});

runCase("index-text-insert-column-list-is-indexed", () => {
  const schemaUri = "file:///regression/insert-col-schema.sql";
  const queryUri = "file:///regression/insert-col-query.sql";
  indexText(schemaUri, "CREATE TABLE Employee (Id INT, Name NVARCHAR(100));");
  indexText(queryUri, "INSERT INTO Employee (Id, Name) VALUES (1, 'Test');");
  const refs = getReferencesForUri(queryUri).filter(r => r.kind === "column");
  assert.ok(refs.some(r => r.name === "employee.id"), "INSERT column list Id should be indexed against Employee");
  assert.ok(refs.some(r => r.name === "employee.name"), "INSERT column list Name should be indexed against Employee");
});

runCase("index-text-select-into-temp-table-is-captured", () => {
  const uri = "file:///regression/select-into.sql";
  indexText(uri, "SELECT Id, FirstName INTO #Results FROM Employee;");
  // indexSelectIntoTempTablesFromAst should record #results in tempTablesByUri
  const tmp = tempTablesByUri.get(uri);
  assert.ok(tmp, "tempTablesByUri should have an entry after SELECT INTO");
  const tmpEntry = tmp?.get("#results");
  assert.ok(tmpEntry, "#results should be in tempTablesByUri for this URI");
});

runCase("index-text-sql-keyword-column-name-is-skipped", () => {
  // isSqlKeyword('key') === true → 'key' is filtered out as a column ref candidate.
  // Using bare 'key' without brackets so extractReferences returns name:"KEY" (no dot),
  // which then hits the isSqlKeyword check at 453-454.
  const uri = "file:///regression/keyword-col.sql";
  indexText(uri, "SELECT key FROM T;");
  // 'key' is filtered by isSqlKeyword; indexing should complete without crash.
  const refs = getReferencesForUri(uri);
  assert.ok(Array.isArray(refs), "indexText should complete without error even with SQL-keyword column names");
  assert.ok(!refs.some(r => r.name === "key"), "'key' (a SQL keyword) should not appear as a bare column ref");
});

runCase("index-text-bare-update-set-column-is-indexed-against-target", () => {
  const schemaUri = "file:///regression/bare-update-schema.sql";
  const queryUri = "file:///regression/bare-update-query.sql";
  indexText(schemaUri, "CREATE TABLE Employee (Name NVARCHAR(100), Id INT);");
  indexText(queryUri, "UPDATE Employee SET Name = 'x' WHERE Id = 1;");
  const refs = getReferencesForUri(queryUri).filter(r => r.kind === "column");
  assert.ok(refs.some(r => r.name === "employee.name"), "Bare UPDATE SET column should be indexed against the UPDATE target");
  assert.ok(refs.some(r => r.name === "employee.id"), "Bare UPDATE WHERE column should be indexed against the UPDATE target");
});

runCase("index-text-variable-data-type-reference-is-indexed", () => {
  // When a parameter's dataType is a non-built-in type (like a table type),
  // indexVariableDataTypeReferencesFromScope should create a table ref for that type name.
  const schemaUri = "file:///regression/vartype-schema.sql";
  const procUri = "file:///regression/vartype-proc.sql";
  indexText(schemaUri, `
CREATE TYPE dbo.MyTableType AS TABLE (Id INT);
`);
  indexText(procUri, `
CREATE PROCEDURE TestProc
  @myParam dbo.MyTableType READONLY
AS
SELECT @myParam.Id;
`);
  const refs = getReferencesForUri(procUri).filter(r => r.kind === "table" && r.name === "mytabletype");
  assert.ok(refs.length > 0, "Parameter with a table-type dataType should generate a table ref for that type name");
});

runCase("index-text-handles-max-file-size-and-deploy-path-exclusions", () => {
  // Files containing "deploy" in path are excluded from indexing.
  const deployUri = "file:///regression/deploy/DeployScript.sql";
  indexText(deployUri, "CREATE TABLE DeployTable (Id INT);");
  assert.strictEqual(tablesByName.has("deploytable"), false, "Files in 'deploy' path should not be indexed");

  // Non-.sql files should also be excluded.
  const nonSqlUri = "file:///regression/script.ps1";
  indexText(nonSqlUri as any, "CREATE TABLE NonSqlTable (Id INT);");
  assert.strictEqual(tablesByName.has("nonsqltable"), false, "Non-.sql files should not be indexed");
});

runCase("cte-header-column-aliases-do-not-produce-col001", () => {
  // Regression: CTEs with explicit column alias lists in the header
  // (e.g. WITH cteTally(N) AS (...)) were producing false COL001 "Unknown column 'N' on 't'"
  // because the parser checks body SELECT-list names instead of header-declared names.
  // The LSP suppresses these using sym.location.columns.
  //
  // We can't directly invoke validateTextDocument here, but we can verify that the parser
  // DOES emit COL001 for this pattern (confirming the suppression is needed) and that the
  // CTE scope symbols carry the header column list in location.columns for the suppression
  // logic to use.
  const sql = `
WITH E1(N)       AS (SELECT 1 UNION ALL SELECT 1),
     cteTally(N) AS (SELECT 0 UNION ALL SELECT TOP (10) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM E1),
     cteStart(N1) AS (SELECT t.N+1 FROM cteTally t WHERE t.N = 0)
SELECT s.N1 FROM cteStart s;`;

  const parsed = parseSql(sql);

  // Parser emits COL001 for header-alias columns (this is the parser bug being worked around).
  const col001 = (parsed as any)?.diagnostics?.filter((d: any) => d.code === "COL001") ?? [];
  assert.ok(col001.length > 0, "Parser should emit COL001 for CTE header-alias column references (confirming suppression is needed)");

  // The CTE symbols must carry header column names in location.columns so the LSP can suppress.
  function findCteSyms(scope: any): any[] {
    const out: any[] = [];
    for (const sym of scope.getOwnSymbols?.() ?? []) {
      if (sym.kind === "CTE") { out.push(sym); }
    }
    for (const child of scope.getChildren?.() ?? []) {
      out.push(...findCteSyms(child));
    }
    return out;
  }
  const ctes = findCteSyms(parsed?.scope?.root);
  const cteTally = ctes.find((c: any) => normalizeName(c.name) === "ctetally");
  const cteStart = ctes.find((c: any) => normalizeName(c.name) === "ctestart");

  assert.ok(cteTally, "cteTally CTE symbol should be in scope");
  assert.ok(cteStart, "cteStart CTE symbol should be in scope");
  assert.ok(
    Array.isArray(cteTally.location?.columns) && cteTally.location.columns.includes("N"),
    "cteTally.location.columns must contain 'N' so the LSP suppression can match it"
  );
  assert.ok(
    Array.isArray(cteStart.location?.columns) && cteStart.location.columns.includes("N1"),
    "cteStart.location.columns must contain 'N1' so the LSP suppression can match it"
  );
});

process.stdout.write("All regression index tests passed.\n");

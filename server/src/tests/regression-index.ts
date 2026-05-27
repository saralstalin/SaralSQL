import * as assert from "assert";
import { TextDocument } from "vscode-languageserver-textdocument";
import { SqlCmdPreprocessor } from "@saralsql/tsql-parser";
import { parseSql } from "../sql-parser";
import { getCteColumns, getDisplaySymbolName, resolveAliasFromAst, resolveColumnFromAst } from "../ast-utils";
import { getLineStarts, normalizeName, offsetAt } from "../text-utils";
import { collectAmbiguousColumnDiagnostics } from "../diagnostic-helpers";
import {
  indexText,
  getRefs,
  getReferencesForUri,
  aliasesByUri,
  definitions,
  referencesIndex,
  columnsByTable,
  tablesByName,
  tableTypesByName,
  tempTablesByUri
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
  const resolved = resolveColumnFromAst(parsed?.ast, "DepartmentId");
  assert.strictEqual(resolved, null, "Nested SELECT columns should not leak into outer statement resolution");
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
});

runCase("sqlcmd-unresolved-include-is-surfaced", () => {
  const pre = new SqlCmdPreprocessor();
  const parsed = pre.process(":r missing.sql\nSELECT 1;");
  const hasUnresolvedInclude = (parsed?.issues ?? []).some((i: any) => String(i.code ?? "").toUpperCase() === "SQLCMD_UNRESOLVED_INCLUDE");
  assert.ok(hasUnresolvedInclude, "Unresolved SQLCMD include should be surfaced as parser issue");
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

process.stdout.write("All regression index tests passed.\n");

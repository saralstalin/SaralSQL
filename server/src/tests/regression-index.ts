import * as assert from "assert";
import { parseSql } from "../sql-parser";
import { resolveColumnFromAst } from "../ast-utils";
import { getLineStarts } from "../text-utils";
import { collectAmbiguousColumnDiagnostics } from "../diagnostic-helpers";
import {
  indexText,
  getRefs,
  aliasesByUri,
  definitions,
  referencesIndex,
  columnsByTable,
  tablesByName,
  tableTypesByName
} from "../definitions";

function resetIndexState(): void {
  aliasesByUri.clear();
  definitions.clear();
  referencesIndex.clear();
  columnsByTable.clear();
  tablesByName.clear();
  tableTypesByName.clear();
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

runCase("derived-table-alias-column-resolution", () => {
  const uri = "file:///regression/derived-table-alias-columns.sql";
  const sql = `
SELECT d.EmployeeId
FROM (SELECT EmployeeId FROM Employee) d
WHERE d.EmployeeId > 0;
`;

  indexText(uri, sql);
  assert.ok(getRefs("d.employeeid").length > 0, "Derived table alias projected columns should be indexed as alias-qualified columns");
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

process.stdout.write("All regression index tests passed.\n");

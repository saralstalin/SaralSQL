// server/src/tests/index-text.ts
import { indexText } from "../definitions";     // <-- correct relative path
import { definitions, columnsByTable, tablesByName } from "../definitions";

const sql = `CREATE VIEW DepartmentSalaryInfo
AS
SELECT
  d.DepartmentId,
  d.DepartmentName,
  d.HeadEmployeeId,
  h.FirstName AS HeadFirstName,
  h.LastName AS HeadLastName,
  SUM(emp.Salary) AS TotalSalary
FROM [dbo].[Department] d
INNER JOIN Employee emp ON d.DepartmentId = emp.DepartmentId
LEFT JOIN Employee h ON d.HeadEmployeeId = EmployeeId
GROUP BY
  d.DepartmentId,
  d.DepartmentName,
  d.HeadEmployeeId,
  h.FirstName,
  h.LastName;`;

indexText("file:///tmp/DepartmentSalaryInfo.sql", sql);

console.log("=== definitions ===");
console.dir(definitions.get("file:///tmp/DepartmentSalaryInfo.sql"), { depth: 10 });

console.log("=== columnsByTable ===");
console.dir(Array.from(columnsByTable.entries()), { depth: 10 });

console.log("=== tablesByName ===");
console.dir(Array.from(tablesByName.entries()), { depth: 10 });

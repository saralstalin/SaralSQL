import {parseColumnsFromCreateView} from "../definitions";

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

console.log(parseColumnsFromCreateView(sql, 0));

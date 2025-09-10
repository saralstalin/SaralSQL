import { TextDocument } from "vscode-languageserver-textdocument";
import { indexText, definitions, findColumnInTable } from "../definitions";
import { Parser } from "node-sql-parser";

function cleanForAst(sql: string): string {
  return sql
    // Strip CREATE/ALTER PROC headers + params up to AS BEGIN
    .replace(/CREATE\s+(OR\s+ALTER\s+)?PROCEDURE[\s\S]*?AS\s+BEGIN/i, "")
    // Strip trailing END
    .replace(/END\s*;?$/i, "")
    // Normalize variables like @EmployeeId → 1
    .replace(/@\w+/g, "1")
    // Handle TOP (n) or TOP n
    .replace(/\bTOP\s*(\(\s*\d+\s*\)|\d+)/gi, "")
    // Replace NEWID() → 1
    .replace(/\bNEWID\s*\(\s*\)/gi, "1")
    // Replace GETDATE() → literal date
    .replace(/\bGETDATE\s*\(\s*\)/gi, "'2024-01-01'");
}

async function run() {
  const sql = `CREATE OR ALTER PROCEDURE UpdateTransportRequest
    @RequestId INT,
    @EmployeeId INT,
    @RequestDate DATE,
    @PickLocation VARCHAR(100),
    @DropLocation VARCHAR(100)
AS
BEGIN
    UPDATE TransportRequests
    SET EmployeeId = @EmployeeId,
        RequestDate = @RequestDate,
        PickLocation = @PickLocation,
        DropLocation = @DropLocation
    WHERE RequestId = @RequestId;
END;`;

  // Build a fake TextDocument
  const doc = TextDocument.create("file:///test.sql", "sql", 1, sql);

  // Index it so definitions map is populated
  indexText(doc.uri, sql);

  // Debug: print AST using cleaned SQL
  const parser = new Parser();
  const cleaned = cleanForAst(sql);
  const ast = parser.astify(cleaned, { database: "transactsql" });
  console.log("=== AST ===");
  console.log(JSON.stringify(ast, null, 2));

  // Debug: try finding a column
  const col = findColumnInTable("TransportRequests", "EmployeeId");
  console.log("=== Lookup ===");
  console.log(col);
}

run().catch(console.error);

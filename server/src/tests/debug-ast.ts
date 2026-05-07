import { TextDocument } from "vscode-languageserver-textdocument";
import { indexText, definitions, findColumnInTable } from "../definitions";
import { analyze } from "@saralsql/tsql-parser";

function cleanForAst(sql: string): string {
  return sql
    // Strip CREATE/ALTER PROC headers + params up to AS BEGIN
    .replace(/CREATE\s+(OR\s+ALTER\s+)?PROCEDURE[\s\S]*?AS\s+BEGIN/i, "")
    // Strip trailing END
    .replace(/END\s*;?$/i, "");
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
    UPDATE TransportRequests tr
    SET tr.EmployeeId = @EmployeeId,
        tr.RequestDate = @RequestDate,
        tr.PickLocation = @PickLocation,
        tr.DropLocation = @DropLocation
    WHERE RequestId = @RequestId;
END;`;

  // Build a fake TextDocument
  const doc = TextDocument.create("file:///test.sql", "sql", 1, sql);

  // Index it so definitions map is populated
  indexText(doc.uri, sql);

  // Debug: print AST using cleaned SQL
  const cleaned = cleanForAst(sql);
  const result = analyze(cleaned);
  console.log("=== AST ===");
  console.log(JSON.stringify(result.ast, null, 2));


  // Debug: try finding a column
  const col = findColumnInTable("TransportRequests", "EmployeeId");
  console.log("=== Lookup ===");
  console.log(col);
}

run().catch(console.error);

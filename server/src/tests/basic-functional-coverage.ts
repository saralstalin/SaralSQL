import * as assert from "assert";
import { TextDocument } from "vscode-languageserver-textdocument";
import { Position } from "vscode-languageserver/node";
import { parseSql } from "../sql-parser";
import { normalizeName, getWordRangeAtPosition } from "../text-utils";
import {
  indexText,
  definitions,
  referencesIndex,
  tablesByName,
  tableTypesByName,
  findReferenceAtPosition,
  findColumnInTable,
  getReferencesForUri,
  aliasesByUri,
  columnsByTable,
  tempTablesByUri
} from "../definitions";
import { onDefinitionProvider } from "../definition-provider";
import { doHoverProvider } from "../hover-provider";
import { findReferencesForWordProvider, isAmbiguousBareColumnAtPositionProvider } from "../references-provider";
import { onRenameProvider } from "../rename-provider";
import { onCompletionProvider } from "../completion-provider";
import { collectAmbiguousColumnDiagnostics } from "../diagnostic-helpers";
import { resolveAliasTableName, resolveSymbolCaseInsensitive, getCteColumns, getDisplaySymbolName } from "../ast-utils";
import { resolveColumnAtOffset } from "../column-resolution";

function resetIndexState(): void {
  aliasesByUri.clear();
  definitions.clear();
  referencesIndex.clear();
  columnsByTable.clear();
  tablesByName.clear();
  tableTypesByName.clear();
  tempTablesByUri.clear();
}

function posAt(text: string, needle: string, occurrence = 1, charOffset = 0): Position {
  let idx = -1;
  let from = 0;
  for (let i = 0; i < occurrence; i++) {
    idx = text.indexOf(needle, from);
    if (idx < 0) {
      throw new Error(`Needle not found: ${needle} (occurrence ${occurrence})`);
    }
    from = idx + needle.length;
  }
  const pre = text.slice(0, idx + charOffset);
  const lines = pre.split(/\r?\n/);
  return { line: lines.length - 1, character: lines[lines.length - 1].length };
}

function toNormUri(rawUri: string): string {
  return rawUri.toLowerCase();
}

function findStatementLocalColumnOwner(
  _statementText: string,
  columnName: string,
  scopeAtPos?: any,
  parsed?: any,
  offset?: number,
  localDefsByName?: Map<string, any>
): any {
  if (typeof offset !== "number") {
    return null;
  }
  const resolved = resolveColumnAtOffset({
    parsed,
    offset,
    columnName,
    tokenText: columnName,
    tablesByName,
    tableTypesByName,
    scopeAtPos,
    localDefsByName,
    resolverOptions: { allowQualifiedSchemaLookup: false }
  });
  if (resolved.status === "resolved" && resolved.owner?.column) {
    return {
      kindLabel: resolved.owner.kindLabel,
      ownerName: resolved.owner.ownerName,
      column: resolved.owner.column
    };
  }
  return null;
}

function getCurrentStatement(doc: TextDocument, _position: Position): string {
  return doc.getText();
}

function getResolvedObjectKindLabel(name: string, _def?: any, options?: { titleCase?: boolean }): string {
  const t = tableTypesByName.has(normalizeName(name)) ? "table type" : "table";
  if (options?.titleCase) {
    return t === "table type" ? "Table Type" : "Table";
  }
  return t;
}

function getHoverColumnLabel(column: any, tokenText?: string): string {
  const raw = String(column?.rawName ?? column?.name ?? "").trim();
  return raw || String(tokenText ?? "");
}

function getSymbolLocalColumns(sym: any): any[] | undefined {
  if (!sym) {return undefined;}
  if (Array.isArray(sym.localColumns) && sym.localColumns.length > 0) {
    return sym.localColumns.map((c: any) => ({
      rawName: c?.rawName ?? c?.name ?? "",
      name: c?.normalizedName ?? normalizeName(String(c?.rawName ?? c?.name ?? "")),
      type: c?.dataType ?? c?.type ?? undefined
    }));
  }
  return Array.isArray(sym.columns) ? sym.columns : undefined;
}

function getParserAliasColumnNames(_parsed: any, sym: any): string[] {
  if (!Array.isArray(sym?.columns)) {return [];}
  return sym.columns.map((c: any) => String(c?.rawName ?? c?.name ?? c ?? "")).filter(Boolean);
}

function endsWithDotToken(linePrefix: string): boolean {
  return /\.\s*$/.test(linePrefix);
}

function getAliasBeforeDot(linePrefix: string): string | null {
  const m = linePrefix.match(/([A-Za-z_][A-Za-z0-9_]*)\.\s*$/);
  return m ? m[1] : null;
}

function isFromJoinTableContext(linePrefix: string): boolean {
  return /\b(from|join)\s+$/i.test(linePrefix.trim());
}

function isInSelectProjectionContext(parsed: any, offset: number, linePrefix: string): boolean {
  const text = String(linePrefix ?? "");
  if (!/\bselect\b/i.test(text)) {return false;}
  if (/\bfrom\b/i.test(text)) {return false;}
  const stmt = parsed?.ast?.body?.find((s: any) => typeof s?.start === "number" && typeof s?.end === "number" && offset >= s.start && offset <= s.end);
  return Boolean(stmt && stmt.type === "SelectStatement");
}

function getCompletionParsedDocument(doc: TextDocument, offset: number): { parsed: any; offset: number } | null {
  const text = doc.getText();
  const injected = `${text.slice(0, offset)}__X__${text.slice(offset)}`;
  const parsed = parseSql(injected);
  if (!parsed?.ast) {return null;}
  return { parsed, offset: offset + 5 };
}

async function run(): Promise<void> {
  resetIndexState();

  const schemaUri = "file:///basic/schema.sql";
  const procUri = "file:///basic/proc.sql";

  const schemaSql = `
CREATE TABLE dbo.Employee (
  EmployeeId INT,
  DepartmentId INT,
  FirstName NVARCHAR(100),
  LastName NVARCHAR(100),
  Email NVARCHAR(200)
);
CREATE TABLE dbo.Department (
  DepartmentId INT,
  DepartmentName NVARCHAR(200),
  HeadEmployeeId INT
);
CREATE TYPE dbo.EmployeeInputType AS TABLE (
  EmployeeId INT,
  DepartmentId INT
);
`;

  const procSql = `
CREATE OR ALTER PROCEDURE dbo.BasicCoverage
  @Input dbo.EmployeeInputType READONLY
AS
BEGIN
  BEGIN TRY
    ;WITH CteEmp AS (
      SELECT e.EmployeeId, e.DepartmentId, e.FirstName
      FROM dbo.Employee e
    )
    SELECT c.EmployeeId, c.FirstName
    FROM CteEmp c;

    SELECT i.
    FROM @Input i;

    SELECT s.EmployeeId, s.DoesNotExist
    FROM (
      SELECT e.EmployeeId, e.Email
      FROM dbo.Employee e
      JOIN dbo.Department d ON d.DepartmentId = e.DepartmentId
    ) s;
  END TRY
  BEGIN CATCH
    SELECT ERROR_MESSAGE() AS ErrorMessage;
  END CATCH
END;
`;

  indexText(schemaUri, schemaSql);
  indexText(procUri, procSql);

  const doc = TextDocument.create(procUri, "sql", 1, procSql);
  const parsed = parseSql(procSql);

  // Hover in CTE usage
  const hoverPos = posAt(procSql, "c.FirstName", 1, 3);
  const hover = await doHoverProvider(doc, hoverPos, {
    toNormUri,
    definitions,
    tablesByName,
    tableTypesByName,
    findReferenceAtPosition,
    getParsedDocument: () => parsed,
    getPropertyAccessAtOffset: () => null,
    getWordRangeAtPosition,
    getUpdateSetTargetTable: () => null,
    findStatementLocalColumnOwner,
    getCurrentStatement,
    getHoverColumnLabel,
    getResolvedObjectKindLabel,
    resolveSymbolCaseInsensitive,
    getSymbolLocalColumns,
    getCteColumns,
    getDisplaySymbolName,
    resolveAliasTableName: (sym: any) => resolveAliasTableName(sym) ?? "",
    isFunctionCallInAst: () => ({ isFunc: false, rawName: "" }),
    getParserAliasColumnNames,
    safeLog: () => {}
  });
  assert.ok(hover, "Hover should resolve inside CTE projection usage");

  // Definition on a deterministic simple query
  const defUri = "file:///basic/def.sql";
  const defSql = `
SELECT e.EmployeeId
FROM dbo.Employee e;
`;
  indexText(defUri, defSql);
  const defDoc = TextDocument.create(defUri, "sql", 1, defSql);
  const defParsed = parseSql(defSql);
  const defPos = posAt(defSql, "e.EmployeeId", 1, 3);
  const def = await onDefinitionProvider(defDoc, defPos, {
    toNormUri,
    definitions,
    tablesByName,
    tableTypesByName,
    referencesIndex,
    getParsedDocument: () => defParsed,
    findReferenceAtPosition,
    isAmbiguousBareColumnAtPosition: () => false,
    getWordRangeAtPosition,
    findStatementLocalColumnOwner,
    getCurrentStatement,
    findColumnInTable,
    findDerivedAliasProjectedColumnRange: () => null,
    getResolutionSourceColumns: () => [],
    resolveSymbolCaseInsensitive,
    resolveAliasTableName,
    getCteColumns
  });
  assert.ok(Array.isArray(def) && def.length > 0, "Definition should resolve for qualified source column");

  // References and rename in procedure body
  const refPos = posAt(procSql, "e.DepartmentId", 2, 2);
  const refs = findReferencesForWordProvider("e.DepartmentId", doc, {
    toNormUri,
    tablesByName,
    tableTypesByName,
    definitions,
    referencesIndex,
    findReferenceAtPosition,
    getParsedDocument: () => parsed,
    findStatementLocalColumnOwner,
    getCurrentStatement
  }, refPos);
  assert.ok(refs.length > 0, "References should work in procedure with TRY/CATCH + CTE");

  const rename = onRenameProvider(
    {
      textDocument: { uri: procUri },
      position: refPos,
      newName: "DeptId"
    } as any,
    doc,
    {
      getWordRangeAtPosition,
      findReferencesForWord: (w, d, p) => findReferencesForWordProvider(w, d, {
        toNormUri,
        tablesByName,
        tableTypesByName,
        definitions,
        referencesIndex,
        findReferenceAtPosition,
        getParsedDocument: () => parsed,
        findStatementLocalColumnOwner,
        getCurrentStatement
      }, p)
    }
  );
  assert.ok(rename?.changes && Object.keys(rename.changes).length > 0, "Rename should produce edits in stored procedure shape");

  // Completion in deterministic FROM context
  const compUri = "file:///basic/completion.sql";
  const compSql = `
SELECT 1
FROM 
`;
  indexText(compUri, compSql);
  const compDoc = TextDocument.create(compUri, "sql", 1, compSql);
  const compParsed = parseSql(compSql);
  const compPos = { line: 2, character: 5 };
  const completions = onCompletionProvider({ textDocument: { uri: compUri }, position: compPos } as any, {
    toNormUri,
    getDocument: (rawUri: string) => rawUri === compUri ? compDoc : undefined,
    safeLog: () => {},
    getParsedDocument: () => compParsed,
    getUpdateSetTargetTable: () => null,
    getInsertColumnTargetTable: () => null,
    endsWithDotToken,
    getAliasBeforeDot,
    resolveSymbolCaseInsensitive,
    getParserAliasColumnNames,
    resolveAliasTableName,
    getCteColumns,
    getCompletionParsedDocument,
    resolveAliasTableFromStatementAst: () => null,
    isFromJoinTableContext,
    isInSelectProjectionContext,
    getStatementTableCandidatesFromAst: () => [],
    tablesByName,
    tableTypesByName
  });
  const labels = new Set(completions.map((c) => String(c.label)));
  assert.ok(labels.has("dbo.Employee"), "FROM-context completion should include dbo.Employee");

  const procRefs = getReferencesForUri(procUri);
  assert.ok(procRefs.some((r) => normalizeName(r.name) === "@input"), "TVP parameter should be indexed");

  // Diagnostics: nested alias unknown column should not be treated as ambiguous.
  const diags = collectAmbiguousColumnDiagnostics(
    parsed,
    [0],
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );
  assert.ok(
    !diags.some((d) => String(d.message).includes("DoesNotExist")),
    "Unknown nested alias column should not be misclassified as ambiguity"
  );

  // Ambiguity helper sanity on qualified token
  const amb = isAmbiguousBareColumnAtPositionProvider(doc, posAt(procSql, "s.DoesNotExist", 1, 2), parsed, {
    toNormUri,
    tablesByName,
    tableTypesByName,
    definitions,
    referencesIndex,
    findReferenceAtPosition,
    getParsedDocument: () => parsed,
    findStatementLocalColumnOwner,
    getCurrentStatement
  });
  assert.strictEqual(amb, false, "Qualified nested-scope token should not be treated as bare ambiguity");

  // Parity: resolver/hover/definition/references agree on the same qualified token.
  const parityUri = "file:///basic/parity-derived-alias.sql";
  const paritySchemaUri = "file:///basic/parity-derived-alias-schema.sql";
  const paritySchemaSql = `
CREATE TABLE dbo.HackathonWinners (
  EmployeeId INT,
  Prize NVARCHAR(100)
);
`;
  indexText(paritySchemaUri, paritySchemaSql);
  const paritySql = `
SELECT d.DepartmentId, e.EmployeeId, e.deptId
FROM Department d
LEFT JOIN (
  SELECT EmployeeId, e.DepartmentId - 2 deptId, e.FirstName, hw.Prize
  FROM Employee e
  JOIN dbo.HackathonWinners hw ON hw.EmployeeId = e.EmployeeId
) AS e ON d.DepartmentId = e.deptId;
`;
  indexText(parityUri, paritySql);
  const parityDoc = TextDocument.create(parityUri, "sql", 1, paritySql);
  const parityParsed = parseSql(paritySql);
  const parityPos = posAt(paritySql, "e.deptId", 2, 3);
  const parityOffset = parityDoc.offsetAt(parityPos);
  const parityResolved = resolveColumnAtOffset({
    parsed: parityParsed,
    offset: parityOffset,
    columnName: "e.deptId",
    tokenText: "e.deptId",
    tablesByName,
    tableTypesByName,
    resolverOptions: { allowQualifiedSchemaLookup: false }
  });
  assert.strictEqual(parityResolved.verdict, "resolved", "Resolver should resolve e.deptId on derived alias surface");

  const parityHover = await doHoverProvider(parityDoc, parityPos, {
    toNormUri,
    definitions,
    tablesByName,
    tableTypesByName,
    findReferenceAtPosition,
    getParsedDocument: () => parityParsed,
    getPropertyAccessAtOffset: () => null,
    getWordRangeAtPosition,
    getUpdateSetTargetTable: () => null,
    findStatementLocalColumnOwner,
    getCurrentStatement,
    getHoverColumnLabel,
    getResolvedObjectKindLabel,
    resolveSymbolCaseInsensitive,
    getSymbolLocalColumns,
    getCteColumns,
    getDisplaySymbolName,
    resolveAliasTableName: (sym: any) => resolveAliasTableName(sym) ?? "",
    isFunctionCallInAst: () => ({ isFunc: false, rawName: "" }),
    getParserAliasColumnNames,
    safeLog: () => {}
  });
  assert.ok(parityHover, "Hover should resolve the same token as resolver");

  const parityDef = await onDefinitionProvider(parityDoc, parityPos, {
    toNormUri,
    definitions,
    tablesByName,
    tableTypesByName,
    referencesIndex,
    getParsedDocument: () => parityParsed,
    findReferenceAtPosition,
    isAmbiguousBareColumnAtPosition: () => false,
    getWordRangeAtPosition,
    findStatementLocalColumnOwner,
    getCurrentStatement,
    findColumnInTable,
    findDerivedAliasProjectedColumnRange: () => null,
    getResolutionSourceColumns: () => [],
    resolveSymbolCaseInsensitive,
    resolveAliasTableName,
    getCteColumns
  });
  assert.ok(Array.isArray(parityDef) && parityDef.length > 0, "Definition should resolve for same token");

  const parityRefs = findReferencesForWordProvider("e.deptId", parityDoc, {
    toNormUri,
    tablesByName,
    tableTypesByName,
    definitions,
    referencesIndex,
    findReferenceAtPosition,
    getParsedDocument: () => parityParsed,
    findStatementLocalColumnOwner,
    getCurrentStatement
  }, parityPos);
  assert.ok(parityRefs.length > 0, "References should resolve for same token");

  // Basic indexing sanity
  assert.ok(procRefs.length > 0, "Procedure shape should be indexed");

  process.stdout.write("Basic functional coverage checks passed.\n");
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});

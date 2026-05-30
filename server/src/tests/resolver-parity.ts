import * as assert from "assert";
import { TextDocument } from "vscode-languageserver-textdocument";
import { Position } from "vscode-languageserver/node";
import { parseSql } from "../sql-parser";
import { getWordRangeAtPosition, normalizeName } from "../text-utils";
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
import { doHoverProvider } from "../hover-provider";
import { onDefinitionProvider } from "../definition-provider";
import { findReferencesForWordProvider } from "../references-provider";
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
  return resolved.verdict === "resolved" ? resolved.owner ?? null : null;
}

function getCurrentStatement(_doc: TextDocument, _position: Position): string {
  return "";
}

function getHoverColumnLabel(colDef: any, fallback?: string): string {
  return String(colDef?.rawName ?? colDef?.name ?? fallback ?? "");
}

function getResolvedObjectKindLabel(_normName: string, owner: any, options?: { titleCase?: boolean }): string {
  const isType = Boolean(owner && tableTypesByName.get(normalizeName(owner.name ?? owner.rawName ?? "")) === owner);
  const label = isType ? "table type" : "table";
  if (options?.titleCase) {
    return label.replace(/^./, c => c.toUpperCase());
  }
  return label;
}

function getSymbolLocalColumns(sym: any): any[] | undefined {
  return Array.isArray(sym?.columns) ? sym.columns : undefined;
}

function getParserAliasColumnNames(_parsed: any, sym: any): string[] {
  if (!Array.isArray(sym?.columns)) {
    return [];
  }
  const names = new Set<string>();
  for (const c of sym.columns) {
    const raw = String(c?.rawName ?? c?.name ?? "").trim();
    if (raw) {
      names.add(raw);
    }
  }
  return Array.from(names);
}

function findDerivedAliasProjectedColumnRange(
  doc: TextDocument,
  _parsed: any,
  _offset: number,
  _tableName: string,
  colName: string
): any[] | null {
  const text = doc.getText();
  const idx = text.toLowerCase().indexOf(String(colName).toLowerCase());
  if (idx < 0) {
    return null;
  }
  return [{
    uri: doc.uri,
    range: {
      start: doc.positionAt(idx),
      end: doc.positionAt(idx + String(colName).length)
    }
  }];
}

async function run(): Promise<void> {
  resetIndexState();
  const schemaUri = "file:///parity/schema.sql";
  const schemaSql = `
CREATE TABLE dbo.Department (DepartmentId INT);
CREATE TABLE dbo.Employee (EmployeeId INT, DepartmentId INT, FirstName NVARCHAR(50));
CREATE TABLE dbo.HackathonWinners (EmployeeId INT, Prize NVARCHAR(100));
`;
  const queryUri = "file:///parity/query.sql";
  const querySql = `
SELECT d.DepartmentId, e.EmployeeId, e.deptId
FROM Department d
LEFT JOIN (
  SELECT EmployeeId, e.DepartmentId - 2 deptId, e.FirstName, hw.Prize
  FROM Employee e
  JOIN dbo.HackathonWinners hw ON hw.EmployeeId = e.EmployeeId
) AS e ON d.DepartmentId = e.deptId;
`;
  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);

  const doc = TextDocument.create(queryUri, "sql", 1, querySql);
  const parsed = parseSql(querySql);
  const indexedRefs = getReferencesForUri(queryUri);
  const tokenRef = [...indexedRefs].reverse().find((r) => r.kind === "column" && normalizeName(String(r.name)) === "e.deptid");
  assert.ok(tokenRef, "Indexed token e.deptId should exist");
  const pos = { line: tokenRef!.line, character: tokenRef!.start + 2 };
  const offset = doc.offsetAt(pos);

  const resolved = resolveColumnAtOffset({
    parsed,
    offset,
    columnName: "e.deptId",
    tokenText: "e.deptId",
    tablesByName,
    tableTypesByName,
    resolverOptions: { allowQualifiedSchemaLookup: false }
  });
  assert.strictEqual(resolved.verdict, "resolved", "Resolver should resolve derived alias token");

  const hover = await doHoverProvider(doc, pos, {
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
  assert.ok(hover, "Hover should resolve same token");

  const def = await onDefinitionProvider(doc, pos, {
    toNormUri,
    definitions,
    tablesByName,
    tableTypesByName,
    referencesIndex,
    getParsedDocument: () => parsed,
    findReferenceAtPosition,
    isAmbiguousBareColumnAtPosition: () => false,
    getWordRangeAtPosition,
    findStatementLocalColumnOwner,
    getCurrentStatement,
    findColumnInTable,
    findDerivedAliasProjectedColumnRange,
    getResolutionSourceColumns: () => [],
    resolveSymbolCaseInsensitive,
    resolveAliasTableName,
    getCteColumns
  });
  assert.ok(Array.isArray(def) && def.length > 0, "Definition should resolve same token");

  const refs = findReferencesForWordProvider("e.deptId", doc, {
    toNormUri,
    tablesByName,
    tableTypesByName,
    definitions,
    referencesIndex,
    findReferenceAtPosition,
    getParsedDocument: () => parsed,
    findStatementLocalColumnOwner,
    getCurrentStatement
  }, pos);
  assert.ok(refs.length > 0, "References should resolve same token");

  process.stdout.write("Resolver parity checks passed.\n");
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});

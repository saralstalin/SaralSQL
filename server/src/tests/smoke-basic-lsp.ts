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
import { onCompletionProvider } from "../completion-provider";
import { doHoverProvider } from "../hover-provider";
import { findReferencesForWordProvider, isAmbiguousBareColumnAtPositionProvider } from "../references-provider";
import { onRenameProvider } from "../rename-provider";
import { collectAmbiguousColumnDiagnostics } from "../diagnostic-helpers";
import { resolveAliasTableName, resolveSymbolCaseInsensitive, getCteColumns, getDisplaySymbolName } from "../ast-utils";
import { resolveBareColumnAtOffset } from "../column-resolution";

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
  statementText: string,
  columnName: string,
  scopeAtPos?: any,
  parsed?: any,
  offset?: number,
  localDefsByName?: Map<string, any>
): any {
  if (typeof offset !== "number") {
    return null;
  }
  const resolved = resolveBareColumnAtOffset({
    parsed,
    offset,
    columnName,
    tablesByName,
    tableTypesByName,
    scopeAtPos,
    localDefsByName
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
  if (!sym) return undefined;
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
  const out = new Set<string>();
  const add = (v: any) => {
    const s = String(v ?? "").trim();
    if (s) out.add(s);
  };

  if (Array.isArray(sym?.columns)) {
    for (const c of sym.columns) {
      add(c?.rawName ?? c?.name ?? c);
    }
  }

  const queryCols = Array.isArray(sym?.location?.table?.query?.columns)
    ? sym.location.table.query.columns
    : [];
  for (const col of queryCols) {
    if (!col || col?.wildcard === true) continue;
    add(col?.alias);
    add(col?.outputName);
    add(col?.sourceName);
    const expr = col?.expression;
    if (expr?.type === "Identifier") {
      if (Array.isArray(expr.parts) && expr.parts.length > 0) {
        add(expr.parts[expr.parts.length - 1]);
      } else {
        add(expr.name);
      }
    } else if (expr?.type === "MemberExpression") {
      add(expr.property);
    }
  }

  return Array.from(out);
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
  if (!/\bselect\b/i.test(text)) {
    return false;
  }
  if (/\bfrom\b/i.test(text)) {
    return false;
  }
  const stmt = parsed?.ast?.body?.find((s: any) => typeof s?.start === "number" && typeof s?.end === "number" && offset >= s.start && offset <= s.end);
  return Boolean(stmt && stmt.type === "SelectStatement");
}

function getCompletionParsedDocument(doc: TextDocument, offset: number): { parsed: any; offset: number } | null {
  const text = doc.getText();
  const injected = `${text.slice(0, offset)}__X__${text.slice(offset)}`;
  const parsed = parseSql(injected);
  if (!parsed?.ast) {
    return null;
  }
  return { parsed, offset: offset + 5 };
}

async function run(): Promise<void> {
  resetIndexState();

  const schemaUri = "file:///smoke/schema.sql";
  const queryUri = "file:///smoke/query.sql";

  const schemaSql = `
CREATE TABLE dbo.Users (
  Id INT,
  Name NVARCHAR(50),
  RegionId INT
);
CREATE TABLE dbo.Regions (
  RegionId INT,
  Name NVARCHAR(100)
);
`;

  const querySql = `
SELECT u.Id, u.Name
FROM dbo.Users u
WHERE u.Id = 1;
`;

  const ambSql = `
SELECT Name
FROM dbo.Users u
JOIN dbo.Regions r ON r.RegionId = u.RegionId;
`;

  indexText(schemaUri, schemaSql);
  indexText(queryUri, querySql);

  const doc = TextDocument.create(queryUri, "sql", 1, querySql);
  const parsed = parseSql(querySql);
  const normUri = toNormUri(queryUri);

  // Hover: simple SELECT columns should include datatype and not normalized owner key.
  const hoverPos = posAt(querySql, "u.Name", 1, 2);
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
  const hoverText = String((hover as any)?.contents?.value ?? "");
  assert.ok(/nvarchar/i.test(hoverText), `Hover should include column datatype. Actual hover: ${hoverText}`);
  assert.ok(hoverText.includes("dbo.Users"), "Hover should include raw owner table name");
  assert.ok(!hoverText.includes("dbo.users"), "Hover should not render normalized owner name");

  // Definition: qualified column should resolve.
  const defPos = posAt(querySql, "u.Name", 1, 2);
  const def = await onDefinitionProvider(doc, defPos, {
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
    findDerivedAliasProjectedColumnRange: () => null,
    getResolutionSourceColumns: () => [],
    resolveSymbolCaseInsensitive,
    resolveAliasTableName,
    getCteColumns
  });
  assert.ok(Array.isArray(def) && def.length > 0, "Definition should resolve for qualified column");

  // References: should include usages in current file.
  const refs = findReferencesForWordProvider("u.Name", doc, {
    toNormUri,
    tablesByName,
    tableTypesByName,
    definitions,
    referencesIndex,
    findReferenceAtPosition,
    getParsedDocument: () => parsed,
    findStatementLocalColumnOwner,
    getCurrentStatement
  }, defPos);
  assert.ok(refs.length > 0, "References should return hits");

  // Rename: should produce edits.
  const rename = onRenameProvider(
    {
      textDocument: { uri: queryUri },
      position: defPos,
      newName: "DisplayName"
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
  assert.ok(rename?.changes && Object.keys(rename.changes).length > 0, "Rename should produce edits");

  // Diagnostics: ambiguous bare column in join.
  const ambParsed = parseSql(ambSql);
  const diags = collectAmbiguousColumnDiagnostics(
    ambParsed,
    [0],
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );
  assert.ok(diags.some(d => String(d.message).includes("Ambiguous column 'Name'")), "Ambiguity diagnostic should fire");

  // Ambiguity helper should return false for qualified column token.
  const isAmb = isAmbiguousBareColumnAtPositionProvider(doc, defPos, parsed, {
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
  assert.strictEqual(isAmb, false, "Qualified token should not be ambiguous");

  // References index sanity
  assert.ok(getReferencesForUri(queryUri).length > 0, "References should be indexed for query file");

  // Case 2: bare-column simple select should still resolve hover/definition.
  const bareUri = "file:///smoke/bare.sql";
  const bareSql = `
SELECT Name
FROM dbo.Users;
`;
  indexText(bareUri, bareSql);
  const bareDoc = TextDocument.create(bareUri, "sql", 1, bareSql);
  const bareParsed = parseSql(bareSql);
  const barePos = posAt(bareSql, "Name", 1);
  const bareHover = await doHoverProvider(bareDoc, barePos, {
    toNormUri,
    definitions,
    tablesByName,
    tableTypesByName,
    findReferenceAtPosition,
    getParsedDocument: () => bareParsed,
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
  const bareHoverText = String((bareHover as any)?.contents?.value ?? "");
  assert.ok(/nvarchar/i.test(bareHoverText), `Bare hover should include datatype. Actual hover: ${bareHoverText}`);
  const bareDef = await onDefinitionProvider(bareDoc, barePos, {
    toNormUri,
    definitions,
    tablesByName,
    tableTypesByName,
    referencesIndex,
    getParsedDocument: () => bareParsed,
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
  assert.ok(Array.isArray(bareDef) && bareDef.length > 0, "Bare column definition should resolve in single-source select");

  // Case 3: alias-qualified select should resolve without ambiguity.
  const aliasUri = "file:///smoke/alias.sql";
  const aliasSql = `
SELECT u.Name
FROM dbo.Users u;
`;
  indexText(aliasUri, aliasSql);
  const aliasDoc = TextDocument.create(aliasUri, "sql", 1, aliasSql);
  const aliasParsed = parseSql(aliasSql);
  const aliasPos = posAt(aliasSql, "u.Name", 1, 2);
  const aliasAmb = isAmbiguousBareColumnAtPositionProvider(aliasDoc, aliasPos, aliasParsed, {
    toNormUri,
    tablesByName,
    tableTypesByName,
    definitions,
    referencesIndex,
    findReferenceAtPosition,
    getParsedDocument: () => aliasParsed,
    findStatementLocalColumnOwner,
    getCurrentStatement
  });
  assert.strictEqual(aliasAmb, false, "Alias-qualified column should not be ambiguous");

  // Case 4: ambiguous bare column in multi-source join should warn.
  const joinUri = "file:///smoke/join-amb.sql";
  const joinSql = `
SELECT Name
FROM dbo.Users u
JOIN dbo.Regions r ON r.RegionId = u.RegionId;
`;
  indexText(joinUri, joinSql);
  const joinParsed = parseSql(joinSql);
  const joinDiags = collectAmbiguousColumnDiagnostics(
    joinParsed,
    [0],
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );
  assert.ok(
    joinDiags.some(d => String(d.message).includes("Ambiguous column 'Name'")),
    "Bare Name in multi-source join should emit ambiguity diagnostic"
  );

  // Case 5: ORDER BY select-alias should not be ambiguous; hover should bind to projection owner (not base-table column owner).
  const orderUri = "file:///smoke/order-by-alias.sql";
  const orderSql = `
SELECT e.EmployeeId EmployeeId, et.EmployeeId, et.TrainingId, et.Progress
FROM EmployeeTraining et
JOIN dbo.Employee e ON e.EmployeeId = et.EmployeeId
ORDER BY EmployeeId DESC;
`;
  const orderSchemaUri = "file:///smoke/order-by-alias-schema.sql";
  const orderSchemaSql = `
CREATE TABLE dbo.Employee (
  EmployeeId INT
);
CREATE TABLE dbo.EmployeeTraining (
  EmployeeId INT,
  TrainingId INT,
  Progress INT
);
`;
  indexText(orderSchemaUri, orderSchemaSql);
  indexText(orderUri, orderSql);
  const orderParsed = parseSql(orderSql);
  const orderDiags = collectAmbiguousColumnDiagnostics(
    orderParsed,
    [0],
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );
  assert.ok(
    orderDiags.some(d => String(d.message).includes("Ambiguous column 'EmployeeId'")),
    "ORDER BY EmployeeId should emit ambiguity when projected output name is duplicated"
  );

  const orderDoc = TextDocument.create(orderUri, "sql", 1, orderSql);
  const orderHoverPos = posAt(orderSql, "EmployeeId DESC", 1, 2);
  const orderHover = await doHoverProvider(orderDoc, orderHoverPos, {
    toNormUri,
    definitions,
    tablesByName,
    tableTypesByName,
    findReferenceAtPosition,
    getParsedDocument: () => orderParsed,
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
  const orderHoverText = String((orderHover as any)?.contents?.value ?? "");
  assert.ok(
    orderHoverText.toLowerCase().includes("ambiguous in order by"),
    `ORDER BY duplicate output alias hover should report ambiguity. Actual hover: ${orderHoverText}`
  );

  // Case 6: completion should offer alias-scoped subquery projected columns only.
  const compUri = "file:///smoke/completion-subquery-projection.sql";
  const compSql = `
SELECT 1
FROM (
  SELECT u.Id, u.Name
  FROM dbo.Users u
) s
WHERE s.;
`;
  indexText(compUri, compSql);
  const compDoc = TextDocument.create(compUri, "sql", 1, compSql);
  const compParsed = parseSql(compSql);
  const compPos = posAt(compSql, "s.", 1, 2);
  const completionItems = onCompletionProvider(
    { textDocument: { uri: compUri }, position: compPos } as any,
    {
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
    }
  );
  const completionLabels = new Set(completionItems.map(i => String(i.label)));
  assert.ok(completionLabels.has("Id"), "Completion for s. should include projected Id");
  assert.ok(completionLabels.has("Name"), "Completion for s. should include projected Name");
  assert.ok(!completionLabels.has("RegionId"), "Completion for s. should not leak non-projected base column RegionId");

  // Case 7: completion without source scope should not leak global table columns.
  const noSourceUri = "file:///smoke/completion-no-source.sql";
  const noSourceSql = `
SELECT 
`;
  indexText(noSourceUri, noSourceSql);
  const noSourceDoc = TextDocument.create(noSourceUri, "sql", 1, noSourceSql);
  const noSourceParsed = parseSql(noSourceSql);
  const noSourcePos = { line: 1, character: 7 };
  const noSourceItems = onCompletionProvider(
    { textDocument: { uri: noSourceUri }, position: noSourcePos } as any,
    {
      toNormUri,
      getDocument: (rawUri: string) => rawUri === noSourceUri ? noSourceDoc : undefined,
      safeLog: () => {},
      getParsedDocument: () => noSourceParsed,
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
    }
  );
  const noSourceLabels = new Set(noSourceItems.map(i => String(i.label)));
  assert.ok(!noSourceLabels.has("Name"), "No-source completion should not globally suggest table column Name");
  assert.ok(noSourceLabels.has("SELECT"), "No-source completion should still provide keyword suggestions");

  process.stdout.write("Smoke basic LSP checks passed.\n");
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});

import {
  createConnection,
  ProposedFeatures,
  TextDocuments,
  InitializeParams,
  InitializeResult,
  TextDocumentSyncKind,
  DefinitionParams,
  Location,
  ReferenceParams,
  CompletionItem,
  CompletionItemKind,
  CompletionParams,
  Diagnostic,
  DiagnosticSeverity,
  Hover,
  MarkupKind,
  DocumentSymbol,
  SymbolKind,
  Range,
  RenameParams,
  WorkspaceEdit,
  TextEdit,
  Position
} from "vscode-languageserver/node";
import { TextDocument } from "vscode-languageserver-textdocument";
import * as fs from "fs";
import * as fg from "fast-glob";
import * as url from "url";
import {
  normalizeName
  , getWordRangeAtPosition
  , isSqlKeyword
  , offsetAt
  , getLineStarts
  , offsetToPosition
} from "./text-utils";
import {
  setIndexReady
  , getIndexReady
  , indexText
  , definitions
  , ReferenceDef
  , columnsByTable
  , tablesByName
  , aliasesByUri
  , deleteRefsForFile
  , referencesIndex
  , findReferenceAtPosition
  , getReferencesForUri
  , findColumnInTable
  , findTableOrColumn
  , getRefs
  , tableTypesByName
} from "./definitions";
import { parseSql } from "./sql-parser";
import { collectAmbiguousColumnDiagnostics, hasBlockingParseIssues } from "./diagnostic-helpers";
import { getCteColumns, resolveAliasTableName, resolveSymbolCaseInsensitive } from "./ast-utils";
import { LruCache } from "./lru-cache";

// ---------- Connection + Documents ----------
const connection = createConnection(ProposedFeatures.all);
const documents: TextDocuments<TextDocument> = new TextDocuments(TextDocument);
const workspaceDocuments = new Map<string, TextDocument>();
const parsedDocumentCache = new LruCache();
let enableValidation = true;
let showParseIssues = false;
let enableSchemaValidation = false;
const DEBUG = process.env.SARALSQL_DEBUG === "1";
const dbg = (...args: any[]) => { if (DEBUG) { console.debug('[HOVER]', ...args); } };

// Call this after building the definitions index
async function markIndexReady() {
  setIndexReady();

  connection.console.log(
    `[index] getIndexReady=${getIndexReady()}`
  );

  if (enableValidation) {
    await validateWorkspaceDocuments();
  } else {
    clearWorkspaceDiagnostics();
  }
}

async function validateWorkspaceDocuments(): Promise<void> {
  const openDocs = new Map(documents.all().map(doc => [doc.uri, doc]));
  const validatedUris = new Set<string>();

  for (const doc of openDocs.values()) {
    try {
      await validateTextDocument(doc);
      validatedUris.add(doc.uri);
    } catch (e) {
      connection.console.error(
        `[validateWorkspace] open document validate threw: ${String(e)}`
      );
    }
  }

  let validatedCount = validatedUris.size;

  for (const doc of workspaceDocuments.values()) {
    if (validatedUris.has(doc.uri)) {
      continue;
    }

    try {
      await validateTextDocument(doc);
      validatedUris.add(doc.uri);
      validatedCount++;

      if (validatedCount % 25 === 0) {
        await new Promise(resolve => setTimeout(resolve, 0));
      }
    } catch (e) {
      connection.console.error(
        `[validateWorkspace] workspace document validate threw for ${doc.uri}: ${String(e)}`
      );
    }
  }

  connection.console.log(
    `[validateWorkspace] validated ${validatedUris.size} workspace SQL documents`
  );
}

function clearWorkspaceDiagnostics(): void {
  const uris = new Set<string>();

  for (const doc of workspaceDocuments.values()) {
    uris.add(doc.uri);
  }

  for (const doc of documents.all()) {
    uris.add(doc.uri);
  }

  for (const uri of uris) {
    connection.sendDiagnostics({
      uri,
      diagnostics: []
    });
  }
}

function toNormUri(rawUri: string) {
  try {
    let uri = rawUri;
    if (!uri.startsWith('file://')) {
      uri = url.pathToFileURL(uri).toString();
    }
    const prefix = 'file:///';
    if (uri.toLowerCase().startsWith(prefix)) {
      const pathPart = decodeURIComponent(uri.substring(prefix.length));
      const normalizedPath = pathPart.replace(/\\/g, '/').replace(/^([A-Za-z]):\//, (m, d) => d.toLowerCase() + ':/');
      return prefix + encodeURI(normalizedPath);
    }
    return uri;
  } catch (e) {
    return rawUri;
  }
}

function safeLog(msg: string): void {
  if (process.env.SARALSQL_DEBUG) {
    connection.console.log(`[SaralSQL] ${msg}`);
  }
}

function safeError(msg: string, err?: unknown): void {
  connection.console.error(`[SaralSQL] ${msg}: ${err instanceof Error ? err.stack || err.message : String(err)}`);
}

function getParsedDocument(doc: TextDocument): any {
  const text = doc.getText();
  const version = typeof doc.version === "number" ? doc.version : -1;
  const cacheKey = `${doc.uri}::${version}`;
  const cached = parsedDocumentCache.get(cacheKey);
  if (cached) {
    return cached.parsed;
  }

  const parsed = parseSql(text);
  parsedDocumentCache.set(cacheKey, { parsed });
  return parsed;
}

function applySaralSqlSettings(settings: any): void {
  enableValidation =
    settings?.saralsql?.showDiagnostics ??
    settings?.showDiagnostics ??
    settings?.saralsql?.enableValidation ??
    settings?.enableValidation ??
    true;
  showParseIssues =
    settings?.saralsql?.showParseIssues ?? settings?.showParseIssues ?? false;
  enableSchemaValidation =
    settings?.saralsql?.enableSchemaValidation ??
    settings?.enableSchemaValidation ??
    false;

  safeLog(
    `Validation ${enableValidation ? "enabled" : "disabled"}, parse issues ${showParseIssues ? "enabled" : "disabled"}, schema validation ${enableSchemaValidation ? "enabled" : "disabled"}`
  );
}

async function indexWorkspace(): Promise<void> {
  try {
    const folders = await connection.workspace.getWorkspaceFolders?.();
    if (!folders) {
      return;
    }

    for (const folder of folders) {
      const folderPath = url.fileURLToPath(folder.uri);
      const files = await fg.glob("**/*.sql", { cwd: folderPath, absolute: true });
      for (const file of files) {
        try {
          const text = fs.readFileSync(file, "utf8");
          const uri = url.pathToFileURL(file).toString();
          workspaceDocuments.set(uri, TextDocument.create(uri, "sql", 0, text));
          indexText(uri, text);
        } catch (err) {
          safeError(`Failed to index ${file}`, err);
        }
      }
    }
    safeLog("Workspace indexing complete.");
  } catch (err) {
    safeError("Workspace indexing failed", err);
  }
}

// ---------- LSP Handlers ----------
connection.onInitialize((_params: InitializeParams): InitializeResult => {
  return {
    capabilities: {
      textDocumentSync: TextDocumentSyncKind.Incremental,
      definitionProvider: true,
      referencesProvider: true,
      completionProvider: { triggerCharacters: [".", " "] },
      hoverProvider: true,
      documentSymbolProvider: true,
      workspaceSymbolProvider: true,
      renameProvider: true
    }
  };
});

connection.onInitialized(async () => {
  try {
    connection.console.log("[init] start");

    const settings = await connection.workspace.getConfiguration("saralsql");
    applySaralSqlSettings(settings);

    await indexWorkspace();

    connection.console.log("[init] workspace indexed");

    await markIndexReady();

    connection.console.log("[init] markIndexReady done");
  } catch (err) {
    safeError("Indexing failed", err);
  }
});

documents.onDidOpen(async (e) => {
  workspaceDocuments.set(e.document.uri, e.document);
  indexText(e.document.uri, e.document.getText());

  if (enableValidation) {
    await validateTextDocument(e.document);
  }
});

documents.onDidClose((e) => {
  // Do Nothing
});

const pending = new Map<string, NodeJS.Timeout>();

documents.onDidChangeContent((e) => {
  connection.console.log("[change] fired");

  const uri = e.document.uri;
  const tmr = pending.get(uri);

  if (tmr) {
    clearTimeout(tmr);
  }

  pending.set(
    uri,
    setTimeout(async () => {
      try {
        connection.console.log("[change] debounce run");

        workspaceDocuments.set(uri, e.document);
        indexText(uri, e.document.getText());
        connection.console.log("[change] indexed");

        connection.console.log(
          `[change] enableValidation=${enableValidation}`
        );

        connection.console.log("[change] before validate");

        await validateTextDocument(e.document);

        connection.console.log("[change] after validate");
      } catch (err) {
        connection.console.error(
          `[change] ERROR: ${String(err)}`
        );
      } finally {
        pending.delete(uri);
      }
    }, 200)
  );
});

// --- Definitions ---
connection.onDefinition(async (params: DefinitionParams): Promise<Location[] | null> => {
  try {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) { return null; }

    const normUri = toNormUri(doc.uri);
    const parsed = getParsedDocument(doc);
    const offset = doc.offsetAt(params.position);

    const match = findReferenceAtPosition(normUri, params.position.line, params.position.character);

    if (!match) {
      // Fallback: look up word under cursor globally in definitions
      const wordRange = getWordRangeAtPosition(doc, params.position);
      if (!wordRange) { return null; }
      const rawWord = doc.getText(wordRange);
      const word = normalizeName(rawWord);
      const fallback = findTableOrColumn(word);
      return fallback.length > 0 ? fallback : null;
    }

    if (match.kind === "parameter") {
      const byUri = referencesIndex.get(match.name);
      const fileRefs = byUri?.get(normUri) || [];
      const first = fileRefs[0];
      if (first) {
        return [{
          uri: first.uri,
          range: {
            start: { line: first.line, character: first.start },
            end: { line: first.line, character: first.end }
          }
        }];
      }
      return null;
    }

    if (match.kind === "table") {
      const norm = normalizeName(match.name);
      const defs = definitions.get(norm);
      if (defs && defs.length > 0) {
        return defs.map(d => ({
          uri: d.uri,
          range: {
            start: { line: d.line, character: 0 },
            end: { line: d.line, character: 200 }
          }
        }));
      }
      const tblDef = tablesByName.get(norm);
      if (tblDef) {
        return [{
          uri: tblDef.uri,
          range: {
            start: { line: tblDef.line, character: 0 },
            end: { line: tblDef.line, character: 200 }
          }
        }];
      }

      if (parsed?.scope?.root) {
        const scopeAtPos = parsed.scope.root.findInnermost(offset);
        const cteSym = resolveSymbolCaseInsensitive(scopeAtPos, norm);
        if (cteSym?.kind === "CTE" && typeof cteSym.location?.start === "number" && typeof cteSym.location?.end === "number") {
          return [{
            uri: doc.uri,
            range: {
              start: doc.positionAt(cteSym.location.start),
              end: doc.positionAt(cteSym.location.end)
            }
          }];
        }
      }
    }

    if (match.kind === "column") {
      const parts = match.name.split('.');
      if (parts.length === 2) {
        const tableName = parts[0];
        const colName = parts[1];
        if (parsed?.scope?.root) {
          const scopeAtPos = parsed.scope.root.findInnermost(offset);
          const cteSym = resolveSymbolCaseInsensitive(scopeAtPos, tableName);
          if (cteSym?.kind === "CTE") {
            const cteCol = getCteColumns(cteSym).find(c => c.name === normalizeName(colName));
            if (cteCol?.start !== undefined && cteCol?.end !== undefined) {
              return [{
                uri: doc.uri,
                range: {
                  start: doc.positionAt(cteCol.start),
                  end: doc.positionAt(cteCol.end)
                }
              }];
            }
          }
        }
        return findColumnInTable(tableName, colName);
      }
    }

    return null;
  } catch (err) {
    safeError("[onDefinition] handler error", err);
    return null;
  }
});

// --- Completion ---
connection.onCompletion((params: CompletionParams): CompletionItem[] => {
  try {
    const rawUri = params.textDocument.uri;
    const normUri = toNormUri(rawUri);

    const doc = documents.get(rawUri) || documents.get(normUri);
    if (!doc) {
      safeLog(`[completion] no document found for ${rawUri}`);
      return [];
    }

    const position = params.position;
    const offset = doc.offsetAt(position);
    const linePrefix = doc.getText({
      start: { line: position.line, character: 0 },
      end: { line: position.line, character: position.character }
    });

    const items: CompletionItem[] = [];
    const parsed = getParsedDocument(doc);
    const scopeAtPos = parsed?.scope?.root?.findInnermost(offset);
    const variableItems = getVariableCompletionItems(scopeAtPos);
    const fullText = doc.getText();
    const updateSetTarget = getUpdateSetTargetTable(parsed, offset);
    const insertColumnTarget = getInsertColumnTargetTable(parsed, fullText, offset);

    if (updateSetTarget && !endsWithDotToken(linePrefix)) {
      const targetNorm = normalizeName(updateSetTarget);
      const def = tablesByName.get(targetNorm) || tableTypesByName.get(targetNorm);
      if (def?.columns?.length) {
        for (const col of def.columns) {
          items.push({
            label: col.rawName,
            kind: CompletionItemKind.Field,
            detail: col.type ? `Column in ${def.rawName} (${col.type})` : `Column in ${def.rawName}`,
            insertText: col.rawName
          });
        }
        items.push(...variableItems);
        return items;
      }
    }

    if (insertColumnTarget) {
      const targetNorm = normalizeName(insertColumnTarget);
      const def = tablesByName.get(targetNorm) || tableTypesByName.get(targetNorm);
      if (def?.columns?.length) {
        for (const col of def.columns) {
          items.push({
            label: col.rawName,
            kind: CompletionItemKind.Field,
            detail: col.type ? `Column in ${def.rawName} (${col.type})` : `Column in ${def.rawName}`,
            insertText: col.rawName
          });
        }
        return items;
      }
    }

    // 1. Alias-dot completions, e.g. "t." or "[t]."
    const aliasRaw = getAliasBeforeDot(linePrefix);
    if (aliasRaw) {
      const aliasNorm = normalizeName(aliasRaw);

      if (scopeAtPos) {
        const sym = scopeAtPos.resolve(aliasNorm);
        if (sym) {
          let targetTable = aliasNorm;
          if (sym.kind === "Alias") {
            targetTable = normalizeName(sym.metadata?.tableName as string || (sym.location as any).table?.name || (sym.location as any).table || aliasNorm);
          }

          let resolved = tablesByName.get(targetTable) || tableTypesByName.get(targetTable);
          if (!resolved && sym.dataType) {
            const typeKey = normalizeName(sym.dataType);
            resolved = tableTypesByName.get(typeKey) || tablesByName.get(typeKey);
          }

          if (resolved && resolved.columns) {
            for (const col of resolved.columns) {
              items.push({
                label: col.rawName,
                kind: CompletionItemKind.Field,
                detail: col.type ? `Column in ${resolved.rawName} (${col.type})` : `Column in ${resolved.rawName}`,
                insertText: col.rawName
              });
            }
          } else if (sym.kind === "Alias") {
            const aliasTarget = normalizeName(resolveAliasTableName(sym) ?? "");
            const cteSym = aliasTarget ? resolveSymbolCaseInsensitive(scopeAtPos, aliasTarget) : null;
            if (cteSym?.kind === "CTE") {
              for (const cteCol of getCteColumns(cteSym)) {
                items.push({
                  label: cteCol.rawName,
                  kind: CompletionItemKind.Field,
                  detail: `Column in CTE ${cteSym.name}`,
                  insertText: cteCol.rawName
                });
              }
            }
          } else if (sym.columns && Array.isArray(sym.columns)) {
            for (const col of sym.columns) {
              const name = typeof col === "string" ? col : (col.name || col.rawName);
              items.push({
                label: name,
                kind: CompletionItemKind.Field,
                detail: `Column in ${sym.name}`,
                insertText: name
              });
            }
          }
        }
      }
      return items;
    }

    // 2. Suggest table names after FROM / JOIN
    if (isFromJoinTableContext(linePrefix)) {
      for (const def of tablesByName.values()) {
        items.push({
          label: def.rawName,
          kind: CompletionItemKind.Class,
          detail: `Table defined in ${def.uri.split("/").pop()}:${def.line + 1}`
        });
      }
      return items;
    }

    // 3. Inside SELECT projection -> suggest columns from local statement scope
    if (isInSelectProjectionContext(parsed, offset, linePrefix)) {
      const visibleTables = new Set<string>();
      const visibleCtes = new Map<string, Array<{ rawName: string }>>();
      if (scopeAtPos) {
        const visibleSymbols = scopeAtPos.getVisibleSymbols();
        for (const sym of visibleSymbols) {
          if (sym.kind === "Table" || sym.kind === "TempTable" || sym.kind === "CTE") {
            visibleTables.add(normalizeName(sym.name));
            if (sym.kind === "CTE") {
              visibleCtes.set(normalizeName(sym.name), getCteColumns(sym).map(c => ({ rawName: c.rawName })));
            }
          } else if (sym.kind === "Alias") {
            const tblName = resolveAliasTableName(sym);
            if (tblName) {
              const tblNorm = normalizeName(tblName);
              visibleTables.add(tblNorm);
              const cteSym = resolveSymbolCaseInsensitive(scopeAtPos, tblNorm);
              if (cteSym?.kind === "CTE") {
                visibleCtes.set(tblNorm, getCteColumns(cteSym).map(c => ({ rawName: c.rawName })));
              }
            }
          }
        }
      }

      if (visibleTables.size === 0) {
        for (const t of getStatementTableCandidatesFromAst(parsed, offset)) {
          visibleTables.add(t);
        }
      }

      let colCount = 0;
      const COL_LIMIT = 500;

      if (visibleTables.size > 0) {
        for (const tbl of visibleTables) {
          const def = tablesByName.get(tbl) || tableTypesByName.get(tbl);
          if (def && def.columns) {
            for (const col of def.columns) {
              if (colCount++ > COL_LIMIT) { break; }
              items.push({
                label: col.rawName,
                kind: CompletionItemKind.Field,
                detail: col.type ? `Column in ${def.rawName} (${col.type})` : `Column in ${def.rawName}`,
                insertText: col.rawName
              });
            }
          } else {
            const cteCols = visibleCtes.get(tbl);
            if (cteCols) {
              for (const col of cteCols) {
                if (colCount++ > COL_LIMIT) { break; }
                items.push({
                  label: col.rawName,
                  kind: CompletionItemKind.Field,
                  detail: `Column in CTE ${tbl}`,
                  insertText: col.rawName
                });
              }
            }
          }
        }
      } else {
        for (const def of tablesByName.values()) {
          if (def.columns) {
            for (const col of def.columns) {
              if (colCount++ > COL_LIMIT) { break; }
              items.push({
                label: col.rawName,
                kind: CompletionItemKind.Field,
                detail: col.type ? `Column in ${def.rawName} (${col.type})` : `Column in ${def.rawName}`,
                insertText: col.rawName
              });
            }
          }
          if (colCount > COL_LIMIT) { break; }
        }
      }

      for (const def of tablesByName.values()) {
        items.push({
          label: def.rawName,
          kind: CompletionItemKind.Class,
          detail: `Table defined in ${def.uri.split("/").pop()}:${def.line + 1}`
        });
      }
      items.push(...variableItems);
      return items;
    }

    // Fallback: tables + keywords
    items.push(...variableItems);

    for (const def of tablesByName.values()) {
      items.push({
        label: def.rawName,
        kind: CompletionItemKind.Class,
        detail: `Table defined in ${def.uri.split("/").pop()}:${def.line + 1}`
      });
    }

    const keywords = [
      "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
      "JOIN", "WHERE", "GROUP BY", "ORDER BY", "FROM", "AS"
    ];
    for (const kw of keywords) { items.push({ label: kw, kind: CompletionItemKind.Keyword }); }

    return items;
  } catch (err) {
    safeError("[completion] handler error", err);
    return [];
  }
});

function getVariableCompletionItems(scopeAtPos: any): CompletionItem[] {
  if (!scopeAtPos || typeof scopeAtPos.getVisibleSymbols !== "function") {
    return [];
  }

  const items: CompletionItem[] = [];
  const seen = new Set<string>();

  for (const sym of scopeAtPos.getVisibleSymbols()) {
    if (sym.kind !== "Parameter" && sym.kind !== "Variable") {
      continue;
    }

    const name = String(sym.name ?? "");
    if (!name || seen.has(name.toLowerCase())) {
      continue;
    }

    seen.add(name.toLowerCase());
    items.push({
      label: name,
      kind: CompletionItemKind.Variable,
      detail: sym.dataType ? `${sym.kind} (${sym.dataType})` : sym.kind,
      insertText: name
    });
  }

  return items;
}

function getDerivedAliasColumnNames(sym: any): string[] {
  const tableNode = sym?.location?.table;
  if (!tableNode || tableNode.type !== "SubqueryExpression") {
    return [];
  }

  const cols = tableNode.query?.columns;
  if (!Array.isArray(cols)) {
    return [];
  }

  const names = new Set<string>();
  for (const col of cols) {
    const output = normalizeName(String(col?.outputName ?? ""));
    if (output) {
      names.add(output);
      continue;
    }

    const source = normalizeName(String(col?.sourceName ?? ""));
    if (source) {
      names.add(source);
      continue;
    }

    const exprName = normalizeName(String(col?.expression?.name ?? ""));
    if (exprName) {
      names.add(exprName);
    }
  }

  return Array.from(names);
}

function getUpdateSetTargetTable(parsed: any, offset: number): string | undefined {
  const ast = parsed?.ast;
  if (!ast) {
    return undefined;
  }

  let match: any = null;
  const visit = (node: any) => {
    if (!node || typeof node !== "object" || match) {
      return;
    }

    if (node.type === "UpdateStatement" && Array.isArray(node.assignments) && node.assignments.length > 0) {
      const first = node.assignments[0];
      const last = node.assignments[node.assignments.length - 1];
      const inSetList = typeof first?.start === "number" && typeof last?.end === "number" && offset >= first.start && offset <= last.end;
      if (inSetList) {
        match = node;
        return;
      }
    }

    if (Array.isArray(node)) {
      for (const item of node) {
        visit(item);
      }
      return;
    }

    for (const value of Object.values(node)) {
      if (value && typeof value === "object") {
        visit(value);
      }
    }
  };

  visit(ast);
  if (!match) {
    return undefined;
  }

  const targetName = String(match?.target?.name ?? "").trim();
  if (!targetName) {
    return undefined;
  }

  const targetNorm = normalizeName(targetName);
  const scopeAtPos = parsed?.scope?.root?.findInnermost(match?.target?.start ?? offset);
  const sym = resolveSymbolCaseInsensitive(scopeAtPos, targetNorm);
  if (sym?.kind === "Alias") {
    return resolveAliasTableName(sym) ?? targetName;
  }

  if (sym?.kind === "Table" || sym?.kind === "TempTable" || sym?.kind === "CTE") {
    return String(sym.name ?? targetName);
  }

  return targetName;
}

function getInsertColumnTargetTable(parsed: any, text: string, offset: number): string | undefined {
  const ast = parsed?.ast;
  if (!ast) {
    return undefined;
  }

  let match: any = null;
  const visit = (node: any) => {
    if (!node || typeof node !== "object" || match) {
      return;
    }

    if (node.type === "InsertStatement" && node.table && typeof node.table.end === "number") {
      const tableEnd = node.table.end;
      const closeBound = typeof node.selectQuery?.start === "number"
        ? node.selectQuery.start
        : (typeof node.values?.start === "number" ? node.values.start : (node.end ?? text.length));

      const segment = text.slice(tableEnd, closeBound);
      const openRel = segment.indexOf("(");
      if (openRel >= 0) {
        const closeRel = segment.indexOf(")", openRel + 1);
        if (closeRel >= 0) {
          const openAbs = tableEnd + openRel;
          const closeAbs = tableEnd + closeRel;
          if (offset >= openAbs + 1 && offset <= closeAbs) {
            match = node;
            return;
          }
        }
      }
    }

    if (Array.isArray(node)) {
      for (const item of node) {
        visit(item);
      }
      return;
    }

    for (const value of Object.values(node)) {
      if (value && typeof value === "object") {
        visit(value);
      }
    }
  };

  visit(ast);
  if (!match) {
    return undefined;
  }

  if (typeof match.table?.name === "string" && match.table.name.trim().length > 0) {
    return match.table.name;
  }

  return undefined;
}

function getAliasBeforeDot(linePrefix: string): string | null {
  const trimmed = linePrefix.replace(/\s+$/, "");
  if (!trimmed.endsWith(".")) {
    return null;
  }

  const beforeDot = trimmed.slice(0, -1).trimEnd();
  if (!beforeDot) {
    return null;
  }

  const lastChar = beforeDot[beforeDot.length - 1];
  if (lastChar === "]") {
    const open = beforeDot.lastIndexOf("[");
    if (open >= 0 && open < beforeDot.length - 1) {
      return beforeDot.slice(open + 1, -1);
    }
    return null;
  }

  if (lastChar === "\"") {
    const open = beforeDot.lastIndexOf("\"", beforeDot.length - 2);
    if (open >= 0 && open < beforeDot.length - 1) {
      return beforeDot.slice(open + 1, -1);
    }
    return null;
  }

  if (lastChar === "`") {
    const open = beforeDot.lastIndexOf("`", beforeDot.length - 2);
    if (open >= 0 && open < beforeDot.length - 1) {
      return beforeDot.slice(open + 1, -1);
    }
    return null;
  }

  let i = beforeDot.length - 1;
  while (i >= 0) {
    const ch = beforeDot[i];
    const isIdent = (ch >= "a" && ch <= "z")
      || (ch >= "A" && ch <= "Z")
      || (ch >= "0" && ch <= "9")
      || ch === "_"
      || ch === "$"
      || ch === "#"
      || ch === "@";
    if (!isIdent) {
      break;
    }
    i--;
  }

  const token = beforeDot.slice(i + 1);
  return token.length > 0 ? token : null;
}

function isFromJoinTableContext(linePrefix: string): boolean {
  const trimmed = linePrefix.replace(/\s+$/, "");
  if (!trimmed) {
    return false;
  }

  const tokenMatch = trimmed.match(/([A-Za-z_][A-Za-z0-9_]*)$/);
  const trailingToken = tokenMatch ? tokenMatch[1].toLowerCase() : "";

  if (trailingToken === "from" || trailingToken === "join") {
    return true;
  }

  if (!trailingToken) {
    return false;
  }

  const withoutToken = trimmed.slice(0, trimmed.length - trailingToken.length).trimEnd();
  const prevMatch = withoutToken.match(/([A-Za-z_][A-Za-z0-9_]*)$/);
  const prevToken = prevMatch ? prevMatch[1].toLowerCase() : "";

  return prevToken === "from" || prevToken === "join";
}

function endsWithDotToken(linePrefix: string): boolean {
  return linePrefix.trimEnd().endsWith(".");
}

function isLikelySelectProjectionByText(linePrefix: string): boolean {
  const trimmed = linePrefix.trimEnd();
  if (!trimmed) {
    return false;
  }

  const lower = trimmed.toLowerCase();
  const idx = lower.lastIndexOf("select");
  if (idx < 0) {
    return false;
  }

  const after = lower.slice(idx + "select".length);
  return after.trim().length > 0 && !isFromJoinTableContext(trimmed);
}

function isInSelectProjectionContext(parsed: any, offset: number, linePrefix: string): boolean {
  const ast = parsed?.ast;
  if (!ast) {
    if (isFromJoinTableContext(linePrefix)) {
      return false;
    }
    return isLikelySelectProjectionByText(linePrefix);
  }

  let inProjection = false;
  const visit = (node: any) => {
    if (!node || typeof node !== "object" || inProjection) {
      return;
    }

    if (
      node.type === "SelectStatement" &&
      typeof node.start === "number" &&
      typeof node.end === "number" &&
      offset >= node.start &&
      offset <= node.end
    ) {
      const fromStart = Array.isArray(node.from) && node.from.length > 0 && typeof node.from[0]?.start === "number"
        ? node.from[0].start
        : node.end;
      if (offset <= fromStart) {
        inProjection = true;
        return;
      }
    }

    if (Array.isArray(node)) {
      for (const item of node) {
        visit(item);
      }
      return;
    }

    for (const value of Object.values(node)) {
      if (value && typeof value === "object") {
        visit(value);
      }
    }
  };

  visit(ast);
  return inProjection;
}

function getContainingStatementNode(ast: any, offset: number): any | null {
  if (!ast || !Array.isArray(ast.body)) {
    return null;
  }

  let best: any | null = null;
  for (const stmt of ast.body) {
    if (typeof stmt?.start !== "number" || typeof stmt?.end !== "number") {
      continue;
    }

    if (offset < stmt.start || offset > stmt.end) {
      continue;
    }

    if (!best || (stmt.end - stmt.start) < (best.end - best.start)) {
      best = stmt;
    }
  }

  return best;
}

function collectTablesFromAstNode(node: any): string[] {
  const candidates = new Set<string>();

  const add = (name: string | undefined) => {
    const n = normalizeName(String(name ?? ""));
    if (n) {
      candidates.add(n);
    }
  };

  const visit = (current: any) => {
    if (!current || typeof current !== "object") {
      return;
    }

    if (current.type === "TableReference") {
      const table = current.table;
      if (typeof table?.name === "string") {
        add(table.name);
      } else if (typeof table === "string") {
        add(table);
      }
    }

    if (current.type === "UpdateStatement") {
      if (typeof current.target?.name === "string") {
        add(current.target.name);
      }
      if (Array.isArray(current.from)) {
        for (const src of current.from) {
          const table = src?.table;
          if (typeof table?.name === "string") {
            add(table.name);
          } else if (typeof table === "string") {
            add(table);
          }
        }
      }
    }

    if (current.type === "InsertStatement") {
      if (typeof current.table?.name === "string") {
        add(current.table.name);
      }
    }

    if (Array.isArray(current)) {
      for (const item of current) {
        visit(item);
      }
      return;
    }

    for (const value of Object.values(current)) {
      if (value && typeof value === "object") {
        visit(value);
      }
    }
  };

  visit(node);
  return Array.from(candidates);
}

function getStatementTableCandidatesFromAst(parsed: any, offset: number): string[] {
  const ast = parsed?.ast;
  const stmt = getContainingStatementNode(ast, offset);
  if (!stmt) {
    return [];
  }

  return collectTablesFromAstNode(stmt);
}

function findStatementLocalColumnOwner(
  statementText: string,
  columnName: string,
  scopeAtPos?: any,
  parsed?: any,
  offset?: number
): { kindLabel: string; ownerName: string; column: any } | null {
  const colNorm = normalizeName(columnName);
  if (!colNorm) {
    return null;
  }

  const tableCandidates = new Set<string>();

  if (scopeAtPos && typeof scopeAtPos.getVisibleSymbols === "function") {
    for (const sym of scopeAtPos.getVisibleSymbols()) {
      if (sym.kind === "Table" || sym.kind === "TempTable") {
        const tableName = normalizeName(String(sym.name ?? ""));
        if (tableName) {
          tableCandidates.add(tableName);
        }
      } else if (sym.kind === "Alias") {
        const tableName = normalizeName(resolveAliasTableName(sym) ?? "");
        if (tableName) {
          tableCandidates.add(tableName);
        }
      } else if (sym.kind === "CTE") {
        const cteCols = getCteColumns(sym);
        const cteCol = cteCols.find(c => normalizeName(c.name) === colNorm);
        if (cteCol) {
          return {
            kindLabel: "CTE",
            ownerName: String(sym.name ?? ""),
            column: cteCol
          };
        }
      }
    }
  }

  if (parsed && typeof offset === "number") {
    for (const t of getStatementTableCandidatesFromAst(parsed, offset)) {
      tableCandidates.add(t);
    }
  }

  const matched: Array<{ kindLabel: string; ownerName: string; column: any }> = [];
  for (const tbl of tableCandidates.values()) {
    const stripped = tbl.replace(/^dbo\./, "");
    const def = tablesByName.get(tbl) || tableTypesByName.get(tbl) || tablesByName.get(stripped) || tableTypesByName.get(stripped);
    if (!def?.columns) {
      continue;
    }

    const col = def.columns.find(c => normalizeName(c.name) === colNorm);
    if (col) {
      matched.push({
        kindLabel: tableTypesByName.has(tbl) ? "table type" : "table",
        ownerName: def.rawName ?? def.name,
        column: col
      });
    }
  }

  if (matched.length === 1) {
    return matched[0];
  }

  return null;
}

// --- References ---
connection.onReferences((params: ReferenceParams): Location[] => {
  try {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) { return []; }

    const range = getWordRangeAtPosition(doc, params.position);
    if (!range) { return []; }

    const rawWord = doc.getText(range);
    return findReferencesForWord(rawWord, doc, params.position);
  } catch (err) {
    safeError("[references] handler error", err);
    return [];
  }
});

// --- Renames ---
connection.onRenameRequest((params: RenameParams): WorkspaceEdit | null => {
  try {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) { return null; }

    const range = getWordRangeAtPosition(doc, params.position);
    if (!range) { return null; }

    const rawWord = doc.getText(range);
    const newName = params.newName;

    const locations = findReferencesForWord(rawWord, doc, params.position);
    if (!locations || locations.length === 0) { return null; }

    const editsByUri: { [uri: string]: TextEdit[] } = {};
    const seen = new Set<string>();

    for (const loc of locations) {
      const key = `${loc.uri}:${loc.range.start.line}:${loc.range.start.character}:${loc.range.end.line}:${loc.range.end.character}`;
      if (seen.has(key)) { continue; }
      seen.add(key);

      const uri = loc.uri.startsWith("file://") ? loc.uri : url.pathToFileURL(loc.uri).toString();

      if (!editsByUri[uri]) { editsByUri[uri] = []; }
      editsByUri[uri].push({ range: loc.range, newText: newName });
    }

    return { changes: editsByUri };
  } catch (err) {
    safeError("[rename] handler error", err);
    return null;
  }
});

// --- Hover ---
connection.onHover(async (params): Promise<Hover | null> => {
  const doc = documents.get(params.textDocument.uri);
  if (!doc) {
    return null;
  }
  return await doHover(doc, params.position);
});

// --- Document Symbols ---
connection.onDocumentSymbol((params): DocumentSymbol[] => {
  try {
    const uri = toNormUri(params.textDocument.uri);
    const defs = definitions.get(uri) || [];
    const symbols: DocumentSymbol[] = [];

    for (const def of defs) {
      const tableSymbol: DocumentSymbol = {
        name: def.rawName,
        kind: SymbolKind.Class,
        range: Range.create(def.line, 0, def.line, 200),
        selectionRange: Range.create(def.line, 0, def.line, 200),
        children: []
      };

      if (def.columns) {
        tableSymbol.children = def.columns.map((col) => {
          const startChar = typeof (col as any).start === "number" ? (col as any).start : 0;
          const endChar = typeof (col as any).end === "number" ? (col as any).end : 200;
          return {
            name: col.rawName,
            kind: SymbolKind.Field,
            range: Range.create(col.line, startChar, col.line, endChar),
            selectionRange: Range.create(col.line, startChar, col.line, endChar),
            detail: col.type || undefined
          };
        });
      }

      symbols.push(tableSymbol);
    }

    return symbols;
  } catch (err) {
    safeError("[documentSymbol] handler error", err);
    return [];
  }
});

// ---------- Helpers ----------
export function getCurrentStatement(doc: TextDocument, position: Position): string {
  const text = doc.getText();
  const offset = doc.offsetAt(position);
  const parsed = getParsedDocument(doc);
  if (!parsed || !parsed.ast || !parsed.ast.body) {
    return doc.getText({
      start: { line: position.line, character: 0 },
      end: { line: position.line, character: Number.MAX_VALUE }
    });
  }

  for (const stmt of parsed.ast.body) {
    if (stmt.start <= offset && stmt.end >= offset) {
      return text.slice(stmt.start, stmt.end);
    }
  }

  return doc.getText({
    start: { line: position.line, character: 0 },
    end: { line: position.line, character: Number.MAX_VALUE }
  });
}

function findReferencesForWord(rawWord: string, doc: TextDocument, position?: Position): Location[] {
  if (!rawWord || rawWord.trim().length === 0) { return []; }
  const rawNorm = normalizeName(rawWord);
  if (isSqlKeyword(rawNorm)) { return []; }

  const normUri = toNormUri(doc.uri);
  const results: Location[] = [];
  const seen = new Set<string>();

  const pushLocFromRef = (r: ReferenceDef) => {
    const key = `${r.uri}:${r.line}:${r.start}:${r.end}`;
    if (seen.has(key)) { return; }
    seen.add(key);
    results.push({
      uri: r.uri,
      range: {
        start: { line: r.line, character: r.start },
        end: { line: r.line, character: r.end }
      }
    });
  };

  const match = position
    ? findReferenceAtPosition(normUri, position.line, position.character)
    : undefined;

  if (match) {
    const byUri = referencesIndex.get(match.name);
    if (byUri) {
      for (const arr of byUri.values()) {
        for (const r of arr) {
          pushLocFromRef(r);
        }
      }
    }
    if (match.kind === "table") {
      const defs = definitions.get(normalizeName(match.name));
      if (defs) {
        for (const d of defs) {
          pushLocFromRef({
            name: d.name,
            uri: d.uri,
            line: d.line,
            start: 0,
            end: d.rawName.length,
            kind: "table"
          });
        }
      }
    }
  } else {
    const parts = rawWord.split('.');
    const cleanWord = normalizeName(parts.pop() || rawWord);

    for (const [key, byUri] of referencesIndex.entries()) {
      if (key === cleanWord || key.endsWith("." + cleanWord)) {
        for (const arr of byUri.values()) {
          for (const r of arr) {
            pushLocFromRef(r);
          }
        }
      }
    }
  }

  return results;
}

async function doHover(doc: TextDocument, pos: Position): Promise<Hover | null> {
  try {
    const text = doc.getText();
    const offset = doc.offsetAt(pos);
    const normUri = toNormUri(doc.uri);

    const range = getWordRangeAtPosition(doc, pos);
    if (!range) { return null; }

    const wordRangeText = doc.getText(range);

    const match = findReferenceAtPosition(normUri, pos.line, pos.character);
    const parsed = getParsedDocument(doc);

    if (!match) {
      const word = normalizeName(wordRangeText);
      const def = tablesByName.get(word) || tableTypesByName.get(word);
      if (def && def.columns) {
        const rows = def.columns.map(c => `- \`${c.rawName}\`${c.type ? ` ${c.type}` : ""}`);
        const kindLabel = tablesByName.has(word) ? "Table" : "Table Type";
        const body = `**${kindLabel}** \`${def.rawName}\`\n\n${rows.join("\n")}`;
        return { contents: { kind: MarkupKind.Markdown, value: body }, range };
      }

      const updateSetTarget = getUpdateSetTargetTable(parsed, offset);
      if (updateSetTarget) {
        const targetNorm = normalizeName(updateSetTarget);
        const colDef = (tablesByName.get(targetNorm) || tableTypesByName.get(targetNorm))
          ?.columns
          ?.find(c => normalizeName(c.name) === word);

        if (colDef) {
          const owner = tablesByName.get(targetNorm) || tableTypesByName.get(targetNorm);
          const kindLabel = tableTypesByName.has(targetNorm) ? "table type" : "table";
          const typePart = colDef.type ? ` â€” ${colDef.type}` : "";
          const value = `**Column** \`${colDef.rawName}\`${typePart}\n\nDefined in **${kindLabel}** \`${owner?.rawName ?? targetNorm}\``;
          return { contents: { kind: MarkupKind.Markdown, value }, range };
        }
      }

      const hoverScope = parsed?.scope?.root?.findInnermost(offset);
      const stmtOwner = findStatementLocalColumnOwner(getCurrentStatement(doc, pos), word, hoverScope, parsed, offset);
      if (stmtOwner) {
        const typePart = stmtOwner.column.type ? ` â€” ${stmtOwner.column.type}` : "";
        const value = `**Column** \`${stmtOwner.column.rawName ?? stmtOwner.column.name}\`${typePart}\n\nDefined in **${stmtOwner.kindLabel}** \`${stmtOwner.ownerName}\``;
        return { contents: { kind: MarkupKind.Markdown, value }, range };
      }
      return null;
    }

    if (match.kind === "parameter") {
      let dataType = "unknown";
      let columns: any[] | undefined = undefined;
      let isTableVariable = false;
      let paramDisplay = match.name;

      if (parsed?.scope?.root) {
        const scopeAtPos = parsed.scope.root.findInnermost(offset);
          const sym = resolveSymbolCaseInsensitive(scopeAtPos, match.name) ?? resolveSymbolCaseInsensitive(scopeAtPos, wordRangeText);
        if (sym) {
          paramDisplay = sym.name ?? paramDisplay;
          if (sym.dataType) {
            dataType = sym.dataType;
            const typeKey = normalizeName(dataType);
            const typeDef = tableTypesByName.get(typeKey) || tablesByName.get(typeKey);
            if (typeDef && typeDef.columns) {
              columns = typeDef.columns;
              isTableVariable = true;
            }
          }
          if (sym.columns && Array.isArray(sym.columns)) {
            columns = sym.columns;
            isTableVariable = true;
          }
        }
      }

      if (isTableVariable && columns) {
        const rows = columns.map(c => `- \`${c.rawName ?? c.name}\`${c.type ? ` ${c.type}` : ""}`);
        const body = `**Table Variable** \`${paramDisplay}\` — \`${dataType}\`\n\n${rows.join("\n")}`;
        return { contents: { kind: MarkupKind.Markdown, value: body }, range };
      }

      const value = `**Parameter** \`${paramDisplay}\` — \`${dataType}\``;
      return { contents: { kind: MarkupKind.Markdown, value }, range };
    }

    if (match.kind === "table") {
      const norm = normalizeName(match.name);
      const def = tablesByName.get(norm) || tableTypesByName.get(norm);
      if (def && def.columns) {
        const rows = def.columns.map(c => `- \`${c.rawName}\`${c.type ? ` ${c.type}` : ""}`);
        const kindLabel = tablesByName.has(norm) ? "Table" : "Table Type";
        const body = `**${kindLabel}** \`${def.rawName}\`\n\n${rows.join("\n")}`;
        return { contents: { kind: MarkupKind.Markdown, value: body }, range };
      }

      if (parsed?.scope?.root) {
        const scopeAtPos = parsed.scope.root.findInnermost(offset);
        const cteSym = resolveSymbolCaseInsensitive(scopeAtPos, norm);
        if (cteSym?.kind === "CTE") {
          const cteCols = getCteColumns(cteSym);
          if (cteCols.length > 0) {
            const rows = cteCols.map(c => `- \`${c.rawName}\``);
            const body = `**CTE** \`${cteSym.name}\`\n\n${rows.join("\n")}`;
            return { contents: { kind: MarkupKind.Markdown, value: body }, range };
          }
        }
      }

      if (parsed?.scope?.root) {
        const scopeAtPos = parsed.scope.root.findInnermost(offset);
        const aliasSym = resolveSymbolCaseInsensitive(scopeAtPos, norm);
        if (aliasSym?.kind === "Alias") {
          const aliasTableName = String(aliasSym.metadata?.tableName ?? aliasSym.location?.table?.name ?? "");
          const aliasTableNorm = normalizeName(aliasTableName);
          const aliasTableDef = tablesByName.get(aliasTableNorm) || tableTypesByName.get(aliasTableNorm);
          if (aliasTableDef?.columns) {
            const rows = aliasTableDef.columns.map(c => `- \`${c.rawName}\`${c.type ? ` ${c.type}` : ""}`);
            const kindLabel = tablesByName.has(aliasTableNorm) ? "Table" : "Table Type";
            const body = `**Alias** \`${aliasSym.name}\`\n\nResolves to **${kindLabel}** \`${aliasTableDef.rawName}\`\n\n${rows.join("\n")}`;
            return { contents: { kind: MarkupKind.Markdown, value: body }, range };
          }
        }
      }
    }

    if (match.kind === "column") {
      const parts = match.name.split('.');
      if (parts.length === 1) {
        const hoverScope = parsed?.scope?.root?.findInnermost(offset);
        const stmtOwner = findStatementLocalColumnOwner(getCurrentStatement(doc, pos), parts[0], hoverScope, parsed, offset);
        if (stmtOwner) {
          const typePart = stmtOwner.column.type ? ` â€” ${stmtOwner.column.type}` : "";
          const value = `**Column** \`${stmtOwner.column.rawName ?? stmtOwner.column.name}\`${typePart}\n\nDefined in **${stmtOwner.kindLabel}** \`${stmtOwner.ownerName}\``;
          return { contents: { kind: MarkupKind.Markdown, value }, range };
        }
      }

      if (parts.length === 2) {
        const tableName = parts[0];
        const colName = parts[1];

        let colDef: any = null;
        let containerName = tableName;
        let isType = false;
        let isCte = false;
        let aliasToken: string | undefined = undefined;

        if (tableName.startsWith("@")) {
          const parsed = getParsedDocument(doc);
          if (parsed?.scope?.root) {
            const scopeAtPos = parsed.scope.root.findInnermost(offset);
            const sym = resolveSymbolCaseInsensitive(scopeAtPos, tableName);
            if (sym) {
              if (sym.columns && Array.isArray(sym.columns)) {
                colDef = sym.columns.find((c: any) => normalizeName(c.rawName ?? c.name) === colName);
                containerName = sym.rawName ?? sym.name;
              } else if (sym.dataType) {
                const typeKey = normalizeName(sym.dataType);
                const typeDef = tableTypesByName.get(typeKey) || tablesByName.get(typeKey);
                if (typeDef && typeDef.columns) {
                  colDef = typeDef.columns.find((c: any) => normalizeName(c.rawName ?? c.name) === colName);
                  containerName = typeDef.rawName ?? typeDef.name;
                  isType = typeDef === tableTypesByName.get(typeKey);
                  aliasToken = sym.rawName ?? sym.name;
                }
              }
            }
          }
        } else {
          const def = tablesByName.get(tableName) || tableTypesByName.get(tableName);
          if (def && def.columns) {
            colDef = def.columns.find(c => normalizeName(c.name) === colName);
            containerName = def.rawName ?? def.name;
            isType = tableTypesByName.has(tableName);
          } else if (parsed?.scope?.root) {
            const scopeAtPos = parsed.scope.root.findInnermost(offset);
            const cteSym = resolveSymbolCaseInsensitive(scopeAtPos, tableName);
            if (cteSym?.kind === "CTE") {
              const cteCol = getCteColumns(cteSym).find(c => c.name === colName);
              if (cteCol) {
                colDef = { name: cteCol.name, rawName: cteCol.rawName };
                containerName = cteSym.name ?? tableName;
                isCte = true;
              }
            }
            const aliasSym = resolveSymbolCaseInsensitive(scopeAtPos, tableName);
            if (aliasSym?.kind === "Alias") {
              const derivedColumns = getDerivedAliasColumnNames(aliasSym);
              if (derivedColumns.includes(colName)) {
                colDef = { name: colName, rawName: colName };
                containerName = aliasSym.name ?? tableName;
              }
            }
          }
        }

        if (colDef) {
          const kindLabel = isCte ? "CTE" : (isType ? "table type" : "table");
          const typePart = colDef.type ? ` — ${colDef.type}` : "";
          const aliasPart = aliasToken ? ` (parameter \`${aliasToken}\`)` : "";
          const value = `**Column** \`${colDef.rawName ?? colDef.name}\`${typePart}\n\nDefined in **${kindLabel}** \`${containerName}\`${aliasPart}`;
          return { contents: { kind: MarkupKind.Markdown, value }, range };
        }
      }
    }

    return null;
  } catch (e) {
    safeLog(`[doHover] error: ${String(e)}`);
    return null;
  }
}

export async function validateTextDocument(doc: TextDocument): Promise<void> {
  if (typeof enableValidation !== "undefined" && !enableValidation) {
    connection.sendDiagnostics({ uri: doc.uri, diagnostics: [] });
    return;
  }

  connection.console.log("[validate] ENTER");
  connection.console.log(`[validate] indexReady=${getIndexReady()}`);

  const diagnostics: Diagnostic[] = [];
  const text = doc.getText();
  const lineStarts = getLineStarts(text);

  let parsed: any = null;
  let hasParseIssues = false;

  function toDiagnostic(diag: any, source: string): Diagnostic | null {
    let range: Range;

    if (typeof diag.start === "number") {
      const startPos = offsetToPosition(diag.start, lineStarts);
      const endPos = offsetToPosition(diag.end ?? diag.start, lineStarts);

      range = {
        start: {
          line: startPos.line,
          character: startPos.character
        },
        end: {
          line: endPos.line,
          character: endPos.character
        }
      };
    } else if (diag.range && typeof diag.range.start === "number") {
      const startPos = offsetToPosition(diag.range.start, lineStarts);
      const endPos = offsetToPosition(diag.range.end, lineStarts);

      range = {
        start: {
          line: startPos.line,
          character: startPos.character
        },
        end: {
          line: endPos.line,
          character: endPos.character
        }
      };
    } else {
      return null;
    }

    return {
      severity: mapSeverity(diag.severity),
      range,
      message: String(diag.message ?? "SQL diagnostic"),
      source
    };
  }

  try {
    parsed = parseSql(text);

    const combinedDiagnostics = parsed?.diagnostics ?? [];
    const parserIssues = (parsed?.issues?.length
      ? parsed.issues
      : combinedDiagnostics.filter((diag: any) => {
          const source = String(diag.source ?? "").toLowerCase();
          const code = String(diag.code ?? "").toUpperCase();
          return source === "parser" || code.startsWith("PARSE_");
        })) ?? [];

    hasParseIssues = hasBlockingParseIssues(parsed, parserIssues);

    if (hasParseIssues) {
      if (showParseIssues) {
        for (const issue of parserIssues) {
          const diagnostic = toDiagnostic(issue, "SaralSQL Parser");
          if (diagnostic) {
            diagnostics.push(diagnostic);
          }
        }
      }

      connection.sendDiagnostics({
        uri: doc.uri,
        diagnostics
      });
      return;
    }

    const semanticDiags = parsed?.semanticDiagnostics?.length
      ? parsed.semanticDiagnostics
      : combinedDiagnostics.filter((diag: any) => {
          const source = String(diag.source ?? "").toLowerCase();
          const code = String(diag.code ?? "").toUpperCase();
          return source !== "parser" && !code.startsWith("PARSE_");
        });

    for (const diag of semanticDiags) {
      const diagnostic = toDiagnostic(diag, "SaralSQL Parser");
      if (diagnostic) {
        diagnostics.push(diagnostic);
      }
    }
  } catch (e) {
    safeLog(`[validate] Parser diagnostics failed: ${String(e)}`);
  }

  if (!parsed?.scope?.root) {
    connection.sendDiagnostics({
      uri: doc.uri,
      diagnostics
    });
    return;
  }

  if (enableSchemaValidation && (typeof getIndexReady !== "function" || getIndexReady())) {
    connection.console.log("[validate] entered schema block");

    try {
      const normDocUri = toNormUri(doc.uri);
      const fileAliases = aliasesByUri.get(normDocUri);
      const derivedAliases = new Set<string>();
      const cteNames = new Set<string>();
      const seenTables = new Set<string>();
      const seenColumns = new Set<string>();
      const diagnosticTextCache = new Map<string, string>();

      function makeOffsetRange(start: number, end: number): Range {
        const startPos = offsetToPosition(start, lineStarts);
        const endPos = offsetToPosition(end, lineStarts);

        return {
          start: {
            line: startPos.line,
            character: startPos.character
          },
          end: {
            line: endPos.line,
            character: endPos.character
          }
        };
      }

      function addUnknownTable(name: string, line: number, start: number, end: number) {
        addUnknownTableAt(name, line, start, line, end);
      }

      function addUnknownTableAt(name: string, startLine: number, startChar: number, endLine: number, endChar: number) {
        if (!name) {return;}

        const clean = normalizeName(name);

        if (clean.startsWith("#") || clean.startsWith("@")) {return;}
        if (isSystemTableReference(clean)) {return;}
        if (tableExists(clean)) {return;}

        if (seenTables.has(clean)) {return;}
        seenTables.add(clean);

        diagnostics.push({
          severity: DiagnosticSeverity.Error,
          range: {
            start: { line: startLine, character: startChar },
            end: { line: endLine, character: endChar }
          },
          message: `Unknown table '${getTextAtRange(startLine, startChar, endLine, endChar) || name}'`,
          source: "SaralSQL"
        });
      }

      function addWrongColumn(
        table: string,
        column: string,
        line: number,
        start: number,
        end: number,
        tableDisplay?: string
      ) {
        const key = `${table}.${column}:${line}:${start}`;
        if (seenColumns.has(key)) {return;}
        seenColumns.add(key);

        diagnostics.push({
          severity: DiagnosticSeverity.Error,
          range: {
            start: { line, character: start },
            end: { line, character: end }
          },
          message: `Column '${getTextAtRange(line, start, line, end) || column}' not found in table '${tableDisplay ?? table}'`,
          source: "SaralSQL"
        });
      }

      function getDisplayTableName(table: string): string {
        const norm = normalizeName(table);
        const stripped = norm.replace(/^dbo\./, "");
        const def = tablesByName.get(norm) || tableTypesByName.get(norm) || tablesByName.get(stripped) || tableTypesByName.get(stripped);
        return def?.rawName ?? table;
      }

      function getTextAtRange(startLine: number, startChar: number, endLine: number, endChar: number): string {
        const key = `${startLine}:${startChar}:${endLine}:${endChar}`;
        const cached = diagnosticTextCache.get(key);
        if (cached !== undefined) {
          return cached;
        }

        const value = doc.getText({
          start: { line: startLine, character: startChar },
          end: { line: endLine, character: endChar }
        }).trim();
        diagnosticTextCache.set(key, value);
        return value;
      }

      function isAliasMutationTarget(ref: ReferenceDef): boolean {
        if (ref.context !== "update-target" && ref.context !== "delete-target") {
          return false;
        }

        return Boolean(fileAliases?.has(normalizeName(ref.name)));
      }

      function collectDerivedAliases(scope: any): void {
        const symbols = typeof scope?.getOwnSymbols === "function"
          ? scope.getOwnSymbols()
          : Object.values(scope?.symbols ?? {});

        for (const sym of symbols) {
          if (sym?.kind === "CTE") {
            cteNames.add(normalizeName(sym.name));
          }

          if (sym?.kind !== "Alias") {
            continue;
          }

          if (sym.location?.table?.type === "SubqueryExpression") {
            derivedAliases.add(normalizeName(sym.name));
          }
        }

        const children = typeof scope?.getChildren === "function"
          ? scope.getChildren()
          : (scope?.children ?? []);

        for (const child of children) {
          collectDerivedAliases(child);
        }
      }

      collectDerivedAliases(parsed.scope.root);

      for (const ref of getReferencesForUri(normDocUri)) {
        if (ref.validateSchema === false) {
          continue;
        }

        if (ref.kind === "table") {
          if (isAliasMutationTarget(ref)) {
            continue;
          }

          if (cteNames.has(normalizeName(ref.name))) {
            continue;
          }

          addUnknownTable(ref.name, ref.line, ref.start, ref.end);
          continue;
        }

        if (ref.kind !== "column") {
          continue;
        }

        const lastDot = ref.name.lastIndexOf(".");
        if (lastDot <= 0) {
          continue;
        }

        const table = normalizeName(ref.name.slice(0, lastDot));
        const column = normalizeName(ref.name.slice(lastDot + 1));

        if (!table || table.startsWith("@")) {
          continue;
        }

        if (derivedAliases.has(table)) {
          continue;
        }

        if (cteNames.has(table)) {
          continue;
        }

        if (isSystemTableReference(table)) {
          continue;
        }

        if (!tableExists(table)) {
          addUnknownTable(table, ref.line, ref.start, ref.end);
          continue;
        }

        if (!columnExists(table, column)) {
          addWrongColumn(table, column, ref.line, ref.start, ref.end, getDisplayTableName(table));
        }
      }

      function visitScope(scope: any) {
        for (const sym of Object.values(scope.symbols ?? {}) as any[]) {
          if (sym.kind === "Alias" && sym.location?.table) {
            const t = sym.location.table;

            if (
              typeof t.start === "number" &&
              typeof t.end === "number"
            ) {
              const range = makeOffsetRange(t.start, t.end);
              addUnknownTableAt(t.name, range.start.line, range.start.character, range.end.line, range.end.character);
            }
          }

          if (sym.kind === "Table" && sym.location?.nameNode) {
            const n = sym.location.nameNode;

            if (
              typeof n.start === "number" &&
              typeof n.end === "number"
            ) {
              const range = makeOffsetRange(n.start, n.end);
              addUnknownTableAt(n.name, range.start.line, range.start.character, range.end.line, range.end.character);
            }
          }
        }

        for (const child of scope.children ?? []) {
          visitScope(child);
        }
      }

      visitScope(parsed.scope.root);

      for (const diag of collectAmbiguousColumnDiagnostics(parsed, lineStarts, tablesByName, tableTypesByName, "SaralSQL")) {
        diagnostics.push(diag);
      }
    } catch (e) {
      safeLog(`[validate] Schema validation failed: ${String(e)}`);
    }
  }

  connection.console.log(
    `[validate] sending diagnostics count=${diagnostics.length}`
  );

  connection.sendDiagnostics({
    uri: doc.uri,
    diagnostics
  });
}

function tableExists(tableName: string): boolean {
  const norm = normalizeName(tableName);
  if (isSystemTableReference(norm)) { return true; }

  if (tablesByName.has(norm) || tableTypesByName.has(norm)) { return true; }
  if (columnsByTable.has(norm)) { return true; }

  const stripped = norm.replace(/^dbo\./, "");
  if (tablesByName.has(stripped) || tableTypesByName.has(stripped)) { return true; }
  if (columnsByTable.has(stripped)) { return true; }

  return false;
}

function columnExists(tableName: string, columnName: string): boolean {
  const norm = normalizeName(tableName);
  if (isSystemTableReference(norm)) { return true; }
  const column = normalizeName(columnName);
  const direct = columnsByTable.get(norm);
  if (direct?.has(column)) { return true; }

  const stripped = norm.replace(/^dbo\./, "");
  const strippedCols = columnsByTable.get(stripped);
  if (strippedCols?.has(column)) { return true; }

  return false;
}

function isSystemTableReference(tableName: string): boolean {
  const norm = normalizeName(tableName);

  if (!norm) {
    return false;
  }

  if (norm.startsWith("sys.") || norm.startsWith("information_schema.")) {
    return true;
  }

  if (norm.includes(".sys.") || norm.includes(".information_schema.")) {
    return true;
  }

  const systemObjectPrefixes = [
    "sysdm_",
    "sys.dm_",
    "sysall_",
    "sysobjects",
    "sysindexes",
    "syscolumns",
    "systypes"
  ];

  return systemObjectPrefixes.some(prefix => norm.startsWith(prefix));
}

function mapSeverity(severity: unknown): DiagnosticSeverity {
  const s = String(severity ?? "").toLowerCase();

  switch (s) {
    case "error":
      return DiagnosticSeverity.Error;

    case "warning":
    case "warn":
      return DiagnosticSeverity.Warning;

    case "info":
    case "information":
      return DiagnosticSeverity.Information;

    case "hint":
      return DiagnosticSeverity.Hint;

    default:
      return DiagnosticSeverity.Error;
  }
}

connection.onDidChangeConfiguration(change => {
  const settings = (change.settings || {}) as any;

  applySaralSqlSettings(settings);

  if (enableValidation) {
    void validateWorkspaceDocuments();
  } else {
    clearWorkspaceDiagnostics();
  }
});

// ---------- Startup ----------
documents.listen(connection);
if (require.main === module) {
  connection.listen();
}
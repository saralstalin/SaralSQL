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
  CompletionParams,
  CodeAction,
  CodeActionKind,
  CodeActionParams,
  Diagnostic,
  DiagnosticSeverity,
  Hover,
  MarkupKind,
  DocumentSymbol,
  SymbolKind,
  Range,
  RenameParams,
  WorkspaceEdit,
  Position
} from "vscode-languageserver/node";
import { TextDocument } from "vscode-languageserver-textdocument";
import * as fs from "fs";
import * as fg from "fast-glob";
import * as url from "url";
import * as path from "path";
import { isSqlProjectUri, normalizeFileLikeUri } from "./uri-utils";
import { indexStore } from "./index-store";
import { rebuildSqlProjMembershipFromWorkspaceFolders, shouldContributeToWorkspaceSchemaFor, toSqlProjMembershipKey, type SqlProjItemKind } from "./workspace-sqlproj";
import { addUnknownTableAtProvider, addWrongColumnProvider, buildSchemaValidationContextProvider, buildTableRefsByNameProvider, clearWorkspaceDiagnosticsProvider, collectParserDiagnosticsProvider, columnExistsInContextProvider, createDiagnosticTextReaderProvider, createOffsetRangeProvider, getDisplayTableNameProvider, tableExistsInContextProvider, validateWorkspaceDocumentsProvider } from "./validation-provider";
import { findReferencesForWordProvider, isAmbiguousBareColumnAtPositionProvider } from "./references-provider";
import { buildCodeActionsForDocument } from "./code-actions-provider";
import { doHoverProvider } from "./hover-provider";
import { onDefinitionProvider } from "./definition-provider";
import { onCompletionProvider } from "./completion-provider";
import { onRenameProvider } from "./rename-provider";
import { onDocumentSymbolProvider } from "./document-symbol-provider";
import {
  normalizeName
  , isSqlKeyword
  , getWordRangeAtPosition
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
  , getRefs
  , tableTypesByName
  , tempTablesByUri
} from "./definitions";
import { parseSql, type ParseResult } from "./sql-parser";
import { SARAL_DIAGNOSTIC_CODES, buildDiagnosticSeverityOverrides, buildDisabledDiagnosticCodes, buildReadableBareColumnCodeAction, buildSelectStarExpansionCodeActions, buildUpdateNoLockCodeAction, collectAmbiguousColumnDiagnostics, collectReadableBareColumnDiagnostics, collectStringComparisonDiagnostics, hasBlockingParseIssues, normalizeSaralSqlSettings, resolveDiagnosticSeverity, shouldSuppressDiagnosticCode } from "./diagnostic-helpers";
import { getCteColumns, resolveAliasTableName, resolveSymbolCaseInsensitive, resolveAliasFromAst, getDisplaySymbolName } from "./ast-utils";
import { collectNearestScopeColumnOwners } from "./scope-column-resolver";
import { resolveColumnAtOffset } from "./column-resolution";
import { LruCache } from "./lru-cache";

// ---------- Connection + Documents ----------
const connection = createConnection(ProposedFeatures.all);
const documents: TextDocuments<TextDocument> = new TextDocuments(TextDocument);
const workspaceDocuments = new Map<string, TextDocument>();
const parsedDocumentCache = new LruCache();
let enableValidation = true;
let showParseIssues = false;
let enableSchemaValidation = false;
let sqlProjStrictBuildMembership = true;
let sqlProjWarnMissingFile = true;
let sqlProjMissingFileSeverity: DiagnosticSeverity = DiagnosticSeverity.Warning;
let disabledDiagnosticCodes = new Set<string>();
let diagnosticSeverityOverrides = new Map<string, DiagnosticSeverity>();
const schemaDiagnosticCodes = new Set<string>([
  SARAL_DIAGNOSTIC_CODES.UnknownTable,
  SARAL_DIAGNOSTIC_CODES.UnknownColumn,
  SARAL_DIAGNOSTIC_CODES.AmbiguousColumn,
  SARAL_DIAGNOSTIC_CODES.ReadabilityQualifyColumn,
  SARAL_DIAGNOSTIC_CODES.StringComparison
]);
const DEBUG = process.env.SARALSQL_DEBUG === "1";
const dbg = (...args: any[]) => { if (DEBUG) { console.debug("[SaralSQL]", ...args); } };
const sqlProjItemKindBySqlUri = new Map<string, SqlProjItemKind>();
let hasSqlProjInWorkspace = false;
let indexWorkspaceInFlight: Promise<void> | null = null;
let indexWorkspaceQueued = false;

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
  await validateWorkspaceDocumentsProvider(
    documents,
    workspaceDocuments,
    validateTextDocument,
    (message) => connection.console.log(message)
  );
}

function clearWorkspaceDiagnostics(): void {
  clearWorkspaceDiagnosticsProvider(
    documents,
    workspaceDocuments,
    (uri, diagnostics) => connection.sendDiagnostics({ uri, diagnostics })
  );
}

function toNormUri(rawUri: string) {
  return normalizeFileLikeUri(rawUri);
}

function isSqlProjectFileUri(rawUri: string): boolean {
  return isSqlProjectUri(rawUri);
}

function shouldContributeToWorkspaceSchema(rawUri: string): boolean {
  return shouldContributeToWorkspaceSchemaFor(
    sqlProjItemKindBySqlUri,
    hasSqlProjInWorkspace,
    sqlProjStrictBuildMembership,
    rawUri
  );
}

function maybePushSqlProjMissingFileDiagnostic(uri: string, text: string, diagnostics: Diagnostic[]): void {
  if (!hasSqlProjInWorkspace || !sqlProjWarnMissingFile) {
    return;
  }
  if (sqlProjItemKindBySqlUri.has(toSqlProjMembershipKey(uri))) {
    return;
  }
  const firstLineEnd = (() => {
    const newlineIdx = text.indexOf("\n");
    if (newlineIdx >= 0) {
      return newlineIdx;
    }
    return Math.max(1, text.length);
  })();
  diagnostics.push({
    code: "SSDT001",
    severity: sqlProjMissingFileSeverity,
    range: {
      start: { line: 0, character: 0 },
      end: { line: 0, character: firstLineEnd }
    },
    message: "File is not included in any .sqlproj item. It is validated locally but excluded from workspace schema contribution.",
    source: "SaralSQL"
  });
}

async function rebuildSqlProjMembershipFromWorkspace(): Promise<void> {
  const folders = await connection.workspace.getWorkspaceFolders?.();
  hasSqlProjInWorkspace = await rebuildSqlProjMembershipFromWorkspaceFolders(
    folders,
    sqlProjItemKindBySqlUri,
    safeError
  );
}

function safeLog(msg: string): void {
  if (process.env.SARALSQL_DEBUG) {
    connection.console.log(`[SaralSQL] ${msg}`);
  }
}

function safeError(msg: string, err?: unknown): void {
  connection.console.error(`[SaralSQL] ${msg}: ${err instanceof Error ? err.stack || err.message : String(err)}`);
}

function getParsedDocument(doc: TextDocument): ParseResult | null {
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
  settings = normalizeSaralSqlSettings(settings);
  disabledDiagnosticCodes = buildDisabledDiagnosticCodes(settings);
  diagnosticSeverityOverrides = buildDiagnosticSeverityOverrides(settings);

  enableValidation = settings?.showDiagnostics ?? settings?.enableValidation ?? true;
  showParseIssues =
    settings?.showParseIssues ?? false;
  enableSchemaValidation = settings?.enableSchemaValidation ?? false;
  sqlProjStrictBuildMembership = settings?.sqlproj?.strictBuildMembership ?? true;
  sqlProjWarnMissingFile = settings?.sqlproj?.warnMissingProjectFile ?? true;
  sqlProjMissingFileSeverity = parseSeveritySetting(settings?.sqlproj?.missingProjectFileSeverity, DiagnosticSeverity.Warning);

  safeLog(
    `Validation ${enableValidation ? "enabled" : "disabled"}, parse issues ${showParseIssues ? "enabled" : "disabled"}, schema validation ${enableSchemaValidation ? "enabled" : "disabled"}, sqlproj strict build membership ${sqlProjStrictBuildMembership ? "enabled" : "disabled"}, disabled diagnostics ${disabledDiagnosticCodes.size}`
  );
}

function parseSeveritySetting(value: unknown, fallback: DiagnosticSeverity): DiagnosticSeverity {
  const norm = String(value ?? "").trim().toLowerCase();
  switch (norm) {
    case "error":
      return DiagnosticSeverity.Error;
    case "warning":
    case "warn":
      return DiagnosticSeverity.Warning;
    case "information":
    case "info":
      return DiagnosticSeverity.Information;
    case "hint":
      return DiagnosticSeverity.Hint;
    default:
      return fallback;
  }
}

async function indexWorkspace(): Promise<void> {
  if (indexWorkspaceInFlight) {
    indexWorkspaceQueued = true;
    await indexWorkspaceInFlight;
    return;
  }

  indexWorkspaceInFlight = (async () => {
    try {
    const folders = await connection.workspace.getWorkspaceFolders?.();
    if (!folders) {
      return;
    }

    workspaceDocuments.clear();
    indexStore.clearAll();
    await rebuildSqlProjMembershipFromWorkspace();

    for (const folder of folders) {
      const folderPath = url.fileURLToPath(folder.uri);
      const files = await fg.glob("**/*.sql", { cwd: folderPath, absolute: true });
      for (const file of files) {
        try {
          const text = fs.readFileSync(file, "utf8");
          const uri = url.pathToFileURL(file).toString();
          workspaceDocuments.set(uri, TextDocument.create(uri, "sql", 0, text));
          indexText(uri, text, { includeInWorkspaceSchema: shouldContributeToWorkspaceSchema(uri) });
        } catch (err) {
          safeError(`Failed to index ${file}`, err);
        }
      }
    }

    // Pass 2: re-index every workspace SQL document after all definitions are loaded.
    // This prevents first-pass reference/schema resolution drift caused by file-ordering
    // during initial workspace warm-up.
    for (const doc of workspaceDocuments.values()) {
      try {
        indexText(doc.uri, doc.getText(), { includeInWorkspaceSchema: shouldContributeToWorkspaceSchema(doc.uri) });
      } catch (err) {
        safeError(`Failed second-pass reindex for ${doc.uri}`, err);
      }
    }
    safeLog("Workspace indexing complete.");
  } catch (err) {
    safeError("Workspace indexing failed", err);
  }
  })();

  try {
    await indexWorkspaceInFlight;
  } finally {
    indexWorkspaceInFlight = null;
    if (indexWorkspaceQueued) {
      indexWorkspaceQueued = false;
      await indexWorkspace();
    }
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
      renameProvider: true,
      codeActionProvider: {
        codeActionKinds: [CodeActionKind.QuickFix]
      }
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

connection.onDidChangeWatchedFiles(async (change) => {
  try {
    const hasSqlProjChange = (change?.changes ?? []).some(c => toNormUri(c.uri).toLowerCase().endsWith(".sqlproj"));
    if (!hasSqlProjChange) {
      return;
    }

    connection.console.log("[watch] .sqlproj change detected; running incremental schema sync");
    const previousMembership = new Map(sqlProjItemKindBySqlUri);
    const previousHasSqlProj = hasSqlProjInWorkspace;

    await rebuildSqlProjMembershipFromWorkspace();

    const impactedUris = new Set<string>();
    for (const uri of workspaceDocuments.keys()) {
      const before = shouldContributeToWorkspaceSchemaFor(previousMembership, previousHasSqlProj, sqlProjStrictBuildMembership, uri);
      const after = shouldContributeToWorkspaceSchema(uri);
      if (before !== after) {
        impactedUris.add(uri);
      }
    }

    for (const uri of new Set<string>([...previousMembership.keys(), ...sqlProjItemKindBySqlUri.keys()])) {
      const beforeKind = previousMembership.get(uri) ?? null;
      const afterKind = sqlProjItemKindBySqlUri.get(uri) ?? null;
      if (beforeKind !== afterKind) {
        impactedUris.add(uri);
      }
    }

    for (const uri of impactedUris) {
      const doc = workspaceDocuments.get(uri);
      if (!doc) {
        continue;
      }
      indexText(uri, doc.getText(), { includeInWorkspaceSchema: shouldContributeToWorkspaceSchema(uri) });
    }

    for (const uri of impactedUris) {
      const openDoc = documents.get(uri);
      const doc = openDoc ?? workspaceDocuments.get(uri);
      if (doc) {
        await validateTextDocument(doc);
      }
    }
  } catch (err) {
    safeError("Watched file change handling failed", err);
  }
});

documents.onDidOpen(async (e) => {
  workspaceDocuments.set(e.document.uri, e.document);
  if (isSqlProjectFileUri(e.document.uri)) {
    await indexWorkspace();
  } else {
    indexText(e.document.uri, e.document.getText(), { includeInWorkspaceSchema: shouldContributeToWorkspaceSchema(e.document.uri) });
  }

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
        if (isSqlProjectFileUri(uri)) {
          await indexWorkspace();
        } else {
          indexText(uri, e.document.getText(), { includeInWorkspaceSchema: shouldContributeToWorkspaceSchema(uri) });
        }
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
    return onDefinitionProvider(doc, params.position, {
      toNormUri,
      definitions,
      tablesByName,
      tableTypesByName,
      referencesIndex,
      getParsedDocument,
      findReferenceAtPosition,
      isAmbiguousBareColumnAtPosition,
      getWordRangeAtPosition,
      findStatementLocalColumnOwner,
      getCurrentStatement,
      findColumnInTable,
      findDerivedAliasProjectedColumnRange,
      getResolutionSourceColumns,
      resolveSymbolCaseInsensitive,
      resolveAliasTableName,
      getCteColumns
    });
  } catch (err) {
    safeError("[onDefinition] handler error", err);
    return null;
  }
});

// --- Completion ---
connection.onCompletion((params: CompletionParams): CompletionItem[] => {
  try {
    return onCompletionProvider(params, {
      toNormUri,
      getDocument: (rawUri, normUri) => documents.get(rawUri) || documents.get(normUri),
      safeLog,
      getParsedDocument,
      getUpdateSetTargetTable,
      getInsertColumnTargetTable,
      endsWithDotToken,
      getAliasBeforeDot,
      resolveSymbolCaseInsensitive,
      getParserAliasColumnNames,
      resolveAliasTableName,
      getCteColumns,
      getCompletionParsedDocument,
      resolveAliasTableFromStatementAst,
      isFromJoinTableContext,
      isInSelectProjectionContext,
      getStatementTableCandidatesFromAst,
      tablesByName,
      tableTypesByName
    });
  } catch (err) {
    safeError("[completion] handler error", err);
    return [];
  }
});

function getParserAliasColumnNames(parsed: ParseResult | null | undefined, sym: any, localDefsByName?: Map<string, any>): string[] {
  const names = new Map<string, string>();

  if (Array.isArray(sym?.columns)) {
    for (const col of sym.columns) {
      const name = String(typeof col === "string" ? col : (col?.rawName ?? col?.name ?? "")).trim();
      const norm = normalizeName(name);
      if (norm) {
        names.set(norm, name);
      }
    }
  }

  const aliasName = normalizeName(sym?.name ?? "");
  const sources = Array.isArray((parsed as any)?.lineage?.sources) ? (parsed as any).lineage.sources : [];

  const addColumnsFromSchemaSourceAlias = (sourceAlias: string): void => {
    const targetAlias = normalizeName(sourceAlias);
    if (!targetAlias) {
      return;
    }
    for (const src of sources) {
      const srcAlias = normalizeName(String(src?.alias ?? src?.name ?? ""));
      if (srcAlias !== targetAlias) {
        continue;
      }
      const candidates = [
        normalizeName(String(src?.baseName ?? "")),
        normalizeName(String(src?.name ?? ""))
      ].filter(Boolean);
      for (const candidate of candidates) {
        const stripped = candidate.replace(/^dbo\./, "");
        const def = localDefsByName?.get(candidate)
          || localDefsByName?.get(stripped)
          || tablesByName.get(candidate)
          || tablesByName.get(stripped)
          || tableTypesByName.get(candidate)
          || tableTypesByName.get(stripped);
        if (!def || !Array.isArray(def.columns)) {
          continue;
        }
        for (const c of def.columns) {
          const raw = String(c?.rawName ?? c?.name ?? "").trim();
          const norm = normalizeName(raw);
          if (norm) {
            names.set(norm, raw);
          }
        }
      }
    }
  };

  for (const source of sources) {
    const sourceName = normalizeName(source?.alias ?? source?.name ?? "");
    if (!aliasName || sourceName !== aliasName || !Array.isArray(source?.projection)) {
      continue;
    }

    for (const projection of source.projection) {
      const name = String(projection?.name ?? "").trim();
      const norm = normalizeName(name);
      if (norm && norm !== "*") {
        names.set(norm, name);
      }
    }
  }

  // Expand wildcard projections in derived/subquery aliases using lineage source projections.
  // Example: SELECT d.*, e.Address ... FROM ... AS s
  const queryCols = sym?.location?.table?.query?.columns;
  if (Array.isArray(queryCols)) {
    for (const col of queryCols) {
      if (col?.type !== "Column" || col?.wildcard !== true || col?.expression?.type !== "WildcardExpression") {
        continue;
      }
      const prefix = normalizeName(String(col?.expression?.tablePrefix?.name ?? ""));
      if (!prefix) {
        continue;
      }
      for (const source of sources) {
        const sourceName = normalizeName(source?.alias ?? source?.name ?? "");
        if (sourceName !== prefix || !Array.isArray(source?.projection)) {
          continue;
        }
        for (const projection of source.projection) {
          const name = String(projection?.name ?? "").trim();
          const norm = normalizeName(name);
          if (norm) {
            names.set(norm, name);
          }
        }
      }
    }
    // If wildcard source projection could not be expanded from lineage, hydrate from indexed schema.
    if (names.size === 0) {
      for (const col of queryCols) {
        if (col?.type !== "Column" || col?.wildcard !== true || col?.expression?.type !== "WildcardExpression") {
          continue;
        }
        const prefix = normalizeName(String(col?.expression?.tablePrefix?.name ?? ""));
        if (prefix) {
          addColumnsFromSchemaSourceAlias(prefix);
        }
      }
    }
  }

  return Array.from(names.values());
}

function getUpdateSetTargetTable(parsed: ParseResult | null, offset: number): string | undefined {
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
        let insideRhs = false;
        for (const assignment of node.assignments) {
          const expr = assignment.expression ?? assignment.value ?? assignment.right;
          if (expr && typeof expr.start === "number" && typeof expr.end === "number") {
            if (offset >= expr.start && offset <= expr.end) {
              insideRhs = true;
              break;
            }
          }
        }
        if (!insideRhs) {
          match = node;
          return;
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

function getInsertColumnTargetTable(parsed: any, offset: number): string | undefined {
  const ast = parsed?.ast;
  if (!ast) {
    return undefined;
  }

  let match: any = null;
  const visit = (node: any) => {
    if (!node || typeof node !== "object" || match) {
      return;
    }

    if (node.type === "InsertStatement" && Array.isArray(node.columnNodes) && node.columnNodes.length > 0) {
      const first = node.columnNodes[0];
      const last = node.columnNodes[node.columnNodes.length - 1];
      if (typeof first?.start === "number" && typeof last?.end === "number" && offset >= first.start && offset <= last.end) {
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
    return false;
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
  if (!ast) { return null; }
  const statements = Array.isArray(ast?.body) ? ast.body : (Array.isArray(ast) ? ast : [ast]);
  let best: any | null = null;
  for (const stmt of statements) {
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

function resolveAliasTableFromStatementAst(parsed: ParseResult | null, offset: number, aliasNorm: string): string | null {
  const stmt = getContainingStatementNode(parsed?.ast, offset);
  if (!stmt) {
    return null;
  }
  return resolveAliasFromAst(aliasNorm, stmt);
}

function getCompletionParsedDocument(doc: TextDocument, offset: number): { parsed: ParseResult; offset: number } | null {
  const text = doc.getText();
  const patchedText = `${text.slice(0, offset)}__X__${text.slice(offset)}`;
  const parsed = parseSql(patchedText);
  if (!parsed?.ast) {
    return null;
  }

  return {
    parsed,
    offset: offset + 5
  };
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

      if (Array.isArray(current.joins)) {
        for (const join of current.joins) {
          const joinTable = join?.table;
          if (typeof joinTable?.name === "string") {
            add(joinTable.name);
          } else if (typeof joinTable === "string") {
            add(joinTable);
          }
        }
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

function getResolutionSourceColumns(resolution: any): Array<{ table: string; column: string }> {
  const out: Array<{ table: string; column: string }> = [];
  const seen = new Set<string>();

  const add = (tableRaw: unknown, columnRaw: unknown): void => {
    const table = normalizeName(String(tableRaw ?? ""));
    const column = normalizeName(String(columnRaw ?? "").split(".").pop() ?? "");
    if (!table || !column) {
      return;
    }
    const key = `${table}.${column}`;
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    out.push({ table, column });
  };

  for (const input of resolution?.inputs ?? []) {
    if (input?.kind === "column") {
      add(input.source, input.name);
    }
  }

  return out;
}

function getHoverColumnLabel(column: any, tokenText?: string): string {
  const raw = String(column?.rawName ?? column?.name ?? "").trim();
  const token = String(tokenText ?? "").trim();
  if (token) {
    const tokenNorm = normalizeName(token.split(".").pop() ?? token);
    const rawNorm = normalizeName(raw);
    if (tokenNorm && rawNorm && tokenNorm === rawNorm) {
      return token.split(".").pop() ?? token;
    }
  }
  return raw || token || "column";
}

function getSymbolLocalColumns(sym: any): any[] | undefined {
  if (!sym) {
    return undefined;
  }
  if (Array.isArray(sym.localColumns) && sym.localColumns.length > 0) {
    return sym.localColumns.map((c: any) => ({
      rawName: c?.rawName ?? c?.name ?? "",
      name: c?.normalizedName ?? normalizeName(String(c?.rawName ?? c?.name ?? "")),
      type: c?.dataType ?? c?.type ?? undefined,
      location: c?.location
    }));
  }
  if (Array.isArray(sym.columns) && sym.columns.length > 0) {
    return sym.columns;
  }
  return undefined;
}

function getPropertyAccessAtOffset(parsed: ParseResult | null, offset: number): any | null {
  const accesses = parsed?.columns?.propertyAccesses;
  if (!Array.isArray(accesses) || accesses.length === 0) {
    return null;
  }
  return accesses.find((a: any) => {
    const s = Number(a?.location?.start);
    const e = Number(a?.location?.end);
    return Number.isFinite(s) && Number.isFinite(e) && offset >= s && offset <= e;
  }) ?? null;
}

function getResolvedObjectKindLabel(
  key: string,
  def?: any,
  opts?: { titleCase?: boolean }
): string {
  const norm = normalizeName(key);
  const stripped = norm.replace(/^dbo\./, "");
  const isType = tableTypesByName.has(norm)
    || tableTypesByName.has(stripped)
    || (def && normalizeName(String(def.kind ?? "")).includes("type"));
  if (opts?.titleCase) {
    return isType ? "Table Type" : "Table";
  }
  return isType ? "table type" : "table";
}

function findDerivedAliasProjectedColumnRange(
  doc: TextDocument,
  parsed: ParseResult | null,
  offset: number,
  aliasName: string,
  columnName: string
): Location[] | null {
  if (!parsed?.scope?.root) {
    return null;
  }
  const scopeAtPos = parsed.scope.root.findInnermost(offset);
  const aliasSym = resolveSymbolCaseInsensitive(scopeAtPos, aliasName);
  if (!aliasSym || aliasSym.kind !== "Alias" || !aliasSym?.location?.table?.query) {
    return null;
  }

  const queryCols = Array.isArray(aliasSym.location.table.query?.columns)
    ? aliasSym.location.table.query.columns
    : [];
  const target = normalizeName(columnName);
  if (!target || queryCols.length === 0) {
    return null;
  }

  for (const col of queryCols) {
    if (!col || col.wildcard === true) {
      continue;
    }
    const candidates: string[] = [];
    const pushCandidate = (value: any): void => {
      const norm = normalizeName(String(value ?? ""));
      if (norm) {
        candidates.push(norm);
      }
    };

    pushCandidate(col.alias);
    pushCandidate(col.outputName);
    pushCandidate(col.sourceName);

    const expr = col.expression;
    if (expr?.type === "Identifier") {
      if (Array.isArray(expr.parts) && expr.parts.length > 0) {
        pushCandidate(expr.parts[expr.parts.length - 1]);
      } else {
        pushCandidate(expr.name);
      }
    } else if (expr?.type === "MemberExpression") {
      pushCandidate(expr.property);
    }

    if (!candidates.includes(target)) {
      continue;
    }

    const start = Number(col.start ?? col.expression?.start);
    const end = Number(col.end ?? col.expression?.end);
    if (!Number.isFinite(start) || !Number.isFinite(end)) {
      continue;
    }

    return [{
      uri: doc.uri,
      range: {
        start: doc.positionAt(start),
        end: doc.positionAt(end)
      }
    }];
  }

  return null;
}

function findStatementLocalColumnOwner(
  statementText: string,
  columnName: string,
  scopeAtPos?: any,
  parsed?: ParseResult | null,
  offset?: number,
  localDefsByName?: Map<string, any>
): { kindLabel: string; ownerName: string; column: any } | null {
  if (typeof offset !== "number") {
    const colNorm = normalizeName(columnName);
    if (!colNorm) {
      return null;
    }
    const owners = collectNearestScopeColumnOwners(scopeAtPos, colNorm, tablesByName, tableTypesByName, localDefsByName);
    if (owners.length === 1) {
      return {
        kindLabel: owners[0].kindLabel,
        ownerName: owners[0].ownerName,
        column: owners[0].column
      };
    }
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

function isAmbiguousBareColumnAtPosition(doc: TextDocument, position: Position, parsed: ParseResult | null): boolean {
  return isAmbiguousBareColumnAtPositionProvider(doc, position, parsed, {
    toNormUri,
    tablesByName,
    tableTypesByName,
    definitions,
    referencesIndex,
    findReferenceAtPosition,
    getParsedDocument,
    findStatementLocalColumnOwner,
    getCurrentStatement
  });
}

// --- References ---
connection.onReferences((params: ReferenceParams): Location[] => {
  try {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) { return []; }
    const parsed = getParsedDocument(doc);
    if (isAmbiguousBareColumnAtPosition(doc, params.position, parsed)) {
      return [];
    }

    const range = getWordRangeAtPosition(doc, params.position);
    if (!range) { return []; }

    const rawWord = doc.getText(range);
    return findReferencesForWord(rawWord, doc, params.position);
  } catch (err) {
    safeError("[references] handler error", err);
    return [];
  }
});

// --- Code Actions ---
connection.onCodeAction((params: CodeActionParams): CodeAction[] => {
  try {
    const doc = documents.get(params.textDocument.uri);
    return buildCodeActionsForDocument(params, doc, {
      buildReadableBareColumnCodeAction,
      buildUpdateNoLockCodeAction,
      buildSelectStarExpansionCodeActions,
      getLineStarts,
      getParsedDocument,
      tablesByName,
      tableTypesByName
    });
  } catch (err) {
    safeError("[codeAction] handler error", err);
    return [];
  }
});

// --- Renames ---
connection.onRenameRequest((params: RenameParams): WorkspaceEdit | null => {
  try {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) { return null; }
    const parsed = getParsedDocument(doc);
    if (isAmbiguousBareColumnAtPosition(doc, params.position, parsed)) {
      return null;
    }
    return onRenameProvider(params, doc, {
      getWordRangeAtPosition,
      findReferencesForWord
    });
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
    return onDocumentSymbolProvider(params.textDocument.uri, {
      toNormUri,
      definitions,
      Range,
      symbolKindClass: SymbolKind.Class,
      symbolKindField: SymbolKind.Field
    });
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
  return findReferencesForWordProvider(rawWord, doc, {
    toNormUri,
    tablesByName,
    tableTypesByName,
    definitions,
    referencesIndex,
    findReferenceAtPosition,
    getParsedDocument,
    findStatementLocalColumnOwner,
    getCurrentStatement
  }, position);
}

async function doHover(doc: TextDocument, pos: Position): Promise<Hover | null> {
  return await doHoverProvider(doc, pos, {
    toNormUri,
    definitions,
    tablesByName,
    tableTypesByName,
    findReferenceAtPosition,
    getParsedDocument,
    getPropertyAccessAtOffset,
    getWordRangeAtPosition,
    getUpdateSetTargetTable: (parsed, offset) => getUpdateSetTargetTable(parsed, offset) ?? null,
    findStatementLocalColumnOwner,
    getCurrentStatement,
    getHoverColumnLabel,
    getResolvedObjectKindLabel,
    resolveSymbolCaseInsensitive,
    getSymbolLocalColumns,
    getCteColumns,
    getDisplaySymbolName,
    resolveAliasTableName: (sym) => resolveAliasTableName(sym) ?? "",
    isFunctionCallInAst,
    getParserAliasColumnNames,
    safeLog
  });
}


export async function validateTextDocument(doc: TextDocument): Promise<void> {
  if (typeof enableValidation !== "undefined" && !enableValidation) {
    connection.sendDiagnostics({ uri: doc.uri, diagnostics: [] });
    return;
  }

  connection.console.log("[validate] ENTER");
  connection.console.log(`[validate] indexReady=${getIndexReady()}`);
  const indexReady = typeof getIndexReady === "function" ? getIndexReady() : true;
  const schemaValidationReady = enableSchemaValidation && indexReady;
  const validationVersion = doc.version;
  const sendDiagnosticsIfCurrent = (nextDiagnostics: Diagnostic[]) => {
    const latestOpen = documents.get(doc.uri);
    if (latestOpen && latestOpen.version !== validationVersion) {
      return;
    }
    connection.sendDiagnostics({ uri: doc.uri, diagnostics: nextDiagnostics });
  };

  const diagnostics: Diagnostic[] = [];
  const text = doc.getText();
  const lineStarts = getLineStarts(text);

  let parsed: any = null;
  let hasParseIssues = false;

  try {
    const result = collectParserDiagnosticsProvider(
      doc,
      text,
      lineStarts,
      diagnostics,
      mapSeverity,
      offsetToPosition,
      {
        parseSql,
        hasBlockingParseIssues,
        showParseIssues,
        schemaValidationReady,
        schemaDiagnosticCodes,
        resolveExistingSqlCmdInclude: (docUri, message) => Boolean(resolveExistingSqlCmdInclude(docUri, message)),
        shouldSuppressDiagnosticCode,
        disabledDiagnosticCodes,
        SARAL_DIAGNOSTIC_CODES
      }
    );
    parsed = result.parsed;
    hasParseIssues = result.hasParseIssues;

    if (hasParseIssues) {
      sendDiagnosticsIfCurrent(diagnostics);
      return;
    }
  } catch (e) {
    safeLog(`[validate] Parser diagnostics failed: ${String(e)}`);
  }

  if (!parsed?.scope?.root) {
    maybePushSqlProjMissingFileDiagnostic(doc.uri, text, diagnostics);
    sendDiagnosticsIfCurrent(diagnostics);
    return;
  }

  // Publish parser/local diagnostics first so users get quick feedback,
  // then publish again after slower schema validation completes.
  if (schemaValidationReady) {
    // Ensure the current document's local schema/references are indexed for this exact text
    // before running schema-dependent diagnostics.
    if (!isSqlProjectFileUri(doc.uri)) {
      indexText(doc.uri, text, { includeInWorkspaceSchema: shouldContributeToWorkspaceSchema(doc.uri) });
    }

    sendDiagnosticsIfCurrent([...diagnostics]);
  }

  if (schemaValidationReady) {
    connection.console.log("[validate] entered schema block");

    try {
      const normDocUri = toNormUri(doc.uri);
      const {
        fileAliases,
        refsForDoc,
        localDefs,
        localDefsByName,
        cteNames,
        seenTables,
        reportedMissingTables,
        seenColumns,
        diagnosticTextCache,
        tableRefsByName,
        propertyAccessStarts
      } = buildSchemaValidationContextProvider({
        normDocUri,
        definitions,
        aliasesByUri,
        getReferencesForUri,
        parsed,
        normalizeName
      });

      function makeOffsetRange(start: number, end: number): Range {
        return createOffsetRangeProvider(lineStarts, offsetToPosition)(start, end);
      }

      function addUnknownTable(name: string, line: number, start: number, end: number, isFallback = false) {
        addUnknownTableAt(name, line, start, line, end, isFallback);
      }

      function addUnknownTableAt(name: string, startLine: number, startChar: number, endLine: number, endChar: number, isFallback = false) {
        addUnknownTableAtProvider(name, startLine, startChar, endLine, endChar, isFallback, {
          shouldSuppressDiagnosticCode,
          SARAL_DIAGNOSTIC_CODES,
          disabledDiagnosticCodes,
          normalizeName,
          reportedMissingTables,
          seenTables,
          lineStarts,
          resolveCteSymbolAtOffset,
          cteNames,
          isSystemTableReference,
          tableExistsInContext,
          getTextAtRange,
          resolveDiagnosticSeverity,
          diagnosticSeverityOverrides,
          diagnostics
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
        addWrongColumnProvider(table, column, line, start, end, tableDisplay, {
          shouldSuppressDiagnosticCode,
          SARAL_DIAGNOSTIC_CODES,
          disabledDiagnosticCodes,
          seenColumns,
          resolveDiagnosticSeverity,
          diagnosticSeverityOverrides,
          getTextAtRange,
          diagnostics
        });
      }

      function getDisplayTableName(table: string): string {
        return getDisplayTableNameProvider(table, {
          normalizeName,
          localDefsByName,
          tablesByName,
          tableTypesByName
        });
      }

      function tableExistsInContext(tableName: string): boolean {
        return tableExistsInContextProvider(tableName, {
          normalizeName,
          isOutputPseudoTableReference,
          isSystemTableReference,
          localDefsByName,
          tableExists
        });
      }

      function columnExistsInContext(tableName: string, columnName: string): boolean {
        return columnExistsInContextProvider(tableName, columnName, {
          normalizeName,
          isOutputPseudoTableReference,
          isSystemTableReference,
          localDefsByName,
          columnExists
        });
      }

      function getSchemaEquivalentTableRefCandidates(tableName: string): ReferenceDef[] {
        const norm = normalizeName(tableName);
        const stripped = norm.replace(/^dbo\./, "");
        const dboPrefixed = stripped ? `dbo.${stripped}` : norm;
        return [
          ...(tableRefsByName.get(norm) ?? []),
          ...(tableRefsByName.get(stripped) ?? []),
          ...(tableRefsByName.get(dboPrefixed) ?? [])
        ];
      }

      const getTextAtRange = createDiagnosticTextReaderProvider(doc, diagnosticTextCache);

      function isAliasMutationTarget(ref: ReferenceDef): boolean {
        if (ref.context !== "update-target" && ref.context !== "delete-target") {
          return false;
        }
        return Boolean(fileAliases?.has(normalizeName(ref.name)));
      }

      function resolveTempTableSymbolAtOffset(tableName: string, offset: number): any | null {
        if (!tableName.startsWith("#")) {
          return null;
        }

        if (parsed?.scope?.root) {
          const scopeAtPos = parsed.scope.root.findInnermost?.(offset) ?? parsed.scope.root;
          const sym = resolveSymbolCaseInsensitive(scopeAtPos, tableName);
          if (sym && (sym.kind === "TempTable" || (sym.kind === "Table" && String(sym.name ?? "").startsWith("#")))) {
            return sym;
          }
        }

        const fallback = tempTablesByUri.get(normDocUri)?.get(normalizeName(tableName));
        if (fallback && offset >= fallback.declaredAt) {
          return {
            kind: "TempTable",
            name: normalizeName(tableName),
            columns: Array.from(fallback.columns.values()).map((c) => ({ name: c, rawName: c }))
          };
        }

        return null;
      }

      function resolveCteSymbolAtOffset(tableName: string, offset: number): any | null {
        if (!parsed?.scope?.root) {
          return null;
        }

        const scopeAtPos = parsed.scope.root.findInnermost?.(offset) ?? parsed.scope.root;
        const sym = resolveSymbolCaseInsensitive(scopeAtPos, tableName);
        return sym?.kind === "CTE" ? sym : null;
      }

      function tempTableSymbolHasColumn(sym: any, columnName: string): boolean {
        if (!sym || !Array.isArray(sym.columns)) {
          return false;
        }

        const target = normalizeName(columnName);
        return sym.columns.some((c: any) => normalizeName(String(c?.rawName ?? c?.name ?? c)) === target);
      }

      function getDerivedAliasProjectedColumns(aliasNorm: string, aliasSym?: any): Set<string> {
        const out = new Set<string>();
        const sources = Array.isArray(parsed?.lineage?.sources) ? parsed.lineage.sources : [];
        const isMeaningfulProjectionName = (value: string): boolean => {
          const n = normalizeName(value);
          return Boolean(n && n !== "expression" && n !== "*");
        };
        const collectQueryProjectionColumns = (query: any): any[] => {
          if (!query || typeof query !== "object") {
            return [];
          }
          if (Array.isArray(query.columns) && query.columns.length > 0) {
            return query.columns;
          }
          if (query.type === "SetOperator") {
            const left = collectQueryProjectionColumns(query.left);
            if (left.length > 0) {
              return left;
            }
            return collectQueryProjectionColumns(query.right);
          }
          return [];
        };

        const addColumnsFromSourceAlias = (sourceAlias: string): void => {
          const targetAlias = normalizeName(sourceAlias);
          if (!targetAlias) {
            return;
          }

          for (const src of sources) {
            const srcAlias = normalizeName(String(src?.alias ?? src?.name ?? ""));
            if (srcAlias !== targetAlias) {
              continue;
            }
            const candidates = [
              normalizeName(String(src?.baseName ?? "")),
              normalizeName(String(src?.name ?? ""))
            ].filter(Boolean);
            for (const candidate of candidates) {
              const stripped = candidate.replace(/^dbo\./, "");
              const def = localDefsByName.get(candidate)
                || localDefsByName.get(stripped)
                || tablesByName.get(candidate)
                || tablesByName.get(stripped)
                || tableTypesByName.get(candidate)
                || tableTypesByName.get(stripped);
              if (!def || !Array.isArray(def.columns)) {
                continue;
              }
              for (const c of def.columns) {
                const n = normalizeName(String(c?.rawName ?? c?.name ?? ""));
                if (n) {
                  out.add(n);
                }
              }
            }
          }
        };

        for (const src of sources) {
          const srcAlias = normalizeName(String(src?.alias ?? src?.name ?? ""));
          if (!srcAlias || srcAlias !== aliasNorm || !Array.isArray(src?.projection)) {
            continue;
          }
          for (const p of src.projection) {
            const n = normalizeName(String(p?.name ?? ""));
            if (n && n !== "*") {
              out.add(n);
            }
          }
        }

        // Read derived subquery select-list projection names directly from AST shape.
        // This catches cases where lineage projection emits generic placeholders.
        const queryCols = collectQueryProjectionColumns(aliasSym?.location?.table?.query);
        if (Array.isArray(queryCols)) {
          for (const col of queryCols) {
            if (!col || col?.wildcard === true) {
              continue;
            }

            const directCandidates = [
              String(col?.alias ?? "").trim(),
              String(col?.name ?? "").trim(),
              String(col?.outputName ?? "").trim(),
              String(col?.sourceName ?? "").trim()
            ].filter(Boolean);

            let projected = directCandidates.find((v) => isMeaningfulProjectionName(v)) ?? "";

            if (!projected) {
              const expr = col?.expression;
              if (expr?.type === "Identifier") {
                if (Array.isArray(expr.parts) && expr.parts.length > 0) {
                  projected = String(expr.parts[expr.parts.length - 1] ?? "").trim();
                } else {
                  projected = String(expr.name ?? "").trim();
                }
              } else if (expr?.type === "MemberExpression") {
                projected = String(expr.property ?? "").trim();
              }
            }

            const n = normalizeName(projected);
            if (isMeaningfulProjectionName(projected)) {
              out.add(n);
            }
          }
        }

        // Expand wildcard projection parts in derived aliases, e.g. SELECT d.*, e.Address ... AS s
        if (Array.isArray(queryCols)) {
          for (const col of queryCols) {
            if (col?.type !== "Column" || col?.wildcard !== true || col?.expression?.type !== "WildcardExpression") {
              continue;
            }
            const prefix = normalizeName(String(col?.expression?.tablePrefix?.name ?? ""));
            if (!prefix) {
              continue;
            }
            for (const src of sources) {
              const srcName = normalizeName(String(src?.alias ?? src?.name ?? ""));
              if (srcName !== prefix || !Array.isArray(src?.projection)) {
                continue;
              }
              for (const p of src.projection) {
                const n = normalizeName(String(p?.name ?? ""));
                if (n && n !== "*" && n !== "expression") {
                  out.add(n);
                }
              }
            }
            // If parser projection for that source alias is empty or wildcard-only, hydrate from indexed schema.
            addColumnsFromSourceAlias(prefix);
          }
        }

        return out;
      }

      function getAliasCandidatesAtOffset(scopeAtPos: any, aliasNorm: string): any[] {
        if (!scopeAtPos || !aliasNorm) {
          return [];
        }
        const visible = typeof scopeAtPos.getVisibleSymbols === "function"
          ? scopeAtPos.getVisibleSymbols()
          : Object.values(scopeAtPos.symbols ?? {});
        return (visible as any[])
          .filter((s: any) => s?.kind === "Alias" && normalizeName(String(s?.name ?? "")) === aliasNorm)
          .filter((s: any) => {
            if (Array.isArray(s?.columns) && s.columns.length > 0) {
              return true;
            }
            const aliasTarget = normalizeName(resolveAliasTableName(s) ?? "");
            return Boolean(aliasTarget);
          });
      }

      function getAliasProjectedColumnSet(aliasNorm: string, aliasSym: any): Set<string> {
        const projected = new Set<string>();
        for (const name of getParserAliasColumnNames(parsed, aliasSym, localDefsByName)) {
          const n = normalizeName(name);
          if (n) {
            projected.add(n);
          }
        }
        for (const n of getDerivedAliasProjectedColumns(aliasNorm, aliasSym)) {
          if (n) {
            projected.add(n);
          }
        }
        return projected;
      }

      function collectCteNames(scope: any): void {
        const symbols = typeof scope?.getOwnSymbols === "function"
          ? scope.getOwnSymbols()
          : Object.values(scope?.symbols ?? {});

        for (const sym of symbols) {
          if (sym?.kind === "CTE") {
            cteNames.add(normalizeName(sym.name));
          }
        }

        const children = typeof scope?.getChildren === "function"
          ? scope.getChildren()
          : (scope?.children ?? []);

        for (const child of children) {
          collectCteNames(child);
        }
      }

      collectCteNames(parsed.scope.root);
      const tableRefsIndexed = buildTableRefsByNameProvider(refsForDoc, normalizeName);
      for (const [k, bucket] of tableRefsIndexed.entries()) {
        tableRefsByName.set(k, bucket as ReferenceDef[]);
      }

      for (const ref of refsForDoc) {
        if (ref.kind === "table") {
          if (ref.validateSchema === false) {
            continue;
          }
          if (ref.context === "create-definition") {
            continue;
          }
          if (isAliasMutationTarget(ref)) {
            continue;
          }

          if (cteNames.has(normalizeName(ref.name))) {
            continue;
          }

          const refOffset = (lineStarts[ref.line] ?? 0) + ref.start;
          if (fileAliases?.has(normalizeName(ref.name))) {
            continue;
          }
          const scopeAtPos = parsed?.scope?.root?.findInnermost?.(refOffset) ?? parsed?.scope?.root;
          const aliasCandidates = getAliasCandidatesAtOffset(scopeAtPos, normalizeName(ref.name));
          if (aliasCandidates.length > 0) {
            continue;
          }
          if (resolveCteSymbolAtOffset(ref.name, refOffset)) {
            continue;
          }
          if (resolveTempTableSymbolAtOffset(ref.name, refOffset)) {
            continue;
          }

          addUnknownTable(ref.name, ref.line, ref.start, ref.end);
          continue;
        }

        if (ref.kind !== "column" && ref.kind !== "unknown") {
          continue;
        }

        const refOffset = (lineStarts[ref.line] ?? 0) + ref.start;
        const refRangeText = getTextAtRange(ref.line, ref.start, ref.line, ref.end);
        const refNameFromRange = normalizeName(refRangeText.replace(/\s+/g, ""));
        const recoveredQualifiedUnknown = ref.kind === "unknown"
          ? recoverQualifiedUnknownNameFromText(text, refOffset, String(ref.name ?? ""))
          : null;
        const refName = (ref.kind === "unknown" && refNameFromRange.includes("."))
          ? refNameFromRange
          : (recoveredQualifiedUnknown ?? String(ref.name ?? ""));
        const lastDot = refName.lastIndexOf(".");
        if (lastDot <= 0) {
          if (ref.validateSchema === false) {
            continue;
          }
          // Bare unknown-column validation:
          // use shared resolver context; only emit when parser provides a statement-local owner hint
          // and the referenced column is truly missing on that owner.
          const bareToken = normalizeName(refName);
          if (!bareToken || bareToken.startsWith("@") || isSqlKeyword(bareToken)) {
            continue;
          }
          const scopeAtPos = parsed?.scope?.root?.findInnermost?.(refOffset) ?? parsed?.scope?.root;
          const resolvedBare = resolveColumnAtOffset({
            parsed,
            offset: refOffset,
            columnName: refName,
            tokenText: refName,
            tablesByName,
            tableTypesByName,
            scopeAtPos,
            localDefsByName,
            resolverOptions: { allowQualifiedSchemaLookup: false }
          });
          if (resolvedBare.status !== "resolved") {
            const hintedOwner = normalizeName(String(resolvedBare.context?.parserHintOwner?.ownerName ?? ""));
            if (hintedOwner && tableExistsInContext(hintedOwner) && !columnExistsInContext(hintedOwner, bareToken)) {
              addWrongColumn(hintedOwner, bareToken, ref.line, ref.start, ref.end, getDisplayTableName(hintedOwner));
            }
          }
          continue;
        }

        const table = normalizeName(refName.slice(0, lastDot));
        const column = normalizeName(refName.slice(lastDot + 1));
        const scopeAtPos = parsed?.scope?.root?.findInnermost?.(refOffset) ?? parsed?.scope?.root;
        const resolvedQualified = resolveColumnAtOffset({
          parsed,
          offset: refOffset,
          columnName: column,
          tokenText: `${table}.${column}`,
          tablesByName,
          tableTypesByName,
          scopeAtPos,
          localDefsByName,
          resolverOptions: { allowQualifiedSchemaLookup: false }
        });
        if (resolvedQualified.verdict === "resolved") {
          continue;
        }
        if (resolvedQualified.verdict === "missing-column") {
          addWrongColumn(table, column, ref.line, ref.start, ref.end, getDisplayTableName(table));
          continue;
        }
        if (ref.validateSchema === false || resolvedQualified.verdict === "ambiguous") {
          continue;
        }
        if (!table || table.startsWith("@") || cteNames.has(table) || isSystemTableReference(table)) {
          continue;
        }
        const tableRef = getSchemaEquivalentTableRefCandidates(table)
          .find((r) => r.context === "insert-target" && r.validateSchema !== false)
          ?? getSchemaEquivalentTableRefCandidates(table).find((r) => r.validateSchema !== false);
        if (tableRef) {
          addUnknownTable(tableRef.name, tableRef.line, tableRef.start, tableRef.end);
        } else {
          addUnknownTable(table, ref.line, ref.start, ref.end, true);
        }
        continue;
      }

      function visitScope(scope: any) {
        for (const sym of Object.values(scope.symbols ?? {}) as any[]) {
          if (sym.kind === "Alias" && sym.location?.table) {
            const t = sym.location.table;
            const tableNodeType = String(t.type ?? "");
            const isTableLikeNode =
              tableNodeType === "Identifier" ||
              tableNodeType === "MemberExpression";

            if (!isTableLikeNode) {
              continue;
            }

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

      for (const diag of collectAmbiguousColumnDiagnostics(parsed, lineStarts, tablesByName, tableTypesByName, "SaralSQL", diagnosticSeverityOverrides)) {
        if (!shouldSuppressDiagnosticCode(String((diag as any).code ?? ""), disabledDiagnosticCodes)) {
          diagnostics.push(diag);
        }
      }

      for (const diag of collectReadableBareColumnDiagnostics(parsed, lineStarts, tablesByName, tableTypesByName, "SaralSQL", diagnosticSeverityOverrides)) {
        if (!shouldSuppressDiagnosticCode(String((diag as any).code ?? ""), disabledDiagnosticCodes)) {
          diagnostics.push(diag);
        }
      }

      for (const diag of collectStringComparisonDiagnostics(parsed, lineStarts, tablesByName, tableTypesByName, "SaralSQL", diagnosticSeverityOverrides)) {
        if (!shouldSuppressDiagnosticCode(String((diag as any).code ?? ""), disabledDiagnosticCodes)) {
          diagnostics.push(diag);
        }
      }
    } catch (e) {
      safeLog(`[validate] Schema validation failed: ${String(e)}`);
    }
  }

  connection.console.log(
    `[validate] sending diagnostics count=${diagnostics.length}`
  );

  maybePushSqlProjMissingFileDiagnostic(doc.uri, text, diagnostics);

  sendDiagnosticsIfCurrent(diagnostics);
}

function tableExists(tableName: string): boolean {
  const norm = normalizeName(tableName);
  if (isOutputPseudoTableReference(norm)) { return true; }
  if (isSystemTableReference(norm)) { return true; }
  const parts = norm.split(".").filter(Boolean);
  const candidates = new Set<string>([norm]);
  const stripped = norm.replace(/^dbo\./, "");
  const dboPrefixed = stripped ? `dbo.${stripped}` : norm;
  if (stripped) {
    candidates.add(stripped);
  }
  if (dboPrefixed) {
    candidates.add(dboPrefixed);
  }
  if (parts.length >= 2) {
    candidates.add(parts.slice(-2).join(".")); // schema.table
  }
  if (parts.length >= 1) {
    candidates.add(parts[parts.length - 1]); // table
  }

  for (const key of candidates) {
    if (!key) {
      continue;
    }
    if (tablesByName.has(key) || tableTypesByName.has(key) || columnsByTable.has(key)) {
      return true;
    }
  }

  return false;
}

function recoverQualifiedUnknownNameFromText(
  text: string,
  absoluteStart: number,
  tokenName: string
): string | null {
  if (!Number.isFinite(absoluteStart) || absoluteStart <= 0) {
    return null;
  }
  const col = normalizeName(tokenName);
  if (!col) {
    return null;
  }

  let i = absoluteStart - 1;
  while (i >= 0 && /\s/.test(text[i])) {
    i -= 1;
  }
  if (i < 0 || text[i] !== ".") {
    return null;
  }

  i -= 1;
  while (i >= 0 && /\s/.test(text[i])) {
    i -= 1;
  }
  if (i < 0) {
    return null;
  }

  let qualifier = "";
  if (text[i] === "]") {
    let j = i - 1;
    while (j >= 0 && text[j] !== "[") {
      j -= 1;
    }
    if (j >= 0) {
      qualifier = text.slice(j, i + 1);
    }
  } else {
    let j = i;
    while (j >= 0 && /[A-Za-z0-9_#@]/.test(text[j])) {
      j -= 1;
    }
    qualifier = text.slice(j + 1, i + 1);
  }

  const qNorm = normalizeName(qualifier);
  if (!qNorm) {
    return null;
  }
  return `${qNorm}.${col}`;
}

function columnExists(tableName: string, columnName: string): boolean {
  const norm = normalizeName(tableName);
  if (isOutputPseudoTableReference(norm)) { return true; }
  if (isSystemTableReference(norm)) { return true; }
  const column = normalizeName(columnName);
  const parts = norm.split(".").filter(Boolean);
  const candidates = new Set<string>([norm]);
  const stripped = norm.replace(/^dbo\./, "");
  const dboPrefixed = stripped ? `dbo.${stripped}` : norm;
  if (stripped) {
    candidates.add(stripped);
  }
  if (dboPrefixed) {
    candidates.add(dboPrefixed);
  }
  if (parts.length >= 2) {
    candidates.add(parts.slice(-2).join(".")); // schema.table
  }
  if (parts.length >= 1) {
    candidates.add(parts[parts.length - 1]); // table
  }

  for (const key of candidates) {
    const cols = columnsByTable.get(key);
    if (cols?.has(column)) {
      return true;
    }
  }

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

  const parts = norm.split(".").filter(Boolean);
  if (parts.length >= 2) {
    for (let i = 0; i < parts.length - 1; i++) {
      if (parts[i] === "sys" || parts[i] === "information_schema") {
        return true;
      }
    }
  }

  // Compatibility for non-canonical shapes that still appear in legacy SQL scripts.
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

function isOutputPseudoTableReference(tableName: string): boolean {
  const norm = normalizeName(tableName);
  return norm === "inserted" || norm === "deleted";
}

function resolveExistingSqlCmdInclude(docUri: string, message: string): string | null {
  const includeMatch = /SQLCMD include was not resolved:\s*(.+?)\.\s*$/i.exec(String(message ?? "").trim());
  if (!includeMatch) {
    return null;
  }

  let includeRaw = String(includeMatch[1] ?? "").trim();
  if (!includeRaw || includeRaw === "<empty>") {
    return null;
  }
  includeRaw = includeRaw.replace(/^["']|["']$/g, "");

  try {
    const docFsPath = url.fileURLToPath(docUri);
    const baseDir = path.dirname(docFsPath);
    const resolved = path.isAbsolute(includeRaw) ? includeRaw : path.resolve(baseDir, includeRaw);
    return fs.existsSync(resolved) ? resolved : null;
  } catch {
    return null;
  }
}

function mapSeverity(severity: unknown, code?: string): DiagnosticSeverity {
  const s = String(severity ?? "").toLowerCase();
  let mapped: DiagnosticSeverity;

  switch (s) {
    case "error":
      mapped = DiagnosticSeverity.Error;
      break;

    case "warning":
    case "warn":
      mapped = DiagnosticSeverity.Warning;
      break;

    case "info":
    case "information":
      mapped = DiagnosticSeverity.Information;
      break;

    case "hint":
      mapped = DiagnosticSeverity.Hint;
      break;

    default:
      mapped = DiagnosticSeverity.Error;
      break;
  }

  return resolveDiagnosticSeverity(code, mapped, diagnosticSeverityOverrides);
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

function isFunctionCallInAst(ast: any, functionNameNorm: string): { isFunc: boolean; rawName: string } {
  let isFunc = false;
  let rawName = functionNameNorm;
  const visit = (n: any) => {
    if (!n || typeof n !== "object" || isFunc) {return;}
    if (n.type === "FunctionCall" && normalizeName(n.name) === functionNameNorm) {
      isFunc = true;
      if (n.name) {rawName = n.name;}
      return;
    }
    if (Array.isArray(n)) {
      for (const item of n) {visit(item);}
      return;
    }
    for (const val of Object.values(n)) {
      if (val && typeof val === "object") {visit(val);}
    }
  };
  visit(ast);
  return { isFunc, rawName };
}

// ---------- Startup ----------
documents.listen(connection);
if (require.main === module) {
  connection.listen();
}

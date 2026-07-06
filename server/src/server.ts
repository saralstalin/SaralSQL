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
  CodeAction,
  CodeActionKind,
  CodeActionParams,
  Diagnostic,
  DiagnosticSeverity,
  Hover,
  MarkupKind,
  DocumentSymbol,
  CodeLens,
  SymbolKind,
  Range,
  RenameParams,
  WorkspaceEdit,
  TextEdit,
  Position,
  FileChangeType
} from "vscode-languageserver/node";
import { TextDocument } from "vscode-languageserver-textdocument";
import * as fs from "fs";
import * as fg from "fast-glob";
import * as url from "url";
import * as path from "path";
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
  , setIndexNotReady
  , getIndexReady
  , indexText
  , definitions
  , ReferenceDef
  , columnsByTable
  , tablesByName
  , aliasesByUri
  , deleteFileFromIndex
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
import { resolveAliasTableName, resolveSymbolCaseInsensitive } from "./ast-utils";
import { collectNearestScopeColumnOwners } from "./scope-column-resolver";
import { resolveBareColumnAtOffset } from "./column-resolution";
import { LruCache } from "./lru-cache";
import {
  toNormUri as toNormUriHelper,
  computeCurrentStatement,
  getVariableCompletionItems,
  getParserAliasColumnNames,
  getUpdateSetTargetTable,
  getInsertColumnTargetTable,
  getAliasBeforeDot,
  isFromJoinTableContext,
  endsWithDotToken,
  isLikelySelectProjectionByText,
  isInSelectProjectionContext,
  getContainingStatementNode,
  resolveAliasTableFromStatementAst,
  collectTablesFromAstNode,
  getStatementTableCandidatesFromAst,
  getResolutionSourceColumns,
  getHoverColumnLabel,
  getSymbolLocalColumns,
  getPropertyAccessAtOffset,
  getResolvedObjectKindLabel,
  findDerivedAliasProjectedColumnRange,
  findStatementLocalColumnOwner,
  isAmbiguousBareColumnAtPosition,
  getDefinitionReferenceLocations,
  getDefinitionReferenceKeys,
  isReferenceHiddenFromObjectUsage,
  isFunctionCallInAst,
} from "./sql-helpers";
import { computeHover } from "./hover-provider";
import { computeDefinition } from "./definition-provider";
import { computeReferences, computeReferencesForWord } from "./references-provider";
import { computeCompletion } from "./completion-provider";
import { computeSchemaDiagnostics, type SchemaValidatorOptions } from "./schema-validator";

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
type SqlProjItemKind = "build" | "none" | "preDeploy" | "postDeploy" | "other";
const sqlProjItemKindBySqlUri = new Map<string, SqlProjItemKind>();
let hasSqlProjInWorkspace = false;

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

function clearDeletedSqlFileState(uri: string): void {
  const normUri = toNormUri(uri);
  const timer = pending.get(normUri);
  if (timer) {
    clearTimeout(timer);
    pending.delete(normUri);
  }

  workspaceDocuments.delete(normUri);
  deleteFileFromIndex(normUri);
  parsedDocumentCache.deleteWhere((key: string) => key.startsWith(`${normUri}::`));
  connection.sendDiagnostics({
    uri: normUri,
    diagnostics: []
  });
}

function toNormUri(rawUri: string) {
  return toNormUriHelper(rawUri);
}

function isSqlProjectFileUri(rawUri: string): boolean {
  return toNormUri(rawUri).toLowerCase().endsWith(".sqlproj");
}

function toSqlProjMembershipKey(rawUri: string): string {
  return toNormUri(rawUri).toLowerCase();
}

function shouldContributeToWorkspaceSchema(rawUri: string): boolean {
  return shouldContributeToWorkspaceSchemaFor(sqlProjItemKindBySqlUri, hasSqlProjInWorkspace, rawUri);
}

function shouldContributeToWorkspaceSchemaFor(
  membership: Map<string, SqlProjItemKind>,
  hasSqlProj: boolean,
  rawUri: string
): boolean {
  if (!sqlProjStrictBuildMembership) {
    return true;
  }
  if (!hasSqlProj) {
    return true;
  }
  const kind = membership.get(toSqlProjMembershipKey(rawUri));
  return kind === "build";
}

function maybePushSqlProjMissingFileDiagnostic(uri: string, text: string, diagnostics: Diagnostic[]): void {
  if (!hasSqlProjInWorkspace || !sqlProjWarnMissingFile) {
    return;
  }
  if (sqlProjItemKindBySqlUri.has(toSqlProjMembershipKey(uri))) {
    return;
  }
  diagnostics.push({
    code: "SSDT001",
    severity: sqlProjMissingFileSeverity,
    range: {
      start: { line: 0, character: 0 },
      end: { line: 0, character: Math.min(1, text.length) }
    },
    message: "File is not included in any .sqlproj item. It is validated locally but excluded from workspace schema contribution.",
    source: "SaralSQL"
  });
}

function decodeXmlAttribute(value: string): string {
  return value
    .replace(/&quot;/g, "\"")
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function registerSqlProjItem(projectDir: string, includeValue: string, kind: SqlProjItemKind): void {
  const trimmed = decodeXmlAttribute(String(includeValue ?? "").trim());
  if (!trimmed || !trimmed.toLowerCase().endsWith(".sql")) {
    return;
  }

  const absPath = path.isAbsolute(trimmed)
    ? trimmed
    : path.resolve(projectDir, trimmed);
  const sqlUri = url.pathToFileURL(absPath).toString();
  sqlProjItemKindBySqlUri.set(toSqlProjMembershipKey(sqlUri), kind);
}

function ingestSqlProj(absSqlProjPath: string): void {
  const projectXml = fs.readFileSync(absSqlProjPath, "utf8");
  const projectDir = path.dirname(absSqlProjPath);

  const collect = (tagName: string, kind: SqlProjItemKind): void => {
    const rx = new RegExp(`<${tagName}\\b[^>]*\\bInclude\\s*=\\s*\"([^\"]+)\"[^>]*>`, "gi");
    for (const m of projectXml.matchAll(rx)) {
      registerSqlProjItem(projectDir, String(m[1] ?? ""), kind);
    }
  };

  collect("Build", "build");
  collect("None", "none");
  collect("PreDeploy", "preDeploy");
  collect("PostDeploy", "postDeploy");
  collect("Content", "other");
}

async function rebuildSqlProjMembershipFromWorkspace(): Promise<void> {
  const folders = await connection.workspace.getWorkspaceFolders?.();
  sqlProjItemKindBySqlUri.clear();
  hasSqlProjInWorkspace = false;
  if (!folders) {
    return;
  }

  for (const folder of folders) {
    const folderPath = url.fileURLToPath(folder.uri);
    const sqlProjFiles = await fg.glob("**/*.sqlproj", { cwd: folderPath, absolute: true });
    if (sqlProjFiles.length > 0) {
      hasSqlProjInWorkspace = true;
    }
    for (const sqlProjFile of sqlProjFiles) {
      try {
        ingestSqlProj(sqlProjFile);
      } catch (err) {
        safeError(`Failed to parse ${sqlProjFile}`, err);
      }
    }
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

let indexWorkspaceInFlight: Promise<void> | null = null;
let indexWorkspaceQueued = false;

// Guard against concurrent reindexing: if a reindex is requested while one is already
// running (e.g. two .sqlproj change events in quick succession), the second caller
// doesn't start its own pass — it marks a re-run as queued and waits on the same
// in-flight promise. That promise only resolves once no more re-runs are queued, so
// every caller observes results from the most recent request, not a stale in-progress one.
async function indexWorkspace(): Promise<void> {
  if (indexWorkspaceInFlight) {
    indexWorkspaceQueued = true;
    await indexWorkspaceInFlight;
    return;
  }

  indexWorkspaceInFlight = runIndexWorkspaceUntilSettled();
  try {
    await indexWorkspaceInFlight;
  } finally {
    indexWorkspaceInFlight = null;
  }
}

async function runIndexWorkspaceUntilSettled(): Promise<void> {
  do {
    indexWorkspaceQueued = false;
    await indexWorkspaceInternal();
  } while (indexWorkspaceQueued);
}

async function indexWorkspaceInternal(): Promise<void> {
  setIndexNotReady();
  try {
    const folders = await connection.workspace.getWorkspaceFolders?.();
    if (!folders) {
      return;
    }

    workspaceDocuments.clear();
    definitions.clear();
    referencesIndex.clear();
    columnsByTable.clear();
    tablesByName.clear();
    tableTypesByName.clear();
    aliasesByUri.clear();
    tempTablesByUri.clear();
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
    setIndexReady();
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
      codeLensProvider: {
        resolveProvider: false
      },
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
    const deletedSqlUris = (change?.changes ?? [])
      .filter(c => c.type === FileChangeType.Deleted && toNormUri(c.uri).toLowerCase().endsWith(".sql"))
      .map(c => toNormUri(c.uri));

    for (const uri of deletedSqlUris) {
      clearDeletedSqlFileState(uri);
    }

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
      const before = shouldContributeToWorkspaceSchemaFor(previousMembership, previousHasSqlProj, uri);
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

    if (enableValidation) {
      await validateWorkspaceDocuments();
    } else {
      clearWorkspaceDiagnostics();
    }
  } catch (err) {
    safeError("Watched file change handling failed", err);
  }
});

documents.onDidOpen(async (e) => {
  workspaceDocuments.set(e.document.uri, e.document);
  if (isSqlProjectFileUri(e.document.uri)) {
    await indexWorkspace();
    await markIndexReady();
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
          await markIndexReady();
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
    return computeDefinition(doc, params.position, getParsedDocument(doc));
  } catch (err) {
    safeError("[onDefinition] handler error", err);
    return null;
  }
});


// --- Completion ---
connection.onCompletion((params: CompletionParams): CompletionItem[] => {
  try {
    const doc = documents.get(params.textDocument.uri) || documents.get(toNormUri(params.textDocument.uri));
    if (!doc) { return []; }
    return computeCompletion(doc, params.position, getParsedDocument(doc));
  } catch (err) {
    safeError("[completion] handler error", err);
    return [];
  }
});



// --- References ---
connection.onReferences((params: ReferenceParams): Location[] => {
  try {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) { return []; }
    return computeReferences(doc, params.position, getParsedDocument(doc));
  } catch (err) {
    safeError("[references] handler error", err);
    return [];
  }
});

// --- Code Actions ---
connection.onCodeAction((params: CodeActionParams): CodeAction[] => {
  try {
    const actions: CodeAction[] = [];
    const doc = documents.get(params.textDocument.uri);
    const docText = doc?.getText() ?? "";
    const lineStarts = doc ? getLineStarts(docText) : [];
    for (const diagnostic of params.context.diagnostics ?? []) {
      const action = buildReadableBareColumnCodeAction(params.textDocument.uri, diagnostic);
      if (action) {
        actions.push(action);
      }
      if (doc) {
        const noLockAction = buildUpdateNoLockCodeAction(
          params.textDocument.uri,
          diagnostic,
          docText,
          lineStarts
        );
        if (noLockAction) {
          actions.push(noLockAction);
        }
      }
    }
    if (doc) {
      const parsed = getParsedDocument(doc);
      const startOffset = (lineStarts[params.range.start.line] ?? 0) + params.range.start.character;
      const endOffset = (lineStarts[params.range.end.line] ?? 0) + params.range.end.character;
      actions.push(
        ...buildSelectStarExpansionCodeActions(
          params.textDocument.uri,
          parsed,
          lineStarts,
          tablesByName,
          tableTypesByName,
          startOffset,
          endOffset
        )
      );
    }
    return actions;
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

connection.onCodeLens((params): CodeLens[] => {
  try {
    const uri = toNormUri(params.textDocument.uri);
    const defs = definitions.get(uri) || [];
    const lenses: CodeLens[] = [];

    for (const def of defs) {
      const kind = String(def.kind ?? "").toUpperCase();
      if (kind !== "TABLE" && kind !== "VIEW" && kind !== "TYPE") {
        continue;
      }

      const locations = getDefinitionReferenceLocations(def);
      const count = locations.length;
      const title = `${count} reference${count === 1 ? "" : "s"}`;

      lenses.push({
        range: Range.create(def.line, 0, def.line, 0),
        command: {
          title,
          command: "saralsql.showReferences",
          arguments: [
            def.uri,
            { line: def.line, character: 0 },
            locations
          ]
        }
      });
    }

    return lenses;
  } catch (err) {
    safeError("[codeLens] handler error", err);
    return [];
  }
});

// ---------- Helpers ----------
export function getCurrentStatement(doc: TextDocument, position: Position): string {
  const text = doc.getText();
  const offset = doc.offsetAt(position);
  const parsed = getParsedDocument(doc);
  const fallbackLine = doc.getText({
    start: { line: position.line, character: 0 },
    end: { line: position.line, character: Number.MAX_VALUE }
  });
  return computeCurrentStatement(text, offset, parsed, fallbackLine);
}

function getCompletionParsedDocument(doc: TextDocument, offset: number): { parsed: ParseResult; offset: number } | null {
  const text = doc.getText();
  const patchedText = `${text.slice(0, offset)}__X__${text.slice(offset)}`;
  const parsed = parseSql(patchedText);
  if (!parsed?.ast) { return null; }
  return { parsed, offset: offset + 5 };
}

function findReferencesForWord(rawWord: string, doc: TextDocument, position?: Position): Location[] {
  const parsed = position ? (getParsedDocument(doc) ?? null) : null;
  return computeReferencesForWord(rawWord, doc, position, parsed);
}

async function doHover(doc: TextDocument, pos: Position): Promise<Hover | null> {
  return computeHover(doc, pos, getParsedDocument(doc));
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
      code: String(diag.code ?? ""),
      severity: mapSeverity(diag.severity, String(diag.code ?? "")),
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
          if (diagnostic && !shouldSuppressDiagnosticCode(String((issue as any).code ?? diagnostic.code ?? ""), disabledDiagnosticCodes)) {
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
      const diagCode = String((diag as any)?.code ?? "").toUpperCase();
      if (!schemaValidationReady && schemaDiagnosticCodes.has(diagCode)) {
        continue;
      }
      if (String((diag as any)?.code ?? "").toUpperCase() === "SQLCMD_UNRESOLVED_INCLUDE") {
        if (resolveExistingSqlCmdInclude(doc.uri, String((diag as any)?.message ?? ""))) {
          continue;
        }
      }
      // COL001: "Unknown column 'N' on 't'" — suppress when the alias 't' points to a CTE
      // whose header declares that column explicitly (e.g. WITH cteTally(N) AS (...)).
      // The parser validates CTE column references against the SELECT-list body, ignoring
      // the column names declared in the CTE header (location.columns).
      if (diagCode === "COL001" && typeof (diag as any).start === "number" && parsed?.scope?.root) {
        const msg = String((diag as any).message ?? "");
        const match = /Unknown column '([^']+)' on '([^']+)'/i.exec(msg);
        if (match) {
          const colName = normalizeName(match[1]);
          const aliasName = normalizeName(match[2]);
          const scopeAtDiag = parsed.scope.root.findInnermost?.((diag as any).start) ?? parsed.scope.root;
          const aliasSym = resolveSymbolCaseInsensitive(scopeAtDiag, aliasName);
          if (aliasSym?.kind === "Alias") {
            const targetName = normalizeName(resolveAliasTableName(aliasSym) ?? "");
            const cteSym = targetName ? resolveSymbolCaseInsensitive(scopeAtDiag, targetName) : null;
            if (cteSym?.kind === "CTE") {
              const headerCols: string[] = Array.isArray(cteSym.location?.columns)
                ? (cteSym.location.columns as any[]).map((c: any) => normalizeName(String(c ?? "")))
                : [];
              if (headerCols.length > 0 && headerCols.includes(colName)) {
                continue; // column is declared in the CTE header — suppress false positive
              }
            }
          }
        }
      }

      const diagnostic = toDiagnostic(diag, "SaralSQL Parser");
      if (diagnostic && !shouldSuppressDiagnosticCode(String((diag as any).code ?? diagnostic.code ?? ""), disabledDiagnosticCodes)) {
          if ((diagnostic.code === SARAL_DIAGNOSTIC_CODES.UnknownColumn || diagnostic.code === SARAL_DIAGNOSTIC_CODES.AmbiguousColumn) && typeof diag.start === "number") {
            const resolution = parsed.columns?.resolutions?.find((r: any) => r.location?.start === diag.start);
            if (resolution?.isUnverifiable) {
              continue;
            }
        }

        diagnostics.push(diagnostic);
      }
    }
  } catch (e) {
    safeLog(`[validate] Parser diagnostics failed: ${String(e)}`);
  }

  if (!parsed?.scope?.root) {
    maybePushSqlProjMissingFileDiagnostic(doc.uri, text, diagnostics);
    connection.sendDiagnostics({
      uri: doc.uri,
      diagnostics
    });
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

    connection.sendDiagnostics({
      uri: doc.uri,
      diagnostics: [...diagnostics]
    });
  }

  if (schemaValidationReady) {
    const schemaOpts: SchemaValidatorOptions = {
      disabledDiagnosticCodes,
      diagnosticSeverityOverrides,
      hasSqlProjInWorkspace,
      sqlProjStrictBuildMembership,
      schemaDiagnosticCodes
    };
    try {
      const normDocUri = toNormUri(doc.uri);
      const schemaDiags = computeSchemaDiagnostics(doc, text, lineStarts, parsed, normDocUri, schemaOpts);
      diagnostics.push(...schemaDiags);
    } catch (e) {
      safeLog(`[validate] Schema validation failed: ${String(e)}`);
    }
  }


  connection.console.log(
    `[validate] sending diagnostics count=${diagnostics.length}`
  );

  maybePushSqlProjMissingFileDiagnostic(doc.uri, text, diagnostics);

  connection.sendDiagnostics({
    uri: doc.uri,
    diagnostics
  });
}

function tableExists(tableName: string): boolean {
  const norm = normalizeName(tableName);
  if (isOutputPseudoTableReference(norm)) { return true; }
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
  if (isOutputPseudoTableReference(norm)) { return true; }
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

// ---------- Startup ----------
documents.listen(connection);
if (require.main === module) {
  connection.listen();
}

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
  , getCurrentStatement
  , getWordRangeAtPosition
  , extractAliases
  , resolveAlias
  , extractSelectAliasesFromStatement
  , isSqlKeyword
  , stripComments
  , stripStrings
  , offsetAt
} from "./text-utils";
import { parseSqlWithWorker, isAstPoolReady } from "./parser-pool";
import { resolveColumnFromAst, normalizeAstTableName, walkAst } from "./ast-utils";
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
  , findColumnInTable
  , findTableOrColumn
  , getRefs
  , tableTypesByName
} from "./definitions";
import {
  buildParamMapForDocAtPos,
  buildLocalTableMapForDocAtPos,
  collectCandidateTablesFromStatement,
  resolveAliasTarget, isTokenLocalTable
} from "./sql-scope-utils"; // adjust path to where you placed the utils    

// ---------- Connection + Documents ----------
const connection = createConnection(ProposedFeatures.all);
const documents: TextDocuments<TextDocument> = new TextDocuments(TextDocument);
let enableValidation = false;
const DEBUG = true; // set to false after debugging
const dbg = (...args: any[]) => { if (DEBUG) { console.debug('[HOVER]', ...args); } };

// Call this after building the definitions index
function markIndexReady() {
  setIndexReady();
  // Re-validate all open documents once we have a full index
  for (const doc of documents.all()) {
    validateTextDocument(doc);
  }
}

function toNormUri(rawUri: string) {
  try {
    // If it's already a file URI, normalize the path portion; otherwise convert path -> file:///
    let uri = rawUri;
    if (!uri.startsWith('file://')) {
      uri = url.pathToFileURL(uri).toString();
    }
    // decode percent encoding so file:///c%3A/... -> file:///c:/...
    const prefix = 'file:///';
    if (uri.toLowerCase().startsWith(prefix)) {
      const pathPart = decodeURIComponent(uri.substring(prefix.length));
      // normalize backslashes and lowercase drive letter on Windows
      const normalizedPath = pathPart.replace(/\\/g, '/').replace(/^([A-Za-z]):\//, (m, d) => d.toLowerCase() + ':/');
      // re-encode only the path portion to keep URI safe
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
      renameProvider: true,
      //codeActionProvider: true
    }
  };
});

connection.onInitialized(async () => {
  try {
    await indexWorkspace();
    markIndexReady();
  } catch (err) {
    safeError("Indexing failed", err);
  }
});

documents.onDidOpen((e) => {
  indexText(e.document.uri, e.document.getText());
  if (enableValidation) { validateTextDocument(e.document); }
});

documents.onDidClose((e) => {
  /*const uri = e.document.uri.startsWith("file://") ? e.document.uri : url.pathToFileURL(e.document.uri).toString();
  const oldDefs = definitions.get(uri) || [];
  for (const d of oldDefs) { tablesByName.delete(d.name); columnsByTable.delete(d.name); }
  definitions.delete(uri);
  aliasesByUri.delete(uri);
  deleteRefsForFile(uri);
  connection.sendDiagnostics({ uri, diagnostics: [] });*/
  //Do Nothing
});

const pending = new Map<string, NodeJS.Timeout>();
documents.onDidChangeContent((e) => {
  const uri = e.document.uri;
  const tmr = pending.get(uri); if (tmr) { clearTimeout(tmr); }
  pending.set(uri, setTimeout(() => {
    indexText(uri, e.document.getText());
    if (enableValidation) { validateTextDocument(e.document); }
    pending.delete(uri);
  }, 200));
});

// --- Definitions ---
connection.onDefinition(async (params: DefinitionParams): Promise<Location[] | null> => {
  const doc = documents.get(params.textDocument.uri);
  if (!doc) { return null; }

  const wordRange = getWordRangeAtPosition(doc, params.position);
  if (!wordRange) { return null; }

  const rawWord = doc.getText(wordRange);
  const word = normalizeName(rawWord);

  const fullText = doc.getText();
  const lineText = doc.getText({
    start: { line: params.position.line, character: 0 },
    end: { line: params.position.line, character: Number.MAX_VALUE }
  });


  // --- 0) Parameter references (NEW) ---
  if (rawWord.startsWith("@")) {
    const normUri = toNormUri(doc.uri);

    // gather all ReferenceDef entries that belong to this file (normUri)
    const refsForUri: ReferenceDef[] = [];
    for (const byUri of referencesIndex.values()) {
      const arr = byUri.get(normUri);
      if (arr && arr.length) { refsForUri.push(...arr); }
    }

    // Now find a parameter entry that exactly matches the token (case-insensitive)
    const match = refsForUri.find(r => r.kind === "parameter" && r.name.toLowerCase() === rawWord.toLowerCase());
    if (match) {
      return [{
        uri: match.uri,
        range: {
          start: { line: match.line, character: match.start },
          end: { line: match.line, character: match.end }
        }
      }];
    }

    // If not found as a parameter, do not fall back to column resolution (prevent mis-classification)
    return null;
  }

  // --- 1) Alias.column case (unchanged) ---
  if (rawWord.includes(".")) {
    const parts = rawWord.split(".");
    if (parts.length === 2) {
      const alias = parts[0].toLowerCase();
      const colName = normalizeName(parts[1]);
      const aliases = extractAliases(fullText);
      const tableName = aliases.get(alias);
      if (tableName) {
        return findColumnInTable(tableName, colName);
      }
    }
  }

  // --- 2) Bare column semantic resolution (with optional AST disambiguation) ---
  const bareColRegex = /\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
  let m: RegExpExecArray | null;
  while ((m = bareColRegex.exec(lineText))) {
    const col = normalizeName(m[1]);
    if (col === word) {
      // gather tables in scope from FROM/JOIN in the current statement (safer than whole file)
      const stmt = getCurrentStatement(doc, params.position) || fullText;
      const tablesInScope: string[] = [];
      const fromJoinRegex = /\b(from|join)\s+([a-zA-Z0-9_\[\]\.]+)/gi;
      let fm: RegExpExecArray | null;
      while ((fm = fromJoinRegex.exec(stmt))) {
        tablesInScope.push(normalizeName(fm[2]));
      }

      // filter tables that actually contain this column
      const candidateTables: string[] = [];
      for (const t of tablesInScope) {
        if (columnsByTable.get(t)?.has(col)) {
          candidateTables.push(t);
        }
      }

      // If multiple candidates, attempt AST-based disambiguation (short timeout)
      if (candidateTables.length > 1) {
        try {
          const ast = await parseSqlWithWorker(stmt, { database: "mssql" }, 500);
          const resolved = resolveColumnFromAst(ast, col); // may return normalized table/alias or null
          if (resolved) {
            // resolved could be an alias name — map via statement/global aliases if possible
            const aliases = extractAliases(fullText);
            const mappedTable = aliases.get(resolved.toLowerCase());
            const resolvedTable = mappedTable ? mappedTable : resolved;

            // Only accept the resolution if it matches one of the candidate tables
            const resolvedNorm = normalizeName(resolvedTable);
            if (candidateTables.includes(resolvedNorm)) {
              const found = findColumnInTable(resolvedNorm, col);
              if (found && found.length > 0) {
                return found;
              }
            }
          }
        } catch (e) {
          safeLog('[onDefinition][AST] parse failed or timed out: ' + String(e));
          // fall through to returning multiple candidates (existing behavior)
        }
      }

      // fallback: return all candidate locations (current behavior)
      const results: Location[] = [];
      for (const t of candidateTables) {
        results.push(...findColumnInTable(t, col));
      }
      if (results.length > 0) {
        return results;
      }
    }
  }

  // --- 3) Table-level keywords (INSERT, FROM, JOIN, UPDATE) ---
  const insertMatch = /insert\s+into\s+([a-zA-Z0-9_\[\]\.]+)/i.exec(fullText);
  if (insertMatch) {
    const res = findColumnInTable(insertMatch[1], word);
    if (res.length > 0) { return res; }
  }

  const fromMatch = /\bfrom\s+([a-zA-Z0-9_\[\]\.]+)(?!\s+[a-zA-Z0-9_]+)/i.exec(fullText);
  if (fromMatch) {
    const res = findColumnInTable(fromMatch[1], word);
    if (res.length > 0) { return res; }
  }

  const joinRegex = /\bjoin\s+([a-zA-Z0-9_\[\]\.]+)(?!\s+[a-zA-Z0-9_]+)/gi;
  let joinMatch: RegExpExecArray | null;
  while ((joinMatch = joinRegex.exec(fullText))) {
    const res = findColumnInTable(joinMatch[1], word);
    if (res.length > 0) { return res; }
  }

  const updateMatch = /\bupdate\s+([a-zA-Z0-9_\[\]\.]+)/i.exec(fullText);
  if (updateMatch) {
    const res = findColumnInTable(updateMatch[1], word);
    if (res.length > 0) { return res; }
  }

  // --- 4) Fallback global ---
  const fallback = findTableOrColumn(word);
  return fallback.length > 0 ? fallback : null;
});


// --- Completion ---
connection.onCompletion((params: CompletionParams): CompletionItem[] => {
  try {
    const rawUri = params.textDocument.uri;
    const normUri = rawUri.startsWith("file://") ? rawUri : url.pathToFileURL(rawUri).toString();

    // try both raw and normalized lookups for maximum robustness
    const doc = documents.get(rawUri) || documents.get(normUri);
    if (!doc) {
      safeLog(`[completion] no document found for ${rawUri}`);
      return [];
    }

    const position = params.position;
    // get the text of the current line up to cursor — do NOT trim (we need trailing '.' context)
    const linePrefix = doc.getText({
      start: { line: position.line, character: 0 },
      end: { line: position.line, character: position.character }
    });

    const items: CompletionItem[] = [];

    // ---------- Case A: alias-dot completions, e.g. "t." or "[t]." ----------
    // allow bracketed alias like [t]
    const aliasDotMatch = /([a-zA-Z0-9_\[\]]+)\.$/.exec(linePrefix);
    if (aliasDotMatch) {
      // alias token typed (may be bracketed like [lw])
      const aliasLower = normalizeName(aliasDotMatch[1]);

      // 1) First try statement-scoped aliases (closest match)
      let aliasesMap = new Map<string, string>();
      try {
        const stmtText = getCurrentStatement(doc, position);
        aliasesMap = extractAliases(stmtText); // returns Map<alias, tableName>
      } catch (e) {
        // fall through to file-level alias map if something goes wrong
        safeLog("[completion] extractAliases(statement) failed: " + String(e));
      }

      // 2) If alias not found in statement scope, fall back to file-level alias table
      if (!aliasesMap.has(aliasLower)) {
        const fileAliases = aliasesByUri.get(normUri) || aliasesByUri.get(rawUri) || new Map<string, string>();
        // merge fileAliases into aliasesMap but keep statement-scoped precedence
        for (const [k, v] of fileAliases.entries()) {
          if (!aliasesMap.has(k)) { aliasesMap.set(k, v); }
        }
      }

      const tableName = aliasesMap.get(aliasLower);
      if (tableName) {
        const def = tablesByName.get(tableName);
        if (def?.columns) {
          // Deduplicate columns by normalized name to handle [Col] vs Col
          const seenCols = new Set<string>();
          const cols: CompletionItem[] = [];
          for (const col of (def.columns as any[])) {
            const normCol = normalizeName(col.rawName || col.name || "");
            if (seenCols.has(normCol)) { continue; }
            seenCols.add(normCol);
            cols.push({
              label: col.rawName,
              kind: CompletionItemKind.Field,
              detail: col.type ? `Column in ${def.rawName} (${col.type})` : `Column in ${def.rawName}`,
              insertText: col.rawName
            });
          }
          return cols;
        }
      } else {
        // helpful debug log so we can see unresolved aliases in enterprise runs
        safeLog(`[completion] alias '${aliasDotMatch}' not found in statement or file aliases for ${normUri}. linePrefix='${linePrefix.slice(-80)}'`);
      }
    }

    // ---------- Case B: after FROM / JOIN -> suggest table names ----------
    if (/\b(from|join)\s+[a-zA-Z0-9_\[\]]*$/i.test(linePrefix)) {
      for (const def of tablesByName.values()) {
        items.push({
          label: def.rawName,
          kind: CompletionItemKind.Class,
          detail: `Table defined in ${def.uri.split("/").pop()}:${def.line + 1}`
        });
      }
      return items;
    }

    // ---------- Case C: inside SELECT -> suggest columns & tables (bounded) ----------
    if (/\bselect\s+.*$/i.test(linePrefix)) {
      let colCount = 0;
      const COL_LIMIT = 500; // safety cap
      for (const def of tablesByName.values()) {
        if ((def as any).columns) {
          for (const col of (def as any).columns) {
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
      for (const def of tablesByName.values()) {
        items.push({
          label: def.rawName,
          kind: CompletionItemKind.Class,
          detail: `Table defined in ${def.uri.split("/").pop()}:${def.line + 1}`
        });
      }
      return items;
    }

    // ---------- Fallback: table names + SQL keywords ----------
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

// --- References ---
connection.onReferences((params: ReferenceParams): Location[] => {
  const doc = documents.get(params.textDocument.uri);
  if (!doc) { return []; }

  const range = getWordRangeAtPosition(doc, params.position);
  if (!range) { return []; }

  const rawWord = doc.getText(range);
  return findReferencesForWord(rawWord, doc, params.position);
});

// --- Renames ---
connection.onRenameRequest((params: RenameParams): WorkspaceEdit | null => {
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

    // ensure valid file:// URI
    const uri = loc.uri.startsWith("file://") ? loc.uri : url.pathToFileURL(loc.uri).toString();

    if (!editsByUri[uri]) { editsByUri[uri] = []; }
    editsByUri[uri].push({ range: loc.range, newText: newName });
  }

  return { changes: editsByUri };
});

// --- Hover ---
connection.onHover(async (params): Promise<Hover | null> => {
  const doc = documents.get(params.textDocument.uri);
  if (!doc) {
    return null;
  }
  return await doHover(doc, params.position);
});

// --- Symbols
connection.onDocumentSymbol((params): DocumentSymbol[] => {
  const uri = params.textDocument.uri;
  const defs = definitions.get(uri) || [];
  const symbols: DocumentSymbol[] = [];

  for (const def of defs) {
    const tableSymbol: DocumentSymbol = {
      name: def.rawName,
      kind: SymbolKind.Class, // table/view/etc.
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
});

// --- Helper: check if a token represents a local table variable (starts with @ or #) ---
function isRawTokenLocal(tok: string | null | undefined): boolean {
  if (!tok) { return false; }
  const s = String(tok).trim();
  if (s.startsWith("@") || s.startsWith("#")) { return true; }
  const n = normalizeName(s);
  return n.startsWith("@") || n.startsWith("#");
}

// --- Helper: produce a set of candidate table/key names from a raw token ---
const tableKeyCandidates = (rawToken: string | null | undefined): string[] => {
  if (!rawToken) { return []; }
  const token = String(rawToken).trim();
  if (!token) { return []; }

  const norm = normalizeName(token); // canonical e.g. "schema.table" or "@param"
  const short = norm.includes('.') ? norm.split('.').pop()! : norm;

  const keys = new Set<string>();
  keys.add(norm);
  keys.add(short);
  keys.add(norm.toLowerCase());
  keys.add(short.toLowerCase());

  if (!norm.startsWith("@") && !norm.startsWith("#")) {
    for (const pfx of ["@", "#"]) {
      keys.add(pfx + norm);
      keys.add(pfx + short);
      keys.add((pfx + norm).toLowerCase());
      keys.add((pfx + short).toLowerCase());
    }
  } else {
    const unpref = norm.replace(/^[@#]/, "");
    const unprefShort = unpref.includes('.') ? unpref.split('.').pop()! : unpref;
    keys.add(unpref);
    keys.add(unprefShort);
    keys.add(unpref.toLowerCase());
    keys.add(unprefShort.toLowerCase());
  }

  return Array.from(keys).filter(Boolean);
};

// --- Helper: lookup a localTableMap entry using several common key shapes (@k, #k, unprefixed) ---
const lookupLocalByKey = (localTableMap: Map<string, any>, k: string | null | undefined) => {
  if (!k) { return undefined; }
  const key = normalizeName(String(k).trim());
  return localTableMap.get(key) ||
    localTableMap.get(`@${key}`) ||
    localTableMap.get(`#${key}`) ||
    localTableMap.get(key.replace(/^[@#]/, ""));
};

// --- Helper: synthesize local table entries for TVP parameters (so @param behaves like a table variable) ---
function synthesizeParamBackedLocals(localTableMap: Map<string, any>, paramMap: Map<string, string>) {
  try {
    for (const [pname, pval] of paramMap.entries()) {
      try {
        if (!pname || !(pname.startsWith("@") || pname.startsWith("#"))) { continue; }
        const typeToken = String(pval).split(/\s+/).find(tok => tok && tok.toLowerCase() !== "readonly");
        if (!typeToken) { continue; }
        const typeKey = normalizeName(typeToken.replace(/^dbo\./i, ""));
        const typeDef = tableTypesByName.get(typeKey) || tablesByName.get(typeKey);
        if (!typeDef || !typeDef.columns) { continue; }
        const synthCols = (typeDef.columns as any[]).map(cc => ({ name: cc.rawName ?? cc.name, type: cc.type }));
        if (!localTableMap.has(pname)) {
          localTableMap.set(pname, { name: pname, columns: synthCols });
        }
      } catch {
        /* ignore inner synthesis errors */
      }
    }
  } catch {
    /* ignore outer synthesis errors */
  }
}

// --- Helper: augment candidateTables set with tokens discovered in FROM/JOIN clauses ---
function augmentCandidateTablesFromFromJoin(candidateTables: Set<string>, cleanedStmt: string) {
  try {
    const fromJoinRe = /\b(?:from|join)\s+([^\s,()]+)(?:\s+(?:as\s+)?([A-Za-z0-9_\[\]"]+))?/gi;
    for (const m of cleanedStmt.matchAll(fromJoinRe)) {
      if (!m) { continue; }
      const tableTok = m[1];
      if (tableTok) {
        const tableNorm = normalizeName(String(tableTok));
        candidateTables.add(tableNorm);
        candidateTables.add(tableNorm.split('.').pop()!);
      }
      const aliasTok = m[2];
      if (aliasTok) {
        const aliasNorm = normalizeName(String(aliasTok));
        candidateTables.add(aliasNorm);
      }
    }
  } catch {
    /* ignore augmentation errors */
  }
}

// --- Helper: decide whether a localTableMap entry should be treated as a genuine local table ---
function isLocalEntryValid(localDef: any, candidateToken?: string | null | undefined): boolean {
  if (!localDef) { return false; }
  if (isRawTokenLocal(candidateToken)) { return true; }
  try {
    const n = String(localDef.name || "").trim();
    if (n.startsWith("@") || n.startsWith("#")) { return true; }
  } catch { /* ignore */ }
  return false;
}

// --- Main hover function ---
async function doHover(doc: TextDocument, pos: Position): Promise<Hover | null> {
  try {

    // === types & helpers for typed column access ===
    type ColumnDef = { name: string; rawName?: string; type?: string };
    const asCols = (arr: any): ColumnDef[] => (arr || []) as ColumnDef[];

    // unified hover formatter with TVP/param and suggestion support
    function formatColumnHover(opts: {
      colRaw: string,
      colType?: string | undefined,
      containerRawName: string,
      containerIsType?: boolean,
      aliasToken?: string | null,
      range?: any,
      suggestAlias?: boolean,
      suggestionAlias?: string | null
    }) {
      const { colRaw, colType, containerRawName, containerIsType, aliasToken, range, suggestAlias, suggestionAlias } = opts;
      const kindLabel = containerIsType ? "table type" : "table";

      // alias/parameter display
      let aliasPart = "";
      if (aliasToken) {
        if (String(aliasToken).startsWith("@") || String(aliasToken).startsWith("#")) {
          aliasPart = ` (parameter \`${aliasToken}\`)`;
        } else {
          aliasPart = ` (alias \`${aliasToken}\`)`;
        }
      }

      // suggestion line when token was unqualified but an alias is available
      let suggestionPart = "";
      if (suggestAlias && suggestionAlias) {
        suggestionPart = `\n\n_Suggestion:_ qualify as \`${suggestionAlias}.${colRaw}\``;
      }

      const typePart = colType ? ` — ${colType}` : "";
      const value = `**Column** \`${colRaw}\`${typePart}\n\nDefined in **${kindLabel}** \`${containerRawName}\`${aliasPart}${suggestionPart}`;
      return { contents: { kind: MarkupKind.Markdown, value }, range };
    }

    const findAliasForDef = (def: any, stmtAliases?: Map<string, string>): string | null => {
      if (!stmtAliases || !def) { return null; }
      try {
        const defNameNorm = normalizeName(def.rawName ?? def.name);
        for (const [aliasKey, mappedTable] of stmtAliases.entries()) {
          const aliasTrim = String(aliasKey).trim();

          // only accept aliasKey that looks like an identifier and is not a SQL keyword
          if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(aliasTrim)) { continue; }
          if (isSqlKeyword(aliasTrim.toLowerCase())) { continue; }

          if (normalizeName(mappedTable) === defNameNorm) {
            // If alias is effectively the same as the table name, skip it
            if (normalizeName(aliasTrim) === defNameNorm) { continue; }

            // return original-cased alias
            return aliasTrim;
          }
        }
      } catch {
        // ignore errors and return null
      }
      return null;
    };

    // helper: unified catalog/local search (catalog + locals, returns Hover or ambiguous)
    // candidateKeys should be tableKeyCandidates(normalizedName)
    const lookupColumnInCandidates = (
      candidateKeys: string[],
      columnToken: string,
      colNorm: string,
      rangeParam?: any,
      aliasesMap?: Map<string, string>,
      wasUnqualified?: boolean
    ): Hover | null => {
      const rangeToUse = typeof rangeParam !== "undefined" ? rangeParam : (typeof range !== "undefined" ? (range as any) : undefined);

      const foundSources: { kind: "local" | "catalog", name: string, hover: Hover }[] = [];
      const seenSourceKeys = new Set<string>();

      // small helpers for guard logic
      const stripDecorators = (s: string | null | undefined) =>
        String(s || "").replace(/^\.+/, "").replace(/[\[\]"`]/g, "").trim();

      const isTrueLocalToken = (raw: string | null | undefined) => {
        const cleaned = stripDecorators(raw);
        return cleaned.startsWith("@") || cleaned.startsWith("#");
      };

      try {
        // 1) scan locals (TVP-backed, temp tables, etc.)
        for (const k of candidateKeys) {
          const local = lookupLocalByKey(localTableMap, k);
          if (!local || !local.columns) { continue; }

          // GUARD: decide whether this local is appropriate for the candidateKeys being resolved
          try {
            const localRawName = String(local.rawName ?? local.name ?? k);
            const cleanedLocalRaw = stripDecorators(localRawName);
            const localIsTrue = isTrueLocalToken(localRawName);
            const localNormKey = normalizeName(cleanedLocalRaw);

            // If this is a true local token (starts with @/#) but candidateKeys do NOT include the local's key,
            // skip it. This prevents @Foo from shadowing dbo.Foo when resolving columns for dbo.Foo.
            if (localIsTrue && !candidateKeys.includes(localNormKey)) {
              continue;
            }

            // If this is a synthetic local (not @/#) and any candidateKey corresponds to a catalog/type,
            // prefer the catalog and skip this synthetic local.
            if (!localIsTrue) {
              let foundInCatalog = false;
              for (const ck of candidateKeys) {
                if (tablesByName.has(ck) || tableTypesByName.has(ck)) { foundInCatalog = true; break; }
              }
              if (foundInCatalog) {
                // skip synthetic local that would shadow a real catalog entry
                continue;
              }
            }
          } catch {
            // if guard logic fails, fall back to using the local entry
          }

          const cLocal = asCols(local.columns).find(cc =>
            (cc.rawName ?? cc.name) === columnToken ||
            normalizeName(cc.name) === colNorm
          );
          if (cLocal) {
            const containerName = local.rawName ?? local.name ?? k;

            // if the candidate key itself is a param/tablevar token, prefer showing the original token when possible.
            // Many `k` values are normalized keys; try to see if stmtAliases or candidateTables have an original form for this local.
            let aliasTokenCandidate: string | null = null;
            // If the local appears as a param key (starts with @/#) use that key. We try to find original-cased key in stmtAliases map.
            if (String(k).startsWith("@") || String(k).startsWith("#")) {
              // search stmtAliases for a mapping that maps to this local container (so we can show the actual alias text)
              if (stmtAliases) {
                for (const [aliasKey, mapped] of stmtAliases.entries()) {
                  if (normalizeName(mapped) === normalizeName(containerName) || normalizeName(mapped) === normalizeName(String(k))) {
                    aliasTokenCandidate = aliasKey; // aliasKey preserves original casing
                    break;
                  }
                }
              }
              // fallback to the normalized k as display if we didn't find original, but prefer original found above.
              if (!aliasTokenCandidate) {
                aliasTokenCandidate = k;
              }
            }

            const suggestionAlias = wasUnqualified && stmtAliases ? findAliasForDef({ rawName: containerName, name: containerName }, stmtAliases) : null;
            const hover = formatColumnHover({
              colRaw: cLocal.rawName ?? cLocal.name,
              colType: (cLocal as any).type,
              containerRawName: containerName,
              containerIsType: false,
              aliasToken: aliasTokenCandidate,
              range: rangeToUse,
              suggestAlias: !!suggestionAlias,
              suggestionAlias
            });
            const sourceKey = `local:${String(containerName).toLowerCase()}`;
            if (!seenSourceKeys.has(sourceKey)) {
              foundSources.push({ kind: "local", name: containerName, hover });
              seenSourceKeys.add(sourceKey);
            }
          }
        }
      } catch (e) {
        // non-fatal
      }

      // 2) scan catalog tables / table-types
      try {
        for (const k of candidateKeys) {
          const candKeys = tableKeyCandidates(String(k));
          for (const ck of candKeys) {
            let def = tablesByName.get(ck);
            let isType = false;
            if (!def) {
              const tdef = tableTypesByName.get(ck);
              if (tdef) { isType = true; def = tdef; }
            }
            if (!def || !def.columns) { continue; }

            const c = (def.columns as any[]).find(cc =>
              (cc.rawName ?? cc.name) === columnToken ||
              normalizeName(cc.name) === colNorm
            );
            if (c) {
              const containerName = def.rawName ?? def.name;
              // find alias (if any) using stmtAliases or aliasesMap — aliasKey preserves original casing



              const aliasTokenForDef = findAliasForDef(def, aliasesMap || stmtAliases);
              const suggestionAlias = (wasUnqualified && aliasTokenForDef) ? aliasTokenForDef : null;
              const hover = formatColumnHover({
                colRaw: c.rawName ?? c.name,
                colType: c.type,
                containerRawName: containerName,
                containerIsType: isType,
                aliasToken: aliasTokenForDef,
                range: rangeToUse,
                suggestAlias: !!suggestionAlias,
                suggestionAlias
              });
              const sourceKey = `catalog:${String(containerName).toLowerCase()}`;
              if (!seenSourceKeys.has(sourceKey)) {
                foundSources.push({ kind: "catalog", name: containerName, hover });
                seenSourceKeys.add(sourceKey);
              }
              break; // stop checking other keys for same def
            }
          }
        }
      } catch (e) {
        // non-fatal
      }

      // Resolve results:
      if (foundSources.length === 0) { return null; }
      if (foundSources.length === 1) { return foundSources[0].hover; }

      // Multiple sources -> ambiguous
      const uniqueNames = Array.from(new Set(foundSources.map(f => f.name)));
      const display = `**Column** \`${columnToken}\` (ambiguous — found in: ${uniqueNames.join(", ")})`;
      return { contents: { kind: MarkupKind.Markdown, value: display }, range: rangeToUse };
    };


    // helper: quick direct table/type listing hover (table name hovered)
    const tableOrTypeHover = (key: string): Hover | null => {
      const def = tablesByName.get(key) || tableTypesByName.get(key);
      if (!def || !def.columns) { return null; }
      const rows = (def.columns as any[]).map(c => `- \`${(c.rawName ?? c.name)}\`${c.type ? ` ${c.type}` : ""}`);
      const kindLabel = tablesByName.has(key) ? `Table` : `Table Type`;
      const nameLabel = def.rawName ?? def.name;
      const body = `**${kindLabel}** \`${nameLabel}\`\n\n${rows.join("\n")}`;
      return { contents: { kind: MarkupKind.Markdown, value: body }, range } as Hover;
    };

    // get word range at cursor and bail if none
    const range = getWordRangeAtPosition(doc, pos);
    if (!range) { return null; }

    // read hovered token and normalized forms
    const rawWord = doc.getText(range);
    const rawWordStripped = normalizeName(rawWord);
    const rawWordDisplay = rawWord;
    let handledUpdateLhs = false;



    // identifier check
    const isIdentifierLike = (s: string) =>
      /^[A-Za-z_][A-Za-z0-9_$.]*$/.test(String(s).replace(/^\.+/, ""));

    // decide if token is qualified (alias.column) and split into qualifier + column token
    const isQualifiedToken = rawWordStripped.includes(".");

    // Only skip if the hovered token is a pure SQL keyword and *not* part of a qualified identifier.
    if (isSqlKeyword(rawWordStripped) && !isQualifiedToken) {
      return null;
    }

    let hoveredQualifier: string | null = null;
    let hoveredColumnToken = rawWordStripped;
    if (isQualifiedToken) {
      const parts = rawWordStripped.split(".");
      hoveredQualifier = parts.slice(0, -1).join(".");
      hoveredColumnToken = parts[parts.length - 1];
    }

    // normalized column token for lookups
    const wordNorm = normalizeName(hoveredColumnToken);

    // get statement text and apply small cleaning for table hints
    const stmtText = getCurrentStatement(doc, pos) || "";
    const stmtNoComments = stripComments(stmtText);
    const cleanedStmt = stmtNoComments.replace(/WITH\s*\((?:NOLOCK|READUNCOMMITTED|UPDLOCK|HOLDLOCK|ROWLOCK|FORCESEEK|INDEX\([^)]*\)|FASTFIRSTROW|XLOCK|REPEATABLEREAD|SERIALIZABLE|,)+?\s*\)/gi, ' ');
    const paramMap = buildParamMapForDocAtPos(doc, pos);

    // statement-level alias map for consistent hovers (used widely below)
    let stmtAliases: Map<string, string> | undefined;
    try {
      stmtAliases = extractAliases(stmtText || cleanedStmt || "");
    } catch { stmtAliases = undefined; }

    // quick resolution if hovering directly over a parameter name
    if (rawWord.startsWith("@") || rawWordStripped.startsWith("@")) {
      const lookup = paramMap.get(rawWordStripped.toLowerCase());
      if (lookup) {
        // attempt to preserve original casing when printing parameter name:
        const paramDisplay = rawWord.startsWith("@") ? rawWord : rawWordStripped;
        return {
          contents: { kind: MarkupKind.Markdown, value: `**Parameter** \`${paramDisplay}\` — \`${lookup}\`` },
          range
        } as Hover;
      }
    }

    // build local table map and synthesize any TVP-backed locals
    const localTableMap = buildLocalTableMapForDocAtPos(doc, pos);
    synthesizeParamBackedLocals(localTableMap, paramMap);


    const checkPreferredTargetDirect = (targetNorm: string, targetRaw?: string): Hover | null => {
      try {
        // Helper: check if a token is a true local token (starts with @ or #)
        const looksLikeLocalToken = (tok: string | undefined | null) => {
          if (!tok) { return false; }
          return String(tok).trim().startsWith("@") || String(tok).trim().startsWith("#");
        };

        const targetLooksLocal = looksLikeLocalToken(targetRaw) || looksLikeLocalToken(targetNorm);

        // If target is NOT a local token, prefer catalog lookup first (avoid @tv shadowing dbo.Table)
        if (!targetLooksLocal) {
          // 1) catalog lookup (table / table type) for the single target — prefer this
          const candKeys = tableKeyCandidates(String(targetNorm));
          for (const ck of candKeys) {
            let def = tablesByName.get(ck);
            let isType = false;
            if (!def) {
              const tdef = tableTypesByName.get(ck);
              if (tdef) { isType = true; def = tdef; }
            }
            if (!def || !def.columns) { continue; }

            const c = (def.columns as any[]).find(cc =>
              (cc.rawName ?? cc.name) === hoveredColumnToken ||
              normalizeName(cc.name) === wordNorm
            );
            if (c) {
              const containerName = def.rawName ?? def.name;

              // choose alias/token display
              let aliasDisplay: string | null = null;
              if (targetRaw) {
                const trimmed = String(targetRaw).trim();
                if (trimmed.startsWith("@") || trimmed.startsWith("#") || /^[A-Za-z_][A-Za-z0-9_]*$/.test(trimmed)) {
                  aliasDisplay = trimmed;
                }
              }
              if (!aliasDisplay) {
                aliasDisplay = findAliasForDef(def, stmtAliases) ?? null;
              }

              return formatColumnHover({
                colRaw: c.rawName ?? c.name,
                colType: c.type,
                containerRawName: containerName,
                containerIsType: isType,
                aliasToken: aliasDisplay ?? null,
                range,
                suggestAlias: false,
                suggestionAlias: null
              });
            }
          }

          // If we got here, no catalog match — fall through to local lookup below.
        }

        // Next: attempt local / TVP-backed lookup (only if appropriate)
        // Lookup local by the canonical targetNorm (existing behavior). But guard:
        // - If the local is a true table-var (#/@) and the caller requested a catalog target,
        //   we should NOT accept it (this case handled by preferring catalog above).
        const local = lookupLocalByKey(localTableMap, targetNorm);
        if (local && local.columns) {
          // Determine if this local is a true local token
          const localRawName = String(local.rawName ?? local.name ?? targetNorm);
          const cleanedLocalRaw = localRawName.replace(/^\.+/, "").replace(/[\[\]"`]/g, "").trim();
          const isTrueLocal = cleanedLocalRaw.startsWith("@") || cleanedLocalRaw.startsWith("#");

          // If target was a catalog-like token and this local is true (@/#) **and** target didn't look local,
          // do not accept the local (prevents @TransportRequests from shadowing dbo.TransportRequests).
          if (isTrueLocal && !targetLooksLocal) {
            // skip local
          } else {
            // accept this local
            const cLocal = asCols(local.columns).find(cc =>
              (cc.rawName ?? cc.name) === hoveredColumnToken ||
              normalizeName(cc.name) === wordNorm
            );
            if (cLocal) {
              const containerName = local.rawName ?? local.name ?? targetNorm;

              // Prefer showing the original token when targetRaw is a param/table-var or a simple identifier.
              let aliasTokenCandidate: string | null = null;
              if (targetRaw) {
                const trimmed = String(targetRaw).trim();
                if (trimmed.startsWith("@") || trimmed.startsWith("#")) {
                  aliasTokenCandidate = trimmed;
                } else if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(trimmed)) {
                  aliasTokenCandidate = trimmed;
                }
              }
              if (!aliasTokenCandidate && (String(targetNorm).startsWith("@") || String(targetNorm).startsWith("#"))) {
                aliasTokenCandidate = targetNorm;
              }

              return formatColumnHover({
                colRaw: cLocal.rawName ?? cLocal.name,
                colType: (cLocal as any).type,
                containerRawName: containerName,
                containerIsType: false,
                aliasToken: aliasTokenCandidate ?? null,
                range,
                suggestAlias: false,
                suggestionAlias: null
              }) as Hover;
            }
          }
        }

        // If target looked like a local (starts with @/#) we didn't check catalog earlier.
        // In that case, or if catalog/local both failed earlier, try catalog now as a last attempt.
        if (targetLooksLocal) {
          const candKeys2 = tableKeyCandidates(String(targetNorm));
          for (const ck of candKeys2) {
            let def = tablesByName.get(ck);
            let isType = false;
            if (!def) {
              const tdef = tableTypesByName.get(ck);
              if (tdef) { isType = true; def = tdef; }
            }
            if (!def || !def.columns) { continue; }

            const c = (def.columns as any[]).find(cc =>
              (cc.rawName ?? cc.name) === hoveredColumnToken ||
              normalizeName(cc.name) === wordNorm
            );
            if (c) {
              const containerName = def.rawName ?? def.name;

              const aliasTokenForDef = findAliasForDef(def, stmtAliases);
              const suggestionAlias = (false && aliasTokenForDef) ? aliasTokenForDef : null;
              return formatColumnHover({
                colRaw: c.rawName ?? c.name,
                colType: c.type,
                containerRawName: containerName,
                containerIsType: isType,
                aliasToken: aliasTokenForDef,
                range,
                suggestAlias: false,
                suggestionAlias: null
              });
            }
          }
        }

      } catch (ex) {
        // non-fatal: return null if any errors
      }
      return null;
    };



    // -----------------------
    // Helper: early-resolve qualified column (e.g., "tsi.EmployeeId")
    // returns Hover or null
    // -----------------------
    const tryResolveQualifiedColumn = (qualifier: string | null, columnToken: string): Hover | null => {
      if (!qualifier || isSqlKeyword(qualifier)) { return null; }
      const qualNorm = normalizeName(String(qualifier));
      try {
        // map alias -> target using existing resolveAliasTarget; we capture raw/resolved forms
        let mappedRaw: string | null = null;
        let mappedNorm: string | null = null;
        try {
          const resolved = resolveAliasTarget(cleanedStmt || "", qualNorm);
          if (resolved) {
            mappedRaw = String(resolved); // preserve original casing of resolved target
            mappedNorm = normalizeName(mappedRaw);
          }
        } catch {
          mappedRaw = qualNorm;
          mappedNorm = qualNorm;
        }

        // 1) If mappedKey looks like a parameter-backed TVP or local, prefer local
        if (mappedNorm && (mappedNorm.startsWith("@") || mappedNorm.startsWith("#"))) {
          const pval = paramMap.get(mappedNorm.toLowerCase());
          if (pval) {
            const typeToken = String(pval).split(/\s+/).find(t => t && t.toLowerCase() !== "readonly");
            const typeKey = typeToken ? normalizeName(typeToken.replace(/^dbo\./i, "")) : null;
            const typeDef = typeKey ? (tableTypesByName.get(typeKey) || tablesByName.get(typeKey)) : null;
            if (typeDef && typeDef.columns) {
              const aliasColNorm = normalizeName(columnToken);
              const c = (typeDef.columns as any[]).find(cc =>
                (cc.rawName ?? cc.name) === columnToken ||
                normalizeName(cc.name) === aliasColNorm
              );
              if (c) {
                // show parameter using mappedRaw (original case) if available, fallback to mappedNorm
                return formatColumnHover({
                  colRaw: c.rawName ?? c.name,
                  colType: c.type,
                  containerRawName: typeDef.rawName ?? typeDef.name,
                  containerIsType: typeDef === tableTypesByName.get(typeKey!),
                  aliasToken: mappedRaw ?? mappedNorm,
                  range,
                  suggestAlias: false,
                  suggestionAlias: null
                }) as Hover;
              }
            }
          }

          // if localTableMap has this mappedKey, use that
          const local = lookupLocalByKey(localTableMap, mappedNorm!);
          if (local && isLocalEntryValid(local, mappedNorm!) && local.columns) {
            const aliasColNorm = normalizeName(columnToken);
            const cLocal = asCols(local.columns).find(cc => normalizeName(cc.name) === aliasColNorm || (cc.rawName ?? cc.name) === columnToken);
            if (cLocal) {
              return formatColumnHover({
                colRaw: cLocal.rawName ?? cLocal.name,
                colType: (cLocal as any).type,
                containerRawName: local.rawName ?? local.name,
                containerIsType: false,
                aliasToken: mappedRaw ?? mappedNorm,
                range,
                suggestAlias: false,
                suggestionAlias: null
              }) as Hover;
            }
          }
        }

        // 2) Otherwise check catalog tables / table types by mappedKey
        const candKeys = tableKeyCandidates((mappedNorm || qualNorm).replace(/^dbo\./i, ""));
        for (const k of candKeys) {
          let def = tablesByName.get(k);
          let isType = false;
          if (!def) {
            const tdef = tableTypesByName.get(k);
            if (tdef) { isType = true; def = tdef; }
          }
          if (!def || !def.columns) { continue; }
          const aliasColNorm = normalizeName(columnToken);
          const c = (def.columns as any[]).find(cc =>
            (cc.rawName ?? cc.name) === columnToken ||
            normalizeName(cc.name) === aliasColNorm
          );
          if (c) {
            const aliasTokenForDef = findAliasForDef(def, stmtAliases);
            return formatColumnHover({
              colRaw: c.rawName ?? c.name,
              colType: c.type,
              containerRawName: def.rawName ?? def.name,
              containerIsType: isType,
              // prefer showing mappedRaw (original case) if it looks param/var, otherwise show alias found
              aliasToken: (mappedRaw && (mappedRaw.startsWith("@") || mappedRaw.startsWith("#"))) ? mappedRaw : aliasTokenForDef,
              range,
              suggestAlias: false,
              suggestionAlias: null
            }) as Hover;
          }
        }
      } catch (ex) {
        // ignore and fall through to null
      }
      return null;
    };

    // collect candidate tables from statement text
    const candidateTables = collectCandidateTablesFromStatement(cleanedStmt || "") || new Set<string>();

    // include INSERT/MERGE/SELECT-...-INTO targets as candidates
    try {
      const insertIntoRe = /\binsert\s+into\s+([@#]?[a-zA-Z0-9_\[\]\."]+)/gi;
      const mergeIntoRe = /\bmerge\s+into\s+([@#]?[a-zA-Z0-9_\[\]\."]+)/gi;
      const selectIntoRe = /\bselect\b[\s\S]*?\binto\s+([@#]?[a-zA-Z0-9_\[\]\."]+)/gi;

      for (const m of cleanedStmt.matchAll(insertIntoRe)) {
        if (m && m[1]) {
          const tnRaw = m[1];
          const tn = normalizeName(tnRaw);
          candidateTables.add(tn);
          candidateTables.add(tn.split('.').pop()!);
        }
      }
      for (const m of cleanedStmt.matchAll(mergeIntoRe)) {
        if (m && m[1]) {
          const tnRaw = m[1];
          const tn = normalizeName(tnRaw);
          candidateTables.add(tn);
          candidateTables.add(tn.split('.').pop()!);
        }
      }
      for (const m of cleanedStmt.matchAll(selectIntoRe)) {
        if (m && m[1]) {
          const tnRaw = m[1];
          const tn = normalizeName(tnRaw);
          candidateTables.add(tn);
          candidateTables.add(tn.split('.').pop()!);
        }
      }
    } catch {
      /* ignore insert/merge/select-into errors */
    }

    // augment candidateTables with explicit FROM/JOIN tokens
    augmentCandidateTablesFromFromJoin(candidateTables, cleanedStmt);

    // --- prefer UPDATE target if hover is on left side of SET assignment (robust; run AFTER candidateTables are built) ---
    try {
      const updateMatch = /\bupdate\s+([@#]?[A-Za-z0-9_\[\]\."]+)(?:\s+(?:as\s+)?([A-Za-z0-9_]+))?/i.exec(cleanedStmt);
      if (updateMatch && updateMatch[1]) {
        const updateTokenRaw = updateMatch[1];                     // original token casing preserved
        const updateTokenNorm = normalizeName(updateTokenRaw);     // normalized for lookups
        let resolvedUpdateRaw: string | null = null;
        let resolvedUpdateNorm: string = updateTokenNorm;
        try {
          const resolved = resolveAliasTarget(cleanedStmt || "", updateTokenNorm);
          if (resolved) {
            resolvedUpdateRaw = String(resolved);
            resolvedUpdateNorm = normalizeName(resolvedUpdateRaw);
          }
        } catch { /* ignore alias resolution */ }

        const setRe = /\bset\b/i.exec(cleanedStmt);
        if (setRe) {
          const stmtAbsStart = doc.getText().indexOf(cleanedStmt);
          const setRegionStart = setRe.index + setRe[0].length;
          const tailAfterSet = cleanedStmt.slice(setRegionStart);
          const tailEndMatch = /\b(from|where)\b/i.exec(tailAfterSet);
          const setRegionEnd = tailEndMatch ? (setRegionStart + tailEndMatch.index) : cleanedStmt.length;
          const assignsText = cleanedStmt.slice(setRegionStart, setRegionEnd);

          // capture left-hand targets (allow qualified identifier)
          const lhsRe = /([@#]?[A-Za-z0-9_\[\]"`\.]+(?:\.[@#]?[A-Za-z0-9_\[\]"`\.]+)*)\s*=/g;
          let m: RegExpExecArray | null;
          const hoverOffset = doc.offsetAt(range.start);

          while ((m = lhsRe.exec(assignsText)) !== null) {
            const leftTok = m[1];
            const leftStartInAssign = m.index;
            const leftAbsStart = (stmtAbsStart >= 0 ? stmtAbsStart : 0) + setRegionStart + leftStartInAssign;
            const leftAbsEnd = leftAbsStart + leftTok.length;

            // if cursor is within the left-hand token's absolute span -> we are hovering LHS
            if (hoverOffset >= leftAbsStart && hoverOffset <= leftAbsEnd) {



              const leftNorm = normalizeName(leftTok);
              const leftIsQualified = leftNorm.includes(".");
              const lhsColToken = leftIsQualified ? leftNorm.split(".").pop()! : leftNorm;
              const lhsQualifierRaw = leftIsQualified ? leftTok.split(".").slice(0, -1).join(".") : null;
              const lhsQualifierNorm = leftIsQualified ? leftNorm.split(".").slice(0, -1).join(".") : null;

              // 1) strict direct check against resolved update target (use raw for display when available)
              const updateHoverStrict = checkPreferredTargetDirect(resolvedUpdateNorm.replace(/^dbo\./i, ""), resolvedUpdateRaw ?? updateTokenRaw);
              if (updateHoverStrict) { handledUpdateLhs = true; return updateHoverStrict; }

              // 2) if token is qualified, resolve that qualifier specifically and try strict direct check for it first
              if (leftIsQualified && lhsQualifierNorm) {
                try {
                  let mappedQualifierRaw: string | null = null;
                  let mappedQualifierNorm: string = lhsQualifierNorm;
                  try {
                    const resolved = resolveAliasTarget(cleanedStmt || "", lhsQualifierNorm);
                    if (resolved) {
                      mappedQualifierRaw = String(resolved);
                      mappedQualifierNorm = normalizeName(mappedQualifierRaw);
                    }
                  } catch {
                    mappedQualifierRaw = lhsQualifierRaw;
                    mappedQualifierNorm = lhsQualifierNorm;
                  }

                  if (mappedQualifierNorm) {
                    const strictQualHover = checkPreferredTargetDirect(String(mappedQualifierNorm).replace(/^dbo\./i, ""), mappedQualifierRaw ?? lhsQualifierRaw ?? undefined);
                    if (strictQualHover) { handledUpdateLhs = true; return strictQualHover; }

                    // fallback to lookupColumnInCandidates for the mapped qualifier (non-strict)
                    const qualKeys = tableKeyCandidates(String(mappedQualifierNorm).replace(/^dbo\./i, ""));
                    const qualHover = lookupColumnInCandidates(
                      qualKeys,
                      lhsColToken,
                      normalizeName(lhsColToken),
                      range,
                      stmtAliases,
                      false // explicitly qualified
                    );
                    if (qualHover) { handledUpdateLhs = true; return qualHover; }
                  }
                } catch {
                  // non-fatal: continue to fallback scanning
                }
              }

              // 3) fallback: try all candidateTables (use normalized keys)
              for (const rawTname of Array.from(candidateTables)) {
                const candidateNorm = normalizeName(String(rawTname).trim());
                const candKeys = tableKeyCandidates(candidateNorm);
                const candHover = lookupColumnInCandidates(
                  candKeys,
                  lhsColToken,
                  normalizeName(lhsColToken),
                  range,
                  stmtAliases,
                  !leftIsQualified
                );
                if (candHover) { handledUpdateLhs = true; return candHover; }
              }

              break; // matched LHS span, stop scanning assigns
            }
          }
        }
      }
    } catch {
      /* ignore UPDATE-SET-left errors */
    }

    // If we're hovering a qualified token like "tsi.EmployeeId", resolve it early and return hover
    if (isQualifiedToken && hoveredQualifier && !isSqlKeyword(hoveredQualifier)) {
      const early = tryResolveQualifiedColumn(hoveredQualifier, hoveredColumnToken);
      if (early) { return early; }
    }

    // `INSERT INTO <target> ( ... )` column list parentheses (not for the SELECT list following it).
    try {
      // Run regex against the original statement text so match offsets align with the doc text.
      const insRe = /\binsert\s+into\s+([^\s(]+)\s*\(\s*([^\)]+)\)/i;
      const m = insRe.exec(stmtText || "");
      if (m && m[1] && m[2]) {
        // Compute absolute offsets using stmtText (directly present in doc).
        const stmtAbsStart = doc.getText().indexOf(stmtText);
        if (stmtAbsStart >= 0) {
          const matchStartInStmt = m.index;        // index inside stmtText
          const matchText = m[0];
          const openParenInMatch = matchText.indexOf("(");
          const closeParenInMatch = matchText.lastIndexOf(")");
          if (openParenInMatch >= 0 && closeParenInMatch >= 0 && closeParenInMatch > openParenInMatch) {
            const colsAbsStart = stmtAbsStart + matchStartInStmt + openParenInMatch + 1; // just after '('
            const colsAbsEnd = stmtAbsStart + matchStartInStmt + closeParenInMatch;      // index of ')'
            const hoverOffset = doc.offsetAt(range.start);

            // Only prefer INSERT target if hover is inside the column-list region.
            if (hoverOffset >= colsAbsStart && hoverOffset <= colsAbsEnd) {
              const insertTargetRaw = m[1]; // preserve raw token text for display (original casing)
              const insertTargetNorm = normalizeName(String(insertTargetRaw)).replace(/^dbo\./i, "");
              const insertColsRaw = m[2];
              const insertCols = new Set<string>(
                insertColsRaw.split(",").map(s => normalizeName(String(s).trim()))
              );

              // If hovered column name is listed in the INSERT column list, prefer the INSERT target.
              if (insertCols.has(wordNorm)) {
                // Strict direct check first (preserves raw token display)
                const insertStrict = checkPreferredTargetDirect(insertTargetNorm, insertTargetRaw);
                if (insertStrict) { return insertStrict; }

                // fallback to normal lookup for the target (pass stmtAliases so suggestions/alias display work)
                const targetKeys = tableKeyCandidates(insertTargetNorm);
                const insertHover = lookupColumnInCandidates(
                  targetKeys,
                  rawWord,
                  wordNorm,
                  range,
                  stmtAliases,
                  !isQualifiedToken
                );
                if (insertHover) { return insertHover; }
              }
            }
          }
        }
      }
    } catch {
      /* ignore insert-target preference errors */
    }

    // direct hover over a local table name (e.g., @tvp or #temp)
    if (isRawTokenLocal(rawWord)) {
      const localDefDirect = localTableMap.get(normalizeName(rawWordStripped));
      if (localDefDirect && isLocalEntryValid(localDefDirect, rawWordStripped)) {
        const rows = localDefDirect.columns.map(c => `- \`${c.name}\`${c.type ? ` ${c.type}` : ""}`);
        const body = `**Local table** \`${localDefDirect.name}\`\n\n${rows.join("\n")}`;
        return { contents: { kind: MarkupKind.Markdown, value: body }, range } as Hover;
      }
    }

    // direct hover over a catalog table or table-type name (use helper)
    if (isIdentifierLike(rawWordStripped) && !isSqlKeyword(rawWordStripped)) {
      const directHover = tableOrTypeHover(rawWordStripped);
      if (directHover) { return directHover; }
    }

    // hover over an alias token: map alias -> target (including param-backed TVPs)
    try {
      const aliasLookupToken = (isQualifiedToken && hoveredQualifier) ? hoveredQualifier : rawWordStripped;
      if (!isSqlKeyword(aliasLookupToken)) {
        const aliasTargetRawOrNull = (() => {
          try {
            const r = resolveAliasTarget(cleanedStmt || "", aliasLookupToken);
            return r ? String(r) : null;
          } catch { return null; }
        })();

        if (aliasTargetRawOrNull) {
          const mappedRawDisplay = String(aliasTargetRawOrNull).replace(/^\.+/, "").replace(/[\[\]"`]/g, "").trim();
          const mappedNorm = normalizeName(String(aliasTargetRawOrNull));

          try {
            const mnorm = normalizeName(String(mappedRawDisplay || "").trim());
            if (mnorm && (mnorm.startsWith("@") || mnorm.startsWith("#"))) {
              const paramTypeRaw = paramMap.get(mnorm.toLowerCase());
              if (paramTypeRaw) {
                const typeToken = String(paramTypeRaw).split(/\s+/).find(t => t && t.toLowerCase() !== "readonly");
                const typeKey = typeToken ? normalizeName(typeToken.replace(/^dbo\./i, "")) : null;
                const typeDef = typeKey ? (tableTypesByName.get(typeKey) || tablesByName.get(typeKey)) : null;
                if (typeDef && typeDef.columns) {
                  const rows = (typeDef.columns as any[]).map(c => `- \`${c.rawName ?? c.name}\`${c.type ? ` ${c.type}` : ""}`);
                  const header = `**Alias** \`${rawWordDisplay}\` → table type \`${typeDef.rawName ?? typeDef.name}\` (parameter \`${mappedRawDisplay}\`)`;
                  const body = `${header}\n\n${rows.join("\n")}`;
                  return { contents: { kind: MarkupKind.Markdown, value: body }, range } as Hover;
                }
              }
            }
          } catch {
            /* ignore alias->param hover errors */
          }

          if (isRawTokenLocal(mappedNorm)) {
            const localMapped = localTableMap.get(mappedNorm);
            if (localMapped && isLocalEntryValid(localMapped, mappedRawDisplay)) {
              const rows = localMapped.columns.map(c => `- \`${c.name}\`${c.type ? ` ${c.type}` : ""}`);
              const body = `**Alias** \`${rawWordDisplay}\` → local table \`${localMapped.name}\`\n\n${rows.join("\n")}`;
              return { contents: { kind: MarkupKind.Markdown, value: body }, range } as Hover;
            }
          }

          let def = tablesByName.get(mappedNorm);
          let defIsType = false;
          if (!def) {
            def = tableTypesByName.get(mappedNorm);
            if (def) { defIsType = true; }
          }
          if (def) {
            if (!isQualifiedToken) {
              const rows: string[] = [];
              if (def.columns && Array.isArray(def.columns)) {
                for (const c of def.columns) {
                  const cname = c.rawName ?? c.name;
                  const ctype = c.type ? ` ${c.type}` : "";
                  rows.push(`- \`${cname}\`${ctype}`);
                }
              }
              const header = defIsType
                ? `**Alias** \`${rawWordDisplay}\` → table type \`${def.rawName ?? def.name}\``
                : `**Alias** \`${rawWordDisplay}\` → table \`${def.rawName ?? def.name}\``;
              const body = `${header}\n\n${rows.join("\n")}`;
              return { contents: { kind: MarkupKind.Markdown, value: body }, range } as Hover;
            }
          }
        }
      }
    } catch {
      /* ignore alias resolution errors */
    }

    // explicit alias.column handling (resolve alias then column, preferring local entries and TVP types)
    try {
      const aliasInfo = resolveAlias(rawWord, doc, pos);
      if (aliasInfo && aliasInfo.table && aliasInfo.column) {
        let resolvedTargetRaw: string | null = null;
        let resolvedTargetNorm: string | null = null;
        try {
          const mapped = resolveAliasTarget(cleanedStmt || "", normalizeName(String(aliasInfo.table)));
          if (mapped) {
            resolvedTargetRaw = String(mapped);
            resolvedTargetNorm = normalizeName(resolvedTargetRaw);
          } else {
            const fullText = doc.getText();
            const fullTextCleaned = fullText.replace(/WITH\s*\((?:NOLOCK|READUNCOMMITTED|UPDLOCK|HOLDLOCK|ROWLOCK|FORCESEEK|INDEX\([^)]*\)|FASTFIRSTROW|XLOCK|REPEATABLEREAD|SERIALIZABLE|,)+?\s*\)/gi, ' ');
            const mappedFull = resolveAliasTarget(fullTextCleaned, normalizeName(String(aliasInfo.table)));
            if (mappedFull) {
              resolvedTargetRaw = String(mappedFull);
              resolvedTargetNorm = normalizeName(String(mappedFull));
            } else {
              resolvedTargetRaw = String(aliasInfo.table);
              resolvedTargetNorm = normalizeName(String(aliasInfo.table));
            }
          }
        } catch {
          resolvedTargetRaw = String(aliasInfo.table);
          resolvedTargetNorm = normalizeName(String(aliasInfo.table));
        }

        const aliasTableKey = normalizeName(String(resolvedTargetRaw).replace(/^dbo\./i, ""));

        try {
          const mappedNorm = normalizeName(String(resolvedTargetRaw || "").trim());
          if (mappedNorm && (mappedNorm.startsWith("@") || mappedNorm.startsWith("#"))) {
            const paramTypeRaw = paramMap.get(mappedNorm.toLowerCase());
            if (paramTypeRaw) {
              let typeToken = paramTypeRaw.split(/\s+/).find(t => t && t.toLowerCase() !== "readonly");
              if (typeToken) {
                const typeKey = normalizeName(typeToken.replace(/^dbo\./i, ""));
                const typeDef = tableTypesByName.get(typeKey) || tablesByName.get(typeKey);
                if (typeDef && typeDef.columns) {
                  const aliasColNorm = aliasInfo.column ? normalizeName(aliasInfo.column) : null;
                  const c = (typeDef.columns as any[]).find(cc =>
                    (cc.rawName ?? cc.name) === aliasInfo.column ||
                    (aliasColNorm !== null && normalizeName(cc.name) === aliasColNorm)
                  );
                  if (c) {
                    return formatColumnHover({
                      colRaw: c.rawName ?? c.name,
                      colType: c.type,
                      containerRawName: typeDef.rawName ?? typeDef.name,
                      containerIsType: typeDef === tableTypesByName.get(typeKey),
                      aliasToken: resolvedTargetRaw ?? mappedNorm,
                      range,
                      suggestAlias: false,
                      suggestionAlias: null
                    }) as Hover;
                  }
                }
              }
            }
          }
        } catch {
          /* ignore param TVP lookup error */
        }

        if (isRawTokenLocal(resolvedTargetNorm)) {
          const localTable = lookupLocalByKey(localTableMap, aliasTableKey);
          if (localTable && isLocalEntryValid(localTable, resolvedTargetRaw || aliasTableKey) && localTable.columns) {
            const aliasColNorm = aliasInfo.column ? normalizeName(aliasInfo.column) : null;
            const cLocal = asCols(localTable.columns).find(cc => normalizeName(cc.name) === aliasColNorm);
            if (cLocal) {
              return formatColumnHover({
                colRaw: cLocal.rawName ?? cLocal.name,
                colType: (cLocal as any).type,
                containerRawName: localTable.rawName ?? localTable.name,
                containerIsType: false,
                aliasToken: resolvedTargetRaw ?? resolvedTargetNorm,
                range,
                suggestAlias: false,
                suggestionAlias: null
              }) as Hover;
            }
          }
        }

        let def = tablesByName.get(aliasTableKey);
        let defIsType = false;
        if (!def) {
          const tdef = tableTypesByName.get(aliasTableKey);
          if (tdef) { defIsType = true; def = tdef; }
        }
        if (def && def.columns) {
          const aliasColNorm = aliasInfo.column ? normalizeName(aliasInfo.column) : null;
          const c = (def.columns as any[]).find(cc =>
            (cc.rawName ?? cc.name) === aliasInfo.column ||
            (aliasColNorm !== null && normalizeName(cc.name) === aliasColNorm)
          );
          if (c) {
            const aliasTokenForDef = findAliasForDef(def, stmtAliases);
            return formatColumnHover({
              colRaw: c.rawName ?? c.name,
              colType: c.type,
              containerRawName: def.rawName ?? def.name,
              containerIsType: defIsType,
              aliasToken: aliasTokenForDef,
              range,
              suggestAlias: false,
              suggestionAlias: null
            }) as Hover;
          }
        }
      }
    } catch {
      /* ignore alias.column resolution errors */
    }

    // ---------- Candidate-only column lookup with INSERT/UPDATE affinity ----------
    if (candidateTables.size > 0) {
      // Build preferred targets list (insert/update) preserving raw tokens
      let preferredTargets: { raw: string, norm: string }[] = [];
      try {
        const insertTargetMatch = /\binsert\s+into\s+([@#]?[a-zA-Z0-9_\[\]\."]+)/i.exec(cleanedStmt || "");
        if (insertTargetMatch && insertTargetMatch[1]) {
          const raw = insertTargetMatch[1];
          const norm = normalizeName(raw).replace(/^dbo\./i, "");
          preferredTargets.push({ raw, norm });
        }
      } catch { /* ignore */ }

      try {
        const updateTargetMatch = /\bupdate\s+([@#]?[a-zA-Z0-9_\[\]\."]+)/i.exec(cleanedStmt || "");
        if (updateTargetMatch && updateTargetMatch[1]) {
          const raw = updateTargetMatch[1];
          const norm = normalizeName(raw).replace(/^dbo\./i, "");
          preferredTargets.push({ raw, norm });
        }
      } catch { /* ignore */ }

      // 1) Try strict direct checks for preferred targets first (strong affinity)
      for (const pref of preferredTargets) {
        try {
          const strictHover = checkPreferredTargetDirect(pref.norm, pref.raw);
          if (strictHover) { return strictHover; }
        } catch {
          // non-fatal
        }
      }

      // 2) If strict pref checks didn't find anything, fall back to soft pref via unified lookup
      for (const pref of preferredTargets) {
        try {
          const prefKeys = tableKeyCandidates(String(pref.norm));
          const prefHover = lookupColumnInCandidates(
            prefKeys,
            rawWord,
            wordNorm,
            range,
            stmtAliases,
            !isQualifiedToken
          );
          if (prefHover) { return prefHover; }
        } catch { /* non-fatal */ }
      }

      // 3) Full scan (collect matches and detect ambiguity)
      const foundHovers: { hover: Hover; source: string }[] = [];
      for (const rawTname of Array.from(candidateTables)) {
        const candidateNorm = normalizeName(String(rawTname).trim());
        const candKeys = tableKeyCandidates(candidateNorm);

        const candHover = lookupColumnInCandidates(
          candKeys,
          rawWord,
          wordNorm,
          range,
          stmtAliases,
          !isQualifiedToken
        );
        if (candHover) {
          foundHovers.push({ hover: candHover, source: candidateNorm });
        }
      }

      if (foundHovers.length === 1) {
        return foundHovers[0].hover;
      } else if (foundHovers.length > 1) {
        const sources = Array.from(new Set(foundHovers.map(f => f.source)));
        const display = `**Column** \`${rawWord}\` (ambiguous — found in: ${sources.join(", ")})`;
        return { contents: { kind: MarkupKind.Markdown, value: display }, range } as Hover;
      }
    }

    // final heuristic: if statement is UPDATE prefer its target table (fallback)
    try {
      if (!handledUpdateLhs) {
        const updateMatch = /\bupdate\s+([@#]?[a-zA-Z0-9_\[\]\."]+)/i.exec(cleanedStmt);
        if (updateMatch && updateMatch[1]) {
          const rawUpdateMatch = updateMatch[1];
          const tnorm = normalizeName(rawUpdateMatch.replace(/^dbo\./i, ""));
          if (isRawTokenLocal(rawUpdateMatch)) {
            const local = lookupLocalByKey(localTableMap, tnorm);
            if (local && isLocalEntryValid(local, rawUpdateMatch) && local.columns) {
              const colDefLocal = asCols(local.columns || []).find(c => normalizeName(c.name) === wordNorm);
              if (colDefLocal) {
                return formatColumnHover({
                  colRaw: colDefLocal.rawName ?? colDefLocal.name,
                  colType: (colDefLocal as any).type,
                  containerRawName: local.rawName ?? local.name,
                  containerIsType: false,
                  aliasToken: rawUpdateMatch, // preserve original casing
                  range,
                  suggestAlias: false,
                  suggestionAlias: null
                }) as Hover;
              }
            }
          }
          const def = tablesByName.get(tnorm);
          if (def && def.columns) {
            const colDef = (def.columns as any[]).find(c => normalizeName((c.name || c.rawName || "")) === wordNorm);
            if (colDef) {
              const aliasTokenForDef = findAliasForDef(def, stmtAliases);
              return formatColumnHover({
                colRaw: colDef.rawName ?? colDef.name,
                colType: (colDef as any).type,
                containerRawName: def.rawName ?? def.name,
                containerIsType: false,
                aliasToken: aliasTokenForDef,
                range,
                suggestAlias: false,
                suggestionAlias: null
              }) as Hover;
            }
          }
        }
      }
    } catch {
      /* ignore final UPDATE heuristic errors */
    }

    // nothing matched
    return null;
  } catch {
    return null;
  }
}

// Full validator with @-skip and SELECT-alias skip
export async function validateTextDocument(doc: TextDocument): Promise<void> {
  // gating: if validation disabled or index not ready, clear diagnostics and return
  if (typeof enableValidation !== "undefined" && !enableValidation) {
    connection.sendDiagnostics({ uri: doc.uri, diagnostics: [] });
    return;
  }
  if (typeof getIndexReady === "function" && !getIndexReady()) {
    connection.sendDiagnostics({ uri: doc.uri, diagnostics: [] });
    return;
  }

  const diagnostics: Diagnostic[] = [];
  const text = doc.getText();
  const lines = text.split(/\r?\n/);

  // file-level aliases (fallback)
  let fileAliases = new Map<string, string>();
  try {
    fileAliases = extractAliases(text);
  } catch {
    fileAliases = new Map();
  }

  // declared cursors detection (skip)
  const cursorRegex = /\bdeclare\s+([a-zA-Z0-9_]+)\s+cursor/gi;
  const cursors = new Set<string>();
  {
    let cm: RegExpExecArray | null;
    while ((cm = cursorRegex.exec(text))) { cursors.add(cm[1].toLowerCase()); }
  }



  // iterate over lines
  for (let i = 0; i < lines.length; i++) {
    const rawLine = lines[i];
    // remove comments but preserve offsets for diagnostics
    const noComments = stripComments(rawLine);
    const cleanNoStr = stripStrings(noComments); // remove strings to avoid false positives
    const trimmedLc = cleanNoStr.trim().toLowerCase();

    // ---------- Table existence checks (from/join/update/into) ----------
    // allow table-like tokens to include # and @ so we can detect temp / table-variables
    const tableRegex = /\b(from|join|update|into)\s+([a-zA-Z0-9_\[\]\.#@]+)/gi;
    {
      let m: RegExpExecArray | null;
      while ((m = tableRegex.exec(cleanNoStr))) {
        const rawTable = m[2];

        // skip declared cursor names
        if (cursors.has(rawTable.toLowerCase())) { continue; }

        // if it's temp (#) or table-variable (@) then we skip "unknown table" diagnostic here,
        // because they aren't in global index; validator will check columns later using local map
        if (rawTable.startsWith("#") || rawTable.startsWith("@")) { continue; }

        const normRaw = normalizeName(rawTable);
        if (!columnsByTable.has(normRaw)) {
          diagnostics.push({
            severity: DiagnosticSeverity.Error,
            range: {
              start: { line: i, character: m.index },
              end: { line: i, character: m.index + rawTable.length }
            },
            message: `Unknown table '${rawTable}'`,
            source: "SaralSQL"
          });
        }
      }
    }

    // ---------- Alias.column validation (statement-scoped) ----------
    // Do alias.column checks only in SQL expression contexts (select/where/on/having/group/order)
    const looksLikeColumnContext = /(^|\b)(select|where|having|on|group\s+by|order\s+by)\b/.test(trimmedLc);
    if (looksLikeColumnContext) {
      // Use statement-scoped data for best accuracy
      const statementText = getCurrentStatement(doc, { line: i, character: 0 }) || "";
      const stmtAliases = extractAliases(statementText);
      // local table map for this statement/proc body (temp/table-vars)
      const localTableMap = buildLocalTableMapForDocAtPos(doc, { line: i, character: 0 } as Position);
      // candidate tables from statement
      const candidateTables = collectCandidateTablesFromStatement(statementText);

      // alias.column pattern - allow bracketed right side
      const colRegex = /([a-zA-Z0-9_]+)\.(\[[^\]]+\]|[a-zA-Z_][a-zA-Z0-9_]*)/g;
      let m: RegExpExecArray | null;
      while ((m = colRegex.exec(cleanNoStr))) {
        const aliasToken = m[1].toLowerCase();
        const rightRaw = m[2];

        // skip variable references
        if (aliasToken.startsWith("@")) { continue; }
        if (rightRaw.startsWith("@")) { continue; }

        // treat common schemas as schemas, not aliases
        if (/^(dbo|sys|information_schema|pg_catalog|public)$/.test(aliasToken)) { continue; }

        const col = normalizeName(rightRaw.replace(/[\[\]]/g, ""));

        // resolve alias -> target via stmtAliases, else fallback to fileAliases
        let target = stmtAliases.get(aliasToken) ?? fileAliases.get(aliasToken);

        if (!target) {
          // Unknown alias - report
          diagnostics.push({
            severity: DiagnosticSeverity.Error,
            range: {
              start: { line: i, character: m.index },
              end: { line: i, character: m.index + m[0].length }
            },
            message: `Unknown alias '${aliasToken}'`,
            source: "SaralSQL"
          });
          continue;
        }

        // If alias points to a subquery placeholder like "__subquery__", skip column existence check
        if (target === "__subquery__") { continue; }

        // If target is temp table or table-variable (starts with # or @) then consult localTableMap
        const targetStr = String(target);
        const key = normalizeName(targetStr.replace(/^dbo\./i, ""));
        const isTempOrVar = targetStr.startsWith("#") || targetStr.startsWith("@");
        if (isTempOrVar) {
          const localDef = localTableMap.get(key);
          if (!localDef) {
            // no local info -> skip validation (better UX than false positive)
            continue;
          }
          const hasCol = localDef.columns.some(c => normalizeName(c.name) === col);
          if (!hasCol) {
            diagnostics.push({
              severity: DiagnosticSeverity.Error,
              range: {
                start: { line: i, character: m.index },
                end: { line: i, character: m.index + m[0].length }
              },
              message: `Column '${col}' not found in local table '${localDef.name}'`,
              source: "SaralSQL"
            });
          }
        } else {
          // Normal catalog table -> check index
          const normTable = normalizeName(String(target).replace(/^dbo\./i, ""));
          const colSet = columnsByTable.get(normTable);
          if (!colSet || !colSet.has(col)) {
            diagnostics.push({
              severity: DiagnosticSeverity.Error,
              range: {
                start: { line: i, character: m.index },
                end: { line: i, character: m.index + m[0].length }
              },
              message: `Column '${col}' not found in table '${target}'`,
              source: "SaralSQL"
            });
          }
        }
      }
    }

    // ---------- Bare column checks (when not qualified by alias) ----------
    // Skip structural lines (FROM/UPDATE/INSERT/INTO/MERGE/APPLY) to avoid false positives
    const isStructuralLine =
      /^((left|right|full|inner|cross)\s+join|join|from|update|insert|into|merge|apply|cross\s+apply|outer\s+apply)\b/
        .test(trimmedLc);
    if (isStructuralLine) { continue; }

    const looksLikeColumnContext2 = /(^|\b)(select|where|having|on|group\s+by|order\s+by)\b/.test(trimmedLc);
    if (!looksLikeColumnContext2) { continue; }

    // Build the set of tables in-scope for the current statement
    const stmtTextForScope = getCurrentStatement(doc, { line: i, character: 0 }) || "";
    const tablesInScope: string[] = [];
    {
      const fromJoinRegex = /\b(from|join)\s+([a-zA-Z0-9_\[\]\.#@]+)/gi;
      let fm: RegExpExecArray | null;
      while ((fm = fromJoinRegex.exec(stmtTextForScope))) {
        const tk = fm[2];
        // skip temp / table-var from global scope list (we'll validate them separately)
        if (tk.startsWith("#") || tk.startsWith("@")) { continue; }
        tablesInScope.push(normalizeName(tk));
      }
    }

    // select list aliases to avoid marking them as missing columns
    const selectAliases = extractSelectAliasesFromStatement(stmtTextForScope);

    // Detect bare identifiers (bracketed or not). This regex approximates tokens in the line.
    const bareColRegex = /\[([a-zA-Z0-9_ ]+)\]|([a-zA-Z_][a-zA-Z0-9_]*)/g;
    {
      let m: RegExpExecArray | null;
      while ((m = bareColRegex.exec(cleanNoStr))) {
        const rawToken = (m[1] || m[2] || "");
        const tokenStart = m.index;

        // skip tokens that are actually part of a variable reference, e.g. "@var"
        const beforeChar = cleanNoStr.charAt(Math.max(0, tokenStart - 1));
        if (beforeChar === "@") { continue; }

        // skip SQL keywords
        const col = normalizeName(rawToken.replace(/[\[\]]/g, ""));
        if (isSqlKeyword(col)) { continue; }

        // skip tokens that are alias members (we'll validate those in alias.column block)
        const beforeSlice = cleanNoStr.slice(Math.max(0, tokenStart - 3), tokenStart);
        if (beforeSlice.endsWith(".")) { continue; }

        // skip "AS alias" patterns (alias definitions)
        const leftSlice = cleanNoStr.slice(0, tokenStart).toLowerCase();
        if (/\bas\s*$/.test(leftSlice)) { continue; }

        // skip select-list aliases
        if (selectAliases.has(col)) { continue; }

        // Now, check whether any table in-scope contains this column
        let found = false;
        for (const t of tablesInScope) {
          const set = columnsByTable.get(t);
          if (set && set.has(col)) { found = true; break; }
        }

        if (!found && tablesInScope.length > 0) {
          diagnostics.push({
            severity: DiagnosticSeverity.Error,
            range: {
              start: { line: i, character: m.index },
              end: { line: i, character: m.index + (m[0] ? m[0].length : rawToken.length) }
            },
            message: `Column '${col}' not found in any table in scope`,
            source: "SaralSQL"
          });
        }
      }
    }
  } // end for lines

  // send diagnostics
  connection.sendDiagnostics({ uri: doc.uri, diagnostics });
}

connection.onDocumentSymbol((params): DocumentSymbol[] => {
  const uri = params.textDocument.uri;
  const defs = definitions.get(uri) || [];
  const symbols: DocumentSymbol[] = [];

  for (const def of defs) {
    const tableSymbol: DocumentSymbol = {
      name: def.rawName,
      kind: SymbolKind.Class, // table/view/etc.
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
});

// ---------- Helpers ----------
function findReferencesForWord(rawWord: string, doc: TextDocument, position?: Position): Location[] {
  // Quick guard
  if (!rawWord || rawWord.trim().length === 0) { return []; }

  // Helpers
  const stripBrackets = (s: string) => String(s).replace(/^\[|\]$/g, "");
  const normalizeKey = (s: string) => normalizeName(stripBrackets(String(s)));
  const rawNorm = normalizeKey(rawWord);

  // Don't attempt to find references for keywords
  if (isSqlKeyword(rawNorm)) { return []; }

  const fullText = doc.getText();
  const stmtText = position ? getCurrentStatement(doc, position) || fullText : fullText;

  const results: Location[] = [];
  const seen = new Set<string>(); // uri:line:start:end

  const pushLocFromRef = (r: { uri: string; line: number; start: number; end: number }) => {
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

  // ---------
  // Utility: collect candidate tables from statement with knowledge of role
  // Returns: {sources: string[], targets: string[], all: string[]}
  // - sources: tables used in FROM/JOIN/USING/SELECT sources
  // - targets: tables that are targets (INSERT INTO, UPDATE, DELETE FROM, MERGE INTO, SELECT INTO)
  // - all: union of normalized table names
  // ---------
  function collectScopedTables(stmt: string): { sources: string[]; targets: string[]; all: string[] } {
    const src = new Set<string>();
    const tgt = new Set<string>();

    // normalize candidate and push helpers
    const addSource = (raw: string) => { const n = normalizeKey(raw.replace(/^dbo\./i, "")); if (n) { src.add(n); } };
    const addTarget = (raw: string) => { const n = normalizeKey(raw.replace(/^dbo\./i, "")); if (n) { tgt.add(n); } };

    // Regexes to capture different clauses. We keep them conservative and tolerant of brackets/quotes/aliases.
    // FROM/JOIN/USING: capture table or subquery alias (we only record the table token part)
    const fromJoinRegex = /\b(?:from|join|apply|using)\s+([A-Za-z0-9_\[\]"\.]+)(?:\s+(?:as\s+)?([A-Za-z0-9_\[\]"]+))?/gi;
    // INSERT INTO / INSERT ... INTO / SELECT ... INTO target
    const insertIntoRegex = /\binsert\s+(?:into\s+)?([A-Za-z0-9_\[\]"\.]+)\b/gi;
    const selectIntoRegex = /\bselect\b[\s\S]*?\binto\s+([A-Za-z0-9_\[\]"\.]+)\b/gi; // may match SELECT ... INTO target
    // UPDATE <table>
    const updateRegex = /\bupdate\s+([A-Za-z0-9_\[\]"\.]+)\b/gi;
    // DELETE FROM <table>
    const deleteRegex = /\bdelete\s+(?:from\s+)?([A-Za-z0-9_\[\]"\.]+)\b/gi;
    // MERGE INTO <table>
    const mergeRegex = /\bmerge\s+into\s+([A-Za-z0-9_\[\]"\.]+)\b/gi;
    // OUTPUT INTO <table>
    const outputIntoRegex = /\boutput\s+into\s+([A-Za-z0-9_\[\]"\.]+)\b/gi;

    let m: RegExpExecArray | null;
    while ((m = fromJoinRegex.exec(stmt))) {
      // m[1] is table token (may be schema.table). If it's a subquery "(" ignore.
      const token = (m[1] || "").trim();
      if (!token) { continue; }
      if (token.startsWith("(")) { continue; }
      addSource(token);
    }

    while ((m = insertIntoRegex.exec(stmt))) {
      const token = (m[1] || "").trim();
      if (!token) { continue; }
      addTarget(token);
    }

    while ((m = selectIntoRegex.exec(stmt))) {
      const token = (m[1] || "").trim();
      if (!token) { continue; }
      addTarget(token);
    }

    while ((m = updateRegex.exec(stmt))) {
      const token = (m[1] || "").trim();
      if (!token) { continue; }
      addTarget(token);
    }

    while ((m = deleteRegex.exec(stmt))) {
      const token = (m[1] || "").trim();
      if (!token) { continue; }
      addTarget(token);
    }

    while ((m = mergeRegex.exec(stmt))) {
      const token = (m[1] || "").trim();
      if (!token) { continue; }
      addTarget(token);
    }

    while ((m = outputIntoRegex.exec(stmt))) {
      const token = (m[1] || "").trim();
      if (!token) { continue; }
      addTarget(token);
    }

    // Also look for "INTO <table>" occurrences (covers some patterns)
    const intoRegex = /\binto\s+([A-Za-z0-9_\[\]"\.]+)\b/gi;
    while ((m = intoRegex.exec(stmt))) {
      const token = (m[1] || "").trim();
      if (!token) { continue; }
      addTarget(token);
    }

    // union all
    const all = new Set<string>([...src, ...tgt]);
    return {
      sources: Array.from(src),
      targets: Array.from(tgt),
      all: Array.from(all)
    };
  }

  // --------------------------
  // 1) Qualified forms (schema.table.column, table.column, alias.col)
  // --------------------------
  if (rawWord.includes(".")) {
    const partsRaw = rawWord.split(".");
    // 1.a handle common alias.col pattern preferentially (if we have position and can extract aliases)
    if (partsRaw.length === 2 && position) {
      const aliasRaw = stripBrackets(partsRaw[0]);
      const aliasNorm = normalizeKey(aliasRaw);
      const colNorm = normalizeKey(partsRaw[1]);

      try {
        const aliases = extractAliases(stmtText); // expected Map<string,string> or similar
        const mappedTable = aliases.get(aliasNorm) ?? aliases.get(aliasRaw.toLowerCase());
        if (mappedTable) {
          const key = `${normalizeKey(mappedTable)}.${colNorm}`;
          const refs = getRefs(key);
          for (const r of refs) { pushLocFromRef(r); }
          if (results.length > 0) { return results; }
        }
      } catch (e) {
        // best-effort alias resolution: fallback below
      }
    }

    // 1.b exact normalized key lookup (handles table.column and schema.table.column)
    const cleanedNorm = normalizeKey(rawWord);
    const direct = getRefs(cleanedNorm);
    for (const r of direct) { pushLocFromRef(r); }
    if (results.length > 0) { return results; }

    // 1.c suffix fallback across index (any key ending with `.table.column` or exact match)
    const suffix = cleanedNorm;
    for (const [key, byUri] of referencesIndex.entries()) {
      if (key === suffix || key.endsWith("." + suffix)) {
        for (const arr of byUri.values()) {
          for (const r of arr) { pushLocFromRef(r); }
        }
      }
    }
    return results;
  }

  // --------------------------
  // 2) Table exact match (global)
  // --------------------------
  {
    const tableRefs = getRefs(rawNorm);
    for (const r of tableRefs) { pushLocFromRef(r); }
    if (results.length > 0) { return results; }
  }

  // --------------------------
  // 3) Bare column resolution (improved scoping & ranking with INSERT/UPDATE/DELETE awareness)
  // --------------------------
  if (!rawWord.includes(".") && !rawWord.startsWith("@")) {
    try {
      // collect candidate tables with role (sources vs targets)
      const scoped = collectScopedTables(stmtText);
      const candidateTables = scoped.all;         // all normalized candidates in statement
      const targetTables = scoped.targets || [];  // destination tables (INSERT/UPDATE/DELETE/etc)
      const sourceTables = scoped.sources || [];  // source tables (FROM/JOIN)

      // build local map and partition into localPreferred vs remote
      const localMap = position ? buildLocalTableMapForDocAtPos(doc, position) : new Map<string, any>();
      const localPreferred: string[] = [];
      const remoteCandidates: string[] = [];
      for (const t of candidateTables) {
        if (isTokenLocalTable(t, localMap as any)) { localPreferred.push(t); }
        else { remoteCandidates.push(t); }
      }

      const addRefsForTableColKey = (tableKey: string, colNorm: string) => {
        const key = `${tableKey}.${colNorm}`;
        const refs = getRefs(key);
        if (refs && refs.length) {
          for (const r of refs) { pushLocFromRef(r); }
          return true;
        }
        return false;
      };

      // Priority A: If column is likely a target (e.g. UPDATE SET lhs, INSERT column list), prefer targets first.
      // Heuristic: if stmt contains targetTables, check them first.
      for (const t of targetTables) {
        if (addRefsForTableColKey(t, rawNorm)) { return results; }
      }

      // Priority B: alias-resolved tables within statement (if any map to candidates)
      try {
        const aliases = extractAliases(stmtText);
        for (const [alias, tableRef] of aliases.entries()) {
          const tableNorm = normalizeKey(String(tableRef));
          // if this alias maps to a candidate table, prefer it
          if (candidateTables.includes(tableNorm)) {
            if (addRefsForTableColKey(tableNorm, rawNorm)) { return results; }
          }
        }
      } catch (e) { /* best-effort */ }

      // Priority C: local preferred (temp / table vars / CTE)
      for (const t of localPreferred) {
        if (addRefsForTableColKey(t, rawNorm)) { return results; }
      }

      // Priority D: source tables from FROM/JOIN (remoteCandidates include these as well)
      // But prefer explicit sourceTables first
      for (const t of sourceTables) {
        if (addRefsForTableColKey(t, rawNorm)) { return results; }
      }

      // Then try remoteCandidates (other statement tables)
      for (const t of remoteCandidates) {
        if (addRefsForTableColKey(t, rawNorm)) {
          // Ambiguity handling: if multiple statement candidates existed and position provided,
          // kick off a background AST disambiguation to improve future lookups.
          if (candidateTables.length > 1 && position) {
            (async () => {
              try {
                if (isAstPoolReady()) {
                  const ast = await parseSqlWithWorker(stmtText, { database: "transactsql" }, 500);
                  if (ast) {
                    const resolved = resolveColumnFromAst(ast, rawNorm);
                    if (resolved) {
                      const aliases = extractAliases(stmtText);
                      const mapped = aliases.get(resolved.toLowerCase());
                      const resolvedTable = mapped ? mapped : resolved;
                      const resolvedNorm = normalizeKey(resolvedTable);
                      if (candidateTables.includes(resolvedNorm)) {
                        try {
                          const exact = findColumnInTable(resolvedNorm, rawNorm);
                          if (exact && exact.length > 0) {
                            for (const loc of exact) {
                              pushLocFromRef({
                                uri: loc.uri,
                                line: loc.range.start.line,
                                start: loc.range.start.character,
                                end: loc.range.end.character
                              });
                            }
                            // Optional: merge into referencesIndex for future speedups
                          }
                        } catch (e) { /* swallow */ }
                      }
                    }
                  }
                }
              } catch (e) {
                safeLog('[refs][AST] background disambiguation failed: ' + String(e));
              }
            })();
          }
          return results;
        }
      }

      // Final fallback: global suffix fallback
      for (const [key, byUri] of referencesIndex.entries()) {
        if (key.endsWith(`.${rawNorm}`)) {
          for (const arr of byUri.values()) {
            for (const r of arr) { pushLocFromRef(r); }
          }
        }
      }
      if (results.length > 0) { return results; }
    } catch (e) {
      // swallow - best-effort
    }
  }


  return results;
}

// --- Capture configuration updates ---
connection.onDidChangeConfiguration(change => {
  const settings = (change.settings || {}) as any;
  // expect setting under "saralsql.enableValidation"
  enableValidation = !!settings.saralsql?.enableValidation;
  safeLog(`Validation ${enableValidation ? "enabled" : "disabled"}`);

  // re-run validation if it just got enabled
  if (enableValidation) {
    for (const doc of documents.all()) {
      validateTextDocument(doc);
    }
  } else {
    // clear diagnostics if disabled
    for (const doc of documents.all()) {
      connection.sendDiagnostics({ uri: doc.uri, diagnostics: [] });
    }
  }
});

// ---------- Startup ----------
documents.listen(connection);
if (require.main === module) {
  // Only listen when this file is the entrypoint
  connection.listen();
}
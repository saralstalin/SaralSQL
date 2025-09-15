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

//hover function
async function doHover(doc: TextDocument, pos: Position): Promise<Hover | null> {
  try {

    const range = getWordRangeAtPosition(doc, pos);
    if (!range) {
      dbg('No range found at position', pos);
      return null;
    }

    const rawWord = doc.getText(range);
    const rawWordStripped = normalizeName(rawWord); // canonical for lookups
    const rawWordDisplay = rawWord; // keep original for human display in hover bodies

    // --- handle qualified tokens (alias.col) minimally ---
    const isQualifiedToken = rawWordStripped.includes(".");
    let hoveredQualifier: string | null = null;
    let hoveredColumnToken = rawWordStripped;
    if (isQualifiedToken) {
      const parts = rawWordStripped.split(".");
      hoveredQualifier = parts.slice(0, -1).join(".");
      hoveredColumnToken = parts[parts.length - 1];
    }

    // use column-only token for column lookups (normalized)
    const wordNorm = normalizeName(hoveredColumnToken);
    dbg('Hovered word:', { rawWord: rawWordDisplay, rawWordStripped, hoveredQualifier, hoveredColumnToken, wordNorm });

    const stmtText = getCurrentStatement(doc, pos) || "";
    // small cleaning of table-hints that break token shapes (idempotent)
    const cleanedStmt = stmtText.replace(/WITH\s*\((?:NOLOCK|READUNCOMMITTED|UPDLOCK|HOLDLOCK|ROWLOCK|FORCESEEK|INDEX\([^)]*\)|FASTFIRSTROW|XLOCK|REPEATABLEREAD|SERIALIZABLE|,)+?\s*\)/gi, ' ');
    dbg('Statement:', { original: stmtText, cleaned: cleanedStmt });

    // param/declare map from util
    const paramMap = buildParamMapForDocAtPos(doc, pos);

    // Helper: does the raw token look like a local table variable / parameter (starts with @ or #)?
    const isRawTokenLocal = (tok: string | null | undefined) => {
      if (!tok) { return false; }
      const s = String(tok).trim();
      if (s.startsWith("@") || s.startsWith("#")) { return true; }
      const n = normalizeName(s);
      return n.startsWith("@") || n.startsWith("#");
    };

    if (rawWord.startsWith("@") || rawWordStripped.startsWith("@")) {
      // use stripped token for lookup (keys in paramMap are normalized to lower-case '@name')
      const lookup = paramMap.get(rawWordStripped.toLowerCase());
      if (lookup) {
        dbg('Parameter found:', rawWordStripped, lookup);
        return {
          contents: {
            kind: MarkupKind.Markdown,
            value: `**Parameter** \`${rawWordStripped}\` — \`${lookup}\``
          },
          range
        } as Hover;
      }
      dbg('No parameter found for:', rawWordStripped);
    }

    // local table map from util (table vars, temp tables, learned insert/select)
    const localTableMap = buildLocalTableMapForDocAtPos(doc, pos);

    // Helper: lookup localTableMap under common key shapes (k, @k, #k, unprefixed)
    const lookupLocalByKey = (k: string | null | undefined) => {
      if (!k) { return undefined; }
      const key = normalizeName(String(k).trim());
      return localTableMap.get(key) ||
        localTableMap.get(`@${key}`) ||
        localTableMap.get(`#${key}`) ||
        localTableMap.get(key.replace(/^[@#]/, ""));
    };
    // --- synthesize parameter-backed local entries for TVPs ---
    // (ensures @param TVPs behave like DECLAREd table variables for hover resolution)
    try {
      for (const [pname, pval] of paramMap.entries()) {
        try {
          if (!pname || !(pname.startsWith("@") || pname.startsWith("#"))) { continue; }
          // pval examples: "TransportRequestsType READONLY" or "dbo.TransportRequestsType readonly"
          const typeToken = String(pval).split(/\s+/).find(tok => tok && tok.toLowerCase() !== "readonly");
          if (!typeToken) { continue; }
          const typeKey = normalizeName(typeToken.replace(/^dbo\./i, ""));
          const typeDef = tableTypesByName.get(typeKey) || tablesByName.get(typeKey);
          if (!typeDef || !typeDef.columns) { continue; }
          const synthCols = (typeDef.columns as any[]).map(cc => ({ name: cc.rawName ?? cc.name, type: cc.type }));
          if (!localTableMap.has(pname)) {
            localTableMap.set(pname, { name: pname, columns: synthCols });
            dbg('Synthesized localTableMap entry from paramMap', { pname, typeKey, cols: synthCols.map(c => c.name) });
          }
        } catch (e) {
          dbg('synth-param->local inner error', e);
        }
      }
    } catch (e) {
      dbg('synth-param->local outer error', e);
    }

    // canonical table key candidates generation (single source)
    const tableKeyCandidates = (rawToken: string | null | undefined): string[] => {
      if (!rawToken) { return []; }
      const token = String(rawToken).trim();
      if (!token) { return []; }

      const norm = normalizeName(token); // canonical e.g. "schema.table" or "@param"
      const short = norm.includes('.') ? norm.split('.').pop()! : norm;

      const keys = new Set<string>();

      // base normalized forms (full + short, lower/upper not strictly necessary but safe)
      keys.add(norm);
      keys.add(short);
      keys.add(norm.toLowerCase());
      keys.add(short.toLowerCase());

      // If the normalized token is not already @/# prefixed, also add @/# variants
      if (!norm.startsWith("@") && !norm.startsWith("#")) {
        for (const pfx of ["@", "#"]) {
          keys.add(pfx + norm);
          keys.add(pfx + short);
          keys.add((pfx + norm).toLowerCase());
          keys.add((pfx + short).toLowerCase());
        }
      } else {
        // If it *is* prefixed, also add the unprefixed variants to help reverse-lookup
        const unpref = norm.replace(/^[@#]/, "");
        const unprefShort = unpref.includes('.') ? unpref.split('.').pop()! : unpref;
        keys.add(unpref);
        keys.add(unprefShort);
        keys.add(unpref.toLowerCase());
        keys.add(unprefShort.toLowerCase());
      }

      // remove any empty strings and return
      return Array.from(keys).filter(Boolean);
    };


    // candidate tables (normalized names) from util, using cleanedStmt
    const candidateTables = collectCandidateTablesFromStatement(cleanedStmt || "") || new Set<string>();

    // Ensure INSERT/MERGE/SELECT-...-INTO targets are included as candidate tables (normalized)
    try {
      const insertIntoRe = /\binsert\s+into\s+([a-zA-Z0-9_\[\]\."]+)/gi;
      const mergeIntoRe = /\bmerge\s+into\s+([a-zA-Z0-9_\[\]\."]+)/gi;
      const selectIntoRe = /\bselect\b[\s\S]*?\binto\s+([a-zA-Z0-9_\[\]\."]+)/gi;

      for (const m of cleanedStmt.matchAll(insertIntoRe)) {
        if (m && m[1]) {
          const tn = normalizeName(m[1]);
          candidateTables.add(tn);
          candidateTables.add(tn.split('.').pop()!);
          dbg('Added INSERT INTO target to candidates:', m[1], tn);
        }
      }
      for (const m of cleanedStmt.matchAll(mergeIntoRe)) {
        if (m && m[1]) {
          const tn = normalizeName(m[1]);
          candidateTables.add(tn);
          candidateTables.add(tn.split('.').pop()!);
          dbg('Added MERGE INTO target to candidates:', m[1], tn);
        }
      }
      for (const m of cleanedStmt.matchAll(selectIntoRe)) {
        if (m && m[1]) {
          const tn = normalizeName(m[1]);
          candidateTables.add(tn);
          candidateTables.add(tn.split('.').pop()!);
          dbg('Added SELECT INTO target to candidates:', m[1], tn);
        }
      }
    } catch (e) {
      dbg('insert/merge/select-into candidate resolution error', e);
    }

    // --- Augment candidateTables with explicit FROM/JOIN table tokens and aliases (normalized) ---
    try {
      const fromJoinRe = /\b(?:from|join)\s+([^\s,()]+)(?:\s+(?:as\s+)?([A-Za-z0-9_\[\]"]+))?/gi;
      for (const m of cleanedStmt.matchAll(fromJoinRe)) {
        if (!m) { continue; }
        const tableTok = m[1];
        if (tableTok) {
          // canonicalize fully using normalizeName
          const tableNorm = normalizeName(String(tableTok));
          candidateTables.add(tableNorm);
          // also add unqualified short name
          candidateTables.add(tableNorm.split('.').pop()!);
        }
        const aliasTok = m[2];
        if (aliasTok) {
          // normalize alias too (so alias token is consistent with normalized keys)
          const aliasNorm = normalizeName(String(aliasTok));
          candidateTables.add(aliasNorm);
        }
      }
      dbg('Augmented candidateTables from FROM/JOIN:', Array.from(candidateTables));
    } catch (e) {
      dbg('Failed to augment candidateTables from FROM/JOIN', e);
    }

    dbg('Candidate tables final:', Array.from(candidateTables));

    // small helper: decide whether a localTableMap entry should be considered a true "local" table
    const isLocalEntryValid = (localDef: any, candidateToken?: string | null | undefined): boolean => {
      if (!localDef) { return false; }
      if (isRawTokenLocal(candidateToken)) { return true; }
      try {
        const n = String(localDef.name || "").trim();
        if (n.startsWith("@") || n.startsWith("#")) { return true; }
      } catch { /* ignore */ }
      return false;
    };

    // --------- INSERT-target preference (keeps earlier stricter derived-subquery check) ---------
    try {
      const insRe = /\binsert\s+into\s+([^\s(]+)\s*\(\s*([^\)]+)\)/i;
      const m = insRe.exec(cleanedStmt || "");
      if (m && m[1] && m[2]) {
        const insertTargetRaw = m[1];
        const insertColsRaw = m[2];
        const insertCols = new Set<string>(
          insertColsRaw.split(",").map(s => normalizeName(String(s).trim()))
        );
        dbg('Insert target detected:', insertTargetRaw, Array.from(insertCols));

        const stmtAbsStart = doc.getText().indexOf(cleanedStmt);
        const insideDerived = (() => {
          const fm = /\bfrom\s*\(/i.exec(cleanedStmt);
          if (!fm || stmtAbsStart < 0) { return false; }
          const parenStart = fm.index + fm[0].indexOf('(');
          let depth = 1;
          for (let i = parenStart + 1; i < cleanedStmt.length; i++) {
            const ch = cleanedStmt[i];
            if (ch === '(') { depth++; }
            else if (ch === ')') {
              depth--;
              if (depth === 0) {
                const absInnerStart = stmtAbsStart + parenStart + 1;
                const absInnerEnd = stmtAbsStart + i;
                const hoverOffset = doc.offsetAt(range.start);
                return hoverOffset >= absInnerStart && hoverOffset <= absInnerEnd;
              }
            }
          }
          return false;
        })();

        if (insertCols.has(wordNorm) || insideDerived) {
          const cleanedTarget = normalizeName(insertTargetRaw);
          const targetKeys = tableKeyCandidates(cleanedTarget);
          dbg('Insert target keys:', targetKeys, { insideDerived });

          // prefer local (but only if local entry is valid)
          for (const k of targetKeys) {
            const local = lookupLocalByKey(k);
            if (local && isLocalEntryValid(local, insertTargetRaw)) {
              const found = local.columns && (local.columns as any[]).find(cc => normalizeName(cc.name) === wordNorm);
              if (found) {
                const md = `**Column** \`${found.name}\`${found.type ? ` ${found.type}` : ""} in local table \`${local.name}\``;
                dbg('Insert-target matched local:', md);
                return { contents: { kind: MarkupKind.Markdown, value: md }, range } as Hover;
              }
            } else if (local) {
              dbg('Found localTableMap entry but skipping as not a genuine local entry:', k, local && local.name);
            }
          }

          // then catalog
          for (const k of targetKeys) {
            const def = tablesByName.get(k);
            if (def && def.columns) {
              const found = (def.columns as any[]).find(cc =>
                (cc.rawName ?? cc.name) === hoveredColumnToken ||
                normalizeName(cc.name) === wordNorm
              );
              if (found) {
                const md = `**Column** \`${found.rawName ?? found.name}\`${found.type ? ` ${found.type}` : ""} in table \`${def.rawName ?? def.name}\``;
                dbg('Insert-target matched catalog:', md);
                return { contents: { kind: MarkupKind.Markdown, value: md }, range } as Hover;
              }
            }
          }
          dbg('Insert-target indicated but not found in local/catalog for:', wordNorm);
        }
      }
    } catch (e) {
      dbg('Insert-target preference error:', e);
    }

    // ---------- rest of original resolution flow (kept intact) ----------

    // 1) Local table direct hover (token itself local)
    if (isRawTokenLocal(rawWord)) {
      const localDefDirect = localTableMap.get(normalizeName(rawWordStripped));
      if (localDefDirect && isLocalEntryValid(localDefDirect, rawWordStripped)) {
        const rows = localDefDirect.columns.map(c => `- \`${c.name}\`${c.type ? ` ${c.type}` : ""}`);
        const body = `**Local table** \`${localDefDirect.name}\`\n\n${rows.join("\n")}`;
        dbg('Local table hover:', localDefDirect.name, rows);
        return { contents: { kind: MarkupKind.Markdown, value: body }, range } as Hover;
      }
      dbg('No local table found for (or skipped non-local):', rawWordStripped);
    }

    // 2) Catalog table direct hover (also check table-types)
    const directTableDef = tablesByName.get(rawWordStripped);
    if (directTableDef) {
      const rows: string[] = [];
      if (directTableDef.columns && Array.isArray(directTableDef.columns)) {
        for (const c of directTableDef.columns) {
          const cname = c.rawName ?? c.name;
          const ctype = c.type ? ` ${c.type}` : "";
          rows.push(`- \`${cname}\`${ctype}`);
        }
      }
      const body = `**Table** \`${directTableDef.rawName ?? directTableDef.name}\`\n\n${rows.join("\n")}`;
      dbg('Catalog table hover:', directTableDef.name, rows);
      return { contents: { kind: MarkupKind.Markdown, value: body }, range } as Hover;
    }

    // If not a real table, check table-type index
    const directTypeDef = tableTypesByName.get(rawWordStripped);
    if (directTypeDef) {
      const rows: string[] = [];
      if (directTypeDef.columns && Array.isArray(directTypeDef.columns)) {
        for (const c of directTypeDef.columns) {
          const cname = c.rawName ?? c.name;
          const ctype = c.type ? ` ${c.type}` : "";
          rows.push(`- \`${cname}\`${ctype}`);
        }
      }
      const body = `**Table Type** \`${directTypeDef.rawName ?? directTypeDef.name}\`\n\n${rows.join("\n")}`;
      dbg('Catalog table-type hover:', directTypeDef.name, rows);
      return { contents: { kind: MarkupKind.Markdown, value: body }, range } as Hover;
    }

    // 3) Hover over an alias token (map alias -> target using extractAliases via util resolve)
    try {
      const aliasLookupToken = (isQualifiedToken && hoveredQualifier) ? hoveredQualifier : rawWordStripped;
      const aliasTarget = resolveAliasTarget(cleanedStmt || "", aliasLookupToken);
      if (aliasTarget) {
        const mappedRawDisplay = String(aliasTarget).replace(/^\.+/, "").replace(/[\[\]"`]/g, "").trim();
        const mappedNorm = normalizeName(String(aliasTarget));
        dbg('Alias target resolved:', { rawWord: rawWordStripped, aliasLookupToken, aliasTarget, mappedRawDisplay, mappedNorm });

        // --- NEW: if alias maps to a parameter (@...), prefer the parameter's TVP type for alias hover ---
        try {
          const mappedNorm = normalizeName(String(aliasTarget || "").trim()); // e.g. "@transportrequests"
          if (mappedNorm && (mappedNorm.startsWith("@") || mappedNorm.startsWith("#"))) {
            // paramMap was created earlier in doHover
            const paramTypeRaw = paramMap.get(mappedNorm.toLowerCase());
            if (paramTypeRaw) {
              // pick first non-READONLY token as type
              const typeToken = String(paramTypeRaw).split(/\s+/).find(t => t && t.toLowerCase() !== "readonly");
              const typeKey = typeToken ? normalizeName(typeToken.replace(/^dbo\./i, "")) : null;
              const typeDef = typeKey ? (tableTypesByName.get(typeKey) || tablesByName.get(typeKey)) : null;

              if (typeDef && typeDef.columns) {
                // Option A: return hover immediately showing the type columns (clean and direct)
                const rows = (typeDef.columns as any[]).map(c => `- \`${c.rawName ?? c.name}\`${c.type ? ` ${c.type}` : ""}`);
                const header = `**Alias** \`${rawWordDisplay}\` → table type \`${typeDef.rawName ?? typeDef.name}\` (parameter \`${mappedNorm}\`)`;
                const body = `${header}\n\n${rows.join("\n")}`;
                dbg('Alias -> parameter TVP hover (direct):', { mappedNorm, typeKey, rows: rows.length });
                return { contents: { kind: MarkupKind.Markdown, value: body }, range } as Hover;

                // Option B (alternative): synthesize a localTableMap entry so later local-table logic can run:
                // const synthCols = (typeDef.columns as any[]).map((cc: any) => ({ name: cc.rawName ?? cc.name, type: cc.type }));
                // localTableMap.set(mappedNorm, { name: mappedNorm, columns: synthCols });
              } else {
                dbg('Alias -> parameter type not indexed:', { mappedNorm, paramTypeRaw, typeKey });
              }
            } else {
              dbg('Alias maps to param but paramMap has no entry:', mappedNorm);
            }
          }
        } catch (e) {
          dbg('alias->param hover error', e);
        }


        // If this maps to a local table in this file, check localTableMap
        if (isRawTokenLocal(mappedNorm)) {
          const localMapped = localTableMap.get(mappedNorm);
          if (localMapped && isLocalEntryValid(localMapped, mappedRawDisplay)) {
            const rows = localMapped.columns.map(c => `- \`${c.name}\`${c.type ? ` ${c.type}` : ""}`);
            const body = `**Alias** \`${rawWordDisplay}\` → local table \`${localMapped.name}\`\n\n${rows.join("\n")}`;
            dbg('Local table alias hover:', localMapped.name, rows);
            return { contents: { kind: MarkupKind.Markdown, value: body }, range } as Hover;
          }
          dbg('Alias target found in localTableMap but skipped as non-local', mappedNorm, localMapped && localMapped.name);
        }

        // Fall back to catalog/global index lookup
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
            dbg('Catalog table alias hover:', def.name, rows);
            return { contents: { kind: MarkupKind.Markdown, value: body }, range } as Hover;
          } else {
            dbg('Skipping alias-as-table hover because token is qualified; def exists:', mappedNorm);
          }
        } else {
          dbg('No catalog table for alias target:', mappedNorm);
        }
      }
    } catch (e) {
      dbg('Alias resolution error:', e);
    }

    // 4) alias.column explicit handling (resolve alias -> target, prefer local table map)
    try {
      const aliasInfo = resolveAlias(rawWord, doc, pos);
      if (aliasInfo && aliasInfo.table && aliasInfo.column) {
        let resolvedTarget: string | null = null;
        let resolvedRaw: string | null = null;
        try {
          const mapped = resolveAliasTarget(cleanedStmt || "", normalizeName(String(aliasInfo.table)));
          if (mapped) {
            resolvedTarget = mapped;
            resolvedRaw = normalizeName(String(mapped));
            dbg('Alias.column resolution (cleanedStmt):', { table: aliasInfo.table, column: aliasInfo.column, resolvedTarget, resolvedRaw });
          } else {
            const fullText = doc.getText();
            const fullTextCleaned = fullText.replace(/WITH\s*\((?:NOLOCK|READUNCOMMITTED|UPDLOCK|HOLDLOCK|ROWLOCK|FORCESEEK|INDEX\([^)]*\)|FASTFIRSTROW|XLOCK|REPEATABLEREAD|SERIALIZABLE|,)+?\s*\)/gi, ' ');
            const mappedFull = resolveAliasTarget(fullTextCleaned, normalizeName(String(aliasInfo.table)));
            if (mappedFull) {
              resolvedTarget = mappedFull;
              resolvedRaw = normalizeName(String(mappedFull));
              dbg('Alias.column resolution (fullText):', { table: aliasInfo.table, column: aliasInfo.column, resolvedTarget, resolvedRaw });
            } else {
              resolvedTarget = String(aliasInfo.table);
              resolvedRaw = normalizeName(String(aliasInfo.table));
              dbg('Alias.column resolution (fallback to table):', { table: aliasInfo.table, column: aliasInfo.column, resolvedTarget, resolvedRaw });
            }
          }
        } catch (e) {
          resolvedTarget = String(aliasInfo.table);
          resolvedRaw = normalizeName(String(aliasInfo.table));
          dbg('Alias.column target resolution error:', e, { resolvedTarget, resolvedRaw });
        }

        const aliasTableKey = normalizeName(String(resolvedTarget).replace(/^dbo\./i, ""));

        // --- NEW: if alias maps to a procedure parameter (@...), prefer the parameter's declared TVP type ---
        try {
          const mappedNorm = normalizeName(String(resolvedTarget || "").trim()); // e.g. "@transportrequests" or "transportrequests"
          if (mappedNorm && (mappedNorm.startsWith("@") || mappedNorm.startsWith("#"))) {
            // paramMap keys are normalized to lower-case '@name'
            const paramTypeRaw = paramMap.get(mappedNorm.toLowerCase());
            if (paramTypeRaw) {
              // paramTypeRaw looks like: "TransportRequests READONLY" or "dbo.TransportRequests readonly" or "TransportRequests"
              // extract the type token (first token that is not READONLY)
              let typeToken = paramTypeRaw.split(/\s+/).find(t => t && t.toLowerCase() !== "readonly");
              if (typeToken) {
                // canonicalize the type name for lookup
                const typeKey = normalizeName(typeToken.replace(/^dbo\./i, ""));
                // Look up table type definitions (you index table types into tableTypesByName)
                const typeDef = tableTypesByName.get(typeKey) || tablesByName.get(typeKey);
                if (typeDef && typeDef.columns) {
                  const aliasColNorm = aliasInfo.column ? normalizeName(aliasInfo.column) : null;
                  const c = (typeDef.columns as any[]).find(cc =>
                    (cc.rawName ?? cc.name) === aliasInfo.column ||
                    (aliasColNorm !== null && normalizeName(cc.name) === aliasColNorm)
                  );
                  if (c) {
                    const label = typeDef === tableTypesByName.get(typeKey) ? `table type \`${typeDef.rawName ?? typeDef.name}\`` : `table \`${typeDef.rawName ?? typeDef.name}\``;
                    const value = `**Column** \`${c.rawName ?? c.name}\`${c.type ? ` ${c.type}` : ""} in ${label} (parameter \`${mappedNorm}\`)`;
                    dbg('Alias -> parameter TVP column hover:', { mappedNorm, typeKey, value });
                    return {
                      contents: { kind: MarkupKind.Markdown, value },
                      range
                    } as Hover;
                  } else {
                    dbg('Parameter TVP found but column not present on type:', mappedNorm, typeKey);
                  }
                } else {
                  dbg('Parameter type not found in tableTypesByName/tablesByName:', mappedNorm, typeToken, typeKey);
                }
              } else {
                dbg('Param type token extraction failed for:', mappedNorm, paramTypeRaw);
              }
            } else {
              dbg('No paramMap entry for alias token:', mappedNorm);
            }
          }
        } catch (e) {
          dbg('parameter-TV P lookup error:', e);
        }

        // Only accept local table if the local entry is genuinely local-ish
        if (isRawTokenLocal(resolvedRaw)) {
          const localTable = localTableMap.get(aliasTableKey);
          if (localTable && isLocalEntryValid(localTable, resolvedRaw) && localTable.columns) {
            const aliasColNorm = aliasInfo.column ? normalizeName(aliasInfo.column) : null;
            const cLocal = (localTable.columns || []).find(cc => normalizeName(cc.name) === aliasColNorm);
            if (cLocal) {
              const value = `**Column** \`${cLocal.name}\`${cLocal.type ? ` ${cLocal.type}` : ""} in local table \`${localTable.name}\``;
              dbg('Local table column hover:', value);
              return {
                contents: { kind: MarkupKind.Markdown, value },
                range
              } as Hover;
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
            const containerLabel = defIsType ? `table type \`${def.rawName ?? def.name}\`` : `table \`${def.rawName ?? def.name}\``;
            const value = `**Column** \`${c.rawName ?? c.name}\`${c.type ? ` ${c.type}` : ""} in ${containerLabel}`;
            dbg('Catalog table column hover:', value);
            return {
              contents: { kind: MarkupKind.Markdown, value },
              range
            } as Hover;
          }
          dbg('No matching column in catalog table:', aliasTableKey, aliasColNorm);
        }
      }
    } catch (e) {
      dbg('Alias.column resolution error:', e);
    }

    // --- Small fix: prefer UPDATE target ONLY when hover is on the left side of a SET assignment ---
    try {
      const updateMatch = /\bupdate\s+([@#]?[A-Za-z0-9_\[\]\."]+)(?:\s+(?:as\s+)?([A-Za-z0-9_]+))?/i.exec(cleanedStmt);
      if (updateMatch && updateMatch[1]) {
        const updateTokenRaw = normalizeName(updateMatch[1]);
        const updateAliasFromClause = updateMatch[2] ? normalizeName(updateMatch[2]) : null;

        // try to resolve alias->table for update token
        let resolvedUpdateTargetRaw = updateTokenRaw;
        try {
          const resolved = resolveAliasTarget(cleanedStmt || "", updateTokenRaw);
          if (resolved) { resolvedUpdateTargetRaw = normalizeName(String(resolved)); }
        } catch { /* ignore */ }

        const setRe = /\bset\b/i.exec(cleanedStmt);
        if (setRe) {
          const stmtAbsStart = doc.getText().indexOf(cleanedStmt);
          const setRegionStart = setRe.index + setRe[0].length;
          const tailAfterSet = cleanedStmt.slice(setRegionStart);
          const tailEndMatch = /\b(from|where)\b/i.exec(tailAfterSet);
          const setRegionEnd = tailEndMatch ? (setRegionStart + tailEndMatch.index) : cleanedStmt.length;
          const assignsText = cleanedStmt.slice(setRegionStart, setRegionEnd);

          const lhsRe = /([A-Za-z0-9_\[\]"`\.]+(?:\.[A-Za-z0-9_\[\]"`\.]+)*)\s*=/g;
          let m: RegExpExecArray | null;
          const hoverOffset = doc.offsetAt(range.start);
          let hoverIsOnLeftSide = false;
          let matchedLeftToken: string | null = null;

          while ((m = lhsRe.exec(assignsText)) !== null) {
            const leftTok = m[1];
            const leftStartInAssign = m.index;
            const leftAbsStart = stmtAbsStart + setRegionStart + leftStartInAssign;
            const leftAbsEnd = leftAbsStart + leftTok.length;
            if (hoverOffset >= leftAbsStart && hoverOffset <= leftAbsEnd) {
              hoverIsOnLeftSide = true;
              matchedLeftToken = leftTok;
              break;
            }
          }

          if (hoverIsOnLeftSide && matchedLeftToken) {
            const matchedLeftStripped = normalizeName(String(matchedLeftToken)).trim();
            const leftIsQualified = matchedLeftStripped.includes(".");
            const leftQualifier = leftIsQualified ? matchedLeftStripped.split(".").slice(0, -1).join(".") : null;
            const lhsColToken = leftIsQualified ? matchedLeftStripped.split(".").pop()! : matchedLeftStripped;
            const lhsColNorm = normalizeName(lhsColToken);

            const hoverMatchesLhs = (normalizeName(hoveredColumnToken) === lhsColNorm) ||
              (!hoveredQualifier && normalizeName(hoveredColumnToken) === lhsColNorm) ||
              (hoveredQualifier && leftQualifier && normalizeName(hoveredQualifier) === normalizeName(leftQualifier));

            if (hoverMatchesLhs) {
              const updateTargetKeys = tableKeyCandidates(resolvedUpdateTargetRaw.replace(/^dbo\./i, ""));
              // prefer local first
              for (const k of updateTargetKeys) {
                const local = lookupLocalByKey(k);
                if (local && local.columns) {
                  const foundLocal = (local.columns || []).find(cc => normalizeName(cc.name) === wordNorm);
                  if (foundLocal) {
                    const md = `**Column** \`${foundLocal.name}\`${foundLocal.type ? ` ${foundLocal.type}` : ""} in local table \`${local.name}\``;
                    dbg('UPDATE-SET-left matched local target:', md);
                    return { contents: { kind: MarkupKind.Markdown, value: md }, range } as Hover;
                  }
                }
              }
              // then prefer catalog
              for (const k of updateTargetKeys) {
                const def = tablesByName.get(k);
                if (def && def.columns) {
                  const foundDef = (def.columns as any[]).find(cc =>
                    (cc.rawName ?? cc.name) === hoveredColumnToken ||
                    normalizeName(cc.name) === wordNorm
                  );
                  if (foundDef) {
                    const md = `**Column** \`${foundDef.rawName ?? foundDef.name}\`${foundDef.type ? ` ${foundDef.type}` : ""} in table \`${def.rawName ?? def.name}\``;
                    dbg('UPDATE-SET-left matched catalog target:', md);
                    return { contents: { kind: MarkupKind.Markdown, value: md }, range } as Hover;
                  }
                }
              }
            }
          }
        }
      } else {
        dbg('No UPDATE target found in statement');
      }
    } catch (e) {
      dbg('UPDATE-SET-left small-fix error:', e);
    }

    // 5) Candidate-only column lookup (search only in candidate tables discovered in statement)
    if (candidateTables.size > 0) {
      const matches: string[] = [];
      const seenSourceKeys = new Set<string>(); // dedupe by canonical table identity

      for (const rawTname of Array.from(candidateTables)) {
        const candidateNorm = normalizeName(String(rawTname).trim());

        // 1) consider local map if token is local-style
        if (isRawTokenLocal(candidateNorm)) {
          const local = localTableMap.get(candidateNorm);
          if (local && local.columns) {
            for (const col of local.columns) {
              if (normalizeName(col.name) === wordNorm) {
                const sourceKey = String(local.name || candidateNorm).toLowerCase();
                if (!seenSourceKeys.has(sourceKey)) {
                  matches.push(`${col.name}${col.type ? ` ${col.type}` : ""} in local table ${local.name}`);
                  seenSourceKeys.add(sourceKey);
                  dbg('Local table column match (candidate):', candidateNorm, local.name, col.name);
                } else {
                  dbg('Skipping duplicate local match for same source:', sourceKey);
                }
                break;
              }
            }
          } else {
            dbg('Candidate token looked local but no entry in localTableMap for:', candidateNorm);
          }
        }

        // 2) fallback to catalog: try each table-key variant for the candidate token
        const candKeys = tableKeyCandidates(candidateNorm);
        for (const k of candKeys) {
          let def = tablesByName.get(k);
          let defIsType = false;
          if (!def) {
            const tdef = tableTypesByName.get(k);
            if (tdef) { defIsType = true; def = tdef; }
          }
          if (!def || !def.columns) {
            dbg('No catalog table or columns for candidate key:', k);
            continue;
          }

          for (const col of def.columns) {
            if ((col.rawName ?? col.name) === hoveredColumnToken || normalizeName(col.name || col.rawName || "") === wordNorm) {
              const canonicalName = String(def.rawName ?? def.name).trim();
              const sourceKey = canonicalName.toLowerCase();
              if (!seenSourceKeys.has(sourceKey)) {
                const containerLabel = defIsType ? `table type ${def.rawName ?? def.name}` : `table ${def.rawName ?? def.name}`;
                matches.push(`${col.rawName ?? col.name}${col.type ? ` ${col.type}` : ""} in ${containerLabel}`);
                seenSourceKeys.add(sourceKey);
                dbg('Catalog table column match (candidate):', k, col.rawName ?? col.name, containerLabel);
              } else {
                dbg('Skipping duplicate catalog match for same source:', sourceKey);
              }
              break;
            }
          }
        }
      }

      if (matches.length === 1) {
        dbg('Single column match (candidates):', matches[0]);
        return { contents: { kind: MarkupKind.Markdown, value: `**Column** ${matches[0]}` }, range } as Hover;
      } else if (matches.length > 1) {
        dbg('Ambiguous column matches (candidates):', matches);
        return { contents: { kind: MarkupKind.Markdown, value: `**Column** \`${hoveredColumnToken}\` (ambiguous — found in: ${matches.join(", ")})` }, range } as Hover;
      }
      dbg('No column matches found in candidate tables for:', wordNorm);
    } else {
      dbg('No candidate tables found');
    }

    // 6) Heuristic: if statement is UPDATE prefer its target (final pass)
    try {
      const updateMatch = /\bupdate\s+([a-zA-Z0-9_\[\]\."]+)/i.exec(cleanedStmt);
      if (updateMatch && updateMatch[1]) {
        const rawUpdateMatch = normalizeName(updateMatch[1]);
        const tnorm = normalizeName(rawUpdateMatch.replace(/^dbo\./i, ""));
        const rawUpdateTarget = rawUpdateMatch;
        dbg('UPDATE target:', { raw: rawUpdateTarget, normalized: tnorm });

        if (isRawTokenLocal(rawUpdateTarget)) {
          const local = localTableMap.get(tnorm);
          if (local && isLocalEntryValid(local, rawUpdateTarget) && local.columns) {
            const colDefLocal = (local.columns || []).find(c => normalizeName(c.name) === wordNorm);
            if (colDefLocal) {
              const value = `**Column** \`${colDefLocal.name}\`${colDefLocal.type ? ` ${colDefLocal.type}` : ""} in local table \`${local.name}\``;
              dbg('UPDATE local table column hover:', value);
              return { contents: { kind: MarkupKind.Markdown, value }, range } as Hover;
            }
            dbg('No matching column in UPDATE local table:', tnorm, wordNorm);
          }
        }

        const def = tablesByName.get(tnorm);
        if (def && def.columns) {
          const colDef = (def.columns as any[]).find(c => normalizeName((c.name || c.rawName || "")) === wordNorm);
          if (colDef) {
            const value = `**Column** \`${colDef.rawName ?? colDef.name}\`${colDef.type ? ` ${colDef.type}` : ""} in table \`${def.rawName ?? def.name}\``;
            dbg('UPDATE catalog table column hover:', value);
            return { contents: { kind: MarkupKind.Markdown, value }, range } as Hover;
          }
          dbg('No matching column in UPDATE catalog table:', tnorm, wordNorm);
        }
      } else {
        dbg('No UPDATE target found in statement');
      }
    } catch (e) {
      dbg('UPDATE heuristic error:', e);
    }

    dbg('No hover result for:', rawWordDisplay);
    return null;
  } catch (ex) {
    dbg('doHover top-level error:', ex);
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

  // --------------------------
  // 4) Final fallback: none found
  // --------------------------
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
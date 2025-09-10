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
import { parseSqlWithWorker, astPool } from "./parser-pool";
import { resolveColumnFromAst, resolveAliasFromAst, walkAst } from "./ast-utils";
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
} from "./definitions";

// ---------- Connection + Documents ----------
const connection = createConnection(ProposedFeatures.all);
const documents: TextDocuments<TextDocument> = new TextDocuments(TextDocument);
let enableValidation = false;
const HOVER_DEBUG = true;

// Call this after building the definitions index
function markIndexReady() {
    setIndexReady();
    // Re-validate all open documents once we have a full index
    for (const doc of documents.all()) {
        validateTextDocument(doc);
    }
}

function toNormUri(rawUri: string): string {
    return rawUri.startsWith("file://") ? rawUri : url.pathToFileURL(rawUri).toString();
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
    const uri = e.document.uri.startsWith("file://") ? e.document.uri : url.pathToFileURL(e.document.uri).toString();
    const oldDefs = definitions.get(uri) || [];
    for (const d of oldDefs) { tablesByName.delete(d.name); columnsByTable.delete(d.name); }
    definitions.delete(uri);
    aliasesByUri.delete(uri);
    deleteRefsForFile(uri);
    connection.sendDiagnostics({ uri, diagnostics: [] });
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

// --- Completion (robust) ---
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
            let aliasRaw = aliasDotMatch[1].replace(/^\[|\]$/g, "");
            const aliasLower = aliasRaw.toLowerCase();

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
                        const normCol = (col.rawName || col.name || "").replace(/^\[|\]$/g, "").toLowerCase();
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
                safeLog(`[completion] alias '${aliasRaw}' not found in statement or file aliases for ${normUri}. linePrefix='${linePrefix.slice(-80)}'`);
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

connection.onHover(async (params): Promise<Hover | null> => {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) { return null; }
    return await doHover(doc, params.position);
});

export async function doHover(doc: TextDocument, pos: Position): Promise<Hover | null> {
    // helpers for debug
    const dbg = (...args: any[]) => { if (HOVER_DEBUG) { console.log("[hover]", ...args); } };

    const range = getWordRangeAtPosition(doc, pos);
    if (!range) { dbg("no word range"); return null; }

    const rawWord = doc.getText(range);
    const rawWordStripped = rawWord.replace(/^\[|\]$/g, "");
    const word = normalizeName(rawWordStripped);

    dbg("hover rawWord=", rawWord, "normalized=", word);

    // ---------- AST-based resolution ----------
    const stmtText = getCurrentStatement(doc, pos);
    if (stmtText) {
        try {
            dbg("stmtText (first 200):", stmtText.slice(0, 200).replace(/\n/g, "\\n"));

            // parse only the statement (use pool + caching)
            const ast = await parseSqlWithWorker(stmtText, { database: "transactsql" }, 800);
            dbg("ast:", !!ast, ast?.type);

            if (ast) {
                // Compute absolute offset of cursor and offset inside the statement occurrence
                const absOffset = typeof (doc as any).offsetAt === "function" ? (doc as any).offsetAt(pos) : offsetAt(doc, pos);
                const fullText = doc.getText();

                // Find the occurrence of stmtText that contains the absOffset (or best fallback)
                let stmtIndex = -1;
                let from = 0;
                while (true) {
                    const idx = fullText.indexOf(stmtText, from);
                    if (idx === -1) { break; }
                    if (absOffset >= idx && absOffset <= idx + stmtText.length) { stmtIndex = idx; break; }
                    from = idx + 1;
                }
                if (stmtIndex === -1) {
                    // last occurrence before cursor, otherwise first occurrence
                    const lastBefore = fullText.lastIndexOf(stmtText, absOffset);
                    stmtIndex = lastBefore !== -1 ? lastBefore : fullText.indexOf(stmtText);
                }
                if (stmtIndex === -1) { stmtIndex = 0; }
                const offsetInStmt = absOffset - stmtIndex;
                dbg("absOffset", absOffset, "stmtIndex", stmtIndex, "offsetInStmt", offsetInStmt);

                // Recursively walk AST to find a column_ref node that encloses offsetInStmt.
                // Be defensive: node location can be in different shapes (start/end or location.start.offset)
                let foundColNode: any = null;
                function visit(node: any) {
                    if (!node || typeof node !== "object" || foundColNode) { return; }

                    // Detect column node shapes
                    const isColRef = (node.type === "column_ref" || node.ast === "column_ref") && (node.column || node.column === 0);
                    if (isColRef) {
                        // possible shapes for location:
                        // - node.start/node.end
                        // - node.location.start/node.location.end (numbers)
                        // - node.location.start.offset / node.location.end.offset
                        let nodeStart: number | null = null;
                        let nodeEnd: number | null = null;
                        try {
                            if (typeof node.start === "number" && typeof node.end === "number") {
                                nodeStart = node.start;
                                nodeEnd = node.end;
                            } else if (node.location && typeof node.location.start === "number" && typeof node.location.end === "number") {
                                nodeStart = node.location.start;
                                nodeEnd = node.location.end;
                            } else if (node.location && node.location.start && typeof node.location.start.offset === "number" &&
                                node.location.end && typeof node.location.end.offset === "number") {
                                nodeStart = node.location.start.offset;
                                nodeEnd = node.location.end.offset;
                            }
                        } catch (e) { /* ignore */ }

                        if (nodeStart !== null && nodeEnd !== null) {
                            dbg("col node with loc", node.column, nodeStart, nodeEnd);
                            if (offsetInStmt >= nodeStart && offsetInStmt <= nodeEnd) {
                                foundColNode = node;
                                return;
                            }
                        } else {
                            // No precise location: match on name as fallback
                            const nodeColName = normalizeName(String(node.column));
                            if (nodeColName === word) {
                                dbg("col node matched by name (no location):", nodeColName);
                                foundColNode = node;
                                return;
                            }
                        }
                    }

                    // Recurse
                    for (const k of Object.keys(node)) {
                        const v = node[k];
                        if (Array.isArray(v)) {
                            for (const el of v) {
                                visit(el);
                                if (foundColNode) { return; }
                            }
                        } else if (v && typeof v === "object") {
                            visit(v);
                            if (foundColNode) { return; }
                        }
                    }
                }

                visit(ast);

                dbg("foundColNode?", !!foundColNode, foundColNode && foundColNode.column);

                // Resolve table name from node (alias -> table) or from DML target or via resolveColumnFromAst
                let resolvedTable: string | null = null;

                if (foundColNode) {
                    // If the node has a table (alias or explicit), map it via statement aliases
                    if (foundColNode.table) {
                        const aliases = extractAliases(stmtText);
                        const maybe = String(foundColNode.table);
                        const mapped = aliases.get(maybe.toLowerCase());
                        resolvedTable = normalizeName((mapped ?? maybe).replace(/^dbo\./i, ""));
                        dbg("resolvedTable from node.table", resolvedTable);
                    } else {
                        // if unqualified, some AST node types imply a target table (UPDATE/DELETE/INSERT)
                        if (ast.type === "update" || ast.type === "delete" || ast.type === "insert") {
                            const tnode = Array.isArray(ast.table) ? ast.table[0] : ast.table;
                            const cand = tnode && (tnode.table || (tnode.expr && (tnode.expr.table || tnode.expr.name)));
                            if (cand) {
                                resolvedTable = normalizeName(String(cand).replace(/^dbo\./i, ""));
                                dbg("resolvedTable from DML target", resolvedTable);
                            }
                        }
                    }
                }

                // If still not resolved, try resolveColumnFromAst which knows about selects/inserts/updates
                if (!resolvedTable) {
                    try {
                        const resolved = resolveColumnFromAst(ast, word);
                        if (resolved) {
                            const aliases = extractAliases(stmtText);
                            const mapped = aliases.get(resolved.toLowerCase());
                            resolvedTable = normalizeName((mapped ?? resolved).replace(/^dbo\./i, ""));
                            dbg("resolvedTable from resolveColumnFromAst", resolvedTable);
                        }
                    } catch (e) {
                        dbg("resolveColumnFromAst threw", String(e));
                    }
                }

                // If we have a table, lookup its definition and the column
                if (resolvedTable) {
                    const def = tablesByName.get(resolvedTable);
                    if (def && def.columns) {
                        const colDef = (def.columns as any[]).find(c => normalizeName((c.name || c.rawName || "")) === word);
                        if (colDef) {
                            dbg("hover result from AST table/col", resolvedTable, colDef.rawName || colDef.name);
                            return {
                                contents: {
                                    kind: MarkupKind.Markdown,
                                    value: `**Column** \`${colDef.rawName ?? colDef.name}\`${colDef.type ? ` ${colDef.type}` : ""} in table \`${def.rawName ?? def.name}\``
                                }
                            } as Hover;
                        }
                    } else {
                        dbg("no def for resolvedTable", resolvedTable);
                    }
                }
            }
        } catch (e) {
            safeLog("[hover] AST parse/resolution failed: " + String(e));
        }
    }

    // ---------- Fallback: alias.column handling ----------
    try {
        const aliasInfo = resolveAlias(rawWord, doc, pos);
        if (aliasInfo.table && aliasInfo.column) {
            const def = tablesByName.get(normalizeName(aliasInfo.table));
            if (def && def.columns) {
                const aliasColNorm = aliasInfo.column ? normalizeName(aliasInfo.column) : null;
                const c = (def.columns as any[]).find(cc =>
                    (cc.rawName ?? cc.name) === aliasInfo.column ||
                    (aliasColNorm !== null && normalizeName(cc.name) === aliasColNorm)
                );
                if (c) {
                    return {
                        contents: {
                            kind: MarkupKind.Markdown,
                            value: `**Column** \`${c.rawName ?? c.name}\`${c.type ? ` ${c.type}` : ""} in table \`${def.rawName ?? def.name}\``
                        }
                    } as Hover;
                }
            }
        }
        if (aliasInfo.table && !aliasInfo.column) {
            return {
                contents: {
                    kind: MarkupKind.Markdown,
                    value: `**Alias** \`${rawWord}\` for table \`${aliasInfo.table}\``
                }
            } as Hover;
        }
    } catch (e) {
        dbg("alias resolution failed", String(e));
    }

    // ---------- Fallback: FROM/JOIN context (regex-based) ----------
    const candidateTables = new Set<string>();
    const fromJoinRegex = /\b(from|join)\s+([a-zA-Z0-9_\[\]\.]+)/gi;
    if (stmtText) {
        let mm: RegExpExecArray | null;
        while ((mm = fromJoinRegex.exec(stmtText))) {
            candidateTables.add(normalizeName(mm[2].replace(/^dbo\./i, "")));
        }
    }

    const matches: string[] = [];
    for (const def of tablesByName.values()) {
        if (candidateTables.has(normalizeName(def.name)) && def.columns) {
            for (const col of def.columns) {
                if (normalizeName(col.name || col.rawName || "") === word) {
                    matches.push(`${col.rawName ?? col.name}${col.type ? ` ${col.type}` : ""} in table ${def.rawName ?? def.name}`);
                }
            }
        }
    }

    if (matches.length === 1) {
        return { contents: { kind: MarkupKind.Markdown, value: `**Column** ${matches[0]}` } };
    } else if (matches.length > 1) {
        return { contents: { kind: MarkupKind.Markdown, value: `**Column** \`${rawWord}\` (ambiguous, found in: ${matches.join(", ")})` } };
    }

    // ---------- Global fallback: table / column by name ----------
    for (const def of tablesByName.values()) {
        if (normalizeName(def.name) === word) {
            return { contents: { kind: MarkupKind.Markdown, value: `**Table** \`${def.rawName ?? def.name}\`` } };
        }
        if (def.columns) {
            for (const col of def.columns) {
                if (normalizeName(col.name || col.rawName || "") === word) {
                    return {
                        contents: {
                            kind: MarkupKind.Markdown,
                            value: `**Column** \`${col.rawName ?? col.name}\`${col.type ? ` ${col.type}` : ""} in table \`${def.rawName ?? def.name}\``
                        }
                    } as Hover;
                }
            }
        }
    }

    return null;
}

// Full validator with @-skip and SELECT-alias skip
async function validateTextDocument(doc: TextDocument): Promise<void> {
    if (!enableValidation || !getIndexReady()) {
        connection.sendDiagnostics({ uri: doc.uri, diagnostics: [] });
        return;
    }

    const diagnostics: Diagnostic[] = [];
    const text = doc.getText();
    const lines = text.split(/\r?\n/);

    const aliases = extractAliases(text);

    // Declared cursors
    const cursorRegex = /\bdeclare\s+([a-zA-Z0-9_]+)\s+cursor/gi;
    const cursors = new Set<string>();
    {
        let cm: RegExpExecArray | null;
        while ((cm = cursorRegex.exec(text))) { cursors.add(cm[1].toLowerCase()); }
    }

    for (let i = 0; i < lines.length; i++) {
        const noComments = stripComments(lines[i]);
        const clean = noComments; // keep original for ranges
        const cleanNoStr = stripStrings(noComments); // ✅ ignore strings while preserving indexes
        const trimmedLc = cleanNoStr.trim().toLowerCase();

        // ---------- Table checks ----------
        const tableRegex = /\b(from|join|update|into)\s+([a-zA-Z0-9_\[\]\.#@]+)/gi; // ✅ allow # and @ in token
        {
            let m: RegExpExecArray | null;
            while ((m = tableRegex.exec(cleanNoStr))) {
                const rawTable = m[2];

                // ✅ Skip declared cursor names, temp tables (#, ##), and table variables (@T)
                if (cursors.has(rawTable.toLowerCase())) { continue; }
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

        // ---------- Alias.column checks ----------
        const looksLikeColumnContext =
            /(^|\b)(select|where|having|on|group\s+by|order\s+by)\b/.test(trimmedLc);

        if (looksLikeColumnContext) {
            const colRegex = /([a-zA-Z0-9_]+)\.(\[[^\]]+\]|[a-zA-Z_][a-zA-Z0-9_]*)/g;
            let m: RegExpExecArray | null;
            while ((m = colRegex.exec(cleanNoStr))) {
                const alias = m[1].toLowerCase();
                const rightRaw = m[2];

                // Skip variables on either side (rare but safe)
                if (alias.startsWith("@")) { continue; }
                if (rightRaw.startsWith("@")) { continue; }

                // Treat common schemas as schemas, not aliases
                if (/^(dbo|sys|information_schema|pg_catalog|public)$/.test(alias)) {
                    continue;
                }

                const col = normalizeName(rightRaw.replace(/[\[\]]/g, ""));
                const table = aliases.get(alias);

                if (!table) {
                    diagnostics.push({
                        severity: DiagnosticSeverity.Error,
                        range: {
                            start: { line: i, character: m.index },
                            end: { line: i, character: m.index + m[0].length }
                        },
                        message: `Unknown alias '${alias}'`,
                        source: "SaralSQL"
                    });
                } else if (table !== "__subquery__" &&
                    !columnsByTable.get(normalizeName(table))?.has(col)) {
                    diagnostics.push({
                        severity: DiagnosticSeverity.Error,
                        range: {
                            start: { line: i, character: m.index },
                            end: { line: i, character: m.index + m[0].length }
                        },
                        message: `Column '${col}' not found in table '${table}'`,
                        source: "SaralSQL"
                    });
                }
            }
        }

        // ---------- Bare column checks ----------
        const isStructuralLine =
            /^((left|right|full|inner|cross)\s+join|join|from|update|insert|into|merge|apply|cross\s+apply|outer\s+apply)\b/
                .test(trimmedLc);
        if (isStructuralLine) { continue; }

        const looksLikeColumnContext2 =
            /(^|\b)(select|where|having|on|group\s+by|order\s+by)\b/.test(trimmedLc);
        if (!looksLikeColumnContext2) { continue; }

        // Build tables-in-scope from the WHOLE STATEMENT (not just this line)
        const statementText = getCurrentStatement(doc, { line: i, character: 0 });
        const tablesInScope: string[] = [];
        {
            const fromJoinRegex = /\b(from|join)\s+([a-zA-Z0-9_\[\]\.#@]+)/gi; // ✅ include # and @
            let fm: RegExpExecArray | null;
            while ((fm = fromJoinRegex.exec(statementText))) {
                const tk = fm[2];
                if (tk.startsWith("#") || tk.startsWith("@")) { continue; } // ✅ temp/table var not indexed
                tablesInScope.push(normalizeName(tk));
            }
        }

        // ✅ Robust SELECT-alias extraction for the current statement
        const selectAliases = extractSelectAliasesFromStatement(statementText);

        // Bracketed or bare identifiers; strip [] before normalizing
        const bareColRegex = /\[([a-zA-Z0-9_ ]+)\]|([a-zA-Z_][a-zA-Z0-9_]*)/g;
        {
            let m: RegExpExecArray | null;
            while ((m = bareColRegex.exec(cleanNoStr))) {
                const rawToken = (m[1] || m[2] || "");
                const tokenStart = m.index;

                // ✅ Skip SQL variables like @var — the '@' is just before the match
                const beforeChar = cleanNoStr.charAt(Math.max(0, tokenStart - 1));
                if (beforeChar === "@") { continue; }

                const tokenEnd = tokenStart + (m[0] ? m[0].length : rawToken.length);
                const afterChar = cleanNoStr.charAt(tokenEnd);

                const col = normalizeName(rawToken.replace(/[\[\]]/g, ""));

                // Skip SQL keywords
                if (isSqlKeyword(col)) { continue; }

                // Skip "... AS <token>"
                const leftSlice = cleanNoStr.slice(0, m.index).toLowerCase();
                if (/\bas\s*$/.test(leftSlice)) { continue; }

                // Skip alias.column adjacency
                if (beforeChar === "." || afterChar === ".") { continue; }

                // ✅ Skip if this token is a SELECT-list alias
                if (selectAliases.has(col)) { continue; }

                // Check if any in-scope table contains this column
                let found = false;
                for (const t of tablesInScope) {
                    if (columnsByTable.get(t)?.has(col)) { found = true; break; }
                }

                if (!found && tablesInScope.length > 0) {
                    diagnostics.push({
                        severity: DiagnosticSeverity.Error,
                        range: {
                            start: { line: i, character: m.index },
                            end: { line: i, character: m.index + rawToken.length }
                        },
                        message: `Column '${col}' not found in any table in scope`,
                        source: "SaralSQL"
                    });
                }
            }
        }
    }

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
    const word = normalizeName(rawWord);
    const fullText = doc.getText();
    const lines = fullText.split(/\r?\n/);
    const results: Location[] = [];

    // --- helper: get current statement text (works with optional `position`) ---
    function _getCurrentStatementLocal(): string {
        if (!position) { return fullText; }

        const currentLine = position.line;
        let start = currentLine;
        let end = currentLine;

        // look upward until BEGIN or ; or top
        for (let i = currentLine; i >= 0; i--) {
            if (/^\s*begin\b/i.test(lines[i]) || /;\s*$/.test(lines[i])) {
                start = i + 1;
                break;
            }
            if (i === 0) { start = 0; }
        }

        // look downward until END or ; or bottom
        for (let i = currentLine; i < lines.length; i++) {
            if (/^\s*end\b/i.test(lines[i]) || /;\s*$/.test(lines[i])) {
                end = i;
                break;
            }
            if (i === lines.length - 1) { end = lines.length - 1; }
        }

        return lines.slice(start, end + 1).join("\n");
    }

    // Use the local helper (safe for optional `position`)
    const statementText = _getCurrentStatementLocal();

    // --- 1) Handle alias.column explicitly ---
    if (rawWord.includes(".")) {
        const parts = rawWord.split(".");
        if (parts.length === 2) {
            const alias = parts[0].toLowerCase();
            const colName = normalizeName(parts[1]);
            const aliases = extractAliases(statementText);
            const table = aliases.get(alias);
            if (table) {
                const key = `${normalizeName(table)}.${colName}`;
                const refs = getRefs(key);
                for (const r of refs) {
                    results.push({
                        uri: r.uri,
                        range: {
                            start: { line: r.line, character: r.start },
                            end: { line: r.line, character: r.end }
                        }
                    });
                }
                return results;
            }
        }
    }

    // --- 2) Table match ---
    const tableRefs = getRefs(word);
    for (const r of tableRefs) {
        results.push({
            uri: r.uri,
            range: {
                start: { line: r.line, character: r.start },
                end: { line: r.line, character: r.end }
            }
        });
    }

    // --- 3) Column match with scoped tables ---
    if (!rawWord.includes(".")) {
        const candidateTables = new Set<string>();

        const fromJoinRegex = /\b(from|join)\s+([a-zA-Z0-9_\[\]\.]+)(?:\s+[a-zA-Z0-9_]+)?/gi;
        let m: RegExpExecArray | null;
        while ((m = fromJoinRegex.exec(statementText))) {
            const tableName = normalizeName(m[2].replace(/^dbo\./i, ""));
            candidateTables.add(tableName);
        }

        // scoped lookup
        for (const table of candidateTables) {
            const key = `${table}.${word}`;
            const refs = getRefs(key);
            for (const r of refs) {
                results.push({
                    uri: r.uri,
                    range: {
                        start: { line: r.line, character: r.start },
                        end: { line: r.line, character: r.end }
                    }
                });
            }
        }

        // fallback to global if none — updated for the Map<string, Map<string, ReferenceDef[]>> shape
        if (results.length === 0) {
            for (const [key, byUri] of referencesIndex.entries()) {
                if (key.endsWith(`.${word}`)) {
                    for (const arr of byUri.values()) {
                        for (const r of arr) {
                            results.push({
                                uri: r.uri,
                                range: {
                                    start: { line: r.line, character: r.start },
                                    end: { line: r.line, character: r.end }
                                }
                            });
                        }
                    }
                }
            }
        }
    }

    return results;
}

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
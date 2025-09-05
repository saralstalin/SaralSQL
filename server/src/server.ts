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

// ---------- Connection + Documents ----------
const connection = createConnection(ProposedFeatures.all);
const documents: TextDocuments<TextDocument> = new TextDocuments(TextDocument);
const columnsByTable = new Map<string, Set<string>>();
const aliasesByUri = new Map<string, Map<string, string>>();
const definitions = new Map<string, SymbolDef[]>();
const referencesIndex = new Map<string, Map<string, ReferenceDef[]>>();


function setRefsForFile(name: string, uri: string, refs: ReferenceDef[]) {
    let byUri = referencesIndex.get(name);
    if (!byUri) { byUri = new Map(); referencesIndex.set(name, byUri); }
    byUri.set(uri, refs);
}
function deleteRefsForFile(uri: string) {
    for (const [, byUri] of referencesIndex) { byUri.delete(uri); }
    for (const [name, byUri] of Array.from(referencesIndex.entries())) {
        if (byUri.size === 0) { referencesIndex.delete(name); }
    }
}
function getRefs(name: string): ReferenceDef[] {
    const byUri = referencesIndex.get(name);
    if (!byUri) { return []; }
    const out: ReferenceDef[] = [];
    for (const arr of byUri.values()) { out.push(...arr); }
    return out;
}

const tablesByName = new Map<string, SymbolDef>();

let isIndexReady = false;
let enableValidation = false;

// Call this after building the definitions index
function markIndexReady() {
    isIndexReady = true;
    // Re-validate all open documents once we have a full index
    for (const doc of documents.all()) {
        validateTextDocument(doc);
    }
}

const SQL_KEYWORDS = new Set([
    "select", "from", "join", "on", "where", "and", "or", "insert", "update", "delete", "into", "as",
    "count", "group", "by", "order", "create", "procedure", "function", "view", "table", "begin", "end",
    "if", "else", "while", "case", "when", "then", "declare", "set", "values", "fetch", "next", "rows", "only",
    "distinct", "union", "all", "outer", "inner", "left", "right", "full", "top", "limit", "offset",
    "having", "over", "newid", "row_number", "desc", "asc", "sum", "min", "max", "null", "is", "exec"
]);

// ---------- Index structures ----------
interface ColumnDef {
    name: string;
    rawName: string;
    type?: string;
    line: number;
    start?: number; // ← add
    end?: number;   // ← add
}

interface SymbolDef {
    name: string;
    rawName: string;
    uri: string;
    line: number;
    columns?: ColumnDef[];
}

interface ReferenceDef {
    name: string; // normalized table or column name
    uri: string;
    line: number;
    start: number;
    end: number;
    kind: "table" | "column";
}


// ---------- Helpers ----------

function isSqlKeyword(token: string): boolean {
    return SQL_KEYWORDS.has(token.toLowerCase());
}


function stripStrings(s: string): string {
    // Replace T-SQL string literals (including N'...') with spaces to preserve indices
    return s.replace(/N?'(?:''|[^'])*'/g, (m) => " ".repeat(m.length));
}

function normalizeName(name: string): string {
    if (!name) { return ""; }

    // remove square brackets and lowercase
    let n = name.replace(/[\[\]]/g, "").toLowerCase().trim();

    const parts = n.split(".");
    if (parts.length === 2) {
        const [schema, object] = parts;
        if (schema === "dbo") {
            return object; // strip dbo
        }
        return `${schema}.${object}`;
    }
    return n;
}


// Safety caps 
const MAX_FILE_SIZE_BYTES = 8 * 1024;   // 8 KB (skip larger files)
const MAX_REFS_PER_FILE = 5000;         // cap number of reference objects created per file
const DEPLOY_BLOCK_SUBSTRING = "deploy"; // block any path that contains this substring

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

function stripComments(sql: string): string {
    let result = sql.replace(/--.*$/gm, "");
    result = result.replace(/\/\*[\s\S]*?\*\//g, "");
    return result;
}

function extractAliases(text: string): Map<string, string> {
    const aliases = new Map<string, string>();

    // Table aliases: supports [dbo].[X] [a], dbo.[X] a, schema.X AS alias
    const tableAliasRegex =
        /\b(from|join)\s+((?:\[?[a-zA-Z0-9_]+\]?)(?:\.\[?[a-zA-Z0-9_]+\]?)?)\s+(?:as\s+)?(\[?[a-zA-Z0-9_]+\]?)/gi;
    let m: RegExpExecArray | null;
    while ((m = tableAliasRegex.exec(text))) {
        const rawTable = m[2];
        const alias = m[3].replace(/[\[\]]/g, "").toLowerCase(); // strip brackets
        aliases.set(alias, normalizeName(rawTable));
    }

    // Subquery aliases: FROM (SELECT ...) x or FROM (SELECT ...) AS x
    const subqueryAliasRegex = /\)\s+(?:as\s+)?([a-zA-Z0-9_]+)/gi;
    let sm: RegExpExecArray | null;
    while ((sm = subqueryAliasRegex.exec(text))) {
        const alias = sm[1].toLowerCase();
        aliases.set(alias, "__subquery__");
    }

    return aliases;
}



// --- helpers for CREATE TABLE parsing ---
function splitByCommasRespectingParens(s: string): string[] {
    const parts: string[] = [];
    let start = 0, depth = 0, inQuote: string | null = null;
    for (let i = 0; i < s.length; i++) {
        const ch = s[i];
        if (inQuote) {
            if (ch === inQuote) { inQuote = null; }
            continue;
        }
        if (ch === "'" || ch === '"') { inQuote = ch; continue; }
        if (ch === "(") { depth++; }
        else if (ch === ")") { depth = Math.max(0, depth - 1); }
        else if (ch === "," && depth === 0) {
            parts.push(s.slice(start, i));
            start = i + 1;
        }
    }
    parts.push(s.slice(start));
    return parts.map(p => p.trim()).filter(Boolean);
}

function isTableConstraintRow(s: string): boolean {
    const head = s.trim().toLowerCase();
    return head.startsWith("constraint")
        || head.startsWith("primary key")
        || head.startsWith("foreign key")
        || head.startsWith("unique")
        || head.startsWith("check");
}

function splitCreateBodyIntoRowsWithPositions(
    text: string,
    defLineIndex: number
): Array<{ text: string; line: number; col: number }> {
    const lines = text.split(/\r?\n/);



    // find first '(' from the definition line onwards
    let startLine = defLineIndex, parenIdx = -1;
    for (let i = defLineIndex; i < lines.length; i++) {
        const j = lines[i].indexOf("(");
        if (j >= 0) { startLine = i; parenIdx = j; break; }
    }
    if (parenIdx < 0) { return []; }

    const rows: Array<{ text: string; line: number; col: number }> = [];
    let buf = "";
    let depth = 0;
    let inQuote: '"' | "'" | null = null;

    // The start position of the *current* row we are building:
    let rowStartLine = startLine;
    let rowStartCol = parenIdx + 1;

    const flushRow = () => {
        const trimmed = buf.trim();
        if (trimmed) { rows.push({ text: trimmed, line: rowStartLine, col: rowStartCol }); }
        buf = "";
    };

    for (let i = startLine; i < lines.length; i++) {
        const line = lines[i];
        const kStart = (i === startLine ? parenIdx + 1 : 0);

        // If we are at a new physical line and buffer is empty, reset the row start to here.
        if (kStart === 0 && buf.length === 0) {
            rowStartLine = i;
            rowStartCol = 0;
        }

        for (let k = kStart; k < line.length; k++) {
            const ch = line[k];
            const next = k + 1 < line.length ? line[k + 1] : "";

            // line comment
            if (!inQuote && ch === "-" && next === "-") {
                break; // ignore the rest of the line
            }

            if (inQuote) {
                buf += ch;
                if (ch === inQuote) { inQuote = null; }
                continue;
            }

            if (ch === "'" || ch === '"') {
                inQuote = ch as '"' | "'";
                buf += ch;
                continue;
            }

            if (ch === "(") { depth++; buf += ch; continue; }
            if (ch === ")") {
                if (depth === 0) {
                    flushRow();
                    return rows;
                }
                depth--; buf += ch; continue;
            }

            if (ch === "," && depth === 0) {
                // current row ends before this comma
                flushRow();

                // next row starts after this comma (possibly same line)
                rowStartLine = i;

                // Find the first non-space after the comma to set a precise col
                let nextCol = k + 1;
                while (nextCol < line.length && /\s/.test(line[nextCol])) { nextCol++; }
                rowStartCol = nextCol;

                continue; // do not include comma in buf
            }

            // If buf is empty and we just hit the first non-space char, lock start col precisely
            if (buf.length === 0 && !/\s/.test(ch)) {
                rowStartLine = i;
                rowStartCol = k;
            }

            buf += ch;
        }

        // keep line separation (helps when rows span multiple lines)
        buf += "\n";
    }

    // Fallback: if closing ')' not seen, flush what we collected
    const trimmed = buf.trim();
    if (trimmed) { rows.push({ text: trimmed, line: rowStartLine, col: rowStartCol }); }
    return rows;
}



// NEW: linear parser (block-scoped, no whole-file rescans, safer constraint handling)
function parseColumnsFromCreateBlock(
    blockText: string,
    startLine: number
): Array<{ name: string; rawName: string; line: number; start: number; end: number }> {
    const rows = splitCreateBodyIntoRowsWithPositions(blockText, startLine);
    const out: Array<{ name: string; rawName: string; line: number; start: number; end: number }> = [];

    const allLines = blockText.split(/\r?\n/);

    for (const r of rows) {
        const m = /^\s*(\[([^\]]+)\]|"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))/.exec(r.text);
        if (!m) { continue; }

        const raw = m[1]; // preserves brackets/quotes
        const tokenName = (m[2] ?? m[3] ?? m[4] ?? "").trim();

        // Only skip if the *first* token is an unquoted/unbracketed constraint keyword
        const isConstraint =
            !m[2] && !m[3] && /^(?:constraint|primary|foreign|check|unique|index)$/i.test(tokenName);
        if (isConstraint) { continue; }

        const name = normalizeName(tokenName);

        // Compute true start column by searching this raw token on the actual source line.
        // If not found (rare formatting), fall back to r.col.
        const sourceLine = allLines[r.line] ?? "";
        let startCol = sourceLine.indexOf(raw);
        if (startCol < 0) {
            // try bare token
            startCol = sourceLine.search(new RegExp(`\\b${tokenName.replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&")}\\b`));
        }
        if (startCol < 0) { startCol = r.col; }

        out.push({
            name,
            rawName: raw,
            line: r.line,
            start: startCol,
            end: startCol + raw.length
        });
    }

    return out;
}


// ---------- Indexing ----------
function indexText(uri: string, text: string): void {
    const normUri = uri.startsWith("file://") ? uri : url.pathToFileURL(uri).toString();


    if (normUri.toLowerCase().includes(`/${DEPLOY_BLOCK_SUBSTRING}`) || normUri.toLowerCase().includes(`\\${DEPLOY_BLOCK_SUBSTRING}`)) {
        safeLog(`[indexText] skipping due to deploy folder match: ${normUri}`);
        return;
    }

    if (!normUri.toLowerCase().endsWith(".sql")) {
        safeLog(`[indexText] skipped non-sql file: ${normUri}`);
        return;
    }

    // 2) Skip files that are larger than the configured threshold (avoid heavy files)
    const byteLen = Buffer.byteLength(text || "", "utf8");
    if (byteLen > MAX_FILE_SIZE_BYTES) {
        safeLog(`[indexText] skipping large file (${byteLen} bytes) ${normUri}`);
        return;
    }

    // Reset any old defs/refs for this file (purge stale def caches too)
    const oldDefs = definitions.get(normUri) || [];
    for (const d of oldDefs) { tablesByName.delete(d.name); columnsByTable.delete(d.name); }
    definitions.delete(normUri);
    deleteRefsForFile(normUri);

    const defs: SymbolDef[] = [];
    const lines = text.split(/\r?\n/);
    const cleanLines = lines.map(stripComments); // <-- compute once
    const localRefs: ReferenceDef[] = [];

    // Precompile regexes once (reset lastIndex before each use)
    const tableRegexG = /\b(from|join|update|into)\s+([a-zA-Z0-9_\[\]\.]+)/gi;
    const aliasColRegexG = /([a-zA-Z0-9_]+)\.(\[[^\]]+\]|[a-zA-Z_][a-zA-Z0-9_]*)/g;
    const bareColRegexG = /\[([a-zA-Z0-9_ ]+)\]|([a-zA-Z_][a-zA-Z0-9_]*)/g;
    const fromJoinRegexG = /\b(from|join)\s+([a-zA-Z0-9_\[\]\.]+)/gi;

    // Cache for statement scopes per line (avoids O(N²))
    const stmtScopeByLine = new Map<number, { text: string; tablesInScope: string[] }>();

    // --- scan for definitions ---
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        const match = /^\s*CREATE\s+(PROCEDURE|FUNCTION|VIEW|TABLE|TYPE)\s+([a-zA-Z0-9_\[\]\.]+)/i.exec(line);
        if (!match) { continue; }

        const kind = match[1].toUpperCase();
        const rawName = match[2];
        const norm = normalizeName(rawName);

        const sym: SymbolDef = { name: norm, rawName, uri: normUri, line: i };

        if (kind === "TABLE" || (kind === "TYPE" && /\bAS\s+TABLE\b/i.test(line))) {
            // Build block from current line and parse columns (linear helper)
            const createBlock = lines.slice(i).join("\n");
            const cols = parseColumnsFromCreateBlock(createBlock, 0);

            // Keep columnsByTable in sync (normalized)
            const set = new Set<string>();
            for (const c of cols) { set.add(c.name); }
            columnsByTable.set(norm, set);

            // Definitions for columns with correct absolute positions
            (sym as any).columns = cols.map(c => {
                const fileLine = i + c.line + 1;   // correct offset: skip CREATE line
                const sourceLineText = lines[fileLine] || "";

                let startCol = sourceLineText.indexOf(c.rawName);
                if (startCol < 0) { startCol = sourceLineText.indexOf(c.name); }
                if (startCol < 0) { startCol = Math.max(0, sourceLineText.search(/\S/)); }

                return {
                    name: c.name,
                    rawName: c.rawName,
                    line: fileLine,
                    start: startCol,
                    end: startCol + c.rawName.length
                };
            }) as any;

            // Local column refs (definition locations)
            for (const c of cols) {
                const fileLine = i + c.line + 1;
                const sourceLineText = lines[fileLine] || "";

                let startCol = sourceLineText.indexOf(c.rawName);
                if (startCol < 0) { startCol = sourceLineText.indexOf(c.name); }
                if (startCol < 0) { startCol = Math.max(0, sourceLineText.search(/\S/)); }

                localRefs.push({
                    name: `${norm}.${c.name}`,
                    uri: normUri,
                    line: fileLine,
                    start: startCol,
                    end: startCol + c.rawName.length,
                    kind: "column"
                });
            }
        }

        defs.push(sym);

        // Definition ref for the object name on its CREATE line
        const idx = line.toLowerCase().indexOf(rawName.toLowerCase());
        if (idx >= 0) {
            localRefs.push({
                name: norm,
                uri: normUri,
                line: i,
                start: idx,
                end: idx + rawName.length,
                kind: "table"
            });
        }
    }

    // Aliases computed once and reused
    const aliases = extractAliases(text);

    // --- scan for usages (tables, alias columns, bare columns) ---
    for (let i = 0; i < lines.length; i++) {
        const clean = cleanLines[i];

        // table usages
        tableRegexG.lastIndex = 0;
        for (let m = tableRegexG.exec(clean); m; m = tableRegexG.exec(clean)) {
            const raw = m[2];
            const tnorm = normalizeName(raw);

            const keywordIndex = m.index;
            const tableIndex = keywordIndex + m[0].indexOf(raw);

            localRefs.push({
                name: tnorm,
                uri: normUri,
                line: i,
                start: tableIndex,
                end: tableIndex + raw.length,
                kind: "table"
            });
        }

        // alias.column usages
        aliasColRegexG.lastIndex = 0;
        for (let m2 = aliasColRegexG.exec(clean); m2; m2 = aliasColRegexG.exec(clean)) {
            const alias = m2[1].toLowerCase();
            const col = normalizeName(m2[2].replace(/[\[\]]/g, ""));
            const table = aliases.get(alias);
            if (!table) { continue; }

            const key = `${normalizeName(table)}.${col}`;
            localRefs.push({
                name: key,
                uri: normUri,
                line: i,
                start: m2.index,
                end: m2.index + m2[0].length,
                kind: "column"
            });
        }

        // unaliased column usages (allow [Column])
        bareColRegexG.lastIndex = 0;
        for (let m3 = bareColRegexG.exec(clean); m3; m3 = bareColRegexG.exec(clean)) {
            const tokenRaw = (m3[1] || m3[2] || "");
            const col = normalizeName(tokenRaw.replace(/[\[\]]/g, ""));
            if (isSqlKeyword(col)) { continue; }

            // skip if part of alias.column
            const tokenStart = m3.index;
            const tokenEnd = m3.index + (m3[0] ? m3[0].length : tokenRaw.length);
            const beforeChar = clean.charAt(Math.max(0, tokenStart - 1));
            const afterChar = clean.charAt(tokenEnd);
            if (beforeChar === "." || afterChar === ".") { continue; }

            // Get (cached) statement text and tables-in-scope for this line
            let scope = stmtScopeByLine.get(i);
            if (!scope) {
                const stmtText = getCurrentStatement(
                    documents.get(normUri as any) ?? { getText: () => text } as any,
                    { line: i, character: 0 }
                );
                const tablesInScope: string[] = [];
                fromJoinRegexG.lastIndex = 0;
                for (let fm = fromJoinRegexG.exec(stmtText); fm; fm = fromJoinRegexG.exec(stmtText)) {
                    tablesInScope.push(normalizeName(fm[2]));
                }
                scope = { text: stmtText, tablesInScope };
                stmtScopeByLine.set(i, scope);
            }

            const { tablesInScope } = scope;
            if (tablesInScope.length === 0) { continue; } // fast skip

            const candidateTables: string[] = [];
            for (const t of tablesInScope) {
                if (columnsByTable.get(t)?.has(col)) { candidateTables.push(t); }
            }

            if (candidateTables.length === 1) {
                localRefs.push({
                    name: `${candidateTables[0]}.${col}`,
                    uri: normUri,
                    line: i,
                    start: m3.index,
                    end: m3.index + tokenRaw.length,
                    kind: "column"
                });
            } else if (candidateTables.length > 1) {
                for (const t of candidateTables) {
                    localRefs.push({
                        name: `${t}.${col}`,
                        uri: normUri,
                        line: i,
                        start: m3.index,
                        end: m3.index + tokenRaw.length,
                        kind: "column"
                    });
                }
            }
        }
    }

    // Save definitions
    definitions.set(normUri, defs);

    // De-dupe local refs and persist per-file by name
    const byName = new Map<string, ReferenceDef[]>();
    for (const ref of localRefs) { const arr = byName.get(ref.name) || []; arr.push(ref); byName.set(ref.name, arr); }
    for (const [name, arr] of byName) {
        const seen = new Set<string>();
        const dedup = arr.filter(r => { const k = `${r.line}:${r.start}:${r.end}`; if (seen.has(k)) { return false; } seen.add(k); return true; });
        setRefsForFile(name, normUri, dedup);
    }

    // Save aliases (reuse the map we already computed)
    aliasesByUri.set(normUri, aliases);

    // Index tables by name for quick definition lookup
    for (const def of defs) { tablesByName.set(def.name, def); }

    safeLog(
        `[indexText] indexed uri=${normUri}, defs=${defs.length}, tablesByName=${tablesByName.size}, aliases=${(aliasesByUri.get(normUri)?.size) || 0}`
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


// --- Definitions (unchanged) ---
function findColumnInTable(tableName: string, colName: string): Location[] {
    const results: Location[] = [];
    for (const defs of definitions.values()) {
        for (const def of defs) {
            if (normalizeName(def.name) === normalizeName(tableName) && def.columns) {
                for (const col of def.columns) {
                    if (col.name === colName) {
                        const startChar = typeof (col as any).start === "number" ? (col as any).start : 0;
                        const endChar = typeof (col as any).end === "number" ? (col as any).end : 200;
                        results.push({
                            uri: def.uri,
                            range: {
                                start: { line: col.line, character: startChar },
                                end: { line: col.line, character: endChar }
                            }
                        });
                    }
                }
            }
        }
    }
    return results;
}


function findTableOrColumn(word: string): Location[] {
    const results: Location[] = [];
    for (const defs of definitions.values()) {
        for (const def of defs) {
            if (normalizeName(def.name) === word) {
                results.push({
                    uri: def.uri,
                    range: {
                        start: { line: def.line, character: 0 },
                        end: { line: def.line, character: 200 }
                    }
                });
            }
            if (def.columns) {
                for (const col of def.columns) {
                    if (col.name === word) {
                        results.push({
                            uri: def.uri,
                            range: {
                                start: { line: col.line, character: 0 },
                                end: { line: col.line, character: 200 }
                            }
                        });
                    }
                }
            }
        }
    }
    return results;
}

connection.onDefinition((params: DefinitionParams): Location[] | null => {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) { return null; }

    const wordRange = getWordRangeAtPosition(doc, params.position);
    if (!wordRange) { return null; }

    const rawWord = doc.getText(wordRange);
    let word = normalizeName(rawWord);

    const fullText = doc.getText();
    const lineText = doc.getText({
        start: { line: params.position.line, character: 0 },
        end: { line: params.position.line, character: Number.MAX_VALUE }
    });

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

    // --- 2) Bare column semantic resolution ---
    const bareColRegex = /\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
    let m: RegExpExecArray | null;
    while ((m = bareColRegex.exec(lineText))) {
        const col = normalizeName(m[1]);
        if (col === word) {
            // gather tables in scope from FROM/JOIN in fullText
            const tablesInScope: string[] = [];
            const fromJoinRegex = /\b(from|join)\s+([a-zA-Z0-9_\[\]\.]+)/gi;
            let fm: RegExpExecArray | null;
            while ((fm = fromJoinRegex.exec(fullText))) {
                tablesInScope.push(normalizeName(fm[2]));
            }

            // filter tables that actually contain this column
            const candidateTables: string[] = [];
            for (const t of tablesInScope) {
                if (columnsByTable.get(t)?.has(col)) {
                    candidateTables.push(t);
                }
            }

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
                    if (!aliasesMap.has(k)) {aliasesMap.set(k, v);}
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
                        if (seenCols.has(normCol)) {continue;}
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


connection.onHover((params): Hover | null => {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) { return null; }

    const range = getWordRangeAtPosition(doc, params.position);
    if (!range) { return null; }

    const rawWord = doc.getText(range);
    const word = normalizeName(rawWord);

    const aliasInfo = resolveAlias(rawWord, doc, params.position);

    // --- alias.column ---
    if (aliasInfo.table && aliasInfo.column) {
        for (const defs of definitions.values()) {
            for (const def of defs) {
                if (normalizeName(def.name) === aliasInfo.table && def.columns) {
                    for (const c of def.columns) {
                        if (c.name === aliasInfo.column) {
                            return {
                                contents: {
                                    kind: MarkupKind.Markdown,
                                    value: `**Column** \`${c.rawName}\`${c.type ? ` (${c.type})` : ""} in table \`${def.rawName}\``
                                }
                            };
                        }
                    }
                }
            }
        }
    }

    // --- alias only ---
    if (aliasInfo.table && !aliasInfo.column) {
        return {
            contents: {
                kind: MarkupKind.Markdown,
                value: `**Alias** \`${rawWord}\` for table \`${aliasInfo.table}\``
            }
        };
    }

    // --- table / column scoped ---
    const statementText = getCurrentStatement(doc, params.position);
    const candidateTables = new Set<string>();
    const fromJoinRegex = /\b(from|join)\s+([a-zA-Z0-9_\[\]\.]+)/gi;
    let m: RegExpExecArray | null;
    while ((m = fromJoinRegex.exec(statementText))) {
        const table = normalizeName(m[2].replace(/^dbo\./i, ""));
        candidateTables.add(table);
    }

    const matches: string[] = [];
    for (const defs of definitions.values()) {
        for (const def of defs) {
            if (candidateTables.has(normalizeName(def.name)) && def.columns) {
                for (const col of def.columns) {
                    if (col.name === word) {
                        matches.push(`${col.rawName}${col.type ? ` (${col.type})` : ""} in table ${def.rawName}`);
                    }
                }
            }
        }
    }

    if (matches.length === 1) {
        return { contents: { kind: MarkupKind.Markdown, value: `**Column** ${matches[0]}` } };
    } else if (matches.length > 1) {
        return { contents: { kind: MarkupKind.Markdown, value: `**Column** \`${rawWord}\` (ambiguous, found in: ${matches.join(", ")})` } };
    }

    // --- global fallback ---
    for (const defs of definitions.values()) {
        for (const def of defs) {
            if (normalizeName(def.name) === word) {
                return { contents: { kind: MarkupKind.Markdown, value: `**Table** \`${def.rawName}\`` } };
            }
            if (def.columns) {
                for (const col of def.columns) {
                    if (col.name === word) {
                        return {
                            contents: {
                                kind: MarkupKind.Markdown,
                                value: `**Column** \`${col.rawName}\`${col.type ? ` (${col.type})` : ""} in table \`${def.rawName}\``
                            }
                        };
                    }
                }
            }
        }
    }

    return null;
});


// Full validator with @-skip and SELECT-alias skip
async function validateTextDocument(doc: TextDocument): Promise<void> {
    if (!enableValidation || !isIndexReady) {
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




function extractSelectAliasesFromStatement(statementText: string): Set<string> {
    const out = new Set<string>();

    // Get text after SELECT, before FROM (or end of statement if no FROM)
    const m = /select\b([\s\S]*?)(\bfrom\b|$)/i.exec(statementText);
    if (!m) { return out; }
    const selectList = m[1];

    // Split the SELECT list by top-level commas (handles functions, CASE, windows, etc.)
    const items = splitByCommasRespectingParens(selectList);

    for (const rawItem of items) {
        const item = rawItem.trim();
        if (!item || item === "*") { continue; }

        // 1) AS alias   e.g.,  SUM(x) AS Total  or  [Full Name] AS EmpName
        let asMatch = /\bas\s+(\[?[A-Za-z_][A-Za-z0-9_ ]*\]?)/i.exec(item);
        if (asMatch) {
            const aliasRaw = asMatch[1].replace(/[\[\]]/g, "");
            out.add(normalizeName(aliasRaw));
            continue;
        }

        // 2) trailing alias (no AS)   e.g.,  SUM(x) Total    or  (expr) Alias
        //    heuristic: take last identifier-looking token not part of a dotted path
        const tail = /(\[?[A-Za-z_][A-Za-z0-9_ ]*\]?)\s*$/i.exec(item);
        if (tail) {
            const candidate = tail[1];
            // Avoid catching dotted names or simple column references as aliases
            // (we only accept if there's actually an expression before the alias)
            const before = item.slice(0, item.length - candidate.length).trim();
            const looksLikeExpr = /[\(\)\+\-\*\/%]|case\b|over\b|\brow_number\b|\bsum\b|\bcount\b|\bmin\b|\bmax\b|\bconvert\b|\bcast\b|\bcoalesce\b/i.test(before);
            if (looksLikeExpr) {
                const norm = normalizeName(candidate.replace(/[\[\]]/g, ""));
                if (norm) { out.add(norm); }
            }
        }
    }
    return out;
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
function getWordRangeAtPosition(doc: TextDocument, pos: { line: number; character: number }) {
    const lineText = doc.getText({
        start: { line: pos.line, character: 0 },
        end: { line: pos.line, character: Number.MAX_VALUE }
    });
    const regex = /[a-zA-Z0-9_\[\]\.]+/g;
    let match: RegExpExecArray | null;
    while ((match = regex.exec(lineText))) {
        const start = match.index;
        const end = start + match[0].length;
        if (pos.character >= start && pos.character <= end) {
            return { start: { line: pos.line, character: start }, end: { line: pos.line, character: end } };
        }
    }
    return null;
}

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

function getCurrentStatement(doc: TextDocument, position: Position): string {
    const fullText = doc.getText();
    const lines = fullText.split(/\r?\n/);
    const currentLine = position.line;
    let start = currentLine;
    let end = currentLine;

    // look upward until BEGIN/; or top
    for (let i = currentLine; i >= 0; i--) {
        if (/^\s*begin\b/i.test(lines[i]) || /;\s*$/.test(lines[i])) {
            start = i + 1;
            break;
        }
        if (i === 0) { start = 0; }
    }

    // look downward until END/; or bottom
    for (let i = currentLine; i < lines.length; i++) {
        if (/^\s*end\b/i.test(lines[i]) || /;\s*$/.test(lines[i])) {
            end = i;
            break;
        }
        if (i === lines.length - 1) { end = lines.length - 1; }
    }

    return lines.slice(start, end + 1).join("\n");
}


function resolveAlias(rawWord: string, doc: TextDocument, position: Position): { table?: string; column?: string } {
    const statementText = getCurrentStatement(doc, position);
    const aliases = extractAliases(statementText);

    // alias.column → split and resolve
    if (rawWord.includes(".")) {
        const [aliasRaw, colRaw] = rawWord.split(".");
        if (aliasRaw && colRaw) {
            const alias = aliasRaw.toLowerCase();
            const col = normalizeName(colRaw);
            const table = aliases.get(alias);
            if (table) {
                return { table: normalizeName(table), column: col };
            }
        }
    }

    // alias only → resolve to table
    const aliasTable = aliases.get(normalizeName(rawWord));
    if (aliasTable) {
        return { table: normalizeName(aliasTable) };
    }

    return {};
}

// ---------- Startup ----------
documents.listen(connection);
connection.listen();
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
import { Worker } from 'worker_threads';
import * as crypto from 'crypto';

const WORKER_PATH = require.resolve("./sqlAstWorker.js");
const MAX_WORKERS = 2;


type Pending = { id: string, sql: string, opts: any, resolve: (v: any) => void, reject: (e: any) => void, deadline: number };

class AstWorkerPool {
    workers: Worker[] = [];
    idle: Worker[] = [];
    pending: Pending[] = [];

    // --- minimal additions for backoff/retry ---
    private spawnFailures = 0; // counts recent consecutive spawn failures
    private readonly MAX_SPAWN_ATTEMPTS = 6;      // stop respawning after this many attempts
    private readonly BASE_BACKOFF_MS = 200;       // base backoff (will expon. backoff)
    // ------------------------------------------------

    constructor() { for (let i = 0; i < MAX_WORKERS; i++) { this.spawnWorker(); } }

    spawnWorker() {
        // don't spawn indefinitely if workers keep crashing
        if (this.spawnFailures >= this.MAX_SPAWN_ATTEMPTS) {
            console.error('[AstWorkerPool] max spawn attempts reached, not spawning more workers');
            return;
        }

        const w = new Worker(WORKER_PATH);

        // when a real message comes back from worker, consider it a healthy sign:
        // reduce the failure counter a bit (but keep it >= 0) and then forward to your handler.
        w.on('message', (m: any) => {
            // small stabilization: a successful message likely means the worker is healthy
            this.spawnFailures = Math.max(0, this.spawnFailures - 1);
            this.onMessage(w, m);
        });

        // on exit, remove worker and schedule a respawn using exponential backoff
        w.on('exit', (code: number) => {
            this.workers = this.workers.filter(x => x !== w);
            this.idle = this.idle.filter(x => x !== w);

            // increment failure count and schedule respawn with backoff
            this.spawnFailures++;
            const attempt = this.spawnFailures;
            const delay = Math.min(30_000, this.BASE_BACKOFF_MS * Math.pow(2, Math.max(0, attempt - 1)));
            console.warn(`[AstWorkerPool] worker exited (code=${code}); scheduling respawn in ${delay}ms (attempt ${attempt}/${this.MAX_SPAWN_ATTEMPTS})`);
            setTimeout(() => {
                // double-check the limit again before spawning
                if (this.spawnFailures < this.MAX_SPAWN_ATTEMPTS) {
                    this.spawnWorker();
                } else {
                    console.error('[AstWorkerPool] reached max spawn attempts; not respawning further.');
                }
            }, delay);
        });

        this.workers.push(w);
        this.idle.push(w);
    }

    onMessage(worker: Worker, msg: any) {
        const pIndex = this.pending.findIndex(p => p.id === msg.id);
        if (pIndex === -1) { return; }
        const p = this.pending.splice(pIndex, 1)[0];
        if (msg.error) { p.resolve(null); }
        else { p.resolve(msg.ast); }
        this.idle.push(worker);
        this.runQueue();
    }

    runQueue() {
        while (this.idle.length && this.pending.length) {
            const w = this.idle.shift()!;
            const p = this.pending.shift()!;
            try {
                w.postMessage({ id: p.id, sql: p.sql, opts: p.opts });
            } catch (e) {
                // if postMessage fails, requeue pending and respawn worker
                this.pending.unshift(p);
                // use spawnWorker() — it now checks max attempts and won't spin forever
                this.spawnWorker();
            }
        }
    }

    parse(sql: string, opts: any, timeout = 800) {
        return new Promise((resolve, reject) => {
            const id = Math.random().toString(36).slice(2);
            this.pending.push({ id, sql, opts, resolve, reject, deadline: Date.now() + timeout });
            // If an idle worker exists, post next pending
            const w = this.idle.shift();
            if (w) {
                const p = this.pending[0];
                w.postMessage({ id: p.id, sql: p.sql, opts: p.opts });
            }
            // simple timeout
            setTimeout(() => {
                const idx = this.pending.findIndex(p => p.id === id);
                if (idx !== -1) { this.pending.splice(idx, 1); resolve(null); }
            }, timeout + 50);
        });
    }
}


const astPool = new AstWorkerPool();


class LruCache {
    private map = new Map<string, { value: any; ts: number }>();
    capacity = 2000;
    ttlMs = 30 * 60 * 1000;

    has(k: string): boolean {
        const e = this.map.get(k);
        if (!e) { return false; }
        if (Date.now() - e.ts > this.ttlMs) {
            this.map.delete(k);
            return false;
        }
        return true;
    }

    get(k: string): any | null {
        const e = this.map.get(k);
        if (!e) { return null; }
        if (Date.now() - e.ts > this.ttlMs) {
            this.map.delete(k);
            return null;
        }
        // refresh LRU position
        this.map.delete(k);
        this.map.set(k, e);
        return e.value;
    }

    set(k: string, v: any) {
        if (this.map.size >= this.capacity) {
            const first = this.map.keys().next().value;
            if (first !== undefined) {
                this.map.delete(first);
            }
        }
        this.map.set(k, { value: v, ts: Date.now() });
    }
}
const astCache = new LruCache();

// ---------- Connection + Documents ----------
const connection = createConnection(ProposedFeatures.all);
const documents: TextDocuments<TextDocument> = new TextDocuments(TextDocument);
const columnsByTable = new Map<string, Set<string>>();
const aliasesByUri = new Map<string, Map<string, string>>();
const definitions = new Map<string, SymbolDef[]>();
const referencesIndex = new Map<string, Map<string, ReferenceDef[]>>();


// Helper: check if the pool has at least one worker available (simple readiness)
function isAstPoolReady(): boolean {
    try {
        // astPool is constructed at module load; ensure at least one worker exists
        return Array.isArray((astPool as any).workers) && (astPool as any).workers.length > 0;
    } catch {
        return false;
    }
}


function contentHash(s: string) {
    return crypto.createHash('sha1').update(s).digest('hex');
}


async function parseSqlWithWorker(sql: string, opts: any = {}, timeoutMs = 2000): Promise<any> {
    try {
        // cheap guard - avoid parsing huge statements
        if (!sql || sql.length > 4 * 1024) { return null; }

        const key = contentHash(sql);
        const cached = astCache.get(key);
        if (cached !== null && cached !== undefined) {
            return cached;
        }

        if (!isAstPoolReady()) {
            safeLog('[parseSqlWithWorker] ast pool not ready');
            // cache null so we don't repeatedly try on hot paths
            astCache.set(key, null);
            return null;
        }

        // ask the pool (it returns null on failure/timeout)
        const ast = await astPool.parse(sql, opts, timeoutMs);
        // cache (including null)
        astCache.set(key, ast ?? null);
        return ast ?? null;
    } catch (e) {
        safeLog('[parseSqlWithWorker] unexpected error: ' + String(e));
        return null;
    }
}


// ---------- AST helpers (drop into server.ts) ----------
function walkAst(node: any, fn: (n: any) => void) {
    fn(node);
    for (const key in node) {
        const child = node[key];
        if (Array.isArray(child)) {
            child.forEach(c => typeof c === 'object' && walkAst(c, fn));
        } else if (child && typeof child === 'object') {
            walkAst(child, fn);
        }
    }
}

function normalizeAstTableName(raw: any): string | null {
    if (!raw) { return null; }
    if (typeof raw === "string") { return normalizeName(String(raw).replace(/^dbo\./i, "")); }
    // sometimes table appears as object { table: 'X' } or { db: 'd', table: 't' }
    if (raw.table) { return normalizeName(String(raw.table).replace(/^dbo\./i, "")); }
    if (raw.name) { return normalizeName(String(raw.name).replace(/^dbo\./i, "")); }
    return null;
}

/**
 * Try to resolve which table provides `columnName` inside the given AST.
 * Returns normalized table name (matching columnsByTable keys) or null.
 */
function resolveColumnFromAst(ast: any, columnName: string): string | null {
    if (!ast) { return null; }
    const nodes = Array.isArray(ast) ? ast : [ast];
    columnName = normalizeName(columnName);

    for (const root of nodes) {
        let found: string | null = null;

        // Walk AST; for each SELECT node, build alias->table map and search column_ref nodes
        walkAst(root, (n) => {
            if (!n || typeof n !== "object") { return; }
            if (n.type === "select" || n.ast === "select") {
                const aliasMap = new Map<string, string>(); // aliasLower -> tableNorm
                const fromArr = Array.isArray(n.from) ? n.from : (n.from ? [n.from] : []);
                for (const f of fromArr) {
                    try {
                        // alias may appear in multiple shapes: as / alias / as.value
                        let alias: any = f.as || f.alias || (f.as && f.as.value) || (f.alias && f.alias.value);
                        if (alias && typeof alias === "object") { alias = alias.value || alias.name; }
                        if (typeof alias === "string") { alias = alias.replace(/[\[\]]/g, "").toLowerCase(); }

                        // table may be f.table (string) or f.expr.table etc.
                        let table = null;
                        if (typeof f.table === "string") { table = f.table; }
                        else if (f.table && f.table.value) { table = f.table.value; }
                        else if (f.expr && (f.expr.table || f.expr.name)) { table = f.expr.table || f.expr.name; }

                        if (table) {
                            aliasMap.set((alias || String(table).toLowerCase()), normalizeName(String(table).replace(/^dbo\./i, "")));
                        } else if (f.expr && f.expr.type === "select") {
                            // subquery: preserve alias so column refs pointing to the alias can be detected,
                            // but we can't map subquery -> real table here.
                            if (alias) { aliasMap.set(alias, "__subquery__"); }
                        }
                    } catch (e) { /* ignore malformed from entries */ }
                }

                // find any column_ref matching the requested column
                walkAst(n, (m) => {
                    if (!m || typeof m !== "object") { return; }
                    // typical node-sql-parser column node: { type: 'column_ref', table: 't', column: 'col' }
                    if ((m.type === "column_ref" || m.ast === "column_ref") && m.column) {
                        const col = normalizeName(String(m.column));
                        if (col === columnName) {
                            if (m.table) {
                                const rawTable = String(m.table).replace(/[\[\]]/g, "").toLowerCase();
                                const mapped = aliasMap.get(rawTable) || normalizeName(rawTable.replace(/^dbo\./i, ""));
                                if (mapped) { found = mapped; }
                            } else {
                                // unqualified column_ref — if only one table in aliasMap, attribute to it
                                if (aliasMap.size === 1) {
                                    found = Array.from(aliasMap.values())[0];
                                }
                            }
                        }
                    }
                });
            }
        });

        if (found) { return found; }
    }
    return null;
}

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
    for (const arr of byUri.values()) {
        out.push(...arr);
    }
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
): Array<{ name: string; rawName: string; type?: string; line: number; start: number; end: number }> {
    const rows = splitCreateBodyIntoRowsWithPositions(blockText, startLine);
    const out: Array<{ name: string; rawName: string; type?: string; line: number; start: number; end: number }> = [];

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

        // Try to grab the data type: the word(s) immediately after the column name
        // Example: "Salary INT NOT NULL" → colType = "INT"
        let colType: string | undefined;
        const afterName = r.text.slice(m[0].length).trim();
        const typeMatch = /^([A-Za-z0-9_]+(?:\s*\([^)]*\))?)/.exec(afterName);
        if (typeMatch) {
            colType = typeMatch[1].trim();
        }

        // Compute true start column by searching this raw token on the actual source line.
        const sourceLine = allLines[r.line] ?? "";
        let startCol = sourceLine.indexOf(raw);
        if (startCol < 0) {
            startCol = sourceLine.search(new RegExp(`\\b${tokenName.replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&")}\\b`));
        }
        if (startCol < 0) { startCol = r.col; }

        out.push({
            name,
            rawName: raw,
            type: colType,
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

            // Attempt optional AST parse for this CREATE block to discover any additional columns (pool-only)
            try {
                if (!isAstPoolReady()) {
                    safeLog('[indexText][AST] pool not ready — skipping AST parse for CREATE block');
                } else {
                    // Fire-and-forget parse with short timeout so we don't block indexing
                    parseSqlWithWorker(createBlock, { database: "mssql" }, 800)
                        .then((ast) => {
                            try {
                                if (!ast) { return; }
                                const astColsRaw: string[] = [];
                                if (ast && (ast.type === "create" || (Array.isArray(ast) && ast.length && ast[0].type === "create"))) {
                                    const node = Array.isArray(ast) ? ast[0] : ast;
                                    const defs = node.create_definitions || node.createDefinitions || [];
                                    for (const d of defs) {
                                        try {
                                            const cname = (d.column && (d.column.column || d.column.name)) || d.name || d.field;
                                            if (cname) { astColsRaw.push(String(cname).trim()); }
                                        } catch { /* ignore per-column errors */ }
                                    }
                                }

                                if (astColsRaw.length) {
                                    // prepare sets for dedupe
                                    const existingCols = new Set<string>(cols.map(c => normalizeName(c.name)));
                                    const existingSymCols = new Set<string>((sym.columns || []).map((c: any) => normalizeName((c.rawName || c.name || "").replace(/^\[|\]$/g, ""))));
                                    const set = columnsByTable.get(norm) || new Set<string>();

                                    // collect new local refs that we will add (deduped by pos)
                                    const newLocalRefs: ReferenceDef[] = [];

                                    for (const rawAc of astColsRaw) {
                                        const acNorm = normalizeName(rawAc);
                                        if (existingCols.has(acNorm) || existingSymCols.has(acNorm) || set.has(acNorm)) {
                                            continue;
                                        }

                                        // Add to cols list (keeps downstream logic compatible)
                                        cols.push({ name: acNorm, rawName: rawAc, line: 0, start: 0, end: rawAc.length });

                                        // Add to per-table set
                                        set.add(acNorm);

                                        // Add to sym.columns, avoiding duplicates
                                        if (!sym.columns) { sym.columns = []; }
                                        const alreadyInSym = (sym.columns as any[]).some((c: any) => normalizeName((c.rawName || c.name || "").replace(/^\[|\]$/g, "")) === acNorm);
                                        if (!alreadyInSym) {
                                            (sym.columns as any[]).push({
                                                name: acNorm,
                                                rawName: rawAc,
                                                line: i + 1,
                                                start: 0,
                                                end: rawAc.length
                                            });
                                        }

                                        // prepare local ref
                                        const newRef: ReferenceDef = {
                                            name: `${norm}.${acNorm}`,
                                            uri: normUri,
                                            line: i + 1,
                                            start: 0,
                                            end: rawAc.length,
                                            kind: "column"
                                        };
                                        const posKey = `${newRef.line}:${newRef.start}:${newRef.end}`;
                                        if (!newLocalRefs.some(r => `${r.line}:${r.start}:${r.end}` === posKey)) {
                                            newLocalRefs.push(newRef);
                                        }

                                        safeLog(`[indexText][AST worker] discovered column '${rawAc}' for ${norm}`);
                                    }

                                    // persist set back
                                    columnsByTable.set(norm, set);

                                    // merge newLocalRefs into the per-file references (dedupe by position)
                                    for (const ref of newLocalRefs) {
                                        const key = ref.name;
                                        const existingForKey = getRefs(key).filter(r => r.uri === normUri);

                                        // mergedByPos will dedupe by line:start:end
                                        const mergedByPos = new Map<string, ReferenceDef>();
                                        for (const r of existingForKey) {
                                            mergedByPos.set(`${r.line}:${r.start}:${r.end}`, r);
                                        }
                                        mergedByPos.set(`${ref.line}:${ref.start}:${ref.end}`, ref);

                                        const merged = Array.from(mergedByPos.values());
                                        setRefsForFile(key, normUri, merged);
                                    }
                                }
                            } catch (mergeErr) {
                                safeLog('[indexText][AST worker] merge failed: ' + String(mergeErr));
                            }
                        })
                        .catch((pErr) => {
                            safeLog('[indexText][AST worker] parse failed: ' + String(pErr));
                        });
                }
            } catch (e) {
                safeLog('[indexText][AST worker] unexpected error: ' + String(e));
            }

            // Keep columnsByTable in sync (normalized)
            const set = new Set<string>();
            for (const c of cols) { set.add(normalizeName(c.name)); }
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
                    type: c.type,
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
                // keep current immediate behavior (add multiple candidate refs)
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

                // capture values for the async closure
                const stmtText = scope.text;
                const tokenStart = m3.index;
                const tokenEnd = m3.index + tokenRaw.length;
                const localCandidateTables = [...candidateTables];

                // Attempt AST-based disambiguation asynchronously (pool-only)
                try {
                    if (!isAstPoolReady()) {
                        safeLog('[indexText][AST disambiguation] pool not ready — skipping parse for stmt');
                    } else {
                        parseSqlWithWorker(stmtText, { database: "mssql" }, 700)
                            .then((ast) => {
                                try {
                                    const resolved = resolveColumnFromAst(ast, col);
                                    if (!resolved) { return; }

                                    let resolvedNorm = normalizeName(resolved);

                                    // resolved might be an alias name; map via statement aliases if needed
                                    const stmtAliases = extractAliases(stmtText);
                                    const mapped = stmtAliases.get(resolved.toLowerCase());
                                    if (mapped) { resolvedNorm = normalizeName(mapped); }

                                    // only act if AST resolved to one of our candidate tables
                                    if (!localCandidateTables.includes(resolvedNorm)) { return; }

                                    const resolvedKey = `${resolvedNorm}.${col}`;

                                    // ensure resolved key has a ref for this file/position
                                    const existingForResolved = (referencesIndex.get(resolvedKey)?.get(normUri)) || [];
                                    const hasRef = existingForResolved.some(r => r.line === i && r.start === tokenStart && r.end === tokenEnd);
                                    if (!hasRef) {
                                        const newRef = { name: resolvedKey, uri: normUri, line: i, start: tokenStart, end: tokenEnd, kind: "column" } as ReferenceDef;
                                        // merge with any other refs for this uri (preserve others) and dedupe by pos
                                        const mergedByPos = new Map<string, ReferenceDef>();
                                        for (const r of existingForResolved) {
                                            mergedByPos.set(`${r.line}:${r.start}:${r.end}`, r);
                                        }
                                        mergedByPos.set(`${newRef.line}:${newRef.start}:${newRef.end}`, newRef);
                                        setRefsForFile(resolvedKey, normUri, Array.from(mergedByPos.values()));
                                    }

                                    // remove the previously-added refs for other candidate tables at this exact position
                                    for (const t of localCandidateTables) {
                                        if (t === resolvedNorm) { continue; }
                                        const key = `${t}.${col}`;
                                        const byUri = referencesIndex.get(key);
                                        const arrForThis = byUri?.get(normUri) || [];
                                        const filtered = arrForThis.filter(r => !(r.line === i && r.start === tokenStart && r.end === tokenEnd));
                                        setRefsForFile(key, normUri, filtered);
                                    }

                                    safeLog(`[AST disambiguation] ${col} → ${resolvedNorm} in ${normUri}`);
                                } catch (e) {
                                    safeLog('[AST disambiguation] merge failed: ' + String(e));
                                }
                            })
                            .catch((pErr) => {
                                safeLog('[AST disambiguation] parse failed: ' + String(pErr));
                            });
                    }
                } catch (e) {
                    safeLog('[indexText][AST disambiguation] unexpected: ' + String(e));
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

    const range = getWordRangeAtPosition(doc, params.position);
    if (!range) { return null; }

    const rawWord = doc.getText(range);
    const word = normalizeName(rawWord);

    // --- AST-based resolution ---
    const statementText = getCurrentStatement(doc, params.position);
    if (statementText) {
        try {
            const ast = await astPool.parse(statementText, { dialect: 'tsql' });
            if (ast) {
                const offset = offsetAt(doc, params.position) -
                    offsetAt(doc, { line: range.start.line, character: 0 });
                let tableName: string | null = null;

                walkAst(ast, (node: any) => {
                    if (node.type === 'column_ref' &&
                        normalizeName(node.column) === word &&
                        node.location &&
                        offset >= node.location.start &&
                        offset <= node.location.end) {
                        if (node.table) {
                            tableName = resolveAliasFromAst(node.table, ast) ?? normalizeName(node.table);
                        }
                    }
                });

                if (tableName) {
                    const defs = definitions.get(tableName);
                    if (defs) {
                        for (const def of defs) {
                            if (def.columns) {
                                const col = def.columns.find(c => normalizeName(c.name) === word);
                                if (col) {
                                    return {
                                        contents: {
                                            kind: MarkupKind.Markdown,
                                            value: `**Column** \`${col.rawName}\`${col.type ? ` ${col.type}` : ""} in table \`${def.rawName}\``
                                        }
                                    };
                                }
                            }
                        }
                    }
                }
            }
        } catch (e) {
            console.error('[hover] AST parse failed', e);
        }
    }

    // --- Fallback: alias.column handling ---
    const aliasInfo = resolveAlias(rawWord, doc, params.position);
    if (aliasInfo.table && aliasInfo.column) {
        for (const defs of definitions.values()) {
            for (const def of defs) {
                if (normalizeName(def.name) === aliasInfo.table && def.columns) {
                    for (const c of def.columns) {
                        if (c.name === aliasInfo.column) {
                            return {
                                contents: {
                                    kind: MarkupKind.Markdown,
                                    value: `**Column** \`${c.rawName}\`${c.type ? ` ${c.type}` : ""} in table \`${def.rawName}\``
                                }
                            };
                        }
                    }
                }
            }
        }
    }

    if (aliasInfo.table && !aliasInfo.column) {
        return {
            contents: {
                kind: MarkupKind.Markdown,
                value: `**Alias** \`${rawWord}\` for table \`${aliasInfo.table}\``
            }
        };
    }

    // --- Regex FROM/JOIN context ---
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
                        matches.push(`${col.rawName}${col.type ? ` ${col.type}` : ""} in table ${def.rawName}`);
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

    // --- Global fallback ---
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
                                value: `**Column** \`${col.rawName}\`${col.type ? ` ${col.type}` : ""} in table \`${def.rawName}\``
                            }
                        };
                    }
                }
            }
        }
    }

    return null;
});


function offsetAt(doc: TextDocument, pos: Position) {
    const text = doc.getText();
    const lines = text.split(/\r?\n/);
    let off = 0;
    for (let i = 0; i < pos.line; i++) { off += lines[i].length + 1; }
    return off + pos.character;
}

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

function getCurrentStatement(doc: { getText: (range?: any) => string }, position: { line: number; character: number }): string {
    try {
        const full = doc.getText();
        if (!full) { return ""; }

        // Build lines & compute absolute offset of position
        const lines = full.split(/\r?\n/);
        const lineIdx = Math.max(0, Math.min(position.line, lines.length - 1));
        let offset = 0;
        for (let i = 0; i < lineIdx; i++) { offset += lines[i].length + 1; } // +1 newline
        offset += Math.max(0, Math.min(position.character, lines[lineIdx].length));

        // Scanner that walks forward and records semicolons and WITH positions that are outside strings/comments.
        let inSingle = false;
        let inDouble = false;
        let inBracket = false; // [ ... ]
        let inLineComment = false;
        let inBlockComment = false;

        let lastStmtSep = -1;     // last semicolon index (outside quotes/comments) before offset
        let lastWithIndex = -1;   // last 'WITH' (word) index before offset (outside quotes/comments)

        // Helper to check word boundaries for "WITH"
        function isWordBoundaryChar(ch: string | undefined) {
            if (!ch) { return true; }
            return !(/[A-Za-z0-9_]/.test(ch));
        }

        // forward scan up to offset to record last semicolon and WITH occurrences
        for (let i = 0; i < offset; i++) {
            const ch = full[i];
            const chNext = full[i + 1];

            // handle line comment start --
            if (!inSingle && !inDouble && !inBlockComment && !inLineComment && ch === "-" && chNext === "-") {
                inLineComment = true;
                i++; // skip second '-'
                continue;
            }
            // handle block comment start /*
            if (!inSingle && !inDouble && !inLineComment && !inBlockComment && ch === "/" && chNext === "*") {
                inBlockComment = true;
                i++;
                continue;
            }
            // handle block comment end */
            if (inBlockComment && ch === "*" && chNext === "/") {
                inBlockComment = false;
                i++;
                continue;
            }
            // end of line ends line comment
            if (inLineComment && ch === "\n") {
                inLineComment = false;
                continue;
            }
            if (inLineComment || inBlockComment) {
                continue;
            }

            // bracketed identifier [ ... ]
            if (!inSingle && !inDouble && ch === "[") { inBracket = true; continue; }
            if (inBracket) {
                if (ch === "]") { inBracket = false; }
                continue;
            }

            // single-quote string
            if (!inDouble && ch === "'") {
                // if starting or ending single quote
                if (!inSingle) { inSingle = true; continue; }
                // if inSingle and next is also single => SQL escaped quote '', consume one and stay in string
                if (inSingle && full[i + 1] === "'") { i++; continue; }
                // closing quote
                inSingle = false;
                continue;
            }
            if (inSingle) { continue; }

            // double-quote string (identifiers or strings)
            if (!inSingle && ch === '"') {
                if (!inDouble) { inDouble = true; continue; }
                if (inDouble && full[i + 1] === '"') { i++; continue; }
                inDouble = false;
                continue;
            }
            if (inDouble) { continue; }

            // semicolon outside quotes/comments => statement separator
            if (ch === ";") {
                lastStmtSep = i;
                continue;
            }

            // check for 'WITH' token (case-insensitive) - ensure word boundaries
            const rem = full.length - i;
            if (rem >= 4) {
                const slice4 = full.substr(i, 4);
                if (/^with$/i.test(slice4)) {
                    const prev = full[i - 1];
                    const next = full[i + 4];
                    if (isWordBoundaryChar(prev) && isWordBoundaryChar(next)) {
                        lastWithIndex = i;
                    }
                }
            }
        } // end forward scan to offset

        // Determine start: prefer lastStmtSep+1, but if there is a WITH after that, include it
        let start = lastStmtSep + 1;
        if (lastWithIndex > lastStmtSep) {
            // include leading whitespace/newlines before WITH
            let wstart = lastWithIndex;
            while (wstart > 0 && /\s/.test(full[wstart - 1])) { wstart--; }
            start = wstart;
        }
        if (start < 0) { start = 0; }

        // Find next semicolon after offset (end boundary)
        let end = full.length;
        // resume state for scanning from offset to EOF to find the first semicolon outside strings/comments
        inSingle = false; inDouble = false; inBracket = false; inLineComment = false; inBlockComment = false;
        for (let i = offset; i < full.length; i++) {
            const ch = full[i];
            const chNext = full[i + 1];

            if (!inSingle && !inDouble && !inBlockComment && !inLineComment && ch === "-" && chNext === "-") {
                inLineComment = true; i++; continue;
            }
            if (!inSingle && !inDouble && !inLineComment && !inBlockComment && ch === "/" && chNext === "*") {
                inBlockComment = true; i++; continue;
            }
            if (inBlockComment && ch === "*" && chNext === "/") { inBlockComment = false; i++; continue; }
            if (inLineComment && ch === "\n") { inLineComment = false; continue; }
            if (inLineComment || inBlockComment) { continue; }

            if (!inSingle && !inDouble && ch === "[") { inBracket = true; continue; }
            if (inBracket) { if (ch === "]") { inBracket = false; } continue; }

            if (!inDouble && ch === "'") {
                if (!inSingle) { inSingle = true; continue; }
                if (inSingle && full[i + 1] === "'") { i++; continue; }
                inSingle = false; continue;
            }
            if (inSingle) { continue; }

            if (!inSingle && ch === '"') {
                if (!inDouble) { inDouble = true; continue; }
                if (inDouble && full[i + 1] === '"') { i++; continue; }
                inDouble = false; continue;
            }
            if (inDouble) { continue; }

            if (ch === ";") { end = i; break; }
        }

        const stmt = full.slice(start, end).trim();
        return stmt;
    } catch (e) {
        safeLog('[getCurrentStatement] error: ' + String(e));
        return "";
    }
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

function resolveAliasFromAst(alias: string, ast: any): string | null {
  const aliasNorm = normalizeName(alias);

  function searchFrom(fromArr: any[]): string | null {
    for (const f of fromArr) {
      // FROM Employee e
      if (f.as && normalizeName(f.as) === aliasNorm && f.table) {
        return normalizeName(f.table);
      }

      // FROM Employee (no alias)
      if (!f.as && f.table && normalizeName(f.table) === aliasNorm) {
        return normalizeName(f.table);
      }

      // FROM (SELECT ...) AS picks
      if (f.subquery) {
        // alias refers to the subquery itself
        if (f.as && normalizeName(f.as) === aliasNorm) {
          return "__subquery__";
        }
        // recurse into the inner FROMs
        const innerFrom = Array.isArray(f.subquery.from)
          ? f.subquery.from
          : (f.subquery.from ? [f.subquery.from] : []);
        const found = searchFrom(innerFrom);
        if (found) return found;
      }
    }
    return null;
  }

  const roots = Array.isArray(ast) ? ast : [ast];
  for (const root of roots) {
    if (root.from) {
      const res = searchFrom(Array.isArray(root.from) ? root.from : [root.from]);
      if (res) return res;
    }
  }
  return null;
}




// ---------- Startup ----------
documents.listen(connection);
connection.listen();
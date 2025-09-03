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
const referencesIndex = new Map<string, ReferenceDef[]>();
const tablesByName = new Map<string, SymbolDef>();

let isIndexReady = false;

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
    "distinct", "union", "all", "outer", "inner", "left", "right", "full", "top", "limit", "offset", "having"
]);

// ---------- Index structures ----------
interface ColumnDef {
    name: string;
    rawName: string;
    type?: string;
    line: number;
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
function normalizeName(name: string): string {
    let n = name.toLowerCase();
    n = n.replace(/[\[\]]/g, "");

    const parts = n.split(".");
    if (parts.length === 2) {
        const [schema, object] = parts;
        if (schema === "dbo") {
            return object;
        }
        return `${schema}.${object}`;
    }
    return n;
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

    // 1) Table aliases: FROM Employee e, JOIN ProjectEmployee pe
    const tableAliasRegex = /\b(from|join)\s+([a-zA-Z0-9_\[\]\.]+)\s+([a-zA-Z0-9_]+)/gi;
    let m: RegExpExecArray | null;
    while ((m = tableAliasRegex.exec(text))) {
        const rawTable = m[2];
        const alias = m[3].toLowerCase();
        aliases.set(alias, normalizeName(rawTable));
    }

    // 2) Subquery aliases: FROM (SELECT ...) e
    const subqueryAliasRegex = /\)\s+([a-zA-Z0-9_]+)/gi;
    let sm: RegExpExecArray | null;
    while ((sm = subqueryAliasRegex.exec(text))) {
        const alias = sm[1].toLowerCase();
        aliases.set(alias, "__subquery__");
    }

    return aliases;
}

// ---------- Column Parser ----------
function parseColumnsFromCreate(sql: string, startLine: number): ColumnDef[] {
    const m = /CREATE\s+(?:TABLE|TYPE)\s+[A-Za-z0-9_\[\]\.]+\s*(?:AS\s+TABLE)?\s*\(([\s\S]*?)\)\s*;/i.exec(sql);
    if (!m) {
        return [];
    }

    const body = stripComments(m[1]);
    const lines = body.split(/\r?\n/);

    const cols: ColumnDef[] = [];
    const constraintKeywords = ["primary key", "foreign key", "constraint", "index", "check", "unique"];

    for (let offset = 0; offset < lines.length; offset++) {
        const line = lines[offset];
        if (!line.trim()) {
            continue;
        }

        // --- split this line into column parts by commas not in parentheses ---
        let parts: string[] = [];
        let current = "";
        let depth = 0;
        for (const ch of line) {
            if (ch === "(") { depth++; }
            else if (ch === ")") { depth--; }
            if (ch === "," && depth === 0) {
                parts.push(current);
                current = "";
            } else {
                current += ch;
            }
        }
        if (current.trim()) {
            parts.push(current);
        }

        for (const part of parts) {
            const trimmed = part.trim();
            if (!trimmed) {
                continue;
            }
            const lowered = trimmed.toLowerCase();
            if (constraintKeywords.some((k) => lowered.startsWith(k))) {
                continue;
            }

            // Column name + type
            const match = /^\s*([A-Za-z_][A-Za-z0-9_\[\]]*)\s+(.+)/.exec(trimmed);
            if (match) {
                const rawCol = match[1].replace(/[\[\]]/g, "");
                const type = match[2].split(/\s+/)[0];
                if (!cols.some((c) => c.rawName === rawCol)) {
                    cols.push({
                        name: rawCol.toLowerCase(),
                        rawName: rawCol,
                        type,
                        line: startLine + offset  // correct file line
                    });
                }
            }
        }
    }

    return cols;
}

// ---------- Indexing ----------
function indexText(uri: string, text: string): void {
    // --- normalize URI ---
    const normUri = uri.startsWith("file://") ? uri : url.pathToFileURL(uri).toString();

    // --- cleanup old entries for this file ---
    definitions.delete(normUri);
    for (const [key, refs] of referencesIndex.entries()) {
        const filtered = refs.filter(r => r.uri !== normUri);
        if (filtered.length > 0) {
            referencesIndex.set(key, filtered);
        } else {
            referencesIndex.delete(key);
        }
    }

    const defs: SymbolDef[] = [];
    const lines = text.split(/\r?\n/);
    const localRefs: ReferenceDef[] = [];

    // --- scan for definitions ---
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        const match = /^\s*CREATE\s+(PROCEDURE|FUNCTION|VIEW|TABLE|TYPE)\s+([a-zA-Z0-9_\[\]\.]+)/i.exec(line);
        if (match) {
            const kind = match[1].toUpperCase();
            const rawName = match[2];
            const norm = normalizeName(rawName);

            const sym: SymbolDef = { name: norm, rawName, uri: normUri, line: i };

            if (kind === "TABLE" || (kind === "TYPE" && /\bAS\s+TABLE\b/i.test(line))) {
                const cols = parseColumnsFromCreate(text, i);
                sym.columns = cols;

                // update fast lookup map
                const set = new Set<string>();
                for (const c of cols) set.add(c.name);
                columnsByTable.set(norm, set);

                // also index each column definition as a reference
                for (const c of cols) {
                    const colIdx = lines[i].toLowerCase().indexOf(c.rawName.toLowerCase());
                    if (colIdx >= 0) {
                        localRefs.push({
                            name: `${norm}.${c.name}`,
                            uri: normUri,
                            line: i,
                            start: colIdx,
                            end: colIdx + c.rawName.length,
                            kind: "column"
                        });
                    }
                }

                if (cols.length > 0) {
                    safeLog(
                        `Indexed ${rawName} with ${cols.length} cols: ${cols
                            .map(c => c.rawName + (c.type ? ":" + c.type : ""))
                            .join(", ")}`
                    );
                }
            }

            defs.push(sym);

            // add definition reference (table name itself at correct span)
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
    }

    // --- scan for usage references ---
    const aliases = extractAliases(text);
    for (let i = 0; i < lines.length; i++) {
        const clean = stripComments(lines[i]);

        // --- table usages ---
        const tableRegex = /\b(from|join|update|into)\s+([a-zA-Z0-9_\[\]\.]+)/gi;
        let m: RegExpExecArray | null;
        while ((m = tableRegex.exec(clean))) {
            const raw = m[2];
            const norm = normalizeName(raw);

            const keywordIndex = m.index;
            const tableIndex = keywordIndex + m[0].indexOf(raw);

            localRefs.push({
                name: norm,
                uri: normUri,
                line: i,
                start: tableIndex,
                end: tableIndex + raw.length,
                kind: "table"
            });
        }

        // --- alias.column usages ---
        const colRegex = /([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)/g;
        while ((m = colRegex.exec(clean))) {
            const alias = m[1].toLowerCase();
            const col = normalizeName(m[2]);
            const table = aliases.get(alias);
            if (table) {
                const key = `${normalizeName(table)}.${col}`;
                localRefs.push({
                    name: key,
                    uri: normUri,
                    line: i,
                    start: m.index,
                    end: m.index + m[0].length,
                    kind: "column"
                });
            }
        }

        // --- unaliased column usages ---
        const bareColRegex = /\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
        while ((m = bareColRegex.exec(clean))) {
            const col = normalizeName(m[1]);

            // skip obvious SQL keywords
            if ([
                "from","join","on","where","and","or","select","insert","update","delete",
                "into","as","count","group","by","order"
            ].includes(col)) continue;

            // already handled as alias.column
            if (clean.includes(".")) continue;

            const tablesInScope: string[] = [];
            const fromJoinRegex = /\b(from|join)\s+([a-zA-Z0-9_\[\]\.]+)/gi;
            let fm: RegExpExecArray | null;
            while ((fm = fromJoinRegex.exec(clean))) {
                tablesInScope.push(normalizeName(fm[2]));
            }

            const candidateTables: string[] = [];
            for (const t of tablesInScope) {
                if (columnsByTable.get(t)?.has(col)) {
                    candidateTables.push(t);
                }
            }

            if (candidateTables.length === 1) {
                const key = `${candidateTables[0]}.${col}`;
                localRefs.push({
                    name: key,
                    uri: normUri,
                    line: i,
                    start: m.index,
                    end: m.index + m[1].length,
                    kind: "column"
                });
            } else if (candidateTables.length > 1) {
                for (const t of candidateTables) {
                    const key = `${t}.${col}`;
                    localRefs.push({
                        name: key,
                        uri: normUri,
                        line: i,
                        start: m.index,
                        end: m.index + m[1].length,
                        kind: "column"
                    });
                }
            }
        }
    }

    definitions.set(normUri, defs);

    // --- merge into global referencesIndex (dedup) ---
    const seen = new Set<string>();
    for (const ref of localRefs) {
        const key = `${ref.uri}:${ref.line}:${ref.start}:${ref.end}:${ref.name}`;
        if (seen.has(key)) {continue;}
        seen.add(key);

        const arr = referencesIndex.get(ref.name) || [];
        arr.push(ref);
        referencesIndex.set(ref.name, arr);
    }

    aliasesByUri.set(normUri, extractAliases(text));

    for (const def of defs) {
        tablesByName.set(def.name, def);
    }
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
    validateTextDocument(e.document);
});
documents.onDidChangeContent((e) => {
    indexText(e.document.uri, e.document.getText());
    validateTextDocument(e.document);
});


// --- Definitions (unchanged) ---
function findColumnInTable(tableName: string, colName: string): Location[] {
    const results: Location[] = [];
    for (const defs of definitions.values()) {
        for (const def of defs) {
            if (normalizeName(def.name) === normalizeName(tableName) && def.columns) {
                for (const col of def.columns) {
                    if (col.name === colName) {
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





// --- Completion (unchanged) ---
connection.onCompletion((params: CompletionParams): CompletionItem[] => {
    const uri = params.textDocument.uri;
    const doc = documents.get(uri);
    if (!doc) {return [];}

    const position = params.position;
    const lineText = doc.getText({
        start: { line: position.line, character: 0 },
        end: { line: position.line, character: position.character }
    }).trim();

    const items: CompletionItem[] = [];

    // --- Case 1: Column completions after alias dot (e.) ---
    const aliasMatch = /([a-zA-Z0-9_]+)\.$/.exec(lineText);
    if (aliasMatch) {
        const alias = aliasMatch[1].toLowerCase();
        const aliases = aliasesByUri.get(uri) || new Map();
        const tableName = aliases.get(alias);

        if (tableName) {
            const def = tablesByName.get(tableName);
            if (def?.columns) {
                return def.columns.map(col => ({
                    label: col.rawName,
                    kind: CompletionItemKind.Field,
                    detail: col.type
                        ? `Column in ${def.rawName} (${col.type})`
                        : `Column in ${def.rawName}`
                }));
            }
        }
    }

    // --- Case 2: Table completions after FROM / JOIN ---
    if (/\b(from|join)\s+[a-zA-Z0-9_\[\]]*$/i.test(lineText)) {
        for (const def of tablesByName.values()) {
            items.push({
                label: def.rawName,
                kind: CompletionItemKind.Class,
                detail: `Table defined in ${def.uri.split("/").pop()}:${def.line + 1}`
            });
        }
        return items;
    }

    // --- Case 3: SELECT clause → suggest tables + columns ---
    if (/\bselect\s+.*$/i.test(lineText)) {
        // add columns from all known tables
        for (const def of tablesByName.values()) {
            if (def.columns) {
                for (const col of def.columns) {
                    items.push({
                        label: col.rawName,
                        kind: CompletionItemKind.Field,
                        detail: col.type
                            ? `Column in ${def.rawName} (${col.type})`
                            : `Column in ${def.rawName}`
                    });
                }
            }
        }
        // also add tables
        for (const def of tablesByName.values()) {
            items.push({
                label: def.rawName,
                kind: CompletionItemKind.Class,
                detail: `Table defined in ${def.uri.split("/").pop()}:${def.line + 1}`
            });
        }
        return items;
    }

    // --- Case 4: Fallback → all tables + SQL keywords ---
    for (const def of tablesByName.values()) {
        items.push({
            label: def.rawName,
            kind: CompletionItemKind.Class,
            detail: `Table defined in ${def.uri.split("/").pop()}:${def.line + 1}`
        });
    }

    const keywords = [
        "SELECT", "INSERT", "UPDATE", "DELETE",
        "CREATE", "DROP", "ALTER", "JOIN",
        "WHERE", "GROUP BY", "ORDER BY"
    ];
    for (const kw of keywords) {
        items.push({ label: kw, kind: CompletionItemKind.Keyword });
    }

    return items;
});

// --- References ---
connection.onReferences((params: ReferenceParams): Location[] => {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) {return [];}

    const range = getWordRangeAtPosition(doc, params.position);
    if (!range) {return [];}

    const rawWord = doc.getText(range);
    return findReferencesForWord(rawWord, doc, params.position);
});

connection.onRenameRequest((params: RenameParams): WorkspaceEdit | null => {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) {return null;}

    const range = getWordRangeAtPosition(doc, params.position);
    if (!range) {return null;}

    const rawWord = doc.getText(range);
    const newName = params.newName;

    const locations = findReferencesForWord(rawWord, doc, params.position);
    if (!locations || locations.length === 0) {return null;}

    const editsByUri: { [uri: string]: TextEdit[] } = {};
    const seen = new Set<string>();

    for (const loc of locations) {
        const key = `${loc.uri}:${loc.range.start.line}:${loc.range.start.character}:${loc.range.end.line}:${loc.range.end.character}`;
        if (seen.has(key)) {continue;}
        seen.add(key);

        // ensure valid file:// URI
        const uri = loc.uri.startsWith("file://") ? loc.uri : url.pathToFileURL(loc.uri).toString();

        if (!editsByUri[uri]) {editsByUri[uri] = [];}
        editsByUri[uri].push({ range: loc.range, newText: newName });
    }

    return { changes: editsByUri };
});


connection.onHover((params): Hover | null => {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) {return null;}

    const range = getWordRangeAtPosition(doc, params.position);
    if (!range) {return null;}

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


async function validateTextDocument(doc: TextDocument): Promise<void> {
    if (!isIndexReady) {
        connection.sendDiagnostics({ uri: doc.uri, diagnostics: [] });
        return;
    }
    const diagnostics: Diagnostic[] = [];
    const text = doc.getText();
    const lines = text.split(/\r?\n/);

    // --- Collect aliases (tables + subqueries) ---
    const aliases = extractAliases(text);

    // --- Collect declared cursors ---
    const cursorRegex = /\bdeclare\s+([a-zA-Z0-9_]+)\s+cursor/gi;
    const cursors = new Set<string>();
    let cm: RegExpExecArray | null;
    while ((cm = cursorRegex.exec(text))) {
        cursors.add(cm[1].toLowerCase());
    }

    for (let i = 0; i < lines.length; i++) {
        const clean = stripComments(lines[i]);

        // --- Table checks (support schema + stripped forms) ---
        const tableRegex = /\b(from|join|update|into)\s+([a-zA-Z0-9_\[\]\.]+)/gi;
        let m: RegExpExecArray | null;
        while ((m = tableRegex.exec(clean))) {
            const rawTable = m[2];
            const normRaw = normalizeName(rawTable); // dbo.employee
            const normStripped = normalizeName(rawTable.replace(/^[a-z0-9_]+\./i, "")); // employee

            // 🚫 Skip if this is a declared cursor
            if (cursors.has(rawTable.toLowerCase())) {
                continue;
            }

            if (!columnsByTable.has(normRaw) && !columnsByTable.has(normStripped)) {
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

        // --- Alias.column checks (skip schema-qualified tables) ---
        const colRegex = /([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)/g;
        while ((m = colRegex.exec(clean))) {
            const left = m[1];
            const right = m[2];
            const combined = normalizeName(`${left}.${right}`);

            // 🚫 Skip if "left.right" is a known table
            if (columnsByTable.has(combined)) {
                continue;
            }

            const alias = left.toLowerCase();
            const col = normalizeName(right);
            const table = aliases.get(alias);

            if (!table) {
                // Unknown alias
                diagnostics.push({
                    severity: DiagnosticSeverity.Error,
                    range: {
                        start: { line: i, character: m.index },
                        end: { line: i, character: m.index + m[0].length }
                    },
                    message: `Unknown alias '${alias}'`,
                    source: "SaralSQL"
                });
            } else if (table === "__subquery__") {
                // ✅ Subquery alias → accept without column validation
                continue;
            } else if (
                !columnsByTable.get(normalizeName(table))?.has(col) &&
                !columnsByTable.get(normalizeName(table.replace(/^[a-z0-9_]+\./i, "")))?.has(col)
            ) {
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
            tableSymbol.children = def.columns.map((col) => ({
                name: col.rawName,
                kind: SymbolKind.Field,
                range: Range.create(col.line, 0, col.line, 200),
                selectionRange: Range.create(col.line, 0, col.line, 200),
                detail: col.type || undefined
            }));
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
    let word = normalizeName(rawWord);
    const fullText = doc.getText();
    const lines = fullText.split(/\r?\n/);
    const results: Location[] = [];

    // --- helper: get current statement text ---
    function getCurrentStatement(): string {
        if (!position) {return fullText;}

        const currentLine = position.line;
        let start = currentLine;
        let end = currentLine;

        // look upward until BEGIN or ; or top
        for (let i = currentLine; i >= 0; i--) {
            if (/^\s*begin\b/i.test(lines[i]) || /;\s*$/.test(lines[i])) {
                start = i + 1;
                break;
            }
            if (i === 0) {start = 0;}
        }

        // look downward until END or ; or bottom
        for (let i = currentLine; i < lines.length; i++) {
            if (/^\s*end\b/i.test(lines[i]) || /;\s*$/.test(lines[i])) {
                end = i;
                break;
            }
            if (i === lines.length - 1) {end = lines.length - 1;}
        }

        return lines.slice(start, end + 1).join("\n");
    }

    const statementText = getCurrentStatement();

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
                const refs = referencesIndex.get(key) || [];
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
    const tableRefs = referencesIndex.get(word) || [];
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
            const refs = referencesIndex.get(key) || [];
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

        // fallback to global if none
        if (results.length === 0) {
            for (const [key, refs] of referencesIndex.entries()) {
                if (key.endsWith(`.${word}`)) {
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
            }
        }
    }

    return results;
}

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
        if (i === 0) {start = 0;}
    }

    // look downward until END/; or bottom
    for (let i = currentLine; i < lines.length; i++) {
        if (/^\s*end\b/i.test(lines[i]) || /;\s*$/.test(lines[i])) {
            end = i;
            break;
        }
        if (i === lines.length - 1) {end = lines.length - 1;}
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
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
    CompletionParams
} from "vscode-languageserver/node";
import { TextDocument } from "vscode-languageserver-textdocument";
import * as fs from "fs";
import * as fg from "fast-glob";
import * as url from "url";

// ---------- Connection + Documents ----------
const connection = createConnection(ProposedFeatures.all);
const documents: TextDocuments<TextDocument> = new TextDocuments(TextDocument);

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

const definitions = new Map<string, SymbolDef[]>();

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
    const map = new Map<string, string>();
    const regex = /\b(?:from|join)\s+([a-zA-Z0-9_\[\]\.]+)\s+(?:as\s+)?([a-zA-Z0-9_]+)/gi;
    let m: RegExpExecArray | null;
    while ((m = regex.exec(text))) {
        map.set(m[2].toLowerCase(), normalizeName(m[1]));
    }
    return map;
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
    const defs: SymbolDef[] = [];
    const lines = text.split(/\r?\n/);

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        const match = /^\s*CREATE\s+(PROCEDURE|FUNCTION|VIEW|TABLE|TYPE)\s+([a-zA-Z0-9_\[\]\.]+)/i.exec(line);
        if (match) {
            const kind = match[1].toUpperCase();
            const rawName = match[2];
            const norm = normalizeName(rawName);

            const sym: SymbolDef = { name: norm, rawName, uri, line: i };

            if (kind === "TABLE" || (kind === "TYPE" && /\bAS\s+TABLE\b/i.test(line))) {
                const cols = parseColumnsFromCreate(text, i);
                sym.columns = cols;
                if (cols.length > 0) {
                    safeLog(`Indexed ${rawName} with ${cols.length} cols: ${cols.map((c) => c.rawName + (c.type ? ":" + c.type : "")).join(", ")}`);
                }
            }

            defs.push(sym);
        }
    }

    definitions.set(uri, defs);
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
            completionProvider: { triggerCharacters: [".", " "] }
        }
    };
});

connection.onInitialized(() => {
    indexWorkspace().catch((err) => safeError("Indexing failed", err));
});

documents.onDidOpen((e) => {
    indexText(e.document.uri, e.document.getText());
});
documents.onDidChangeContent((e) => {
    indexText(e.document.uri, e.document.getText());
});
documents.onDidClose((e) => {
    definitions.delete(e.document.uri);
});

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

// --- Definitions ---
connection.onDefinition((params: DefinitionParams): Location[] | null => {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) { return null; }

    const wordRange = getWordRangeAtPosition(doc, params.position);
    if (!wordRange) { return null; }

    const rawWord = doc.getText(wordRange);
    let word = normalizeName(rawWord);

    // Handle alias.column → strip alias
    if (rawWord.includes(".")) {
        const parts = rawWord.split(".");
        if (parts.length === 2) {
            word = normalizeName(parts[1]);
        }
    }

    const fullText = doc.getText();
    const lineText = doc.getText({
        start: { line: params.position.line, character: 0 },
        end: { line: params.position.line, character: Number.MAX_VALUE }
    });

    // --- 1) Alias.column case ---
    const aliasRegex = /([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)/g;
    let match: RegExpExecArray | null;
    while ((match = aliasRegex.exec(lineText))) {
        const alias = match[1].toLowerCase();
        const colName = normalizeName(match[2]);
        if (colName === word) {
            const aliases = extractAliases(fullText);
            const tableName = aliases.get(alias);
            if (tableName) {
                const res = findColumnInTable(tableName, colName);
                if (res.length > 0) { return res; }
            }
        }
    }

    // --- 2) INSERT INTO case ---
    const insertMatch = /insert\s+into\s+([a-zA-Z0-9_\[\]\.]+)/i.exec(fullText);
    if (insertMatch) {
        const res = findColumnInTable(insertMatch[1], word);
        if (res.length > 0) { return res; }
    }

    // --- 3) FROM (no alias) case ---
    const fromMatch = /\bfrom\s+([a-zA-Z0-9_\[\]\.]+)(?!\s+[a-zA-Z0-9_]+)/i.exec(fullText);
    if (fromMatch) {
        const res = findColumnInTable(fromMatch[1], word);
        if (res.length > 0) { return res; }
    }

    // --- 4) JOIN (no alias) case ---
    const joinRegex = /\bjoin\s+([a-zA-Z0-9_\[\]\.]+)(?!\s+[a-zA-Z0-9_]+)/gi;
    let joinMatch: RegExpExecArray | null;
    while ((joinMatch = joinRegex.exec(fullText))) {
        const res = findColumnInTable(joinMatch[1], word);
        if (res.length > 0) { return res; }
    }

    // --- 5) UPDATE case ---
    const updateMatch = /\bupdate\s+([a-zA-Z0-9_\[\]\.]+)/i.exec(fullText);
    if (updateMatch) {
        const res = findColumnInTable(updateMatch[1], word);
        if (res.length > 0) { return res; }
    }

    // --- 6) Fallback global ---
    const fallback = findTableOrColumn(word);
    return fallback.length > 0 ? fallback : null;
});


// --- References ---
connection.onReferences((params: ReferenceParams): Location[] => {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) {
        return [];
    }

    const wordRange = getWordRangeAtPosition(doc, params.position);
    if (!wordRange) {
        return [];
    }
    const rawWord = doc.getText(wordRange);
    const word = normalizeName(rawWord);

    // Determine context: is it a table or a column?
    let isTable = false;
    let isColumn = false;

    for (const defs of definitions.values()) {
        for (const def of defs) {
            if (def.name === word) {
                isTable = true;
            }
            if (def.columns?.some((c) => c.name === word)) {
                isColumn = true;
            }
        }
    }

    const aliases = [word, `dbo.${word}`];
    const locations: Location[] = [];
    const seen = new Set<string>();

    for (const [uri] of definitions.entries()) {
        try {
            const fileDoc = documents.get(uri);
            const text = fileDoc ? fileDoc.getText() : fs.readFileSync(url.fileURLToPath(uri), "utf8");
            const lines = text.split(/\r?\n/);

            for (let i = 0; i < lines.length; i++) {
                const line = lines[i];
                const clean = stripComments(line);
                const normLine = clean.toLowerCase();

                for (const candidate of aliases) {
                    // Only check table refs if the symbol is a table
                    if (isTable) {
                        const tableRefRegex = new RegExp(`\\b(from|join)\\s+${candidate}\\b`, "i");
                        if (tableRefRegex.test(normLine)) {
                            const start = normLine.indexOf(candidate);
                            if (start >= 0) {
                                const key = `${uri}:${i}:${start}:${candidate}`;
                                if (!seen.has(key)) {
                                    seen.add(key);
                                    locations.push({
                                        uri,
                                        range: {
                                            start: { line: i, character: start },
                                            end: { line: i, character: start + candidate.length }
                                        }
                                    });
                                }
                            }
                        }
                    }

                    // Only check column refs if the symbol is a column
                    if (isColumn) {
                        const colAliasRegex = new RegExp(`\\.${candidate}\\b`, "i"); // e.EmployeeId
                        const colBareRegex = new RegExp(`\\b${candidate}\\b`, "i");  // EmployeeId

                        if (colAliasRegex.test(normLine) || colBareRegex.test(normLine)) {
                            const start = normLine.indexOf(candidate);
                            if (start >= 0) {
                                const key = `${uri}:${i}:${start}:${candidate}`;
                                if (!seen.has(key)) {
                                    seen.add(key);
                                    locations.push({
                                        uri,
                                        range: {
                                            start: { line: i, character: start },
                                            end: { line: i, character: start + candidate.length }
                                        }
                                    });
                                }
                            }
                        }
                    }
                }
            }
        } catch (err) {
            safeError(`Failed to read ${uri}`, err);
        }
    }

    return locations;
});

// --- Completion ---
connection.onCompletion((params: CompletionParams): CompletionItem[] => {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) {
        return [];
    }

    const position = params.position;
    const lineText = doc.getText({
        start: { line: position.line, character: 0 },
        end: { line: position.line, character: position.character }
    });

    // Column completions after alias.
    const aliasMatch = /([a-zA-Z0-9_]+)\.$/.exec(lineText);
    if (aliasMatch) {
        const alias = aliasMatch[1].toLowerCase();
        const aliases = extractAliases(doc.getText());
        const tableName = aliases.get(alias);

        if (tableName) {
            for (const defs of definitions.values()) {
                for (const def of defs) {
                    if (def.name === tableName && def.columns) {
                        return def.columns.map((col) => {
                            return {
                                label: col.rawName,
                                kind: CompletionItemKind.Field,
                                detail: col.type ? `Column in ${def.rawName} (${col.type})` : `Column in ${def.rawName}`
                            };
                        });
                    }
                }
            }
        }
    }

    // Fallback: global symbols + keywords
    const items: CompletionItem[] = [];
    for (const defs of definitions.values()) {
        for (const def of defs) {
            items.push({
                label: def.rawName,
                kind: CompletionItemKind.Class,
                detail: `Defined in ${def.uri.split("/").pop()}:${def.line + 1}`
            });
        }
    }
    ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "JOIN"].forEach((kw) => {
        items.push({ label: kw, kind: CompletionItemKind.Keyword });
    });
    return items;
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

// ---------- Startup ----------
documents.listen(connection);
connection.listen();
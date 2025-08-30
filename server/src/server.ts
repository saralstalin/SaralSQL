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
} from 'vscode-languageserver/node';
import { TextDocument } from 'vscode-languageserver-textdocument';
import * as fs from 'fs';
import * as fg from 'fast-glob';
import * as url from 'url';

// ---------- Connection + Documents ----------
const connection = createConnection(ProposedFeatures.all);
const documents: TextDocuments<TextDocument> = new TextDocuments(TextDocument);

// ---------- Index structures ----------
interface ColumnDef {
    name: string;
    rawName: string;
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
        if (schema === "dbo") {return object;}
        return `${schema}.${object}`;
    }
    return n;
}

function safeLog(msg: string) {
    if (process.env.SARALSQL_DEBUG) {
        connection.console.log(`[SaralSQL] ${msg}`);
    }
}
function safeError(msg: string, err?: unknown) {
    connection.console.error(`[SaralSQL] ${msg}: ${err instanceof Error ? err.stack || err.message : String(err)}`);
}

// Strip comments
function stripComments(sql: string): string {
    sql = sql.replace(/--.*$/gm, "");
    sql = sql.replace(/\/\*[\s\S]*?\*\//g, "");
    return sql;
}

// Extract aliases
function extractAliases(text: string): Map<string, string> {
    const map = new Map<string, string>();
    const regex = /\b(?:from|join)\s+([a-zA-Z0-9_\[\]\.]+)\s+(?:as\s+)?([a-zA-Z0-9_]+)/gi;
    let m;
    while ((m = regex.exec(text))) {
        map.set(m[2].toLowerCase(), normalizeName(m[1]));
    }
    return map;
}

// ---------- Column Parser ----------
function parseColumnsFromCreate(sql: string, startLine: number): ColumnDef[] {
    const m = /CREATE\s+TABLE\s+([A-Za-z0-9_\[\]\.]+)\s*\(([\s\S]*?)\)\s*;/i.exec(sql);
    if (!m) {return [];}

    let body = m[2];
    body = stripComments(body);

    // Split by commas not inside parentheses
    let parts: string[] = [];
    let current = "";
    let depth = 0;
    for (const ch of body) {
        if (ch === "(") {depth++;}
        else if (ch === ")") {depth--;}
        if (ch === "," && depth === 0) {
            parts.push(current);
            current = "";
        } else {
            current += ch;
        }
    }
    if (current.trim()) {parts.push(current);}

    const constraintKeywords = ["primary key", "foreign key", "constraint", "index", "check", "unique"];
    const cols: ColumnDef[] = [];
    parts.forEach((p, idx) => {
        const trimmed = p.trim();
        if (!trimmed) {return;}
        const lowered = trimmed.toLowerCase();
        if (constraintKeywords.some(k => lowered.startsWith(k))) {return;}

        const cm = /^\s*([A-Za-z_][A-Za-z0-9_\[\]]*)\b/.exec(trimmed);
        if (cm) {
            const rawCol = cm[1].replace(/[\[\]]/g, "");
            if (!cols.some(c => c.rawName === rawCol)) {
                cols.push({
                    name: rawCol.toLowerCase(),
                    rawName: rawCol,
                    line: startLine + idx
                });
            }
        }
    });
    return cols;
}

// ---------- Indexing ----------
function indexText(uri: string, text: string) {
    const defs: SymbolDef[] = [];
    const lines = text.split(/\r?\n/);

    lines.forEach((line, i) => {
        const match = /^\s*CREATE\s+(PROCEDURE|FUNCTION|VIEW|TABLE)\s+([a-zA-Z0-9_\[\]\.]+)/i.exec(line);
        if (match) {
            const kind = match[1].toUpperCase();
            const rawName = match[2];
            const norm = normalizeName(rawName);

            const sym: SymbolDef = { name: norm, rawName, uri, line: i };

            if (kind === "TABLE") {
                const cols = parseColumnsFromCreate(text, i);
                sym.columns = cols;
                if (cols.length > 0) {
                    safeLog(`Indexed table ${rawName} with ${cols.length} cols: ${cols.map(c => c.rawName).join(", ")}`);
                }
            }

            defs.push(sym);
        }
    });

    definitions.set(uri, defs);
}

async function indexWorkspace() {
    try {
        const folders = await connection.workspace.getWorkspaceFolders?.();
        if (!folders) {return;}

        for (const folder of folders) {
            const folderPath = url.fileURLToPath(folder.uri);
            const files = await fg.glob('**/*.sql', { cwd: folderPath, absolute: true });
            for (const file of files) {
                try {
                    const text = fs.readFileSync(file, 'utf8');
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
connection.onInitialize((_params: InitializeParams): InitializeResult => ({
    capabilities: {
        textDocumentSync: TextDocumentSyncKind.Incremental,
        definitionProvider: true,
        referencesProvider: true,
        completionProvider: { triggerCharacters: ['.', ' '] }
    }
}));

connection.onInitialized(() => { indexWorkspace().catch(err => safeError("Indexing failed", err)); });

documents.onDidOpen(e => { indexText(e.document.uri, e.document.getText()); });
documents.onDidChangeContent(e => { indexText(e.document.uri, e.document.getText()); });
documents.onDidClose(e => { definitions.delete(e.document.uri); });

// --- Definition ---
connection.onDefinition((params: DefinitionParams): Location[] | null => {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) {return null;}

    const wordRange = getWordRangeAtPosition(doc, params.position);
    if (!wordRange) {return null;}
    const word = normalizeName(doc.getText(wordRange));

    const results: Location[] = [];
    for (const defs of definitions.values()) {
        for (const def of defs) {
            if (def.name === word) {
                results.push({
                    uri: def.uri,
                    range: { start: { line: def.line, character: 0 }, end: { line: def.line, character: 200 } }
                });
            }
        }
    }
    return results.length > 0 ? results : null;
});

// --- References ---
connection.onReferences((params: ReferenceParams): Location[] => {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) {return [];}

    const wordRange = getWordRangeAtPosition(doc, params.position);
    if (!wordRange) {return [];}
    const word = normalizeName(doc.getText(wordRange));

    const aliases = [word, `dbo.${word}`];
    const locations: Location[] = [];

    for (const [uri] of definitions.entries()) {
        try {
            const fileDoc = documents.get(uri);
            const text = fileDoc ? fileDoc.getText() : fs.readFileSync(url.fileURLToPath(uri), "utf8");
            const lines = stripComments(text).split(/\r?\n/);

            lines.forEach((line, i) => {
                const normLine = line.toLowerCase();

                for (const candidate of aliases) {
                    const regex = new RegExp(`\\b${candidate}\\b`, "i");
                    if (!regex.test(normLine)) {continue;}

                    // Check if it's a table reference (FROM/JOIN <candidate>)
                    const tableRefRegex = new RegExp(`\\b(from|join)\\s+${candidate}\\b`, "i");
                    const colRefRegex = new RegExp(`\\.${candidate}\\b`, "i");

                    if (tableRefRegex.test(normLine) || colRefRegex.test(normLine)) {
                        const start = normLine.indexOf(candidate);
                        const end = start + candidate.length;
                        if (start >= 0) {
                            locations.push({
                                uri,
                                range: {
                                    start: { line: i, character: start },
                                    end: { line: i, character: end }
                                }
                            });
                        }
                    }
                }
            });
        } catch (err) {
            safeError(`Failed to read ${uri}`, err);
        }
    }

    return locations;
});

// --- Completion ---
connection.onCompletion((params: CompletionParams): CompletionItem[] => {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) {return [];}

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
                        return def.columns.map(col => ({
                            label: col.rawName,
                            kind: CompletionItemKind.Field,
                            detail: `Column in ${def.rawName}`
                        }));
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
                detail: `Defined in ${def.uri.split('/').pop()}:${def.line + 1}`
            });
        }
    }
    ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "JOIN"]
        .forEach(kw => items.push({ label: kw, kind: CompletionItemKind.Keyword }));
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

import { TextDocument, Position, Range } from "vscode-languageserver-textdocument";

const SQL_KEYWORDS = new Set([
    // DML
    "select", "insert", "update", "delete", "merge", "into", "values", "output",

    // DDL
    "create", "alter", "drop", "truncate", "table", "view", "index", "schema",
    "procedure", "function", "trigger", "sequence", "constraint", "default",
    "primary", "key", "foreign", "references", "unique", "check", "clustered", "nonclustered",

    // Query / joins
    "from", "join", "inner", "outer", "left", "right", "full", "cross", "apply",
    "on", "using",

    // Clauses
    "where", "group", "by", "having", "order", "asc", "desc", "distinct",
    "top", "limit", "offset", "fetch", "next", "rows", "only",

    // Set operations
    "union", "intersect", "except", "all",

    // Flow control
    "begin", "end", "if", "else", "while", "case", "when", "then", "try", "catch",

    // Variables / assignments
    "declare", "set", "as",

    // Functions / windowing
    "over", "partition", "row_number", "rank", "dense_rank", "ntile",
    "count", "sum", "min", "max", "avg", "coalesce", "nullif",

    // Predicates / operators
    "and", "or", "not", "between", "in", "exists", "like", "is", "null", "any", "some",

    // Execution
    "exec", "execute", "return", "returns",

    // Transactions
    "begin", "commit", "rollback", "savepoint", "transaction",

    // Security / perms
    "grant", "revoke", "deny",

    // Data types (common)
    "int", "bigint", "smallint", "tinyint", "decimal", "numeric", "money",
    "float", "real", "bit", "char", "varchar", "nvarchar", "text", "ntext",
    "date", "datetime", "smalldatetime", "datetime2", "time", "timestamp",
    "binary", "varbinary", "xml", "json",

    // Misc
    "newid", "identity", "default", "with", "nolock", "readuncommitted",
    "serializable", "repeatableread", "snapshot", "isolation", "level",

    // Other
    "cursor", "fetch", "open", "close", "deallocate", "print"
]);


export function normalizeName(name: string): string {
    if (!name) { return ""; }

    // 1) remove any leading dots (e.g. ".Department")
    // 2) remove all square brackets anywhere
    // 3) lowercase + trim
    let n = String(name).replace(/^\.+/, "").replace(/[\[\]]/g, "").toLowerCase().trim();

    // collapse whitespace around dots and compact dotted parts
    n = n.split('.').map(p => p.trim()).filter(Boolean).join('.');

    // strip dbo. canonicalization: return unqualified object for dbo
    const parts = n.split(".");
    if (parts.length === 2) {
        const [schema, object] = parts;
        if (schema === "dbo") { return object; }
        return `${schema}.${object}`;
    }
    return n;
}

export function getCurrentStatement(
    doc: { getText: (range?: any) => string },
    position: { line: number; character: number }
): string {
    try {
        const full = doc.getText();
        if (!full) { return ""; }

        // --- compute absolute offset by scanning full and counting newlines ---
        // This method is robust for LF and CRLF because it scans the actual characters.
        const targetLine = Math.max(0, position.line);
        const targetChar = Math.max(0, position.character);
        let line = 0;
        let offset = 0;
        const N = full.length;
        // Walk until we've reached the start of targetLine
        while (offset < N && line < targetLine) {
            if (full[offset] === "\n") { line++; }
            offset++;
        }
        // Now offset is the index of the first char on targetLine (or N)
        // Clamp the character position to the actual line length (up to next newline)
        let lineEnd = offset;
        while (lineEnd < N && full[lineEnd] !== "\n") { lineEnd++; }
        const clampedChar = Math.min(targetChar, Math.max(0, lineEnd - offset));
        offset += clampedChar;

        // --- helper state for scanning outside strings/comments/brackets ---
        let inSingle = false;
        let inDouble = false;
        let inBracket = false; // [ ... ]
        let inLineComment = false;
        let inBlockComment = false;

        let lastStmtSep = -1;     // last semicolon index (outside quotes/comments) before offset
        let lastWithIndex = -1;   // last 'WITH' (word) index before offset for CTEs (outside quotes/comments)

        function isWordBoundaryChar(ch: string | undefined) {
            if (ch === undefined || ch === null) { return true; }
            return !(/[A-Za-z0-9_]/.test(ch));
        }

        function isCteWith(fullStr: string, index: number): boolean {
            if (index + 4 > fullStr.length) { return false; }
            const slice4 = fullStr.substr(index, 4);
            if (!/^with$/i.test(slice4)) { return false; }
            const prev = fullStr[index - 1];
            const next = fullStr[index + 4];
            if (!isWordBoundaryChar(prev) || !isWordBoundaryChar(next)) { return false; }

            // Check for CTE pattern: WITH identifier AS (
            // Look ahead a little without trimming (don't remove offsets); allow some whitespace.
            const lookAhead = fullStr.slice(index + 4, index + 200); // limit lookahead
            return /^\s*[A-Za-z0-9_]+\s+AS\s*\(/i.test(lookAhead);
        }

        // --- forward scan up to offset to find last semicolon / WITH before cursor ---
        for (let i = 0; i < offset; i++) {
            const ch = full[i];
            const chNext = full[i + 1];

            // Start of line comment --
            if (!inSingle && !inDouble && !inBlockComment && !inLineComment && ch === "-" && chNext === "-") {
                inLineComment = true;
                i++; // skip second dash
                continue;
            }
            // Start of block comment /*
            if (!inSingle && !inDouble && !inLineComment && !inBlockComment && ch === "/" && chNext === "*") {
                inBlockComment = true;
                i++;
                continue;
            }
            // End of block comment */
            if (inBlockComment && ch === "*" && chNext === "/") {
                inBlockComment = false;
                i++;
                continue;
            }
            // End of line comment at newline
            if (inLineComment && ch === "\n") {
                inLineComment = false;
                continue;
            }
            if (inLineComment || inBlockComment) { continue; }

            // Bracketed identifier [ ... ]
            if (!inSingle && !inDouble && ch === "[") {
                inBracket = true;
                continue;
            }
            if (inBracket) {
                if (ch === "]") { inBracket = false; }
                continue;
            }

            // Single-quote string
            if (!inDouble && ch === "'") {
                if (!inSingle) { inSingle = true; continue; }
                // handle escaped '' inside string
                if (inSingle && full[i + 1] === "'") { i++; continue; }
                inSingle = false;
                continue;
            }
            if (inSingle) { continue; }

            // Double-quote string
            if (!inSingle && ch === '"') {
                if (!inDouble) { inDouble = true; continue; }
                if (inDouble && full[i + 1] === '"') { i++; continue; }
                inDouble = false;
                continue;
            }
            if (inDouble) { continue; }

            // Semicolon => statement separator
            if (ch === ";") {
                lastStmtSep = i;
                continue;
            }

            // Check for WITH (CTE)
            const rem = full.length - i;
            if (rem >= 4 && isCteWith(full, i)) {
                lastWithIndex = i;
            }
        } // end forward scan

        // Determine start: prefer lastStmtSep+1, but if a CTE WITH is after that, include it
        let start = lastStmtSep + 1;
        if (lastWithIndex > lastStmtSep) {
            let wstart = lastWithIndex;
            while (wstart > 0 && /\s/.test(full[wstart - 1])) { wstart--; }
            start = wstart;
        }
        if (start < 0) { start = 0; }

        // --- find end: scan forward from offset for next semicolon outside comments/strings/brackets ---
        inSingle = false; inDouble = false; inBracket = false; inLineComment = false; inBlockComment = false;
        let end = full.length;
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
        return "";
    }
}


export function getWordRangeAtPosition(doc: TextDocument, pos: { line: number; character: number }) {
    const lineText = doc.getText({
        start: { line: pos.line, character: 0 },
        end: { line: pos.line, character: Number.MAX_VALUE }
    });

    const ident = '(?:\\[[^\\]]+\\]|"[^"]+"|`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)';
    const token = `(?:@|#)?${ident}(?:\\.${ident})*`;   // supports [e].FirstName, dbo.Table.Col, e.Col, etc.
    const regex = new RegExp(token, 'g');
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

export function extractAliases(text: string): Map<string, string> {
    const aliases = new Map<string, string>();

    // helper validators
    const isIdentifier = (s: string) => /^[A-Za-z_][A-Za-z0-9_]*$/.test(s);
    const stripBrackets = (s: string) => String(s || "").replace(/^\[|\]$/g, "").replace(/^"|"$/g, "").replace(/`/g, "").trim();
    const stripDecoratorsAndSchema = (s: string | null | undefined) => {
        if (!s) { return ""; }
        let t = String(s);
        t = t.replace(/^\s*dbo\./i, ""); // remove common schema
        t = t.replace(/^\[|\]$/g, "");
        t = t.replace(/^"|"$/g, "");
        t = t.replace(/`/g, "");
        return t.trim();
    };

    // 1) Table aliases from FROM / JOIN: supports [dbo].[X] alias, dbo.X AS alias, allow optional @/# on table token
    const identPart = '(?:\\[[^\\]]+\\]|"[^"]+"|`[^`]+`|[A-Za-z0-9_]+)';
    const tableToken = `[@#]?${identPart}(?:\\s*\\.\\s*${identPart})*`;
    const tableAliasRegex = new RegExp(
        `\\b(?:from|join)\\s+(${tableToken})(?:\\s+(?:as\\s+)?([A-Za-z_][A-Za-z0-9_]*|\\[[A-Za-z0-9_]+\\]))?`,
        'gi'
    );

    let m: RegExpExecArray | null;
    while ((m = tableAliasRegex.exec(text))) {
        const rawTable = m[1];
        const aliasRaw = m[2];
        if (!aliasRaw) {
            continue; // no alias explicitly provided
        }

        const aliasKey = stripBrackets(aliasRaw).trim();
        // Reject obviously invalid alias tokens
        if (!isIdentifier(aliasKey)) { continue; }
        if (isSqlKeyword(aliasKey.toLowerCase())) { continue; }

        // Avoid alias that is identical to table name (e.g. FROM dbo.X X)
        const tblShort = stripDecoratorsAndSchema(rawTable);
        if (normalizeName(tblShort) === normalizeName(aliasKey)) { continue; }

        aliases.set(aliasKey.toLowerCase(), normalizeName(rawTable));
    }

    // 2) Subquery aliases: only capture when preceded directly by FROM/JOIN (...) alias
    const subqueryAliasRegex = /\b(?:from|join)\s*\(\s*([\s\S]*?)\s*\)\s*(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*)/gi;
    let sm: RegExpExecArray | null;
    while ((sm = subqueryAliasRegex.exec(text))) {
        const aliasRaw = sm[2];
        const aliasKey = String(aliasRaw || "").trim();
        if (!isIdentifier(aliasKey)) { continue; }
        if (isSqlKeyword(aliasKey.toLowerCase())) { continue; }
        aliases.set(aliasKey.toLowerCase(), "__subquery__");
    }

    return aliases;
}


export function getLineStarts(text: string): number[] {
    const starts: number[] = [0];
    for (let i = 0; i < text.length; i++) {
        if (text[i] === '\n') { starts.push(i + 1); }
    }
    return starts;
}

export function offsetToPosition(offset: number, lineStarts: number[]): { line: number; character: number } {
    // binary search in lineStarts (each entry is file offset of line start)
    let low = 0, high = lineStarts.length - 1;
    while (low <= high) {
        const mid = (low + high) >> 1;
        if (lineStarts[mid] <= offset) {
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }
    const line = Math.max(0, high);
    const character = offset - lineStarts[line];
    return { line, character };
}

export function resolveAlias(
    rawWord: string,
    doc: TextDocument,
    position: Position
): { table?: string; column?: string } {
    if (!rawWord || rawWord.trim().length === 0) { return {}; }

    const statementText = getCurrentStatement(doc, position);
    const aliases = extractAliases(statementText); // Map(aliasLower -> tableName)
    const cleaned = rawWord.trim();

    // helper: check workspace-known table existence (best-effort)
    const lookupTableExists = (tableName: string): boolean => {
        const key = normalizeName(tableName);
        try {
            // try common globals that your server might have
            // Substitute these names if your code uses different variables
            if (typeof (globalThis as any).definitions === "object" && (globalThis as any).definitions.has) {
                // definitions might be a Map keyed by normalized names
                return (globalThis as any).definitions.has(key);
            }
            if (typeof (globalThis as any).tablesIndex === "object" && (globalThis as any).tablesIndex.has) {
                return (globalThis as any).tablesIndex.has(key);
            }
            if (typeof (globalThis as any).tableDefinitions === "object" && (globalThis as any).tableDefinitions.has) {
                return (globalThis as any).tableDefinitions.has(key);
            }
        } catch (e) {
            // ignore — best-effort
        }
        return false;
    };

    // Dotted forms like "e.col" or "e." or "schema.table.col"
    if (cleaned.includes(".")) {
        const parts = cleaned.split(".");
        const first = parts[0].trim();
        const rest = parts.slice(1).join(".").trim(); // may be empty for "e."

        const aliasKey = normalizeName(first);
        const tableFromAlias = aliases.get(aliasKey);
        if (tableFromAlias) {
            // alias exists in current statement — good to return table (and optional column)
            if (rest && rest.length > 0) {
                return {
                    table: normalizeName(tableFromAlias),
                    column: normalizeName(rest)
                };
            } else {
                return { table: normalizeName(tableFromAlias) };
            }
        }

        // If first part not an alias → try to interpret as schema.table or table.column
        // e.g. "dbo.Table" or "Table.Column" or "schema.table.column"
        // We'll try last two parts as table.column
        if (parts.length >= 2) {
            const possibleTable = parts[parts.length - 2].trim();
            const possibleColumn = parts[parts.length - 1].trim();
            // Only return if the possibleTable is known in workspace
            if (lookupTableExists(possibleTable)) {
                return { table: normalizeName(possibleTable), column: normalizeName(possibleColumn) };
            }
        }

        // No alias and table not known -> don't guess
        return {};
    }

    // Non-dotted: could be an alias, or a table name. Prefer alias.
    const maybeAliasTable = aliases.get(normalizeName(cleaned));
    if (maybeAliasTable) {
        return { table: normalizeName(maybeAliasTable) };
    }

    // Only return a table if it's known in the workspace index
    if (lookupTableExists(cleaned)) {
        return { table: normalizeName(cleaned) };
    }

    // Not an alias, not a known table — do not return anything (avoid treating as alias)
    return {};
}

export function extractSelectAliasesFromStatement(statementText: string): Set<string> {
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
                const norm = normalizeName(candidate);
                if (norm) { out.add(norm); }
            }
        }
    }
    return out;
}

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


function splitCreateBodyIntoRowsWithPositions(
    text: string,
    defLineIndex: number
): Array<{ text: string; line: number; col: number }> {
    const lines = text.split(/\r?\n/);

    // Find first '(' from the definition line onwards
    let startLine = defLineIndex, parenIdx = -1;
    for (let i = defLineIndex; i < lines.length; i++) {
        const j = lines[i].indexOf("(");
        if (j >= 0) { startLine = i; parenIdx = j; break; }
    }
    if (parenIdx < 0) { return []; }

    const rows: Array<{ text: string; line: number; col: number }> = [];
    let buf = "";
    let depth = 1; // Start at 1 since we begin inside '('
    let inQuote: '"' | "'" | null = null;
    let inBracket = false; // Track bracketed identifiers
    let rowStartLine = startLine;
    let rowStartCol = parenIdx + 1;
    let rowStartSet = false; // Track if row start position is set

    const flushRow = () => {
        const trimmed = buf.trim();
        if (trimmed) { rows.push({ text: trimmed, line: rowStartLine, col: rowStartCol }); }
        buf = "";
        rowStartSet = false; // Reset for next row
    };

    for (let i = startLine; i < lines.length; i++) {
        const line = lines[i];
        const kStart = (i === startLine ? parenIdx + 1 : 0);

        for (let k = kStart; k < line.length; k++) {
            const ch = line[k];
            const next = k + 1 < line.length ? line[k + 1] : "";

            // Line comment
            if (!inQuote && !inBracket && ch === "-" && next === "-") {
                break; // Ignore the rest of the line
            }

            // Block comment
            if (!inQuote && !inBracket && ch === "/" && next === "*") {
                k += 2;
                while (k < line.length && !(line[k] === "*" && line[k + 1] === "/")) {
                    k++;
                }
                if (k < line.length) {k++;} // Skip closing */
                continue;
            }

            if (inQuote) {
                buf += ch;
                if (ch === inQuote && next !== inQuote) { inQuote = null; }
                else if (ch === inQuote && next === inQuote) { k++; } // Skip escaped quote
                continue;
            }

            if (inBracket) {
                buf += ch;
                if (ch === "]") { inBracket = false; }
                continue;
            }

            if (ch === "'" || ch === '"') {
                inQuote = ch as '"' | "'";
                buf += ch;
                continue;
            }

            if (ch === "[") {
                inBracket = true;
                buf += ch;
                continue;
            }

            if (ch === "(") {
                depth++;
                buf += ch;
                continue;
            }

            if (ch === ")") {
                depth--;
                buf += ch;
                if (depth === 0) {
                    flushRow();
                    return rows;
                }
                continue;
            }

            if (ch === "," && depth === 1 && !inQuote && !inBracket) {
                flushRow();
                // Set start of next row to first non-whitespace after comma
                rowStartLine = i;
                let nextCol = k + 1;
                while (nextCol < line.length && /\s/.test(line[nextCol])) {
                    nextCol++;
                }
                rowStartCol = nextCol;
                if (nextCol >= line.length && i + 1 < lines.length) {
                    // Move to next line if comma is at end of line
                    rowStartLine = i + 1;
                    rowStartCol = 0;
                    // Skip leading whitespace on next line
                    while (rowStartLine < lines.length && lines[rowStartLine].trim() === "") {
                        rowStartLine++;
                    }
                    if (rowStartLine < lines.length) {
                        let tempCol = 0;
                        while (tempCol < lines[rowStartLine].length && /\s/.test(lines[rowStartLine][tempCol])) {
                            tempCol++;
                        }
                        rowStartCol = tempCol;
                    }
                }
                continue; // Do not include comma in buf
            }

            // Set row start at first non-whitespace character
            if (!rowStartSet && !/\s/.test(ch)) {
                rowStartSet = true;
                rowStartLine = i;
                rowStartCol = k;
            }

            buf += ch;
        }

        // Keep line separation for multi-line rows
        buf += "\n";
    }

    // Flush any remaining content
    const trimmed = buf.trim();
    if (trimmed) { rows.push({ text: trimmed, line: rowStartLine, col: rowStartCol }); }
    return rows;
}



// NEW: linear parser (block-scoped, no whole-file rescans, safer constraint handling)
export function parseColumnsFromCreateBlock(
    blockText: string,
    startLine: number
): Array<{ name: string; rawName: string; type?: string; line: number; start: number; end: number }> {
    const rows = splitCreateBodyIntoRowsWithPositions(blockText, startLine);
    const out: Array<{ name: string; rawName: string; type?: string; line: number; start: number; end: number }> = [];

    const allLines = blockText.split(/\r?\n/);

    for (const r of rows) {
        const m = /^\s*(\[([^\]]+)\]|"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))/.exec(r.text);
        if (!m) { continue; }

        const raw = m[1]; // preserves brackets/quotes for insertText
        const tokenName = (m[2] ?? m[3] ?? m[4] ?? "").trim();

        // Skip only if the first token is an unquoted constraint keyword
        const isConstraint = !m[2] && !m[3] && /^(?:constraint|primary|foreign|check|unique|index)$/i.test(tokenName);
        if (isConstraint) { continue; }

        const name = normalizeName(tokenName);

        // Type immediately after the name (e.g., "INT", "varchar(50)")
        let colType: string | undefined;
        const afterName = r.text.slice(m[0].length).trim();
        const typeMatch = /^([A-Za-z0-9_]+(?:\s*\([^)]*\))?)/.exec(afterName);
        if (typeMatch) { colType = typeMatch[1].trim(); }

        // Work out absolute line and which line inside blockText to read for columns
        // r.line may be relative-to-block or already absolute; handle both.
        let absLine: number;
        let blockLineIdx: number;

        if (typeof r.line === "number") {
            // If r.line looks absolute (>= startLine) or exceeds block length, treat as absolute.
            if (r.line >= startLine || r.line >= allLines.length) {
                absLine = r.line;
                blockLineIdx = r.line - startLine;
            } else {
                // r.line is relative to the block
                absLine = startLine + r.line;
                blockLineIdx = r.line;
            }
        } else {
            // Fallback: assume current row maps to the first line of the block
            absLine = startLine;
            blockLineIdx = 0;
        }

        // Clamp to valid range inside the block
        if (blockLineIdx < 0) { blockLineIdx = 0; }
        if (blockLineIdx >= allLines.length) { blockLineIdx = allLines.length - 1; }

        // Compute precise character range on the source line
        const sourceLine = allLines[blockLineIdx] ?? "";
        let startCol = sourceLine.indexOf(raw);
        if (startCol < 0) {
            // fallback: search for unquoted token
            const esc = tokenName.replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&");
            startCol = sourceLine.search(new RegExp(`\\b${esc}\\b`));
        }
        if (startCol < 0) {
            // ultimate fallback: use provided column from splitter if present
            startCol = typeof r.col === "number" ? r.col : 0;
        }

        out.push({
            name,
            rawName: raw,
            type: colType,
            line: absLine,                    // <<< absolute document line
            start: startCol,
            end: startCol + raw.length
        });
    }

    return out;
}

export function isSqlKeyword(token: string): boolean {
    return SQL_KEYWORDS.has(token.toLowerCase());
}


export function stripStrings(s: string): string {
    // Replace T-SQL string literals (including N'...') with spaces to preserve indices
    return s.replace(/N?'(?:''|[^'])*'/g, (m) => " ".repeat(m.length));
}


export function stripComments(sql: string): string {
    // Replace line comments and block comments with spaces of the same length
    // so offsets remain stable.
    sql = sql.replace(/--.*$/gm, (m) => " ".repeat(m.length));
    sql = sql.replace(/\/\*[\s\S]*?\*\//g, (m) => " ".repeat(m.length));
    return sql;
}

export function offsetAt(doc: TextDocument, pos: Position) {
    const text = doc.getText();
    const lines = text.split(/\r?\n/);
    let off = 0;
    for (let i = 0; i < pos.line; i++) { off += lines[i].length + 1; }
    return off + pos.character;
}
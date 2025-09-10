import { TextDocument, Position, Range } from "vscode-languageserver-textdocument";

const SQL_KEYWORDS = new Set([
    "select", "from", "join", "on", "where", "and", "or", "insert", "update", "delete", "into", "as",
    "count", "group", "by", "order", "create", "procedure", "function", "view", "table", "begin", "end",
    "if", "else", "while", "case", "when", "then", "declare", "set", "values", "fetch", "next", "rows", "only",
    "distinct", "union", "all", "outer", "inner", "left", "right", "full", "top", "limit", "offset",
    "having", "over", "newid", "row_number", "desc", "asc", "sum", "min", "max", "null", "is", "exec", "not"
]);

export function normalizeName(name: string): string {
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

export function getCurrentStatement(doc: { getText: (range?: any) => string }, position: { line: number; character: number }): string {
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
        return "";
    }
}

export function getWordRangeAtPosition(doc: TextDocument, pos: { line: number; character: number }) {
    const lineText = doc.getText({
        start: { line: pos.line, character: 0 },
        end: { line: pos.line, character: Number.MAX_VALUE }
    });
    const regex = /@?[a-zA-Z0-9_\[\]\.]+/g;
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
                    column: normalizeName(rest.replace(/^[\[\]"]+|[\[\]"]+$/g, ""))
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
                const norm = normalizeName(candidate.replace(/[\[\]]/g, ""));
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
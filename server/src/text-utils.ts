import { TextDocument, Position } from "vscode-languageserver-textdocument";

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

    let n = String(name).replace(/^\.+/, "").replace(/[\[\]]/g, "").toLowerCase().trim();

    n = n.split('.').map(p => p.trim()).filter(Boolean).join('.');

    const parts = n.split(".");
    if (parts.length === 2) {
        const [schema, object] = parts;
        if (schema === "dbo") { return object; }
        return `${schema}.${object}`;
    }
    return n;
}

export function getWordRangeAtPosition(doc: TextDocument, pos: { line: number; character: number }) {
    const lineText = doc.getText({
        start: { line: pos.line, character: 0 },
        end: { line: pos.line, character: Number.MAX_VALUE }
    });
    if (!lineText) {
        return null;
    }

    const isTokenChar = (ch: string) => {
        return /[A-Za-z0-9_$#@.\[\]"`]/.test(ch);
    };

    let pivot = pos.character;
    if (pivot >= lineText.length) {
        pivot = lineText.length - 1;
    }
    if (pivot < 0) {
        return null;
    }

    if (!isTokenChar(lineText[pivot]) && pivot > 0 && isTokenChar(lineText[pivot - 1])) {
        pivot -= 1;
    }
    if (!isTokenChar(lineText[pivot])) {
        return null;
    }

    let start = pivot;
    let end = pivot + 1;

    while (start > 0 && isTokenChar(lineText[start - 1])) {
        start--;
    }
    while (end < lineText.length && isTokenChar(lineText[end])) {
        end++;
    }

    while (start < end && lineText[start] === ".") {
        start++;
    }
    while (end > start && lineText[end - 1] === ".") {
        end--;
    }

    if (start >= end) {
        return null;
    }

    return { start: { line: pos.line, character: start }, end: { line: pos.line, character: end } };
}

export function getLineStarts(text: string): number[] {
    const starts: number[] = [0];
    for (let i = 0; i < text.length; i++) {
        if (text[i] === '\n') { starts.push(i + 1); }
    }
    return starts;
}

export function offsetToPosition(offset: number, lineStarts: number[]): { line: number; character: number } {
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

export function isSqlKeyword(token: string): boolean {
    return SQL_KEYWORDS.has(token.toLowerCase());
}

export function offsetAt(doc: TextDocument, pos: Position) {
    if (typeof doc.offsetAt === "function") {
        return doc.offsetAt(pos);
    }

    const text = doc.getText();
    const lines = text.split(/\r?\n/);
    let off = 0;
    for (let i = 0; i < pos.line; i++) {
        off += lines[i].length + 1;
    }
    return off + pos.character;
}

import {normalizeName} from "./text-utils";

export function walkAst(node: any, fn: (n: any) => void) {
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
export function resolveColumnFromAst(ast: any, columnName: string): string | null {
    if (!ast) { return null; }
    const nodes = Array.isArray(ast) ? ast : [ast];
    columnName = normalizeName(columnName);

    for (const root of nodes) {
        let found: string | null = null;

        walkAst(root, (n) => {
            if (!n || typeof n !== "object") { return; }

            // ---------- Handle SELECT ----------
            if (n.type === "select" || n.ast === "select") {
                const aliasMap = new Map<string, string>(); // aliasLower -> tableNorm
                const fromArr = Array.isArray(n.from) ? n.from : (n.from ? [n.from] : []);
                for (const f of fromArr) {
                    try {
                        let alias: any = f.as || f.alias || (f.as && f.as.value) || (f.alias && f.alias.value);
                        if (alias && typeof alias === "object") { alias = alias.value || alias.name; }
                        if (typeof alias === "string") { alias = alias.replace(/[\[\]]/g, "").toLowerCase(); }

                        let table = null;
                        if (typeof f.table === "string") { table = f.table; }
                        else if (f.table && f.table.value) { table = f.table.value; }
                        else if (f.expr && (f.expr.table || f.expr.name)) { table = f.expr.table || f.expr.name; }

                        if (table) {
                            aliasMap.set((alias || String(table).toLowerCase()), normalizeName(String(table).replace(/^dbo\./i, "")));
                        } else if (f.expr && f.expr.type === "select") {
                            if (alias) { aliasMap.set(alias, "__subquery__"); }
                        }
                    } catch { /* ignore malformed from entries */ }
                }

                // find any column_ref matching the requested column
                walkAst(n, (m) => {
                    if (!m || typeof m !== "object") { return; }
                    if ((m.type === "column_ref" || m.ast === "column_ref") && m.column) {
                        const col = normalizeName(String(m.column));
                        if (col === columnName) {
                            if (m.table) {
                                const rawTable = String(m.table).replace(/[\[\]]/g, "").toLowerCase();
                                const mapped = aliasMap.get(rawTable) || normalizeName(rawTable.replace(/^dbo\./i, ""));
                                if (mapped) { found = mapped; }
                            } else {
                                if (aliasMap.size === 1) {
                                    found = Array.from(aliasMap.values())[0];
                                }
                            }
                        }
                    }
                });
            }

            // ---------- Handle UPDATE ----------
            if (n.type === "update") {
                const targetTable = n.table?.[0]?.table;
                if (!targetTable) {return;}

                for (const assignment of n.set || []) {
                    if (normalizeName(assignment.column) === columnName) {
                        found = normalizeName(targetTable);
                    }
                }

                walkAst(n.where, (m) => {
                    if (m && typeof m === "object" && m.type === "column_ref" && m.column) {
                        const col = normalizeName(String(m.column));
                        if (col === columnName) {
                            found = normalizeName(targetTable);
                        }
                    }
                });
            }

            // ---------- Handle INSERT ----------
            if (n.type === "insert") {
                const targetTable = n.table?.[0]?.table;
                if (!targetTable) {return;}

                const cols = (n.columns || []).map((c: any) => normalizeName(String(c)));
                if (cols.includes(columnName)) {
                    found = normalizeName(targetTable);
                }

                if (n.select) {
                    const subFound = resolveColumnFromAst(n.select, columnName);
                    if (subFound) { found = subFound; }
                }
            }

            // ---------- Handle DELETE ----------
            if (n.type === "delete") {
                const targetTable = n.table?.[0]?.table;
                if (!targetTable) {return;}

                walkAst(n.where, (m) => {
                    if (m && typeof m === "object" && m.type === "column_ref" && m.column) {
                        const col = normalizeName(String(m.column));
                        if (col === columnName) {
                            found = normalizeName(targetTable);
                        }
                    }
                });
            }
        });

        if (found) { return found; }
    }
    return null;
}


export function resolveAliasFromAst(alias: string, ast: any): string | null {
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
                if (found) { return found; }
            }
        }
        return null;
    }

    const roots = Array.isArray(ast) ? ast : [ast];
    for (const root of roots) {
        if (root.from) {
            const res = searchFrom(Array.isArray(root.from) ? root.from : [root.from]);
            if (res) { return res; }
        }
    }
    return null;
}
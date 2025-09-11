// /mnt/data/ast-utils.ts
import { normalizeName } from "./text-utils";

export function walkAst(node: any, fn: (n: any) => void) {
  if (!node || typeof node !== "object") { return; }
  fn(node);
  for (const key in node) {
    const child = node[key];
    if (Array.isArray(child)) {
      child.forEach(c => typeof c === "object" && walkAst(c, fn));
    } else if (child && typeof child === "object") {
      walkAst(child, fn);
    }
  }
}

/**
 * Robust extractor for table node shapes produced by node-sql-parser.
 * Accepts string or object shapes like:
 *  - "schema.table"
 *  - { table: 'T' } or { name: 'T' }
 *  - { expr: { table: 'T' } } or { expr: { name: 'T' } }
 *  - { db: 'd', table: 't' }
 */
export function normalizeAstTableName(raw: any): string | null {
  if (!raw) { return null; }
  if (typeof raw === "string") {
    return normalizeName(String(raw).replace(/^dbo\./i, "").replace(/^\[|\]$/g, ""));
  }
  if (raw.table && typeof raw.table === "string") {
    return normalizeName(String(raw.table).replace(/^dbo\./i, "").replace(/^\[|\]$/g, ""));
  }
  if (raw.name && typeof raw.name === "string") {
    return normalizeName(String(raw.name).replace(/^dbo\./i, "").replace(/^\[|\]$/g, ""));
  }
  if (raw.expr && typeof raw.expr === "object") {
    // nested shapes: { expr: { table: 'T' } } or { expr: { name: 'T' } }
    const cand = raw.expr.table || raw.expr.name;
    if (cand && typeof cand === "string") {
      return normalizeName(String(cand).replace(/^dbo\./i, "").replace(/^\[|\]$/g, ""));
    }
  }
  // sometimes the node itself is { db: 'd', table: 't' }
  if (raw.db && raw.table) {
    return normalizeName(String(raw.table).replace(/^dbo\./i, "").replace(/^\[|\]$/g, ""));
  }
  return null;
}

/**
 * Extract a column name string (normalized) from many AST shapes:
 * - string 'col'
 * - { column: 'col' } or { name: 'col' }
 * - column_ref: { type: 'column_ref', column: 'col', table: 't' }
 * - nested shapes { expr: { column: 'col' } } etc.
 */
export function extractColumnName(colNode: any): string | null {
  if (!colNode && colNode !== 0) { return null; }
  if (typeof colNode === "string") {
    return normalizeName(colNode.replace(/^\[|\]$/g, ""));
  }
  // direct properties
  if (colNode.column && typeof colNode.column === "string") {
    return normalizeName(colNode.column.replace(/^\[|\]$/g, ""));
  }
  if (colNode.name && typeof colNode.name === "string") {
    return normalizeName(colNode.name.replace(/^\[|\]$/g, ""));
  }
  if (colNode.value && typeof colNode.value === "string") {
    return normalizeName(colNode.value.replace(/^\[|\]$/g, ""));
  }
  // column_ref shape
  if ((colNode.type === "column_ref" || colNode.ast === "column_ref") && colNode.column) {
    return normalizeName(String(colNode.column).replace(/^\[|\]$/g, ""));
  }
  // nested expr (common with some parser outputs)
  if (colNode.expr && typeof colNode.expr === "object") {
    return extractColumnName(colNode.expr);
  }
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

            const table = normalizeAstTableName(f) || null;
            if (table) {
              aliasMap.set((alias || String(table).toLowerCase()), table);
            } else if (f.expr && f.expr.type === "select" && alias) {
              aliasMap.set(alias, "__subquery__");
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
        const targetTable = normalizeAstTableName(Array.isArray(n.table) ? n.table[0] : n.table);
        if (!targetTable) { return; }

        // n.set can be various shapes; be defensive
        for (const assignment of n.set || []) {
          const aCol = extractColumnName(assignment?.column ?? assignment);
          if (aCol && aCol === columnName) {
            found = targetTable;
          }
          // also detect column_ref anywhere inside assignment
          walkAst(assignment, (m) => {
            if (m && typeof m === "object" && (m.type === "column_ref" || m.ast === "column_ref") && m.column) {
              if (normalizeName(String(m.column)) === columnName) {
                found = targetTable;
              }
            }
          });
        }

        // check where clause references
        walkAst(n.where, (m) => {
          if (m && typeof m === "object" && (m.type === "column_ref" || m.ast === "column_ref") && m.column) {
            const col = normalizeName(String(m.column));
            if (col === columnName) { found = targetTable; }
          }
        });
      }

      // ---------- Handle INSERT ----------
      if (n.type === "insert") {
        const targetTable = normalizeAstTableName(Array.isArray(n.table) ? n.table[0] : n.table);
        if (!targetTable) { return; }

        const cols = (n.columns || []).map((c: any) => extractColumnName(c)).filter(Boolean) as string[];
        if (cols.includes(columnName)) {
          found = targetTable;
        }

        // If insert uses SELECT, try subselect resolution (best-effort)
        if (n.select) {
          const subFound = resolveColumnFromAst(n.select, columnName);
          if (subFound) { found = subFound; }
        }
      }

      // ---------- Handle DELETE ----------
      if (n.type === "delete") {
        const targetTable = normalizeAstTableName(Array.isArray(n.table) ? n.table[0] : n.table);
        if (!targetTable) { return; }

        walkAst(n.where, (m) => {
          if (m && typeof m === "object" && (m.type === "column_ref" || m.ast === "column_ref") && m.column) {
            const col = normalizeName(String(m.column));
            if (col === columnName) {
              found = targetTable;
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

      // FROM (SELECT .) AS picks
      if (f.subquery) {
        if (f.as && normalizeName(f.as) === aliasNorm) {
          return "__subquery__";
        }
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

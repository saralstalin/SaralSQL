// AST utilities for @saralsql/tsql-parser
import { normalizeName } from "./text-utils";

/**
 * Walk through AST nodes recursively
 * Works with any object structure from the parser
 */
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
 * Extract table name from various node types in the new AST
 * - IdentifierNode: name, parts
 * - MemberExpression: object.property pattern
 * - String literals (table names as strings)
 */
export function normalizeAstTableName(raw: any): string | null {
  if (!raw) { return null; }

  // Handle string table names
  if (typeof raw === "string") {
    return normalizeName(String(raw).replace(/^dbo\./i, "").replace(/^\[|\]$/g, ""));
  }

  // Handle IdentifierNode from new parser
  if (raw.type === "Identifier" && raw.name) {
    const name = String(raw.name).replace(/^dbo\./i, "").replace(/^\[|\]$/g, "");
    return normalizeName(name);
  }

  // Handle MemberExpression (qualified names like schema.table)
  if (raw.type === "MemberExpression" && raw.property) {
    return normalizeName(String(raw.property).replace(/^dbo\./i, "").replace(/^\[|\]$/g, ""));
  }

  // Handle nested expr
  if (raw.expr && typeof raw.expr === "object") {
    return normalizeAstTableName(raw.expr);
  }

  // Try parts array if available (IdentifierNode.parts)
  if (Array.isArray(raw.parts) && raw.parts.length > 0) {
    const table = raw.parts[raw.parts.length - 1];
    return normalizeName(String(table).replace(/^dbo\./i, "").replace(/^\[|\]$/g, ""));
  }

  return null;
}

/**
 * Extract column name from Expression or ColumnNode
 */
export function extractColumnName(colNode: any): string | null {
  if (!colNode && colNode !== 0) { return null; }

  // Handle string column names
  if (typeof colNode === "string") {
    return normalizeName(colNode.replace(/^\[|\]$/g, ""));
  }

  // Handle IdentifierNode
  if (colNode.type === "Identifier" && colNode.name) {
    return normalizeName(String(colNode.name).replace(/^\[|\]$/g, ""));
  }

  // Handle MemberExpression (table.column) - extract the property part
  if (colNode.type === "MemberExpression" && colNode.property) {
    return normalizeName(String(colNode.property).replace(/^\[|\]$/g, ""));
  }

  // Handle ColumnNode from SELECT columns
  if (colNode.type === "Column" && colNode.expression) {
    return extractColumnName(colNode.expression);
  }

  // Handle nested expr
  if (colNode.expr && typeof colNode.expr === "object") {
    return extractColumnName(colNode.expr);
  }

  return null;
}

/**
 * Try to resolve which table provides `columnName` inside the given AST
 * Returns normalized table name (matching columnsByTable keys) or null
 */
export function resolveColumnFromAst(ast: any, columnName: string): string | null {
  if (!ast) { return null; }
  const nodes = Array.isArray(ast) ? ast : [ast];
  columnName = normalizeName(columnName);

  for (const root of nodes) {
    let found: string | null = null;

    walkAst(root, (n) => {
      if (found || !n || typeof n !== "object") { return; }

      // Handle SelectNode (new parser)
      if (n.type === "SelectStatement" && n.columns) {
        const aliasMap = new Map<string, string>(); // aliasLower -> tableNorm

        // Build alias map from TableReferences
        if (n.from && Array.isArray(n.from)) {
          for (const tableRef of n.from) {
            if (tableRef.table) {
              const tableName = normalizeAstTableName(tableRef.table);
              const alias = tableRef.alias || null;

              if (tableName) {
                if (alias) {
                  aliasMap.set(normalizeName(alias), tableName);
                } else {
                  aliasMap.set(tableName.toLowerCase(), tableName);
                }
              }

              // Handle joins
              if (tableRef.joins && Array.isArray(tableRef.joins)) {
                for (const join of tableRef.joins) {
                  const joinTable = normalizeAstTableName(join.table);
                  const joinAlias = join.alias || null;
                  if (joinTable) {
                    if (joinAlias) {
                      aliasMap.set(normalizeName(joinAlias), joinTable);
                    } else {
                      aliasMap.set(joinTable.toLowerCase(), joinTable);
                    }
                  }
                }
              }
            }
          }
        }

        // Find column in SELECT columns or WHERE
        if (n.columns && Array.isArray(n.columns)) {
          for (const col of n.columns) {
            const colName = extractColumnName(col);
            if (colName === columnName) {
              // Try to determine the source table
              if (col.expression && col.expression.type === "MemberExpression") {
                const tableRef = col.expression.object;
                if (tableRef && tableRef.type === "Identifier") {
                  const mapped = aliasMap.get(normalizeName(tableRef.name));
                  if (mapped) { found = mapped; }
                }
              } else if (aliasMap.size === 1) {
                // Single table in FROM, use it
                found = Array.from(aliasMap.values())[0];
              }
            }
          }
        }

        // Check WHERE clause
        walkAst(n.where, (m) => {
          if (m && typeof m === "object") {
            if (m.type === "MemberExpression" && m.property) {
              const col = extractColumnName(m);
              if (col === columnName && m.object && m.object.type === "Identifier") {
                const mapped = aliasMap.get(normalizeName(m.object.name));
                if (mapped) { found = mapped; }
              }
            }
          }
        });
      }

      // Handle UpdateStatement
      else if (n.type === "UpdateStatement") {
        const targetTable = normalizeAstTableName(n.target);
        if (!targetTable) { return; }

        // Check assignments
        if (n.assignments && Array.isArray(n.assignments)) {
          for (const assignment of n.assignments) {
            const aCol = extractColumnName(assignment?.column ?? assignment);
            if (aCol && aCol === columnName) {
              found = targetTable;
            }
          }
        }

        // Check WHERE
        walkAst(n.where, (m) => {
          if (m && typeof m === "object") {
            const col = extractColumnName(m);
            if (col === columnName) { found = targetTable; }
          }
        });
      }

      // Handle InsertStatement
      else if (n.type === "InsertStatement") {
        const targetTable = normalizeAstTableName(n.table);
        if (!targetTable) { return; }

        const cols = (n.columns || []).map((c: any) => extractColumnName(c)).filter(Boolean) as string[];
        if (cols.includes(columnName)) {
          found = targetTable;
        }

        if (n.selectQuery) {
          const subFound = resolveColumnFromAst(n.selectQuery, columnName);
          if (subFound) { found = subFound; }
        }
      }

      // Handle DeleteStatement
      else if (n.type === "DeleteStatement") {
        const targetTable = normalizeAstTableName(n.target);
        if (!targetTable) { return; }

        // Check WHERE
        walkAst(n.where, (m) => {
          if (m && typeof m === "object") {
            const col = extractColumnName(m);
            if (col === columnName) { found = targetTable; }
          }
        });
      }

    });

    if (found) { return found; }
  }
  return null;
}

/**
 * Extract qualified name parts from an expression
 */
export function extractQualifiedName(expr: any): { table?: string | null; column?: string | null } {
  if (!expr) { return {}; }

  // Handle MemberExpression (table.column)
  if (expr.type === "MemberExpression") {
    const table = extractColumnName(expr.object);
    const column = expr.property;
    return { table, column };
  }

  // Handle IdentifierNode with parts (schema.table or table.column)
  if (expr.type === "Identifier" && Array.isArray(expr.parts) && expr.parts.length > 1) {
    return {
      table: normalizeName(expr.parts[expr.parts.length - 2]),
      column: normalizeName(expr.parts[expr.parts.length - 1])
    };
  }

  // Handle single identifier
  if (expr.type === "Identifier") {
    return { column: expr.name };
  }

  return {};
}


/**
 * Resolve alias from AST (backward compatibility)
 */
export function resolveAliasFromAst(alias: string, ast: any): string | null {
  const aliasNorm = normalizeName(alias);

  function searchFrom(fromArr: any[]): string | null {
    for (const f of fromArr) {
      // Handle new TableReference format
      if (f.type === "TableReference") {
        if (f.alias && normalizeName(f.alias) === aliasNorm && f.table) {
          return normalizeAstTableName(f.table);
        }
        if (!f.alias && f.table && normalizeAstTableName(f.table) === aliasNorm) {
          return normalizeAstTableName(f.table);
        }
      }

    }
    return null;
  }

  const roots = Array.isArray(ast) ? ast : [ast];
  for (const root of roots) {
    if (root.from) {
      const fromArr = Array.isArray(root.from) ? root.from : [root.from];
      const res = searchFrom(fromArr);
      if (res) { return res; }
    }
  }
  return null;
}

export function resolveAliasTableName(sym: any): string | undefined {
  const metadataName = sym?.metadata?.tableName;
  if (typeof metadataName === "string" && metadataName.length > 0) {
    return metadataName;
  }

  const table = sym?.location?.table;
  if (typeof table === "string") {
    return table;
  }

  if (typeof table?.name === "string") {
    return table.name;
  }

  return undefined;
}

export function getCteColumns(sym: any): Array<{ name: string; rawName: string; start?: number; end?: number }> {
  const cols = sym?.location?.query?.columns;
  if (!Array.isArray(cols)) {
    return [];
  }

  const out: Array<{ name: string; rawName: string; start?: number; end?: number }> = [];
  for (const col of cols) {
    const rawName = String(col?.outputName ?? col?.sourceName ?? col?.expression?.name ?? "").trim();
    if (!rawName) {
      continue;
    }

    out.push({
      name: normalizeName(rawName),
      rawName,
      start: typeof col?.start === "number" ? col.start : undefined,
      end: typeof col?.end === "number" ? col.end : undefined
    });
  }

  return out;
}

export function resolveSymbolCaseInsensitive(scope: any, name: string): any {
  if (!scope || !name) {
    return null;
  }

  const direct = typeof scope.resolve === "function" ? scope.resolve(name) : null;
  if (direct) {
    return direct;
  }

  const nameNorm = name.toLowerCase();
  const visibleSymbols = typeof scope.getVisibleSymbols === "function"
    ? scope.getVisibleSymbols()
    : Object.values(scope.symbols ?? {});

  return visibleSymbols.find((sym: any) => String(sym.name ?? "").toLowerCase() === nameNorm) ?? null;
}
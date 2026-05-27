// AST utilities for @saralsql/tsql-parser
import { normalizeName } from "./text-utils";
import type { ASTNode, Program, Statement } from "@saralsql/tsql-parser";

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

export function getDisplaySymbolName(sym: any): string {
  const raw = String(sym?.rawName ?? sym?.metadata?.rawName ?? sym?.name ?? "").trim();
  return raw || String(sym?.name ?? "");
}

/**
 * Try to resolve which table provides `columnName` inside the given AST
 * Returns normalized table name (matching columnsByTable keys) or null
 */
export function resolveColumnFromAst(ast: any, columnName: string): string | null {
  if (!ast) { return null; }
  const roots = Array.isArray(ast) ? ast : [ast];
  const colNorm = normalizeName(columnName);

  const isStatementNode = (n: any): boolean => {
    const t = String(n?.type ?? "");
    return t.endsWith("Statement") || t === "WithStatement";
  };

  const walkExpressionOnly = (node: any, fn: (n: any) => void): void => {
    if (!node || typeof node !== "object") { return; }
    fn(node);
    for (const key in node) {
      const child = node[key];
      if (Array.isArray(child)) {
        for (const c of child) {
          if (c && typeof c === "object" && !isStatementNode(c)) {
            walkExpressionOnly(c, fn);
          }
        }
      } else if (child && typeof child === "object" && !isStatementNode(child)) {
        walkExpressionOnly(child, fn);
      }
    }
  };

  const resolveSelectStatement = (n: any): string | null => {
    const aliasMap = new Map<string, string>();
    if (Array.isArray(n?.from)) {
      for (const tableRef of n.from) {
        const tableName = normalizeAstTableName(tableRef?.table);
        const alias = tableRef?.alias || null;
        if (tableName) {
          aliasMap.set(alias ? normalizeName(alias) : tableName, tableName);
        }
        if (Array.isArray(tableRef?.joins)) {
          for (const join of tableRef.joins) {
            const joinTable = normalizeAstTableName(join?.table);
            const joinAlias = join?.alias || null;
            if (joinTable) {
              aliasMap.set(joinAlias ? normalizeName(joinAlias) : joinTable, joinTable);
            }
          }
        }
      }
    }

    if (Array.isArray(n?.columns)) {
      for (const col of n.columns) {
        if (extractColumnName(col) !== colNorm) { continue; }
        if (col?.expression?.type === "MemberExpression" && col.expression.object?.type === "Identifier") {
          const mapped = aliasMap.get(normalizeName(col.expression.object.name));
          if (mapped) { return mapped; }
        }
        if (aliasMap.size === 1) {
          return Array.from(aliasMap.values())[0];
        }
      }
    }

    let whereFound: string | null = null;
    walkExpressionOnly(n?.where, (m) => {
      if (whereFound || m?.type !== "MemberExpression" || !m?.property) { return; }
      if (extractColumnName(m) !== colNorm) { return; }
      if (m.object?.type === "Identifier") {
        const mapped = aliasMap.get(normalizeName(m.object.name));
        if (mapped) { whereFound = mapped; }
      }
    });
    return whereFound;
  };

  const resolveSingleStatement = (n: any): string | null => {
    if (!n || typeof n !== "object") { return null; }
    if (n.type === "SelectStatement" && n.columns) {
      return resolveSelectStatement(n);
    }
    if (n.type === "UpdateStatement") {
      const targetTable = normalizeAstTableName(n.target);
      if (!targetTable) { return null; }
      for (const assignment of n.assignments ?? []) {
        const aCol = extractColumnName(assignment?.column ?? assignment);
        if (aCol === colNorm) { return targetTable; }
      }
      let seen = false;
      walkExpressionOnly(n.where, (m) => {
        if (!seen && extractColumnName(m) === colNorm) { seen = true; }
      });
      return seen ? targetTable : null;
    }
    if (n.type === "InsertStatement") {
      const targetTable = normalizeAstTableName(n.table);
      if (!targetTable) { return null; }
      const cols = (n.columns || []).map((c: any) => extractColumnName(c)).filter(Boolean) as string[];
      if (cols.includes(colNorm)) { return targetTable; }
      if (n.selectQuery && n.selectQuery.type === "SelectStatement") {
        return resolveSelectStatement(n.selectQuery);
      }
      return null;
    }
    if (n.type === "DeleteStatement") {
      const targetTable = normalizeAstTableName(n.target);
      if (!targetTable) { return null; }
      let seen = false;
      walkExpressionOnly(n.where, (m) => {
        if (!seen && extractColumnName(m) === colNorm) { seen = true; }
      });
      return seen ? targetTable : null;
    }
    return null;
  };

  for (const root of roots) {
    const resolved = resolveSingleStatement(root);
    if (resolved) { return resolved; }
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
 * Resolve alias from AST (scoped to the provided statement/program subtree)
 */
export function resolveAliasFromAst(alias: string, ast: Program | Statement | null): string | null {
  if (!alias || !ast) {
    return null;
  }

  const aliasNorm = normalizeName(alias);
  const roots: Statement[] = isProgramNode(ast) ? [...(ast.body ?? [])] : [ast];

  for (const root of roots) {
    const resolved = resolveAliasInNode(aliasNorm, root);
    if (resolved) {
      return resolved;
    }
  }

  return null;
}

function isProgramNode(node: Program | Statement): node is Program {
  return String((node as any)?.type ?? "") === "Program" && Array.isArray((node as any)?.body);
}

function resolveAliasInNode(aliasNorm: string, node: ASTNode | Statement | null): string | null {
  if (!node || typeof node !== "object") {
    return null;
  }

  const current = node as any;

  if (current.type === "TableReference") {
    if (current.alias && normalizeName(String(current.alias)) === aliasNorm && current.table) {
      return normalizeAstTableName(current.table);
    }

    if (Array.isArray(current.joins)) {
      for (const join of current.joins) {
        if (join?.alias && normalizeName(String(join.alias)) === aliasNorm && join.table) {
          return normalizeAstTableName(join.table);
        }
      }
    }

    if (!current.alias && current.table && normalizeAstTableName(current.table) === aliasNorm) {
      return normalizeAstTableName(current.table);
    }
  }

  if (Array.isArray(current)) {
    for (const item of current) {
      const resolved = resolveAliasInNode(aliasNorm, item);
      if (resolved) {
        return resolved;
      }
    }
    return null;
  }

  for (const value of Object.values(current)) {
    if (value && typeof value === "object") {
      const resolved = resolveAliasInNode(aliasNorm, value as ASTNode);
      if (resolved) {
        return resolved;
      }
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
  const cols = getQueryProjectionColumns(sym?.location?.query);
  const out: Array<{ name: string; rawName: string; start?: number; end?: number }> = [];
  if (Array.isArray(cols)) {
    for (const col of cols) {
      const rawName = String(col?.outputName ?? col?.sourceName ?? col?.expression?.name ?? "").trim();
      if (!rawName || normalizeName(rawName) === "expression") {
        continue;
      }

      out.push({
        name: normalizeName(rawName),
        rawName,
        start: typeof col?.start === "number" ? col.start : undefined,
        end: typeof col?.end === "number" ? col.end : undefined
      });
    }
  }

  if (out.length === 0 && Array.isArray(sym?.location?.columns)) {
    for (const raw of sym.location.columns) {
      const rawName = String(raw ?? "").trim();
      if (!rawName) {
        continue;
      }
      out.push({
        name: normalizeName(rawName),
        rawName
      });
    }
  }

  return out;
}

function getQueryProjectionColumns(query: any): any[] {
  if (!query || typeof query !== "object") {
    return [];
  }

  if (Array.isArray(query.columns) && query.columns.length > 0) {
    return query.columns;
  }

  // Recursive CTEs and UNION-based CTEs often come as SetOperator nodes.
  if (query.type === "SetOperator") {
    const leftCols = getQueryProjectionColumns(query.left);
    if (leftCols.length > 0) {
      return leftCols;
    }
    return getQueryProjectionColumns(query.right);
  }

  return [];
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

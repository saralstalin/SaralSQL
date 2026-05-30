import { normalizeName } from "./text-utils";

function getSelectOutputName(col: any): string {
  const explicit = normalizeName(String(col?.alias ?? col?.outputName ?? col?.sourceName ?? col?.name ?? ""));
  if (explicit) {
    return explicit;
  }

  const expr = col?.expression;
  if (expr?.type === "Identifier") {
    if (Array.isArray(expr.parts) && expr.parts.length > 0) {
      return normalizeName(String(expr.parts[expr.parts.length - 1] ?? ""));
    }
    return normalizeName(String(expr.name ?? ""));
  }

  if (expr?.type === "MemberExpression") {
    return normalizeName(String(expr.property ?? ""));
  }

  if (expr?.type === "Column" && expr?.expression?.type === "Identifier") {
    if (Array.isArray(expr.expression.parts) && expr.expression.parts.length > 0) {
      return normalizeName(String(expr.expression.parts[expr.expression.parts.length - 1] ?? ""));
    }
    return normalizeName(String(expr.expression.name ?? ""));
  }

  return "";
}

function getOrderByExpressionName(expr: any): string {
  if (expr?.type === "Identifier") {
    if (Array.isArray(expr.parts) && expr.parts.length > 0) {
      return normalizeName(String(expr.parts[expr.parts.length - 1] ?? ""));
    }
    return normalizeName(String(expr.name ?? ""));
  }

  if (expr?.type === "Column") {
    if (Array.isArray(expr?.expression?.parts) && expr.expression.parts.length > 0) {
      return normalizeName(String(expr.expression.parts[expr.expression.parts.length - 1] ?? ""));
    }
    return normalizeName(String(expr?.expression?.name ?? ""));
  }

  return "";
}

function collectOrderByStartsByUniqueness(ast: any, duplicates: boolean): Set<number> {
  const starts = new Set<number>();

  // Collect every SelectStatement in the tree without descending INTO nested
  // SelectStatements from within another SelectStatement — each SELECT's ORDER BY
  // aliases are scoped to that SELECT only and must not bleed into sibling scopes.
  const selects: any[] = [];
  const collectSelects = (node: any): void => {
    if (!node || typeof node !== "object") { return; }
    if (node.type === "SelectStatement") {
      selects.push(node);
      // Deliberately do NOT recurse into this node's children here; nested
      // SelectStatements will be pushed when we process subsequent array/object
      // children at the top-level walk below.
    }
    if (Array.isArray(node)) {
      for (const item of node) { collectSelects(item); }
      return;
    }
    for (const value of Object.values(node)) {
      if (value && typeof value === "object") { collectSelects(value); }
    }
  };
  collectSelects(ast);

  for (const node of selects) {
    if (!Array.isArray(node.columns) || !Array.isArray(node.orderBy)) { continue; }

    const aliasCounts = new Map<string, number>();
    for (const col of node.columns) {
      const name = getSelectOutputName(col);
      if (name) {
        aliasCounts.set(name, (aliasCounts.get(name) ?? 0) + 1);
      }
    }

    if (aliasCounts.size === 0) { continue; }

    for (const order of node.orderBy) {
      const expr = order?.expression;
      if (typeof expr?.start !== "number") { continue; }
      const name = getOrderByExpressionName(expr);
      if (!name) { continue; }
      const count = aliasCounts.get(name) ?? 0;
      if (duplicates ? count > 1 : count === 1) {
        starts.add(expr.start);
      }
    }
  }

  return starts;
}

export function collectOrderByAliasStarts(ast: any): Set<number> {
  return collectOrderByStartsByUniqueness(ast, false);
}

export function collectOrderByDuplicateAliasStarts(ast: any): Set<number> {
  return collectOrderByStartsByUniqueness(ast, true);
}

export function isOrderByDuplicateOutputAtOffset(ast: any, offset: number, tokenText: string): boolean {
  if (!ast || typeof offset !== "number") {
    return false;
  }
  const tokenNorm = normalizeName(String(tokenText ?? "").split(".").pop() ?? "");
  if (!tokenNorm) {
    return false;
  }

  let hit = false;
  const visit = (node: any): void => {
    if (hit || !node || typeof node !== "object") {
      return;
    }
    if (node.type === "SelectStatement" && Array.isArray(node.columns) && Array.isArray(node.orderBy)) {
      const start = Number(node.start);
      const end = Number(node.end);
      if (Number.isFinite(start) && Number.isFinite(end) && (offset < start || offset > end)) {
        return;
      }

      const aliasCounts = new Map<string, number>();
      for (const col of node.columns) {
        const name = getSelectOutputName(col);
        if (name) {
          aliasCounts.set(name, (aliasCounts.get(name) ?? 0) + 1);
        }
      }

      for (const ord of node.orderBy) {
        const expr = ord?.expression;
        if (typeof expr?.start !== "number" || typeof expr?.end !== "number") {
          continue;
        }
        if (offset < expr.start || offset > expr.end) {
          continue;
        }
        const ordName = getOrderByExpressionName(expr);
        if (ordName && ordName === tokenNorm && (aliasCounts.get(ordName) ?? 0) > 1) {
          hit = true;
          return;
        }
      }
    }

    if (Array.isArray(node)) {
      for (const item of node) {
        visit(item);
      }
      return;
    }
    for (const value of Object.values(node)) {
      if (value && typeof value === "object") {
        visit(value);
      }
    }
  };

  visit(ast);
  return hit;
}


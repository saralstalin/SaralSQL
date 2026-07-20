/**
 * Pure SQL-analysis helpers extracted from server.ts.
 * No LSP connection or document-cache references — safe to import in tests.
 */
import { CompletionItem, CompletionItemKind, Location, Position, Range } from "vscode-languageserver/node";
import { TextDocument } from "vscode-languageserver-textdocument";
import * as url from "url";
import { normalizeName, getWordRangeAtPosition, isSqlKeyword } from "./text-utils";
import {
  tablesByName, tableTypesByName, definitions, referencesIndex, type ReferenceDef
} from "./definitions";
import type { ParseResult } from "./sql-parser";
import {
  getCteColumns, resolveAliasTableName, resolveSymbolCaseInsensitive,
  getDisplaySymbolName, resolveAliasFromAst, extractColumnName
} from "./ast-utils";
import { collectNearestScopeColumnOwners } from "./scope-column-resolver";
import { resolveBareColumnAtOffset } from "./column-resolution";

// ---------- URI normalization ----------

export function toNormUri(rawUri: string): string {
  try {
    let uri = rawUri;
    if (!uri.startsWith("file://")) {
      uri = url.pathToFileURL(uri).toString();
    }
    const prefix = "file:///";
    if (uri.toLowerCase().startsWith(prefix)) {
      const pathPart = decodeURIComponent(uri.substring(prefix.length));
      const normalizedPath = pathPart.replace(/\\/g, "/").replace(/^([A-Za-z]):\//, (_m, d) => d.toLowerCase() + ":/");
      return prefix + encodeURI(normalizedPath);
    }
    return uri;
  } catch {
    return rawUri;
  }
}

// ---------- Statement helpers ----------

/**
 * Returns the SQL statement text that contains `offset`, or `fallbackLine` when
 * no AST statement covers the position.  Testable: takes explicit parameters
 * instead of reading from the document cache.
 */
export function computeCurrentStatement(
  text: string,
  offset: number,
  parsed: ParseResult | null,
  fallbackLine: string
): string {
  if (!parsed?.ast?.body) {
    return fallbackLine;
  }
  for (const stmt of parsed.ast.body) {
    if (stmt.start <= offset && stmt.end >= offset) {
      return text.slice(stmt.start, stmt.end);
    }
  }
  return fallbackLine;
}

// ---------- Completion helpers ----------

export function getVariableCompletionItems(scopeAtPos: any): CompletionItem[] {
  if (!scopeAtPos || typeof scopeAtPos.getVisibleSymbols !== "function") {
    return [];
  }
  const items: CompletionItem[] = [];
  const seen = new Set<string>();
  for (const sym of scopeAtPos.getVisibleSymbols()) {
    if (sym.kind !== "Parameter" && sym.kind !== "Variable") { continue; }
    const name = String(sym.name ?? "");
    if (!name || seen.has(name.toLowerCase())) { continue; }
    seen.add(name.toLowerCase());
    items.push({
      label: name,
      kind: CompletionItemKind.Variable,
      detail: sym.dataType ? `${sym.kind} (${sym.dataType})` : sym.kind,
      insertText: name
    });
  }
  return items;
}

export function getParserAliasColumnNames(parsed: ParseResult | null | undefined, sym: any, localDefsByName?: Map<string, any>): string[] {
  const names = new Map<string, string>();

  if (Array.isArray(sym?.columns)) {
    for (const col of sym.columns) {
      const name = String(typeof col === "string" ? col : (col?.rawName ?? col?.name ?? "")).trim();
      const norm = normalizeName(name);
      if (norm) { names.set(norm, name); }
    }
  }

  const aliasName = normalizeName(sym?.name ?? "");
  const sources = Array.isArray((parsed as any)?.lineage?.sources) ? (parsed as any).lineage.sources : [];

  const addColumnsFromSchemaSourceAlias = (sourceAlias: string): void => {
    const targetAlias = normalizeName(sourceAlias);
    if (!targetAlias) { return; }
    for (const src of sources) {
      const srcAlias = normalizeName(String(src?.alias ?? src?.name ?? ""));
      if (srcAlias !== targetAlias) { continue; }
      const candidates = [
        normalizeName(String(src?.baseName ?? "")),
        normalizeName(String(src?.name ?? ""))
      ].filter(Boolean);
      for (const candidate of candidates) {
        const stripped = candidate.replace(/^dbo\./, "");
        const def = localDefsByName?.get(candidate) || localDefsByName?.get(stripped)
          || tablesByName.get(candidate) || tablesByName.get(stripped)
          || tableTypesByName.get(candidate) || tableTypesByName.get(stripped);
        if (!def || !Array.isArray(def.columns)) { continue; }
        for (const c of def.columns) {
          const raw = String(c?.rawName ?? c?.name ?? "").trim();
          const norm = normalizeName(raw);
          if (norm) { names.set(norm, raw); }
        }
      }
    }
  };

  for (const source of sources) {
    const sourceName = normalizeName(source?.alias ?? source?.name ?? "");
    if (!aliasName || sourceName !== aliasName || !Array.isArray(source?.projection)) { continue; }
    for (const projection of source.projection) {
      const name = String(projection?.name ?? "").trim();
      const norm = normalizeName(name);
      if (norm && norm !== "*") { names.set(norm, name); }
    }
  }

  const queryCols = sym?.location?.table?.query?.columns;
  if (Array.isArray(queryCols)) {
    for (const col of queryCols) {
      if (col?.type !== "Column" || col?.wildcard !== true || col?.expression?.type !== "WildcardExpression") { continue; }
      const prefix = normalizeName(String(col?.expression?.tablePrefix?.name ?? ""));
      if (!prefix) { continue; }
      for (const source of sources) {
        const sourceName = normalizeName(source?.alias ?? source?.name ?? "");
        if (sourceName !== prefix || !Array.isArray(source?.projection)) { continue; }
        for (const projection of source.projection) {
          const name = String(projection?.name ?? "").trim();
          const norm = normalizeName(name);
          if (norm) { names.set(norm, name); }
        }
      }
    }
    if (names.size === 0) {
      for (const col of queryCols) {
        if (col?.type !== "Column" || col?.wildcard !== true || col?.expression?.type !== "WildcardExpression") { continue; }
        const prefix = normalizeName(String(col?.expression?.tablePrefix?.name ?? ""));
        if (prefix) { addColumnsFromSchemaSourceAlias(prefix); }
      }
    }
  }

  return Array.from(names.values());
}

export function getAliasBeforeDot(linePrefix: string): string | null {
  const trimmed = linePrefix.replace(/\s+$/, "");
  if (!trimmed.endsWith(".")) { return null; }
  const beforeDot = trimmed.slice(0, -1).trimEnd();
  if (!beforeDot) { return null; }
  const lastChar = beforeDot[beforeDot.length - 1];
  if (lastChar === "]") {
    const open = beforeDot.lastIndexOf("[");
    return (open >= 0 && open < beforeDot.length - 1) ? beforeDot.slice(open + 1, -1) : null;
  }
  if (lastChar === "\"") {
    const open = beforeDot.lastIndexOf("\"", beforeDot.length - 2);
    return (open >= 0 && open < beforeDot.length - 1) ? beforeDot.slice(open + 1, -1) : null;
  }
  if (lastChar === "`") {
    const open = beforeDot.lastIndexOf("`", beforeDot.length - 2);
    return (open >= 0 && open < beforeDot.length - 1) ? beforeDot.slice(open + 1, -1) : null;
  }
  let i = beforeDot.length - 1;
  while (i >= 0) {
    const ch = beforeDot[i];
    const isIdent = (ch >= "a" && ch <= "z") || (ch >= "A" && ch <= "Z") || (ch >= "0" && ch <= "9")
      || ch === "_" || ch === "$" || ch === "#" || ch === "@";
    if (!isIdent) { break; }
    i--;
  }
  const token = beforeDot.slice(i + 1);
  return token.length > 0 ? token : null;
}

export function isFromJoinTableContext(linePrefix: string): boolean {
  const trimmed = linePrefix.replace(/\s+$/, "");
  if (!trimmed) { return false; }
  const tokenMatch = trimmed.match(/([A-Za-z_][A-Za-z0-9_]*)$/);
  const trailingToken = tokenMatch ? tokenMatch[1].toLowerCase() : "";
  if (trailingToken === "from" || trailingToken === "join") { return true; }
  if (!trailingToken) { return false; }
  const withoutToken = trimmed.slice(0, trimmed.length - trailingToken.length).trimEnd();
  const prevMatch = withoutToken.match(/([A-Za-z_][A-Za-z0-9_]*)$/);
  const prevToken = prevMatch ? prevMatch[1].toLowerCase() : "";
  return prevToken === "from" || prevToken === "join";
}

export function endsWithDotToken(linePrefix: string): boolean {
  return linePrefix.trimEnd().endsWith(".");
}

export function isLikelySelectProjectionByText(linePrefix: string): boolean {
  const trimmed = linePrefix.trimEnd();
  if (!trimmed) { return false; }
  const lower = trimmed.toLowerCase();
  const idx = lower.lastIndexOf("select");
  if (idx < 0) { return false; }
  const after = lower.slice(idx + "select".length);
  return after.trim().length > 0 && !isFromJoinTableContext(trimmed);
}

export function isInSelectProjectionContext(parsed: any, offset: number, linePrefix: string): boolean {
  const ast = parsed?.ast;
  if (!ast) { return false; }
  let inProjection = false;
  const visit = (node: any) => {
    if (!node || typeof node !== "object" || inProjection) { return; }
    if (node.type === "SelectStatement" && typeof node.start === "number" && typeof node.end === "number"
      && offset >= node.start && offset <= node.end) {
      const fromStart = Array.isArray(node.from) && node.from.length > 0 && typeof node.from[0]?.start === "number"
        ? node.from[0].start : node.end;
      if (offset <= fromStart) { inProjection = true; return; }
    }
    if (Array.isArray(node)) { for (const item of node) { visit(item); } return; }
    for (const value of Object.values(node)) { if (value && typeof value === "object") { visit(value); } }
  };
  visit(ast);
  return inProjection;
}

// ---------- AST helpers ----------

export function getContainingStatementNode(ast: any, offset: number): any | null {
  if (!ast) { return null; }
  const statements = Array.isArray(ast?.body) ? ast.body : (Array.isArray(ast) ? ast : [ast]);
  let best: any = null;
  for (const stmt of statements) {
    if (typeof stmt?.start !== "number" || typeof stmt?.end !== "number") { continue; }
    if (offset < stmt.start || offset > stmt.end) { continue; }
    if (!best || (stmt.end - stmt.start) < (best.end - best.start)) { best = stmt; }
  }
  return best;
}

export function resolveAliasTableFromStatementAst(parsed: ParseResult | null, offset: number, aliasNorm: string): string | null {
  const stmt = getContainingStatementNode(parsed?.ast, offset);
  if (!stmt) { return null; }
  return resolveAliasFromAst(aliasNorm, stmt);
}

export function collectTablesFromAstNode(node: any): string[] {
  const candidates = new Set<string>();
  const add = (name: string | undefined) => { const n = normalizeName(String(name ?? "")); if (n) { candidates.add(n); } };
  const visit = (current: any) => {
    if (!current || typeof current !== "object") { return; }
    if (current.type === "TableReference") {
      const table = current.table;
      if (typeof table?.name === "string") { add(table.name); } else if (typeof table === "string") { add(table); }
      if (Array.isArray(current.joins)) {
        for (const join of current.joins) {
          const jt = join?.table;
          if (typeof jt?.name === "string") { add(jt.name); } else if (typeof jt === "string") { add(jt); }
        }
      }
    }
    if (current.type === "UpdateStatement") {
      if (typeof current.target?.name === "string") { add(current.target.name); }
      if (Array.isArray(current.from)) {
        for (const src of current.from) {
          const t = src?.table;
          if (typeof t?.name === "string") { add(t.name); } else if (typeof t === "string") { add(t); }
        }
      }
    }
    if (current.type === "InsertStatement" && typeof current.table?.name === "string") { add(current.table.name); }
    if (Array.isArray(current)) { for (const item of current) { visit(item); } return; }
    for (const value of Object.values(current)) { if (value && typeof value === "object") { visit(value); } }
  };
  visit(node);
  return Array.from(candidates);
}

export function getStatementTableCandidatesFromAst(parsed: any, offset: number): string[] {
  const stmt = getContainingStatementNode(parsed?.ast, offset);
  return stmt ? collectTablesFromAstNode(stmt) : [];
}

// Finds the InsertStatement node whose column list contains `offset`.
// Uses the closing `)` of the column list (found via `text`) to correctly handle
// cursor positions after a trailing comma (e.g. `(Col1, |)`).
function findInsertColumnListNode(parsed: any, offset: number, text?: string): any {
  const ast = parsed?.ast;
  if (!ast) { return null; }
  let match: any = null;
  const visit = (node: any) => {
    if (!node || typeof node !== "object" || match) { return; }
    if (node.type === "InsertStatement" && Array.isArray(node.columnNodes) && node.columnNodes.length > 0) {
      const first = node.columnNodes[0];
      const last = node.columnNodes[node.columnNodes.length - 1];
      if (typeof first?.start === "number" && typeof last?.end === "number" && offset >= first.start) {
        // Upper bound: the closing ')' of the column list, so cursor after a trailing
        // comma is still detected as inside the list.
        let upperBound = last.end;
        if (typeof node.selectQuery?.start === "number") {
          upperBound = node.selectQuery.start - 1;
        } else if (text) {
          const closeParen = text.indexOf(')', last.end);
          upperBound = closeParen !== -1 ? closeParen : last.end + 500;
        } else {
          upperBound = last.end + 500;
        }
        if (offset <= upperBound) { match = node; return; }
      }
    }
    if (Array.isArray(node)) { for (const item of node) { visit(item); } return; }
    for (const value of Object.values(node)) { if (value && typeof value === "object") { visit(value); } }
  };
  visit(ast);
  return match;
}

// Finds the UpdateStatement node whose SET list contains `offset`.
// Handles cursor positions in the gap after the last assignment (before WHERE/FROM)
// by scanning forward in `text` for the next WHERE or FROM keyword.
function findUpdateSetNode(parsed: any, offset: number, text?: string): any {
  const ast = parsed?.ast;
  if (!ast) { return null; }
  let match: any = null;
  const visit = (node: any) => {
    if (!node || typeof node !== "object" || match) { return; }
    if (node.type === "UpdateStatement" && Array.isArray(node.assignments) && node.assignments.length > 0) {
      const first = node.assignments[0];
      const last = node.assignments[node.assignments.length - 1];
      if (typeof first?.start !== "number" || typeof last?.end !== "number") { return; }

      // Determine whether offset falls in the SET region.
      // Primary range: first column start → last assignment end (original check).
      // Extended range: gap after last assignment, before WHERE/FROM.
      let inRange = offset >= first.start && offset <= last.end;
      if (!inRange && offset > last.end) {
        // Cursor is after the last assignment — accept only if it's before WHERE/FROM.
        if (text) {
          const afterLast = text.slice(last.end, last.end + 300);
          const boundIdx = afterLast.search(/\b(?:WHERE|FROM)\b/i);
          inRange = boundIdx === -1 ? offset <= last.end + 300 : (last.end + boundIdx) > offset;
        } else {
          // No text: use a small safety buffer (just covers ", " whitespace)
          inRange = offset <= last.end + 10;
        }
      }

      if (inRange) {
        let insideRhs = false;
        for (const assignment of node.assignments) {
          const expr = assignment.expression ?? assignment.value ?? assignment.right;
          if (expr && typeof expr.start === "number" && typeof expr.end === "number"
            && offset >= expr.start && offset <= expr.end) { insideRhs = true; break; }
        }
        if (!insideRhs) { match = node; return; }
      }
    }
    if (Array.isArray(node)) { for (const item of node) { visit(item); } return; }
    for (const value of Object.values(node)) { if (value && typeof value === "object") { visit(value); } }
  };
  visit(ast);
  return match;
}

export function getUpdateSetTargetTable(parsed: ParseResult | null, offset: number, text?: string): string | undefined {
  const match = findUpdateSetNode(parsed, offset, text);
  if (!match) { return undefined; }
  const targetName = String(match?.target?.name ?? "").trim();
  if (!targetName) { return undefined; }
  const targetNorm = normalizeName(targetName);
  const scopeAtPos = parsed?.scope?.root?.findInnermost(match?.target?.start ?? offset);
  const sym = resolveSymbolCaseInsensitive(scopeAtPos, targetNorm);
  if (sym?.kind === "Alias") { return resolveAliasTableName(sym) ?? targetName; }
  if (sym?.kind === "Table" || sym?.kind === "TempTable" || sym?.kind === "CTE") { return String(sym.name ?? targetName); }
  return targetName;
}

export function getInsertColumnTargetTable(parsed: any, offset: number, text?: string): string | undefined {
  const match = findInsertColumnListNode(parsed, offset, text);
  if (!match) { return undefined; }
  return (typeof match.table?.name === "string" && match.table.name.trim().length > 0) ? match.table.name : undefined;
}

export function getInsertUsedColumnNames(parsed: any, offset: number, text?: string): Set<string> {
  const match = findInsertColumnListNode(parsed, offset, text);
  if (!match) { return new Set(); }
  const used = new Set<string>();
  for (const colNode of (match.columnNodes as any[])) {
    const name = extractColumnName(colNode);
    if (name) { used.add(name); }
  }
  return used;
}

export function getUpdateSetUsedColumnNames(parsed: any, offset: number, text?: string): Set<string> {
  const match = findUpdateSetNode(parsed, offset, text);
  if (!match) { return new Set(); }
  const used = new Set<string>();
  for (const assignment of (match.assignments as any[])) {
    const name = extractColumnName(assignment?.column ?? assignment);
    if (name) { used.add(name); }
  }
  return used;
}

export function isFunctionCallInAst(ast: any, functionNameNorm: string): { isFunc: boolean; rawName: string } {
  let isFunc = false;
  let rawName = functionNameNorm;
  const visit = (n: any) => {
    if (!n || typeof n !== "object" || isFunc) { return; }
    if (n.type === "FunctionCall" && normalizeName(n.name) === functionNameNorm) {
      isFunc = true;
      if (n.name) { rawName = n.name; }
      return;
    }
    if (Array.isArray(n)) { for (const item of n) { visit(item); } return; }
    for (const val of Object.values(n)) { if (val && typeof val === "object") { visit(val); } }
  };
  visit(ast);
  return { isFunc, rawName };
}

// ---------- Hover helpers ----------

export function getResolutionSourceColumns(resolution: any): Array<{ table: string; column: string }> {
  const out: Array<{ table: string; column: string }> = [];
  const seen = new Set<string>();
  const add = (tableRaw: unknown, columnRaw: unknown): void => {
    const table = normalizeName(String(tableRaw ?? ""));
    const column = normalizeName(String(columnRaw ?? "").split(".").pop() ?? "");
    if (!table || !column) { return; }
    const key = `${table}.${column}`;
    if (seen.has(key)) { return; }
    seen.add(key);
    out.push({ table, column });
  };
  for (const input of resolution?.inputs ?? []) {
    if (input?.kind === "column") { add(input.source, input.name); }
  }
  return out;
}

export function getHoverColumnLabel(column: any, tokenText?: string): string {
  const raw = String(column?.rawName ?? column?.name ?? "").trim();
  const token = String(tokenText ?? "").trim();
  if (token) {
    const tokenNorm = normalizeName(token.split(".").pop() ?? token);
    const rawNorm = normalizeName(raw);
    if (tokenNorm && rawNorm && tokenNorm === rawNorm) { return token.split(".").pop() ?? token; }
  }
  return raw || token || "column";
}

export function getSymbolLocalColumns(sym: any): any[] | undefined {
  if (!sym) { return undefined; }
  if (Array.isArray(sym.localColumns) && sym.localColumns.length > 0) {
    return sym.localColumns.map((c: any) => ({
      rawName: c?.rawName ?? c?.name ?? "",
      name: c?.normalizedName ?? normalizeName(String(c?.rawName ?? c?.name ?? "")),
      type: c?.dataType ?? c?.type ?? undefined,
      location: c?.location
    }));
  }
  if (Array.isArray(sym.columns) && sym.columns.length > 0) { return sym.columns; }
  return undefined;
}

export function getPropertyAccessAtOffset(parsed: ParseResult | null, offset: number): any | null {
  const accesses = parsed?.columns?.propertyAccesses;
  if (!Array.isArray(accesses) || accesses.length === 0) { return null; }
  return accesses.find((a: any) => {
    const s = Number(a?.location?.start);
    const e = Number(a?.location?.end);
    return Number.isFinite(s) && Number.isFinite(e) && offset >= s && offset <= e;
  }) ?? null;
}

export function getResolvedObjectKindLabel(key: string, def?: any, opts?: { titleCase?: boolean }): string {
  const norm = normalizeName(key);
  const stripped = norm.replace(/^dbo\./, "");
  const isType = tableTypesByName.has(norm) || tableTypesByName.has(stripped)
    || (def && normalizeName(String(def.kind ?? "")).includes("type"));
  if (opts?.titleCase) { return isType ? "Table Type" : "Table"; }
  return isType ? "table type" : "table";
}

export function findDerivedAliasProjectedColumnRange(
  doc: TextDocument,
  parsed: ParseResult | null,
  offset: number,
  aliasName: string,
  columnName: string
): Location[] | null {
  if (!parsed?.scope?.root) { return null; }
  const scopeAtPos = parsed.scope.root.findInnermost(offset);
  const aliasSym = resolveSymbolCaseInsensitive(scopeAtPos, aliasName);
  if (!aliasSym || aliasSym.kind !== "Alias" || !aliasSym?.location?.table?.query) { return null; }
  const queryCols = Array.isArray(aliasSym.location.table.query?.columns) ? aliasSym.location.table.query.columns : [];
  const target = normalizeName(columnName);
  if (!target || queryCols.length === 0) { return null; }
  for (const col of queryCols) {
    if (!col || col.wildcard === true) { continue; }
    const candidates: string[] = [];
    const pushCandidate = (value: any): void => { const norm = normalizeName(String(value ?? "")); if (norm) { candidates.push(norm); } };
    pushCandidate(col.alias); pushCandidate(col.outputName); pushCandidate(col.sourceName);
    const expr = col.expression;
    if (expr?.type === "Identifier") {
      if (Array.isArray(expr.parts) && expr.parts.length > 0) { pushCandidate(expr.parts[expr.parts.length - 1]); }
      else { pushCandidate(expr.name); }
    } else if (expr?.type === "MemberExpression") { pushCandidate(expr.property); }
    if (!candidates.includes(target)) { continue; }
    const start = Number(col.start ?? col.expression?.start);
    const end = Number(col.end ?? col.expression?.end);
    if (!Number.isFinite(start) || !Number.isFinite(end)) { continue; }
    return [{ uri: doc.uri, range: { start: doc.positionAt(start), end: doc.positionAt(end) } }];
  }
  return null;
}

export function findStatementLocalColumnOwner(
  statementText: string,
  columnName: string,
  scopeAtPos?: any,
  parsed?: ParseResult | null,
  offset?: number,
  localDefsByName?: Map<string, any>
): { kindLabel: string; ownerName: string; column: any } | null {
  if (typeof offset !== "number") {
    const colNorm = normalizeName(columnName);
    if (!colNorm) { return null; }
    const owners = collectNearestScopeColumnOwners(scopeAtPos, colNorm, tablesByName, tableTypesByName, localDefsByName);
    if (owners.length === 1) { return { kindLabel: owners[0].kindLabel, ownerName: owners[0].ownerName, column: owners[0].column }; }
    return null;
  }
  const resolved = resolveBareColumnAtOffset({ parsed, offset, columnName, tablesByName, tableTypesByName, scopeAtPos, localDefsByName });
  if (resolved.status === "resolved" && resolved.owner?.column) {
    return { kindLabel: resolved.owner.kindLabel, ownerName: resolved.owner.ownerName, column: resolved.owner.column };
  }
  return null;
}

export function isAmbiguousBareColumnAtPosition(
  doc: TextDocument,
  position: Position,
  parsed: ParseResult | null
): boolean {
  const range = getWordRangeAtPosition(doc, position);
  if (!range) { return false; }
  const rawWord = doc.getText(range).trim();
  if (!rawWord || rawWord.includes(".") || rawWord.startsWith("@")) { return false; }
  const colNorm = normalizeName(rawWord);
  if (!colNorm || isSqlKeyword(colNorm)) { return false; }
  const offset = doc.offsetAt(position);
  const normUri = toNormUri(doc.uri);
  const localDefsByName = new Map<string, any>();
  for (const def of definitions.get(normUri) ?? []) {
    localDefsByName.set(normalizeName(def.name), def);
  }
  const resolved = resolveBareColumnAtOffset({ parsed, offset, columnName: rawWord, tablesByName, tableTypesByName, localDefsByName });
  return resolved.status === "ambiguous";
}

// ---------- Reference / definition helpers ----------

export function getDefinitionReferenceKeys(name: string): string[] {
  const norm = normalizeName(name);
  const keys = new Set<string>([norm]);
  if (norm.startsWith("dbo.")) { keys.add(norm.slice("dbo.".length)); }
  else if (!norm.includes(".")) { keys.add(`dbo.${norm}`); }
  return [...keys];
}

export function isReferenceHiddenFromObjectUsage(ref: ReferenceDef): boolean {
  return ref.context === "create-definition" || ref.context === "alias-declaration";
}

export function getDefinitionReferenceLocations(
  def: { name: string; uri: string; line: number; rawName: string }
): Location[] {
  const locations: Location[] = [];
  const seen = new Set<string>();
  for (const key of getDefinitionReferenceKeys(def.name)) {
    const byUri = referencesIndex.get(key);
    if (!byUri) { continue; }
    for (const refs of byUri.values()) {
      for (const ref of refs) {
        if (isReferenceHiddenFromObjectUsage(ref)) { continue; }
        const locationKey = `${ref.uri}:${ref.line}:${ref.start}:${ref.end}`;
        if (seen.has(locationKey)) { continue; }
        seen.add(locationKey);
        locations.push({ uri: ref.uri, range: { start: { line: ref.line, character: ref.start }, end: { line: ref.line, character: ref.end } } });
      }
    }
  }
  return locations;
}

// Expose the unused Position and Range imports to avoid TS warnings
// (they may be needed by callers importing this module)
export type { Position, Range };

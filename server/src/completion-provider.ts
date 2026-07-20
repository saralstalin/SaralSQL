/**
 * Completion (IntelliSense) logic extracted from server.ts.
 * Testable without the LSP connection.
 */
import { CompletionItem, CompletionItemKind, Position } from "vscode-languageserver/node";
import { TextDocument } from "vscode-languageserver-textdocument";
import { normalizeName } from "./text-utils";
import { tablesByName, tableTypesByName } from "./definitions";
import { parseSql, type ParseResult } from "./sql-parser";
import { getCteColumns, resolveAliasTableName, resolveSymbolCaseInsensitive } from "./ast-utils";
import {
  getVariableCompletionItems, getUpdateSetTargetTable, getInsertColumnTargetTable,
  getInsertUsedColumnNames, getUpdateSetUsedColumnNames,
  getAliasBeforeDot, endsWithDotToken, isFromJoinTableContext, isInSelectProjectionContext,
  getStatementTableCandidatesFromAst, getParserAliasColumnNames, resolveAliasTableFromStatementAst
} from "./sql-helpers";

/** Re-parse with a placeholder token appended at offset, to improve scope resolution. */
function getCompletionParsedDocument(
  text: string,
  offset: number
): { parsed: ParseResult; offset: number } | null {
  const patchedText = `${text.slice(0, offset)}__X__${text.slice(offset)}`;
  const parsed = parseSql(patchedText);
  if (!parsed?.ast) { return null; }
  return { parsed, offset: offset + 5 };
}

function colItem(col: any, ownerName: string): CompletionItem {
  return {
    label: col.rawName,
    kind: CompletionItemKind.Field,
    detail: col.type ? `Column in ${ownerName} (${col.type})` : `Column in ${ownerName}`,
    insertText: col.rawName
  };
}

export function computeCompletion(
  doc: TextDocument,
  pos: Position,
  parsed: ParseResult | null
): CompletionItem[] {
  const text = doc.getText();
  const offset = doc.offsetAt(pos);
  const linePrefix = doc.getText({
    start: { line: pos.line, character: 0 },
    end: { line: pos.line, character: pos.character }
  });

  const items: CompletionItem[] = [];
  const scopeAtPos = parsed?.scope?.root?.findInnermost(offset);
  const variableItems = getVariableCompletionItems(scopeAtPos);

  // ── UPDATE SET columns ─────────────────────────────────────────────────────
  const updateSetTarget = getUpdateSetTargetTable(parsed, offset, text);
  if (updateSetTarget && !endsWithDotToken(linePrefix)) {
    const targetNorm = normalizeName(updateSetTarget);
    const def = tablesByName.get(targetNorm) || tableTypesByName.get(targetNorm);
    if (def?.columns?.length) {
      const usedCols = getUpdateSetUsedColumnNames(parsed, offset, text);
      const cols = def.columns.filter((c: any) => !usedCols.has(normalizeName(c.rawName)));
      items.push(...cols.map((c: any) => colItem(c, def.rawName)));
      items.push(...variableItems);
      return items;
    }
  }

  // ── INSERT column-list columns ─────────────────────────────────────────────
  const insertColumnTarget = getInsertColumnTargetTable(parsed, offset, text);
  if (insertColumnTarget) {
    const targetNorm = normalizeName(insertColumnTarget);
    const def = tablesByName.get(targetNorm) || tableTypesByName.get(targetNorm);
    if (def?.columns?.length) {
      const usedCols = getInsertUsedColumnNames(parsed, offset, text);
      return def.columns
        .filter((c: any) => !usedCols.has(normalizeName(c.rawName)))
        .map((c: any) => colItem(c, def.rawName));
    }
  }

  // ── Alias-dot completions (e.g. "t.") ─────────────────────────────────────
  const aliasRaw = getAliasBeforeDot(linePrefix);
  if (aliasRaw) {
    const aliasNorm = normalizeName(aliasRaw);

    if (scopeAtPos) {
      const sym = resolveSymbolCaseInsensitive(scopeAtPos, aliasNorm);
      if (sym) {
        let targetTable = aliasNorm;
        if (sym.kind === "Alias") {
          targetTable = normalizeName(
            sym.metadata?.tableName as string ||
            (sym.location as any).table?.name ||
            (sym.location as any).table ||
            aliasNorm
          );
        }
        let resolved = tablesByName.get(targetTable) || tableTypesByName.get(targetTable);
        if (!resolved && sym.dataType) {
          const typeKey = normalizeName(sym.dataType);
          resolved = tableTypesByName.get(typeKey) || tablesByName.get(typeKey);
        }
        if (resolved?.columns) {
          items.push(...resolved.columns.map((c: any) => colItem(c, resolved.rawName)));
        } else if (sym.kind === "Alias") {
          const derivedColumns = getParserAliasColumnNames(parsed, sym);
          for (const colName of derivedColumns) {
            items.push({ label: colName, kind: CompletionItemKind.Field, detail: `Column in derived table ${sym.name}`, insertText: colName });
          }
          const aliasTarget = normalizeName(resolveAliasTableName(sym) ?? "");
          const cteSym = aliasTarget ? resolveSymbolCaseInsensitive(scopeAtPos, aliasTarget) : null;
          if (cteSym?.kind === "CTE") {
            for (const cteCol of getCteColumns(cteSym)) {
              items.push({ label: cteCol.rawName, kind: CompletionItemKind.Field, detail: `Column in CTE ${cteSym.name}`, insertText: cteCol.rawName });
            }
          }
        } else if (sym.columns && Array.isArray(sym.columns)) {
          for (const col of sym.columns) {
            const name = typeof col === "string" ? col : (col.name || col.rawName);
            items.push({ label: name, kind: CompletionItemKind.Field, detail: `Column in ${sym.name}`, insertText: name });
          }
        }
      }
    }

    if (items.length === 0) {
      // Re-parse with placeholder for better scope resolution at typing position
      const completionCtx = getCompletionParsedDocument(text, offset);
      const completionParsed = completionCtx?.parsed ?? parsed;
      const completionOffset = completionCtx?.offset ?? offset;
      const completionScope = completionParsed?.scope?.root?.findInnermost?.(completionOffset) ?? completionParsed?.scope?.root;
      const completionSym = completionScope ? resolveSymbolCaseInsensitive(completionScope, aliasNorm) : null;

      if (completionSym?.kind === "Alias") {
        let targetTable = normalizeName(
          completionSym.metadata?.tableName as string ||
          (completionSym.location as any).table?.name ||
          (completionSym.location as any).table ||
          aliasNorm
        );
        if (!targetTable) { targetTable = normalizeName(resolveAliasTableName(completionSym) ?? ""); }
        let resolved = tablesByName.get(targetTable) || tableTypesByName.get(targetTable);
        if (!resolved && completionSym.dataType) {
          const typeKey = normalizeName(completionSym.dataType);
          resolved = tableTypesByName.get(typeKey) || tablesByName.get(typeKey);
        }
        if (resolved?.columns) {
          items.push(...resolved.columns.map((c: any) => colItem(c, resolved.rawName)));
        } else {
          const derivedColumns = getParserAliasColumnNames(completionParsed, completionSym);
          for (const colName of derivedColumns) {
            items.push({ label: colName, kind: CompletionItemKind.Field, detail: `Column in derived table ${completionSym.name}`, insertText: colName });
          }
        }
      }

      if (items.length === 0) {
        const tableFromAst = resolveAliasTableFromStatementAst(completionParsed, completionOffset, aliasNorm);
        if (tableFromAst) {
          const def = tablesByName.get(tableFromAst) || tableTypesByName.get(tableFromAst);
          if (def?.columns) {
            items.push(...def.columns.map((c: any) => colItem(c, def.rawName)));
          }
        }
      }
    }
    return items;
  }

  // ── FROM / JOIN context → table names ─────────────────────────────────────
  if (isFromJoinTableContext(linePrefix)) {
    for (const def of tablesByName.values()) {
      items.push({ label: def.rawName, kind: CompletionItemKind.Class, detail: `Table defined in ${def.uri.split("/").pop()}:${def.line + 1}` });
    }
    return items;
  }

  // ── SELECT projection → visible scope columns ──────────────────────────────
  if (isInSelectProjectionContext(parsed, offset, linePrefix)) {
    const visibleTables = new Set<string>();
    const visibleCtes = new Map<string, Array<{ rawName: string }>>();
    if (scopeAtPos) {
      for (const sym of scopeAtPos.getVisibleSymbols()) {
        if (sym.kind === "Table" || sym.kind === "TempTable" || sym.kind === "CTE") {
          visibleTables.add(normalizeName(sym.name));
          if (sym.kind === "CTE") { visibleCtes.set(normalizeName(sym.name), getCteColumns(sym).map((c: any) => ({ rawName: c.rawName }))); }
        } else if (sym.kind === "Alias") {
          const tblName = resolveAliasTableName(sym);
          if (tblName) {
            const tblNorm = normalizeName(tblName);
            visibleTables.add(tblNorm);
            const cteSym = resolveSymbolCaseInsensitive(scopeAtPos, tblNorm);
            if (cteSym?.kind === "CTE") { visibleCtes.set(tblNorm, getCteColumns(cteSym).map((c: any) => ({ rawName: c.rawName }))); }
          }
        }
      }
    }
    if (visibleTables.size === 0) {
      for (const t of getStatementTableCandidatesFromAst(parsed, offset)) { visibleTables.add(t); }
    }

    let colCount = 0;
    const COL_LIMIT = 500;
    if (visibleTables.size > 0) {
      for (const tbl of visibleTables) {
        const def = tablesByName.get(tbl) || tableTypesByName.get(tbl);
        if (def?.columns) { for (const col of def.columns) { if (colCount++ > COL_LIMIT) { break; } items.push(colItem(col, def.rawName)); } }
        else { const cteCols = visibleCtes.get(tbl); if (cteCols) { for (const col of cteCols) { if (colCount++ > COL_LIMIT) { break; } items.push({ label: col.rawName, kind: CompletionItemKind.Field, detail: `Column in CTE ${tbl}`, insertText: col.rawName }); } } }
      }
    } else {
      for (const def of tablesByName.values()) {
        if (def.columns) { for (const col of def.columns) { if (colCount++ > COL_LIMIT) { break; } items.push(colItem(col, def.rawName)); } }
        if (colCount > COL_LIMIT) { break; }
      }
    }
    for (const def of tablesByName.values()) { items.push({ label: def.rawName, kind: CompletionItemKind.Class, detail: `Table defined in ${def.uri.split("/").pop()}:${def.line + 1}` }); }
    items.push(...variableItems);
    return items;
  }

  // ── Fallback: scope columns ────────────────────────────────────────────────
  if (scopeAtPos) {
    const visibleTables = new Set<string>();
    const visibleCtes = new Map<string, Array<{ rawName: string }>>();
    for (const sym of scopeAtPos.getVisibleSymbols?.() ?? []) {
      if (sym.kind === "Table" || sym.kind === "TempTable" || sym.kind === "CTE") {
        const name = normalizeName(String(sym.name ?? ""));
        if (name) { visibleTables.add(name); if (sym.kind === "CTE") { visibleCtes.set(name, getCteColumns(sym).map((c: any) => ({ rawName: c.rawName }))); } }
      } else if (sym.kind === "Alias") {
        const tblName = resolveAliasTableName(sym);
        if (tblName) {
          const tblNorm = normalizeName(tblName);
          visibleTables.add(tblNorm);
          const cteSym = resolveSymbolCaseInsensitive(scopeAtPos, tblNorm);
          if (cteSym?.kind === "CTE") { visibleCtes.set(tblNorm, getCteColumns(cteSym).map((c: any) => ({ rawName: c.rawName }))); }
        }
      }
    }
    for (const t of getStatementTableCandidatesFromAst(parsed, offset)) { visibleTables.add(t); }
    for (const tbl of visibleTables) {
      const def = tablesByName.get(tbl) || tableTypesByName.get(tbl);
      if (def?.columns) { for (const col of def.columns) { items.push(colItem(col, def.rawName)); } }
      else { const cteCols = visibleCtes.get(tbl); if (cteCols) { for (const col of cteCols) { items.push({ label: col.rawName, kind: CompletionItemKind.Field, detail: `Column in CTE ${tbl}`, insertText: col.rawName }); } } }
    }
    if (items.length > 0) { items.push(...variableItems); return items; }
  }

  // ── Absolute fallback: variables + tables + keywords ──────────────────────
  items.push(...variableItems);
  for (const def of tablesByName.values()) {
    items.push({ label: def.rawName, kind: CompletionItemKind.Class, detail: `Table defined in ${def.uri.split("/").pop()}:${def.line + 1}` });
  }
  const keywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "JOIN", "WHERE", "GROUP BY", "ORDER BY", "FROM", "AS"];
  for (const kw of keywords) { items.push({ label: kw, kind: CompletionItemKind.Keyword }); }
  return items;
}

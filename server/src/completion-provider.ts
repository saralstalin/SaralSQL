import type { CompletionItem, CompletionParams } from "vscode-languageserver/node";
import { CompletionItemKind } from "vscode-languageserver/node";
import { normalizeName } from "./text-utils";
import type { CompletionProviderDeps } from "./provider-types";

function getVariableCompletionItems(scopeAtPos: any): CompletionItem[] {
  if (!scopeAtPos || typeof scopeAtPos.getVisibleSymbols !== "function") {
    return [];
  }

  const items: CompletionItem[] = [];
  const seen = new Set<string>();

  for (const sym of scopeAtPos.getVisibleSymbols()) {
    if (sym.kind !== "Parameter" && sym.kind !== "Variable") {
      continue;
    }

    const name = String(sym.name ?? "");
    if (!name || seen.has(name.toLowerCase())) {
      continue;
    }

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

export function onCompletionProvider(params: CompletionParams, deps: CompletionProviderDeps): CompletionItem[] {
  const rawUri = params.textDocument.uri;
  const normUri = deps.toNormUri(rawUri);

  const doc = deps.getDocument(rawUri, normUri);
  if (!doc) {
    deps.safeLog(`[completion] no document found for ${rawUri}`);
    return [];
  }

  const position = params.position;
  const offset = doc.offsetAt(position);
  const linePrefix = doc.getText({
    start: { line: position.line, character: 0 },
    end: { line: position.line, character: position.character }
  });

  const items: CompletionItem[] = [];
  const parsed = deps.getParsedDocument(doc);
  const scopeAtPos = parsed?.scope?.root?.findInnermost(offset);
  const variableItems = getVariableCompletionItems(scopeAtPos);
  const updateSetTarget = deps.getUpdateSetTargetTable(parsed, offset);
  const insertColumnTarget = deps.getInsertColumnTargetTable(parsed, offset);

  if (updateSetTarget && !deps.endsWithDotToken(linePrefix)) {
    const targetNorm = normalizeName(updateSetTarget);
    const def = deps.tablesByName.get(targetNorm) || deps.tableTypesByName.get(targetNorm);
    if (def?.columns?.length) {
      for (const col of def.columns) {
        items.push({
          label: col.rawName,
          kind: CompletionItemKind.Field,
          detail: col.type ? `Column in ${def.rawName} (${col.type})` : `Column in ${def.rawName}`,
          insertText: col.rawName
        });
      }
      items.push(...variableItems);
      return items;
    }
  }

  if (insertColumnTarget) {
    const targetNorm = normalizeName(insertColumnTarget);
    const def = deps.tablesByName.get(targetNorm) || deps.tableTypesByName.get(targetNorm);
    if (def?.columns?.length) {
      for (const col of def.columns) {
        items.push({
          label: col.rawName,
          kind: CompletionItemKind.Field,
          detail: col.type ? `Column in ${def.rawName} (${col.type})` : `Column in ${def.rawName}`,
          insertText: col.rawName
        });
      }
      return items;
    }
  }

  const aliasRaw = deps.getAliasBeforeDot(linePrefix);
  if (aliasRaw) {
    const aliasNorm = normalizeName(aliasRaw);

    if (scopeAtPos) {
      const sym = deps.resolveSymbolCaseInsensitive(scopeAtPos, aliasNorm);
      if (sym) {
        let targetTable = aliasNorm;
        if (sym.kind === "Alias") {
          targetTable = normalizeName(sym.metadata?.tableName as string || (sym.location as any).table?.name || (sym.location as any).table || aliasNorm);
        }

        let resolved = deps.tablesByName.get(targetTable) || deps.tableTypesByName.get(targetTable);
        if (!resolved && sym.dataType) {
          const typeKey = normalizeName(sym.dataType);
          resolved = deps.tableTypesByName.get(typeKey) || deps.tablesByName.get(typeKey);
        }

        if (resolved && resolved.columns) {
          for (const col of resolved.columns) {
            items.push({
              label: col.rawName,
              kind: CompletionItemKind.Field,
              detail: col.type ? `Column in ${resolved.rawName} (${col.type})` : `Column in ${resolved.rawName}`,
              insertText: col.rawName
            });
          }
        } else if (sym.kind === "Alias") {
          const derivedColumns = deps.getParserAliasColumnNames(parsed, sym);
          if (derivedColumns.length > 0) {
            for (const colName of derivedColumns) {
              items.push({
                label: colName,
                kind: CompletionItemKind.Field,
                detail: `Column in derived table ${sym.name}`,
                insertText: colName
              });
            }
          }
        } else if (sym.kind === "Alias") {
          const aliasTarget = normalizeName(deps.resolveAliasTableName(sym) ?? "");
          const cteSym = aliasTarget ? deps.resolveSymbolCaseInsensitive(scopeAtPos, aliasTarget) : null;
          if (cteSym?.kind === "CTE") {
            for (const cteCol of deps.getCteColumns(cteSym)) {
              items.push({
                label: cteCol.rawName,
                kind: CompletionItemKind.Field,
                detail: `Column in CTE ${cteSym.name}`,
                insertText: cteCol.rawName
              });
            }
          }
        } else if (sym.columns && Array.isArray(sym.columns)) {
          for (const col of sym.columns) {
            const name = typeof col === "string" ? col : (col.name || col.rawName);
            items.push({
              label: name,
              kind: CompletionItemKind.Field,
              detail: `Column in ${sym.name}`,
              insertText: name
            });
          }
        }
      }
    }
    if (items.length === 0) {
      const completionParsedCtx = deps.getCompletionParsedDocument(doc, offset);
      const completionParsed = completionParsedCtx?.parsed ?? parsed;
      const completionOffset = completionParsedCtx?.offset ?? offset;
      const completionScopeAtPos = completionParsed?.scope?.root?.findInnermost?.(completionOffset) ?? completionParsed?.scope?.root;
      const completionSym = completionScopeAtPos ? deps.resolveSymbolCaseInsensitive(completionScopeAtPos, aliasNorm) : null;
      if (completionSym?.kind === "Alias") {
        let targetTable = normalizeName(completionSym.metadata?.tableName as string || (completionSym.location as any).table?.name || (completionSym.location as any).table || aliasNorm);
        if (!targetTable) {
          targetTable = normalizeName(deps.resolveAliasTableName(completionSym) ?? "");
        }
        let resolved = deps.tablesByName.get(targetTable) || deps.tableTypesByName.get(targetTable);
        if (!resolved && completionSym.dataType) {
          const typeKey = normalizeName(completionSym.dataType);
          resolved = deps.tableTypesByName.get(typeKey) || deps.tablesByName.get(typeKey);
        }
        if (resolved?.columns) {
          for (const col of resolved.columns) {
            items.push({
              label: col.rawName,
              kind: CompletionItemKind.Field,
              detail: col.type ? `Column in ${resolved.rawName} (${col.type})` : `Column in ${resolved.rawName}`,
              insertText: col.rawName
            });
          }
        } else {
          const derivedColumns = deps.getParserAliasColumnNames(completionParsed, completionSym);
          if (derivedColumns.length > 0) {
            for (const colName of derivedColumns) {
              items.push({
                label: colName,
                kind: CompletionItemKind.Field,
                detail: `Column in derived table ${completionSym.name}`,
                insertText: colName
              });
            }
          }
        }
      }

      if (items.length === 0) {
        const tableFromAst = deps.resolveAliasTableFromStatementAst(completionParsed, completionOffset, aliasNorm);
        if (tableFromAst) {
          const def = deps.tablesByName.get(tableFromAst) || deps.tableTypesByName.get(tableFromAst);
          if (def?.columns) {
            for (const col of def.columns) {
              items.push({
                label: col.rawName,
                kind: CompletionItemKind.Field,
                detail: col.type ? `Column in ${def.rawName} (${col.type})` : `Column in ${def.rawName}`,
                insertText: col.rawName
              });
            }
          }
        }
      }
    }
    return items;
  }

  if (deps.isFromJoinTableContext(linePrefix)) {
    for (const def of deps.tablesByName.values()) {
      items.push({
        label: def.rawName,
        kind: CompletionItemKind.Class,
        detail: `Table defined in ${def.uri.split("/").pop()}:${def.line + 1}`
      });
    }
    return items;
  }

  if (deps.isInSelectProjectionContext(parsed, offset, linePrefix)) {
    const visibleTables = new Set<string>();
    const visibleCtes = new Map<string, Array<{ rawName: string }>>();
    if (scopeAtPos) {
      const visibleSymbols = scopeAtPos.getVisibleSymbols();
      for (const sym of visibleSymbols) {
        if (sym.kind === "Table" || sym.kind === "TempTable" || sym.kind === "CTE") {
          visibleTables.add(normalizeName(sym.name));
          if (sym.kind === "CTE") {
            visibleCtes.set(normalizeName(sym.name), deps.getCteColumns(sym).map(c => ({ rawName: c.rawName })));
          }
        } else if (sym.kind === "Alias") {
          const tblName = deps.resolveAliasTableName(sym);
          if (tblName) {
            const tblNorm = normalizeName(tblName);
            visibleTables.add(tblNorm);
            const cteSym = deps.resolveSymbolCaseInsensitive(scopeAtPos, tblNorm);
            if (cteSym?.kind === "CTE") {
              visibleCtes.set(tblNorm, deps.getCteColumns(cteSym).map(c => ({ rawName: c.rawName })));
            }
          }
        }
      }
    }

    if (visibleTables.size === 0) {
      for (const t of deps.getStatementTableCandidatesFromAst(parsed, offset)) {
        visibleTables.add(t);
      }
    }

    let colCount = 0;
    const COL_LIMIT = 500;

    if (visibleTables.size > 0) {
      for (const tbl of visibleTables) {
        const def = deps.tablesByName.get(tbl) || deps.tableTypesByName.get(tbl);
        if (def && def.columns) {
          for (const col of def.columns) {
            if (colCount++ > COL_LIMIT) { break; }
            items.push({
              label: col.rawName,
              kind: CompletionItemKind.Field,
              detail: col.type ? `Column in ${def.rawName} (${col.type})` : `Column in ${def.rawName}`,
              insertText: col.rawName
            });
          }
        } else {
          const cteCols = visibleCtes.get(tbl);
          if (cteCols) {
            for (const col of cteCols) {
              if (colCount++ > COL_LIMIT) { break; }
              items.push({
                label: col.rawName,
                kind: CompletionItemKind.Field,
                detail: `Column in CTE ${tbl}`,
                insertText: col.rawName
              });
            }
          }
        }
      }
    } else {
      for (const def of deps.tablesByName.values()) {
        if (def.columns) {
          for (const col of def.columns) {
            if (colCount++ > COL_LIMIT) { break; }
            items.push({
              label: col.rawName,
              kind: CompletionItemKind.Field,
              detail: col.type ? `Column in ${def.rawName} (${col.type})` : `Column in ${def.rawName}`,
              insertText: col.rawName
            });
          }
        }
        if (colCount > COL_LIMIT) { break; }
      }
    }

    for (const def of deps.tablesByName.values()) {
      items.push({
        label: def.rawName,
        kind: CompletionItemKind.Class,
        detail: `Table defined in ${def.uri.split("/").pop()}:${def.line + 1}`
      });
    }
    items.push(...variableItems);
    return items;
  }

  if (scopeAtPos) {
    const visibleTables = new Set<string>();
    const visibleCtes = new Map<string, Array<{ rawName: string }>>();
    const visibleSymbols = scopeAtPos.getVisibleSymbols?.() ?? [];
    for (const sym of visibleSymbols) {
      if (sym.kind === "Table" || sym.kind === "TempTable" || sym.kind === "CTE") {
        const name = normalizeName(String(sym.name ?? ""));
        if (name) { visibleTables.add(name); }
        if (sym.kind === "CTE") {
          visibleCtes.set(name, deps.getCteColumns(sym).map(c => ({ rawName: c.rawName })));
        }
      } else if (sym.kind === "Alias") {
        const tblName = deps.resolveAliasTableName(sym);
        if (tblName) {
          const tblNorm = normalizeName(tblName);
          visibleTables.add(tblNorm);
          const cteSym = deps.resolveSymbolCaseInsensitive(scopeAtPos, tblNorm);
          if (cteSym?.kind === "CTE") {
            visibleCtes.set(tblNorm, deps.getCteColumns(cteSym).map(c => ({ rawName: c.rawName })));
          }
        }
      }
    }

    for (const t of deps.getStatementTableCandidatesFromAst(parsed, offset)) {
      visibleTables.add(t);
    }

    for (const tbl of visibleTables) {
      const def = deps.tablesByName.get(tbl) || deps.tableTypesByName.get(tbl);
      if (def?.columns) {
        for (const col of def.columns) {
          items.push({
            label: col.rawName,
            kind: CompletionItemKind.Field,
            detail: col.type ? `Column in ${def.rawName} (${col.type})` : `Column in ${def.rawName}`,
            insertText: col.rawName
          });
        }
      } else {
        const cteCols = visibleCtes.get(tbl);
        if (cteCols) {
          for (const col of cteCols) {
            items.push({
              label: col.rawName,
              kind: CompletionItemKind.Field,
              detail: `Column in CTE ${tbl}`,
              insertText: col.rawName
            });
          }
        }
      }
    }

    if (items.length > 0) {
      items.push(...variableItems);
      return items;
    }
  }

  items.push(...variableItems);

  for (const def of deps.tablesByName.values()) {
    items.push({
      label: def.rawName,
      kind: CompletionItemKind.Class,
      detail: `Table defined in ${def.uri.split("/").pop()}:${def.line + 1}`
    });
  }

  const keywords = [
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
    "JOIN", "WHERE", "GROUP BY", "ORDER BY", "FROM", "AS"
  ];
  for (const kw of keywords) { items.push({ label: kw, kind: CompletionItemKind.Keyword }); }

  return items;
}

import type { Hover, Position } from "vscode-languageserver/node";
import { MarkupKind } from "vscode-languageserver/node";
import type { TextDocument } from "vscode-languageserver-textdocument";
import { normalizeName } from "./text-utils";
import type { HoverProviderDeps } from "./provider-types";

export async function doHoverProvider(
  doc: TextDocument,
  pos: Position,
  deps: HoverProviderDeps
): Promise<Hover | null> {
  try {
    const offset = doc.offsetAt(pos);
    const normUri = deps.toNormUri(doc.uri);

    const range = deps.getWordRangeAtPosition(doc, pos);
    if (!range) { return null; }

    const wordRangeText = doc.getText(range);

    const match = deps.findReferenceAtPosition(normUri, pos.line, pos.character);
    const parsed = deps.getParsedDocument(doc);
    const propertyAccess = deps.getPropertyAccessAtOffset(parsed, offset);
    if (propertyAccess) {
      const member = String(propertyAccess.member ?? "");
      const baseExpr = String(propertyAccess.baseExpr ?? "");
      const owner = String(propertyAccess.owner ?? "");
      const dataType = String(propertyAccess.dataType ?? "");
      const memberType = String(propertyAccess.memberType ?? "");
      const typePart = memberType ? ` - ${memberType}` : "";
      const ownerPart = owner ? `\n\nDefined on **${dataType || "typed"}** column \`${baseExpr}\` in \`${owner}\`` : "";
      const value = `**Member** \`${member}\`${typePart}${ownerPart}`;
      return { contents: { kind: MarkupKind.Markdown, value }, range };
    }
    const localDefs = deps.definitions.get(normUri) ?? [];
    const localDefsByName = new Map<string, any>();
    for (const def of localDefs) {
      localDefsByName.set(normalizeName(def.name), def);
    }

    if (!match) {
      const word = normalizeName(wordRangeText);
      const updateSetTarget = deps.getUpdateSetTargetTable(parsed, offset);
      if (updateSetTarget) {
        const targetNorm = normalizeName(updateSetTarget);
        const targetStripped = targetNorm.replace(/^dbo\./, "");
        const colDef = (localDefsByName.get(targetNorm) || localDefsByName.get(targetStripped) || deps.tablesByName.get(targetNorm) || deps.tableTypesByName.get(targetNorm))
          ?.columns
          ?.find((c: any) => normalizeName(c.name) === word);

        if (colDef) {
          const owner = localDefsByName.get(targetNorm) || localDefsByName.get(targetNorm.replace(/^dbo\./, "")) || deps.tablesByName.get(targetNorm) || deps.tableTypesByName.get(targetNorm);
          const kindLabel = deps.getResolvedObjectKindLabel(targetNorm, owner);
          const typePart = colDef.type ? ` - ${colDef.type}` : "";
          const value = `**Column** \`${colDef.rawName}\`${typePart}\n\nDefined in **${kindLabel}** \`${owner?.rawName ?? targetNorm}\``;
          return { contents: { kind: MarkupKind.Markdown, value }, range };
        }
      }

      const hoverScope = parsed?.scope?.root?.findInnermost(offset);
      const stmtOwner = deps.findStatementLocalColumnOwner(deps.getCurrentStatement(doc, pos), word, hoverScope, parsed, offset, localDefsByName);
      if (stmtOwner) {
        const typePart = stmtOwner.column.type ? ` - ${stmtOwner.column.type}` : "";
        const displayCol = deps.getHoverColumnLabel(stmtOwner.column, wordRangeText);
        const value = `**Column** \`${displayCol}\`${typePart}\n\nDefined in **${stmtOwner.kindLabel}** \`${stmtOwner.ownerName}\``;
        return { contents: { kind: MarkupKind.Markdown, value }, range };
      }
      return null;
    }

    if (match.kind === "parameter") {
      let dataType = "unknown";
      let columns: any[] | undefined = undefined;
      let isTableVariable = false;
      let paramDisplay = match.name;

      if (parsed?.scope?.root) {
        const scopeAtPos = parsed.scope.root.findInnermost(offset);
        const sym = deps.resolveSymbolCaseInsensitive(scopeAtPos, match.name) ?? deps.resolveSymbolCaseInsensitive(scopeAtPos, wordRangeText);
        if (sym) {
          paramDisplay = sym.name ?? paramDisplay;
          if (sym.dataType) {
            dataType = sym.dataType;
            const typeKey = normalizeName(dataType);
            const typeDef = deps.tableTypesByName.get(typeKey) || deps.tablesByName.get(typeKey);
            if (typeDef && typeDef.columns) {
              columns = typeDef.columns;
              isTableVariable = true;
            }
          }
          const localCols = deps.getSymbolLocalColumns(sym);
          if (localCols && localCols.length > 0) {
            columns = localCols;
            isTableVariable = true;
          }
        }
      }

      if (isTableVariable && columns) {
        const rows = columns.map(c => `- \`${c.rawName ?? c.name}\`${c.type ? ` ${c.type}` : ""}`);
        const body = `**Table Variable** \`${paramDisplay}\` - \`${dataType}\`\n\n${rows.join("\n")}`;
        return { contents: { kind: MarkupKind.Markdown, value: body }, range };
      }

      const value = `**Parameter** \`${paramDisplay}\` - \`${dataType}\``;
      return { contents: { kind: MarkupKind.Markdown, value }, range };
    }

    if (match.kind === "table") {
      const norm = normalizeName(match.name);
      if (norm.startsWith("@")) {
        let dataType = "unknown";
        let columns: any[] | undefined = undefined;
        let isTableVariable = false;
        let paramDisplay = match.name;

        if (parsed?.scope?.root) {
          const scopeAtPos = parsed.scope.root.findInnermost(offset);
          const sym = deps.resolveSymbolCaseInsensitive(scopeAtPos, match.name) ?? deps.resolveSymbolCaseInsensitive(scopeAtPos, wordRangeText);
          if (sym) {
            paramDisplay = sym.name ?? paramDisplay;
            if (sym.dataType) {
              dataType = sym.dataType;
              const typeKey = normalizeName(dataType);
              const typeDef = deps.tableTypesByName.get(typeKey) || deps.tablesByName.get(typeKey);
              if (typeDef && typeDef.columns) {
                columns = typeDef.columns;
                isTableVariable = true;
              }
            }
            const localCols = deps.getSymbolLocalColumns(sym);
            if (localCols && localCols.length > 0) {
              columns = localCols;
              isTableVariable = true;
            }
          }
        }

        if (isTableVariable && columns) {
          const rows = columns.map(c => `- \`${c.rawName ?? c.name}\`${c.type ? ` ${c.type}` : ""}`);
          const body = `**Table Variable** \`${paramDisplay}\` - \`${dataType}\`\n\n${rows.join("\n")}`;
          return { contents: { kind: MarkupKind.Markdown, value: body }, range };
        }

        const value = `**Parameter** \`${paramDisplay}\` - \`${dataType}\``;
        return { contents: { kind: MarkupKind.Markdown, value }, range };
      }
      const strippedNorm = norm.replace(/^dbo\./, "");
      const def = localDefsByName.get(norm) || localDefsByName.get(strippedNorm) || deps.tablesByName.get(norm) || deps.tableTypesByName.get(norm);
      if (def && def.columns) {
        const rows = def.columns.map((c: any) => `- \`${c.rawName}\`${c.type ? ` ${c.type}` : ""}`);
        const kindLabel = deps.getResolvedObjectKindLabel(norm, def, { titleCase: true });
        const body = `**${kindLabel}** \`${def.rawName}\`\n\n${rows.join("\n")}`;
        return { contents: { kind: MarkupKind.Markdown, value: body }, range };
      }

      if (parsed?.scope?.root) {
        const scopeAtPos = parsed.scope.root.findInnermost(offset);
        const cteSym = deps.resolveSymbolCaseInsensitive(scopeAtPos, norm);
        if (cteSym?.kind === "CTE") {
          const cteCols = deps.getCteColumns(cteSym);
          if (cteCols.length > 0) {
            const rows = cteCols.map(c => `- \`${c.rawName}\``);
            const body = `**CTE** \`${deps.getDisplaySymbolName(cteSym)}\`\n\n${rows.join("\n")}`;
            return { contents: { kind: MarkupKind.Markdown, value: body }, range };
          }
        }
      }

      if (parsed?.scope?.root) {
        const scopeAtPos = parsed.scope.root.findInnermost(offset);
        const aliasSym = deps.resolveSymbolCaseInsensitive(scopeAtPos, norm);
        if (aliasSym?.kind === "Alias") {
          const aliasTableNorm = normalizeName(deps.resolveAliasTableName(aliasSym) ?? "");
          const aliasTableDef = localDefsByName.get(aliasTableNorm) || localDefsByName.get(aliasTableNorm.replace(/^dbo\./, "")) || deps.tablesByName.get(aliasTableNorm) || deps.tableTypesByName.get(aliasTableNorm);
          if (aliasTableDef?.columns) {
            const rows = aliasTableDef.columns.map((c: any) => `- \`${c.rawName}\`${c.type ? ` ${c.type}` : ""}`);
            const kindLabel = deps.getResolvedObjectKindLabel(aliasTableNorm, aliasTableDef, { titleCase: true });
            const body = `**Alias** \`${deps.getDisplaySymbolName(aliasSym)}\`\n\nResolves to **${kindLabel}** \`${aliasTableDef.rawName}\`\n\n${rows.join("\n")}`;
            return { contents: { kind: MarkupKind.Markdown, value: body }, range };
          } else if (parsed?.ast) {
            const { isFunc, rawName } = deps.isFunctionCallInAst(parsed.ast, aliasTableNorm);
            if (isFunc) {
              const body = `**Alias** \`${deps.getDisplaySymbolName(aliasSym)}\`\n\nResolves to **table-valued function** \`${rawName}\``;
              return { contents: { kind: MarkupKind.Markdown, value: body }, range };
            }
          }
        }
      }

      if (!localDefsByName.has(norm) && !localDefsByName.has(norm.replace(/^dbo\./, "")) && !deps.tablesByName.has(norm) && !deps.tableTypesByName.has(norm) && parsed?.ast) {
        const { isFunc, rawName } = deps.isFunctionCallInAst(parsed.ast, norm);
        if (isFunc) {
          const body = `**Table-valued function** \`${rawName}\``;
          return { contents: { kind: MarkupKind.Markdown, value: body }, range };
        }
      }
    }

    if (match.kind === "column") {
      const parts = match.name.split(".");
      if (parts.length === 1) {
        const hoverScope = parsed?.scope?.root?.findInnermost(offset);
        const stmtOwner = deps.findStatementLocalColumnOwner(deps.getCurrentStatement(doc, pos), parts[0], hoverScope, parsed, offset, localDefsByName);
        if (stmtOwner) {
          const typePart = stmtOwner.column.type ? ` - ${stmtOwner.column.type}` : "";
          const displayCol = deps.getHoverColumnLabel(stmtOwner.column, wordRangeText);
          const value = `**Column** \`${displayCol}\`${typePart}\n\nDefined in **${stmtOwner.kindLabel}** \`${stmtOwner.ownerName}\``;
          return { contents: { kind: MarkupKind.Markdown, value }, range };
        }
      }

      if (parts.length === 2) {
        const tableName = parts[0];
        const colName = parts[1];

        let colDef: any = null;
        let containerName = tableName;
        let isType = false;
        let isCte = false;
        let aliasToken: string | undefined = undefined;

        if (tableName.startsWith("@")) {
          if (parsed?.scope?.root) {
            const scopeAtPos = parsed.scope.root.findInnermost(offset);
            const sym = deps.resolveSymbolCaseInsensitive(scopeAtPos, tableName);
            if (sym) {
              const localCols = deps.getSymbolLocalColumns(sym);
              if (localCols && Array.isArray(localCols)) {
                colDef = localCols.find((c: any) => normalizeName(c.rawName ?? c.name ?? c) === colName);
                containerName = sym.rawName ?? sym.name;
              } else if (sym.dataType) {
                const typeKey = normalizeName(sym.dataType);
                const typeDef = deps.tableTypesByName.get(typeKey) || deps.tablesByName.get(typeKey);
                if (typeDef && typeDef.columns) {
                  colDef = typeDef.columns.find((c: any) => normalizeName(c.rawName ?? c.name ?? c) === colName);
                  containerName = typeDef.rawName ?? typeDef.name;
                  isType = deps.tableTypesByName.get(typeKey) === typeDef;
                  aliasToken = sym.rawName ?? sym.name;
                }
              }
            }
          }
        } else {
          const def = localDefsByName.get(tableName) || localDefsByName.get(tableName.replace(/^dbo\./, "")) || deps.tablesByName.get(tableName) || deps.tableTypesByName.get(tableName);
          if (def && def.columns) {
            colDef = def.columns.find((c: any) => normalizeName(c.name) === colName);
            containerName = def.rawName ?? def.name;
            isType = deps.tableTypesByName.has(tableName);
          } else if (parsed?.scope?.root) {
            const scopeAtPos = parsed.scope.root.findInnermost(offset);
            const cteSym = deps.resolveSymbolCaseInsensitive(scopeAtPos, tableName);
            if (cteSym?.kind === "CTE") {
              const cteCol = deps.getCteColumns(cteSym).find(c => c.name === colName);
              if (cteCol) {
                colDef = { name: cteCol.name, rawName: cteCol.rawName };
                containerName = cteSym.name ?? tableName;
                isCte = true;
              }
            }

            const aliasSym = deps.resolveSymbolCaseInsensitive(scopeAtPos, tableName);
            if (!colDef && aliasSym?.kind === "Alias") {
              const aliasTarget = normalizeName(deps.resolveAliasTableName(aliasSym) ?? "");
              if (aliasTarget) {
                const targetSym = deps.resolveSymbolCaseInsensitive(scopeAtPos, aliasTarget);
                if (targetSym) {
                  if (Array.isArray(targetSym.columns)) {
                    const localCol = targetSym.columns.find((c: any) => normalizeName(c.rawName ?? c.name ?? c) === colName || normalizeName(c.name ?? c) === colName);
                    if (localCol) {
                      colDef = localCol;
                      containerName = targetSym.rawName ?? targetSym.name ?? aliasTarget;
                    }
                  } else if (targetSym.dataType) {
                    const typeKey = normalizeName(String(targetSym.dataType));
                    const typeDef = deps.tableTypesByName.get(typeKey) || deps.tablesByName.get(typeKey);
                    const typeCol = typeDef?.columns?.find((c: any) => normalizeName(c.rawName ?? c.name ?? c) === colName || normalizeName(c.name ?? c) === colName);
                    if (typeCol) {
                      colDef = typeCol;
                      containerName = typeDef?.rawName ?? typeDef?.name ?? typeKey;
                      isType = true;
                    }
                  }
                }
              }
              const derivedColumns = deps.getParserAliasColumnNames(parsed, aliasSym, localDefsByName);
              if (!colDef && derivedColumns.some(c => normalizeName(c) === colName)) {
                colDef = { name: colName, rawName: colName };
                containerName = deps.getDisplaySymbolName(aliasSym) ?? tableName;
              }
            }
          }
        }

        if (colDef) {
          const kindLabel = isCte ? "CTE" : (isType ? "table type" : "table");
          const typePart = colDef.type ? ` - ${colDef.type}` : "";
          const aliasPart = aliasToken ? ` (parameter \`${aliasToken}\`)` : "";
          const displayCol = deps.getHoverColumnLabel(colDef, wordRangeText);
          const value = `**Column** \`${displayCol}\`${typePart}\n\nDefined in **${kindLabel}** \`${containerName}\`${aliasPart}`;
          return { contents: { kind: MarkupKind.Markdown, value }, range };
        }

        let kindLabel = "table";
        if (parsed?.ast) {
          const { isFunc, rawName } = deps.isFunctionCallInAst(parsed.ast, tableName);
          if (isFunc) {
            kindLabel = "table-valued function";
            containerName = rawName;
          }
        }
        const displayCol = normalizeName(wordRangeText) === colName ? wordRangeText : colName;
        const value = `**Column** \`${displayCol}\`\n\nDefined in **${kindLabel}** \`${containerName}\``;
        return { contents: { kind: MarkupKind.Markdown, value }, range };
      }
    }

    return null;
  } catch (e) {
    deps.safeLog(`[doHover] error: ${String(e)}`);
    return null;
  }
}

/**
 * Hover computation extracted from server.ts so it can be unit-tested
 * without starting the LSP connection.
 *
 * Usage in tests:
 *   import { computeHover } from "../hover-provider";
 *   const result = computeHover(doc, pos, parseSql(sql));
 */
import { Hover, MarkupKind, Position } from "vscode-languageserver/node";
import { TextDocument } from "vscode-languageserver-textdocument";
import { normalizeName, getWordRangeAtPosition } from "./text-utils";
import { tablesByName, tableTypesByName, definitions, findReferenceAtPosition } from "./definitions";
import type { ParseResult } from "./sql-parser";
import { getCteColumns, resolveAliasTableName, resolveSymbolCaseInsensitive, getDisplaySymbolName } from "./ast-utils";
import {
  toNormUri,
  computeCurrentStatement,
  getPropertyAccessAtOffset,
  getResolutionSourceColumns,
  getResolvedObjectKindLabel,
  getUpdateSetTargetTable,
  findStatementLocalColumnOwner,
  getHoverColumnLabel,
  getSymbolLocalColumns,
  isFunctionCallInAst,
  getParserAliasColumnNames,
} from "./sql-helpers";

/**
 * Compute the hover response for a position in a document.
 *
 * @param doc     The text document (used for word-range extraction and line text).
 * @param pos     The cursor position.
 * @param parsed  The already-parsed SQL result (pass `parseSql(doc.getText())`
 *                or the cached version from getParsedDocument).
 */
export function computeHover(
  doc: TextDocument,
  pos: Position,
  parsed: ParseResult | null
): Hover | null {
  try {
    const text = doc.getText();
    const offset = doc.offsetAt(pos);
    const normUri = toNormUri(doc.uri);

    const range = getWordRangeAtPosition(doc, pos);
    if (!range) { return null; }

    const wordRangeText = doc.getText(range);

    const match = findReferenceAtPosition(normUri, pos.line, pos.character);
    const propertyAccess = getPropertyAccessAtOffset(parsed, offset);
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

    const localDefs = definitions.get(normUri) ?? [];
    const localDefsByName = new Map<string, any>();
    for (const def of localDefs) {
      localDefsByName.set(normalizeName(def.name), def);
    }

    if (!match) {
      const word = normalizeName(wordRangeText);
      const matchedResolution = parsed?.columns?.resolutions?.find((r: any) => {
        const s = Number(r?.location?.start);
        const e = Number(r?.location?.end);
        return Number.isFinite(s) && Number.isFinite(e) && offset >= s && offset <= e;
      });

      if (matchedResolution) {
        const sources = getResolutionSourceColumns(matchedResolution);
        if (sources.length > 0) {
          for (const src of sources) {
            const srcStripped = src.table.replace(/^dbo\./, "");
            const def = localDefsByName.get(src.table) || localDefsByName.get(srcStripped)
              || tablesByName.get(src.table) || tableTypesByName.get(src.table);
            const colDef = def?.columns?.find((c: any) => normalizeName(c.name) === src.column);
            if (colDef) {
              const kindLabel = getResolvedObjectKindLabel(src.table, def);
              const typePart = colDef.type ? ` - ${colDef.type}` : "";
              const displayCol = colDef.rawName ?? (normalizeName(wordRangeText) === src.column ? wordRangeText : src.column);
              const displayTable = def?.rawName ?? def?.name ?? src.table;
              const value = `**Column** \`${displayCol}\`${typePart}\n\nDefined in **${kindLabel}** \`${displayTable}\``;
              return { contents: { kind: MarkupKind.Markdown, value }, range };
            }
          }
        }
      }

      const updateSetTarget = getUpdateSetTargetTable(parsed, offset);
      if (updateSetTarget) {
        const targetNorm = normalizeName(updateSetTarget);
        const targetStripped = targetNorm.replace(/^dbo\./, "");
        const colDef = (localDefsByName.get(targetNorm) || localDefsByName.get(targetStripped)
          || tablesByName.get(targetNorm) || tableTypesByName.get(targetNorm))
          ?.columns?.find((c: any) => normalizeName(c.name) === word);
        if (colDef) {
          const ownerDef = localDefsByName.get(targetNorm) || localDefsByName.get(targetNorm.replace(/^dbo\./, ""))
            || tablesByName.get(targetNorm) || tableTypesByName.get(targetNorm);
          const kindLabel = getResolvedObjectKindLabel(targetNorm, ownerDef);
          const typePart = colDef.type ? ` - ${colDef.type}` : "";
          const value = `**Column** \`${colDef.rawName}\`${typePart}\n\nDefined in **${kindLabel}** \`${ownerDef?.rawName ?? targetNorm}\``;
          return { contents: { kind: MarkupKind.Markdown, value }, range };
        }
      }

      const hoverScope = parsed?.scope?.root?.findInnermost(offset);
      const fallbackLine = doc.getText({ start: { line: pos.line, character: 0 }, end: { line: pos.line, character: Number.MAX_VALUE } });
      const stmtText = computeCurrentStatement(text, offset, parsed, fallbackLine);
      const stmtOwner = findStatementLocalColumnOwner(stmtText, word, hoverScope, parsed, offset, localDefsByName);
      if (stmtOwner) {
        const typePart = stmtOwner.column.type ? ` - ${stmtOwner.column.type}` : "";
        const displayCol = getHoverColumnLabel(stmtOwner.column, wordRangeText);
        const value = `**Column** \`${displayCol}\`${typePart}\n\nDefined in **${stmtOwner.kindLabel}** \`${stmtOwner.ownerName}\``;
        return { contents: { kind: MarkupKind.Markdown, value }, range };
      }
      return null;
    }

    if (match.kind === "parameter") {
      let dataType = "unknown";
      let columns: any[] | undefined;
      let isTableVariable = false;
      let paramDisplay = match.name;
      if (parsed?.scope?.root) {
        const scopeAtPos = parsed.scope.root.findInnermost(offset);
        const sym = resolveSymbolCaseInsensitive(scopeAtPos, match.name) ?? resolveSymbolCaseInsensitive(scopeAtPos, wordRangeText);
        if (sym) {
          paramDisplay = sym.name ?? paramDisplay;
          if (sym.dataType) {
            dataType = sym.dataType;
            const typeKey = normalizeName(dataType);
            const typeDef = tableTypesByName.get(typeKey) || tablesByName.get(typeKey);
            if (typeDef?.columns) { columns = typeDef.columns; isTableVariable = true; }
          }
          const localCols = getSymbolLocalColumns(sym);
          if (localCols && localCols.length > 0) { columns = localCols; isTableVariable = true; }
        }
      }
      if (isTableVariable && columns) {
        const rows = columns.map((c: any) => `- \`${c.rawName ?? c.name}\`${c.type ? ` ${c.type}` : ""}`);
        const body = `**Table Variable** \`${paramDisplay}\` — \`${dataType}\`\n\n${rows.join("\n")}`;
        return { contents: { kind: MarkupKind.Markdown, value: body }, range };
      }
      const value = `**Parameter** \`${paramDisplay}\` — \`${dataType}\``;
      return { contents: { kind: MarkupKind.Markdown, value }, range };
    }

    if (match.kind === "table") {
      const norm = normalizeName(match.name);
      if (norm.startsWith("@")) {
        let dataType = "unknown";
        let columns: any[] | undefined;
        let isTableVariable = false;
        let paramDisplay = match.name;
        if (parsed?.scope?.root) {
          const scopeAtPos = parsed.scope.root.findInnermost(offset);
          const sym = resolveSymbolCaseInsensitive(scopeAtPos, match.name) ?? resolveSymbolCaseInsensitive(scopeAtPos, wordRangeText);
          if (sym) {
            paramDisplay = sym.name ?? paramDisplay;
            if (sym.dataType) {
              dataType = sym.dataType;
              const typeKey = normalizeName(dataType);
              const typeDef = tableTypesByName.get(typeKey) || tablesByName.get(typeKey);
              if (typeDef?.columns) { columns = typeDef.columns; isTableVariable = true; }
            }
            const localCols = getSymbolLocalColumns(sym);
            if (localCols && localCols.length > 0) { columns = localCols; isTableVariable = true; }
          }
        }
        if (isTableVariable && columns) {
          const rows = columns.map((c: any) => `- \`${c.rawName ?? c.name}\`${c.type ? ` ${c.type}` : ""}`);
          const body = `**Table Variable** \`${paramDisplay}\` — \`${dataType}\`\n\n${rows.join("\n")}`;
          return { contents: { kind: MarkupKind.Markdown, value: body }, range };
        }
        const value = `**Parameter** \`${paramDisplay}\` — \`${dataType}\``;
        return { contents: { kind: MarkupKind.Markdown, value }, range };
      }

      const strippedNorm = norm.replace(/^dbo\./, "");
      const def = localDefsByName.get(norm) || localDefsByName.get(strippedNorm)
        || tablesByName.get(norm) || tableTypesByName.get(norm);
      if (def?.columns) {
        const rows = def.columns.map((c: any) => `- \`${c.rawName}\`${c.type ? ` ${c.type}` : ""}`);
        const kindLabel = getResolvedObjectKindLabel(norm, def, { titleCase: true });
        const body = `**${kindLabel}** \`${def.rawName}\`\n\n${rows.join("\n")}`;
        return { contents: { kind: MarkupKind.Markdown, value: body }, range };
      }
      if (parsed?.scope?.root) {
        const scopeAtPos = parsed.scope.root.findInnermost(offset);
        const cteSym = resolveSymbolCaseInsensitive(scopeAtPos, norm);
        if (cteSym?.kind === "CTE") {
          const cteCols = getCteColumns(cteSym);
          if (cteCols.length > 0) {
            const rows = cteCols.map((c) => `- \`${c.rawName}\``);
            const body = `**CTE** \`${getDisplaySymbolName(cteSym)}\`\n\n${rows.join("\n")}`;
            return { contents: { kind: MarkupKind.Markdown, value: body }, range };
          }
        }
        const aliasSym = resolveSymbolCaseInsensitive(scopeAtPos, norm);
        if (aliasSym?.kind === "Alias") {
          const aliasTableNorm = normalizeName(resolveAliasTableName(aliasSym) ?? "");
          const aliasTableDef = localDefsByName.get(aliasTableNorm) || localDefsByName.get(aliasTableNorm.replace(/^dbo\./, ""))
            || tablesByName.get(aliasTableNorm) || tableTypesByName.get(aliasTableNorm);
          if (aliasTableDef?.columns) {
            const rows = aliasTableDef.columns.map((c: any) => `- \`${c.rawName}\`${c.type ? ` ${c.type}` : ""}`);
            const kindLabel = getResolvedObjectKindLabel(aliasTableNorm, aliasTableDef, { titleCase: true });
            const body = `**Alias** \`${getDisplaySymbolName(aliasSym)}\`\n\nResolves to **${kindLabel}** \`${aliasTableDef.rawName}\`\n\n${rows.join("\n")}`;
            return { contents: { kind: MarkupKind.Markdown, value: body }, range };
          } else if (parsed?.ast) {
            const { isFunc, rawName } = isFunctionCallInAst(parsed.ast, aliasTableNorm);
            if (isFunc) {
              const body = `**Alias** \`${getDisplaySymbolName(aliasSym)}\`\n\nResolves to **table-valued function** \`${rawName}\``;
              return { contents: { kind: MarkupKind.Markdown, value: body }, range };
            }
          }
        }
      }
      if (!localDefsByName.has(norm) && !localDefsByName.has(norm.replace(/^dbo\./, ""))
        && !tablesByName.has(norm) && !tableTypesByName.has(norm) && parsed?.ast) {
        const { isFunc, rawName } = isFunctionCallInAst(parsed.ast, norm);
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
        const fallbackLine = doc.getText({ start: { line: pos.line, character: 0 }, end: { line: pos.line, character: Number.MAX_VALUE } });
        const stmtText = computeCurrentStatement(text, offset, parsed, fallbackLine);
        const stmtOwner = findStatementLocalColumnOwner(stmtText, parts[0], hoverScope, parsed, offset, localDefsByName);
        if (stmtOwner) {
          const typePart = stmtOwner.column.type ? ` - ${stmtOwner.column.type}` : "";
          const displayCol = getHoverColumnLabel(stmtOwner.column, wordRangeText);
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
        let aliasToken: string | undefined;

        if (tableName.startsWith("@")) {
          if (parsed?.scope?.root) {
            const scopeAtPos = parsed.scope.root.findInnermost(offset);
            const sym = resolveSymbolCaseInsensitive(scopeAtPos, tableName);
            if (sym) {
              paramDisplay: {
                const localCols = getSymbolLocalColumns(sym);
                if (localCols && Array.isArray(localCols)) {
                  colDef = localCols.find((c: any) => normalizeName(c.rawName ?? c.name ?? c) === colName);
                  containerName = sym.rawName ?? sym.name;
                } else if (sym.dataType) {
                  const typeKey = normalizeName(sym.dataType);
                  const typeDef = tableTypesByName.get(typeKey) || tablesByName.get(typeKey);
                  if (typeDef?.columns) {
                    colDef = typeDef.columns.find((c: any) => normalizeName(c.rawName ?? c.name ?? c) === colName);
                    containerName = typeDef.rawName ?? typeDef.name;
                    isType = typeDef === tableTypesByName.get(typeKey);
                    aliasToken = sym.rawName ?? sym.name;
                  }
                }
              }
            }
          }
        } else {
          const def2 = localDefsByName.get(tableName) || localDefsByName.get(tableName.replace(/^dbo\./, ""))
            || tablesByName.get(tableName) || tableTypesByName.get(tableName);
          if (def2?.columns) {
            colDef = def2.columns.find((c: any) => normalizeName(c.name) === colName);
            containerName = def2.rawName ?? def2.name;
            isType = tableTypesByName.has(tableName);
          } else if (parsed?.scope?.root) {
            const scopeAtPos = parsed.scope.root.findInnermost(offset);
            const cteSym = resolveSymbolCaseInsensitive(scopeAtPos, tableName);
            if (cteSym?.kind === "CTE") {
              const cteCol = getCteColumns(cteSym).find((c) => c.name === colName);
              if (cteCol) { colDef = { name: cteCol.name, rawName: cteCol.rawName }; containerName = cteSym.name ?? tableName; isCte = true; }
            }
            const aliasSym = resolveSymbolCaseInsensitive(scopeAtPos, tableName);
            if (aliasSym?.kind === "Alias") {
              const aliasTarget = normalizeName(resolveAliasTableName(aliasSym) ?? "");
              if (aliasTarget) {
                const targetSym = resolveSymbolCaseInsensitive(scopeAtPos, aliasTarget);
                if (targetSym) {
                  if (Array.isArray(targetSym.columns)) {
                    const localCol = targetSym.columns.find((c: any) =>
                      normalizeName(c.rawName ?? c.name ?? c) === colName || normalizeName(c.name ?? c) === colName);
                    if (localCol) { colDef = localCol; containerName = targetSym.rawName ?? targetSym.name ?? aliasTarget; }
                  } else if (targetSym.dataType) {
                    const typeKey = normalizeName(String(targetSym.dataType));
                    const typeDef = tableTypesByName.get(typeKey) || tablesByName.get(typeKey);
                    const typeCol = typeDef?.columns?.find((c: any) =>
                      normalizeName(c.rawName ?? c.name ?? c) === colName || normalizeName(c.name ?? c) === colName);
                    if (typeCol) { colDef = typeCol; containerName = typeDef?.rawName ?? typeDef?.name ?? typeKey; isType = true; }
                  }
                }
              }
              const derivedColumns = getParserAliasColumnNames(parsed, aliasSym, localDefsByName);
              if (!colDef && derivedColumns.some((c) => normalizeName(c) === colName)) {
                colDef = { name: colName, rawName: colName };
                containerName = getDisplaySymbolName(aliasSym) ?? tableName;
              }
            }
          }
        }

        if (colDef) {
          const kindLabel = isCte ? "CTE" : (isType ? "table type" : "table");
          const typePart = colDef.type ? ` — ${colDef.type}` : "";
          const aliasPart = aliasToken ? ` (parameter \`${aliasToken}\`)` : "";
          const displayCol = getHoverColumnLabel(colDef, wordRangeText);
          const value = `**Column** \`${displayCol}\`${typePart}\n\nDefined in **${kindLabel}** \`${containerName}\`${aliasPart}`;
          return { contents: { kind: MarkupKind.Markdown, value }, range };
        } else {
          let kindLabel = "table";
          if (parsed?.ast) {
            const { isFunc, rawName } = isFunctionCallInAst(parsed.ast, tableName);
            if (isFunc) { kindLabel = "table-valued function"; containerName = rawName; }
          }
          const displayCol = normalizeName(wordRangeText) === colName ? wordRangeText : colName;
          const value = `**Column** \`${displayCol}\`\n\nDefined in **${kindLabel}** \`${containerName}\``;
          return { contents: { kind: MarkupKind.Markdown, value }, range };
        }
      }
    }

    return null;
  } catch {
    return null;
  }
}

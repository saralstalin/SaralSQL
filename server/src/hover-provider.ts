import type { Hover, Position } from "vscode-languageserver/node";
import { MarkupKind } from "vscode-languageserver/node";
import type { TextDocument } from "vscode-languageserver-textdocument";
import { normalizeName } from "./text-utils";
import type { HoverProviderDeps } from "./provider-types";
import { isOrderByDuplicateOutputAtOffset } from "./orderby-resolution";
import { resolveColumnAtOffset } from "./column-resolution";

function getColumnTypeText(col: any): string | undefined {
  const raw = col?.type ?? col?.dataType ?? col?.sqlType ?? col?.declaredType ?? col?.returnType;
  if (raw === undefined || raw === null) {
    return undefined;
  }
  const text = String(raw).trim();
  return text.length > 0 ? text : undefined;
}

function isColumnNameMatch(col: any, normalizedTarget: string): boolean {
  const rawNorm = normalizeName(String(col?.rawName ?? ""));
  const nameNorm = normalizeName(String(col?.name ?? ""));
  return rawNorm === normalizedTarget || nameNorm === normalizedTarget;
}

function getDisplayColumnToken(wordRangeText: string, fallback: string): string {
  const tail = String(wordRangeText ?? "").split(".").pop() ?? "";
  const clean = tail.trim();
  return clean.length > 0 ? clean : fallback;
}

function getDisplayOwnerName(
  ownerName: string,
  localDefsByName: Map<string, any>,
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>
): string {
  const norm = normalizeName(ownerName);
  const stripped = norm.replace(/^dbo\./, "");
  const dboPrefixed = stripped ? `dbo.${stripped}` : norm;
  const def = localDefsByName.get(norm)
    || localDefsByName.get(stripped)
    || localDefsByName.get(dboPrefixed)
    || tablesByName.get(norm)
    || tablesByName.get(stripped)
    || tablesByName.get(dboPrefixed)
    || tableTypesByName.get(norm)
    || tableTypesByName.get(stripped)
    || tableTypesByName.get(dboPrefixed);
  return String(def?.rawName ?? ownerName);
}

function getProjectedColumnTypeAtOffset(
  parsed: any,
  offset: number,
  localDefsByName: Map<string, any>,
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>
): string | undefined {
  const matchedResolution = parsed?.columns?.resolutions?.find((r: any) => {
    const s = Number(r?.location?.start);
    const e = Number(r?.location?.end);
    return Number.isFinite(s) && Number.isFinite(e) && offset >= s && offset <= e;
  });
  const inputs = Array.isArray(matchedResolution?.inputs) ? matchedResolution.inputs : [];
  for (const input of inputs) {
    if (String(input?.kind ?? "").toLowerCase() !== "column") {
      continue;
    }
    const sourceNorm = normalizeName(String(input?.source ?? ""));
    const sourceColNorm = normalizeName(String(input?.name ?? "").split(".").pop() ?? "");
    if (!sourceNorm || !sourceColNorm) {
      continue;
    }
    const sourceStripped = sourceNorm.replace(/^dbo\./, "");
    const def = localDefsByName.get(sourceNorm)
      || localDefsByName.get(sourceStripped)
      || tablesByName.get(sourceNorm)
      || tablesByName.get(sourceStripped)
      || tableTypesByName.get(sourceNorm)
      || tableTypesByName.get(sourceStripped);
    const col = def?.columns?.find((c: any) => normalizeName(String(c?.rawName ?? c?.name ?? "")) === sourceColNorm);
    const t = getColumnTypeText(col);
    if (t) {
      return t;
    }
  }
  return undefined;
}

function getDisplayColumnType(
  colDef: any,
  parsed: any,
  offset: number,
  localDefsByName: Map<string, any>,
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>
): string | undefined {
  return getColumnTypeText(colDef)
    ?? getProjectedColumnTypeAtOffset(parsed, offset, localDefsByName, tablesByName, tableTypesByName);
}

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
    if (isOrderByDuplicateOutputAtOffset(parsed?.ast, offset, wordRangeText)) {
      return {
        contents: {
          kind: MarkupKind.Markdown,
          value: `**Column** \`${wordRangeText}\`\n\nAmbiguous in ORDER BY due to duplicate projected output name`
        },
        range
      };
    }
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
          ?.find((c: any) => isColumnNameMatch(c, word));

        if (colDef) {
          const owner = localDefsByName.get(targetNorm) || localDefsByName.get(targetNorm.replace(/^dbo\./, "")) || deps.tablesByName.get(targetNorm) || deps.tableTypesByName.get(targetNorm);
          const kindLabel = deps.getResolvedObjectKindLabel(targetNorm, owner);
          const colType = getColumnTypeText(colDef);
          const typePart = colType ? ` - ${colType}` : "";
          const value = `**Column** \`${colDef.rawName}\`${typePart}\n\nDefined in **${kindLabel}** \`${owner?.rawName ?? targetNorm}\``;
          return { contents: { kind: MarkupKind.Markdown, value }, range };
        }
      }

      const hoverScope = parsed?.scope?.root?.findInnermost(offset);
      const stmtOwner = deps.findStatementLocalColumnOwner(deps.getCurrentStatement(doc, pos), word, hoverScope, parsed, offset, localDefsByName);
      if (stmtOwner) {
        const ownerType = getDisplayColumnType(
          stmtOwner.column,
          parsed,
          offset,
          localDefsByName,
          deps.tablesByName,
          deps.tableTypesByName
        );
        const typePart = ownerType ? ` - ${ownerType}` : "";
        const displayCol = getDisplayColumnToken(wordRangeText, deps.getHoverColumnLabel(stmtOwner.column, wordRangeText));
        const ownerDisplay = getDisplayOwnerName(stmtOwner.ownerName, localDefsByName, deps.tablesByName, deps.tableTypesByName);
        const value = `**Column** \`${displayCol}\`${typePart}\n\nDefined in **${stmtOwner.kindLabel}** \`${ownerDisplay}\``;
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
        const rows = columns.map(c => {
          const t = getColumnTypeText(c);
          return `- \`${c.rawName ?? c.name}\`${t ? ` ${t}` : ""}`;
        });
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
          const rows = columns.map(c => {
            const t = getColumnTypeText(c);
            return `- \`${c.rawName ?? c.name}\`${t ? ` ${t}` : ""}`;
          });
          const body = `**Table Variable** \`${paramDisplay}\` - \`${dataType}\`\n\n${rows.join("\n")}`;
          return { contents: { kind: MarkupKind.Markdown, value: body }, range };
        }

        const value = `**Parameter** \`${paramDisplay}\` - \`${dataType}\``;
        return { contents: { kind: MarkupKind.Markdown, value }, range };
      }
      const strippedNorm = norm.replace(/^dbo\./, "");
      const def = localDefsByName.get(norm) || localDefsByName.get(strippedNorm) || deps.tablesByName.get(norm) || deps.tableTypesByName.get(norm);
      if (def && def.columns) {
        const rows = def.columns.map((c: any) => {
          const t = getColumnTypeText(c);
          return `- \`${c.rawName}\`${t ? ` ${t}` : ""}`;
        });
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
            const rows = aliasTableDef.columns.map((c: any) => {
              const t = getColumnTypeText(c);
              return `- \`${c.rawName}\`${t ? ` ${t}` : ""}`;
            });
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
      const tokenText = String(wordRangeText ?? match.name ?? "").trim();
      const resolved = resolveColumnAtOffset({
        parsed,
        offset,
        columnName: tokenText,
        tokenText,
        tablesByName: deps.tablesByName,
        tableTypesByName: deps.tableTypesByName,
        localDefsByName,
        resolverOptions: { allowQualifiedSchemaLookup: false }
      });
      if (resolved.status === "resolved" && resolved.owner?.column) {
        const ownerType = getDisplayColumnType(
          resolved.owner.column,
          parsed,
          offset,
          localDefsByName,
          deps.tablesByName,
          deps.tableTypesByName
        );
        const typePart = ownerType ? ` - ${ownerType}` : "";
        const displayCol = getDisplayColumnToken(wordRangeText, deps.getHoverColumnLabel(resolved.owner.column, wordRangeText));
        const ownerDisplay = getDisplayOwnerName(resolved.owner.ownerName, localDefsByName, deps.tablesByName, deps.tableTypesByName);
        const value = `**Column** \`${displayCol}\`${typePart}\n\nDefined in **${resolved.owner.kindLabel}** \`${ownerDisplay}\``;
        return { contents: { kind: MarkupKind.Markdown, value }, range };
      }
      return null;
    }

    return null;
  } catch (e) {
    deps.safeLog(`[doHover] error: ${String(e)}`);
    return null;
  }
}

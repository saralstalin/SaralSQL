/**
 * Definition (go-to-definition) logic extracted from server.ts.
 * Testable without the LSP connection.
 */
import { Location, Position } from "vscode-languageserver/node";
import { TextDocument } from "vscode-languageserver-textdocument";
import { normalizeName, getWordRangeAtPosition, isSqlKeyword } from "./text-utils";
import {
  tablesByName, tableTypesByName, definitions, referencesIndex,
  findReferenceAtPosition, findColumnInTable
} from "./definitions";
import type { ParseResult } from "./sql-parser";
import { getCteColumns, resolveSymbolCaseInsensitive } from "./ast-utils";
import {
  toNormUri, getResolutionSourceColumns, findDerivedAliasProjectedColumnRange,
  findStatementLocalColumnOwner, computeCurrentStatement,
  getDefinitionReferenceKeys, isAmbiguousBareColumnAtPosition
} from "./sql-helpers";

export function computeDefinition(
  doc: TextDocument,
  pos: Position,
  parsed: ParseResult | null
): Location[] | null {
  const normUri = toNormUri(doc.uri);
  const offset = doc.offsetAt(pos);
  const localDefsByName = new Map<string, any>();
  for (const def of definitions.get(normUri) ?? []) {
    localDefsByName.set(normalizeName(def.name), def);
  }

  const match = findReferenceAtPosition(normUri, pos.line, pos.character);
  if (isAmbiguousBareColumnAtPosition(doc, pos, parsed)) {
    return null;
  }

  if (!match) {
    const wordRange = getWordRangeAtPosition(doc, pos);
    if (!wordRange) { return null; }
    const rawWord = doc.getText(wordRange);
    const word = normalizeName(rawWord);
    const isBareColumnToken = !rawWord.includes(".") && !rawWord.startsWith("@") && !isSqlKeyword(word);

    const matchedResolution = parsed?.columns?.resolutions?.find((r: any) => {
      const s = Number(r?.location?.start);
      const e = Number(r?.location?.end);
      return Number.isFinite(s) && Number.isFinite(e) && offset >= s && offset <= e;
    });
    if (matchedResolution) {
      const sources = getResolutionSourceColumns(matchedResolution);
      for (const src of sources) {
        const locs = findColumnInTable(src.table, src.column);
        if (locs.length > 0) { return locs; }
      }
    }

    const localDefs = (definitions.get(normUri) || []).filter(
      d => normalizeName(d.name) === word || normalizeName(d.rawName) === word
    );
    if (localDefs.length > 0) {
      return localDefs.map(d => ({ uri: d.uri, range: { start: { line: d.line, character: 0 }, end: { line: d.line, character: 200 } } }));
    }

    if (isBareColumnToken) {
      const scopeAtPos = parsed?.scope?.root?.findInnermost(offset) ?? parsed?.scope?.root;
      const text = doc.getText();
      const fallbackLine = doc.getText({ start: { line: pos.line, character: 0 }, end: { line: pos.line, character: Number.MAX_VALUE } });
      const stmtText = computeCurrentStatement(text, offset, parsed, fallbackLine);
      const stmtOwner = findStatementLocalColumnOwner(stmtText, rawWord, scopeAtPos, parsed, offset, localDefsByName);
      if (stmtOwner?.ownerName) {
        const locs = findColumnInTable(normalizeName(stmtOwner.ownerName), word);
        if (locs.length > 0) { return locs; }
      }
      return null;
    }
    return null;
  }

  if (match.kind === "parameter") {
    const byUri = referencesIndex.get(match.name);
    const fileRefs = byUri?.get(normUri) ?? [];
    const first = fileRefs[0];
    if (first) {
      return [{ uri: first.uri, range: { start: { line: first.line, character: first.start }, end: { line: first.line, character: first.end } } }];
    }
    return null;
  }

  if (match.kind === "table") {
    const norm = normalizeName(match.name);
    if (norm.startsWith("@")) {
      const byUri = referencesIndex.get(norm);
      const fileRefs = byUri?.get(normUri) ?? [];
      const declarationRef = fileRefs.find(r => r.kind === "parameter") ?? fileRefs[0];
      if (declarationRef) {
        return [{ uri: declarationRef.uri, range: { start: { line: declarationRef.line, character: declarationRef.start }, end: { line: declarationRef.line, character: declarationRef.end } } }];
      }
    }
    const defs = definitions.get(norm);
    if (defs && defs.length > 0) {
      return defs.map(d => ({ uri: d.uri, range: { start: { line: d.line, character: 0 }, end: { line: d.line, character: 200 } } }));
    }
    const tblDef = tablesByName.get(norm) || tableTypesByName.get(norm);
    if (tblDef) {
      return [{ uri: tblDef.uri, range: { start: { line: tblDef.line, character: 0 }, end: { line: tblDef.line, character: 200 } } }];
    }
    if (parsed?.scope?.root) {
      const scopeAtPos = parsed.scope.root.findInnermost(offset);
      const cteSym = resolveSymbolCaseInsensitive(scopeAtPos, norm);
      if (cteSym?.kind === "CTE" && typeof cteSym.location?.start === "number" && typeof cteSym.location?.end === "number") {
        return [{ uri: doc.uri, range: { start: doc.positionAt(cteSym.location.start), end: doc.positionAt(cteSym.location.end) } }];
      }
    }
  }

  if (match.kind === "column") {
    const parts = match.name.split(".");
    if (parts.length === 2) {
      const tableName = parts[0];
      const colName = parts[1];
      const derivedLoc = findDerivedAliasProjectedColumnRange(doc, parsed, offset, tableName, colName);
      if (derivedLoc && derivedLoc.length > 0) { return derivedLoc; }

      const matchedResolution = parsed?.columns?.resolutions?.find((r: any) => {
        const s = Number(r?.location?.start);
        const e = Number(r?.location?.end);
        return Number.isFinite(s) && Number.isFinite(e) && offset >= s && offset <= e;
      });
      if (matchedResolution) {
        const sources = getResolutionSourceColumns(matchedResolution);
        for (const src of sources) {
          const locs = findColumnInTable(src.table, src.column);
          if (locs.length > 0) { return locs; }
        }
      }

      if (parsed?.scope?.root) {
        const scopeAtPos = parsed.scope.root.findInnermost(offset);
        const cteSym = resolveSymbolCaseInsensitive(scopeAtPos, tableName);
        if (cteSym?.kind === "CTE") {
          const cteCol = getCteColumns(cteSym).find(c => c.name === normalizeName(colName));
          if (cteCol?.start !== undefined && cteCol?.end !== undefined) {
            return [{ uri: doc.uri, range: { start: doc.positionAt(cteCol.start), end: doc.positionAt(cteCol.end) } }];
          }
        }
      }
      return findColumnInTable(tableName, colName);
    }
  }

  return null;
}

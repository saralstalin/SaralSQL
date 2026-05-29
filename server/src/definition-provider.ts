import type { Location, Position } from "vscode-languageserver/node";
import type { TextDocument } from "vscode-languageserver-textdocument";
import { normalizeName, isSqlKeyword } from "./text-utils";
import type { DefinitionProviderDeps } from "./provider-types";

export async function onDefinitionProvider(
  doc: TextDocument,
  position: Position,
  deps: DefinitionProviderDeps
): Promise<Location[] | null> {
  const normUri = deps.toNormUri(doc.uri);
  const parsed = deps.getParsedDocument(doc);
  const offset = doc.offsetAt(position);
  const localDefsByName = new Map<string, any>();
  for (const def of deps.definitions.get(normUri) ?? []) {
    localDefsByName.set(normalizeName(def.name), def);
  }

  const match = deps.findReferenceAtPosition(normUri, position.line, position.character);
  if (deps.isAmbiguousBareColumnAtPosition(doc, position, parsed)) {
    return null;
  }

  if (!match) {
    const wordRange = deps.getWordRangeAtPosition(doc, position);
    if (!wordRange) { return null; }
    const rawWord = doc.getText(wordRange);
    const word = normalizeName(rawWord);
    const isBareColumnToken = !rawWord.includes(".") && !rawWord.startsWith("@") && !isSqlKeyword(word);
    const localDefs = (deps.definitions.get(normUri) || []).filter(d => normalizeName(d.name) === word || normalizeName(d.rawName) === word);
    if (localDefs.length > 0) {
      return localDefs.map(d => ({
        uri: d.uri,
        range: {
          start: { line: d.line, character: 0 },
          end: { line: d.line, character: 200 }
        }
      }));
    }
    if (isBareColumnToken) {
      const scopeAtPos = parsed?.scope?.root?.findInnermost(offset) ?? parsed?.scope?.root;
      const stmtOwner = deps.findStatementLocalColumnOwner(deps.getCurrentStatement(doc, position), rawWord, scopeAtPos, parsed, offset, localDefsByName);
      if (stmtOwner?.ownerName) {
        const locs = deps.findColumnInTable(normalizeName(stmtOwner.ownerName), word);
        if (locs.length > 0) {
          return locs;
        }
      }
      return null;
    }
    return null;
  }

  if (match.kind === "parameter") {
    const byUri = deps.referencesIndex.get(match.name);
    const fileRefs = byUri?.get(normUri) || [];
    const first = fileRefs[0];
    if (first) {
      return [{
        uri: first.uri,
        range: {
          start: { line: first.line, character: first.start },
          end: { line: first.line, character: first.end }
        }
      }];
    }
    return null;
  }

  if (match.kind === "table") {
    const norm = normalizeName(match.name);
    if (norm.startsWith("@")) {
      const byUri = deps.referencesIndex.get(norm);
      const fileRefs = byUri?.get(normUri) || [];
      const declarationRef = fileRefs.find(r => r.kind === "parameter") ?? fileRefs[0];
      if (declarationRef) {
        return [{
          uri: declarationRef.uri,
          range: {
            start: { line: declarationRef.line, character: declarationRef.start },
            end: { line: declarationRef.line, character: declarationRef.end }
          }
        }];
      }
    }
    const defs = deps.definitions.get(norm);
    if (defs && defs.length > 0) {
      return defs.map(d => ({
        uri: d.uri,
        range: {
          start: { line: d.line, character: 0 },
          end: { line: d.line, character: 200 }
        }
      }));
    }
    const tblDef = deps.tablesByName.get(norm) || deps.tableTypesByName.get(norm);
    if (tblDef) {
      return [{
        uri: tblDef.uri,
        range: {
          start: { line: tblDef.line, character: 0 },
          end: { line: tblDef.line, character: 200 }
        }
      }];
    }

    if (parsed?.scope?.root) {
      const scopeAtPos = parsed.scope.root.findInnermost(offset);
      const cteSym = deps.resolveSymbolCaseInsensitive(scopeAtPos, norm);
      if (cteSym?.kind === "CTE" && typeof cteSym.location?.start === "number" && typeof cteSym.location?.end === "number") {
        return [{
          uri: doc.uri,
          range: {
            start: doc.positionAt(cteSym.location.start),
            end: doc.positionAt(cteSym.location.end)
          }
        }];
      }
    }
  }

  if (match.kind === "column") {
    const parts = match.name.split(".");
    if (parts.length === 2) {
      const tableName = parts[0];
      const colName = parts[1];
      const derivedProjectionLoc = deps.findDerivedAliasProjectedColumnRange(doc, parsed, offset, tableName, colName);
      if (derivedProjectionLoc && derivedProjectionLoc.length > 0) {
        return derivedProjectionLoc;
      }
      const matchedResolution = parsed?.columns?.resolutions?.find((r: any) => {
        const s = Number(r?.location?.start);
        const e = Number(r?.location?.end);
        return Number.isFinite(s) && Number.isFinite(e) && offset >= s && offset <= e;
      });
      if (matchedResolution) {
        const sources = deps.getResolutionSourceColumns(matchedResolution);
        if (sources.length > 0) {
          for (const src of sources) {
            const locs = deps.findColumnInTable(src.table, src.column);
            if (locs.length > 0) {
              return locs;
            }
          }
        }
      }
      if (parsed?.scope?.root) {
        const scopeAtPos = parsed.scope.root.findInnermost(offset);
        const cteSym = deps.resolveSymbolCaseInsensitive(scopeAtPos, tableName);
        if (cteSym?.kind === "CTE") {
          const cteCol = deps.getCteColumns(cteSym).find(c => c.name === normalizeName(colName));
          if (cteCol?.start !== undefined && cteCol?.end !== undefined) {
            return [{
              uri: doc.uri,
              range: {
                start: doc.positionAt(cteCol.start),
                end: doc.positionAt(cteCol.end)
              }
            }];
          }
        }
      }
      return deps.findColumnInTable(tableName, colName);
    }
  }

  return null;
}

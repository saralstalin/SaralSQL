import type { Location, Position } from "vscode-languageserver/node";
import type { TextDocument } from "vscode-languageserver-textdocument";
import { normalizeName, isSqlKeyword } from "./text-utils";
import type { DefinitionProviderDeps } from "./provider-types";
import { resolveColumnAtOffset } from "./column-resolution";
import { buildLocalDefsByName } from "./definitions";

export async function onDefinitionProvider(
  doc: TextDocument,
  position: Position,
  deps: DefinitionProviderDeps
): Promise<Location[] | null> {
  const normUri = deps.toNormUri(doc.uri);
  const parsed = deps.getParsedDocument(doc);
  const offset = doc.offsetAt(position);
  const localDefsByName = buildLocalDefsByName(deps.definitions.get(normUri) ?? []);

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
      const resolvedBare = resolveColumnAtOffset({
        parsed,
        offset,
        columnName: rawWord,
        tokenText: rawWord,
        tablesByName: deps.tablesByName,
        tableTypesByName: deps.tableTypesByName,
        localDefsByName,
        resolverOptions: { allowQualifiedSchemaLookup: false }
      });
      if (resolvedBare.verdict === "resolved" && resolvedBare.owner?.ownerName) {
        const colName = String(resolvedBare.owner.column?.name ?? resolvedBare.owner.column?.rawName ?? rawWord);
        const locs = deps.findColumnInTable(normalizeName(resolvedBare.owner.ownerName), colName);
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
    if (parsed?.scope?.root) {
      const scopeAtPos = parsed.scope.root.findInnermost(offset);
      const scopedSym = deps.resolveSymbolCaseInsensitive(scopeAtPos, norm);
      if (scopedSym?.kind === "Alias" && typeof scopedSym.location?.start === "number" && typeof scopedSym.location?.end === "number") {
        return [{
          uri: doc.uri,
          range: {
            start: doc.positionAt(scopedSym.location.start),
            end: doc.positionAt(scopedSym.location.end)
          }
        }];
      }
      if (scopedSym?.kind === "CTE" && typeof scopedSym.location?.start === "number" && typeof scopedSym.location?.end === "number") {
        return [{
          uri: doc.uri,
          range: {
            start: doc.positionAt(scopedSym.location.start),
            end: doc.positionAt(scopedSym.location.end)
          }
        }];
      }
    }
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

  }

  if (match.kind === "column") {
    const tokenRange = deps.getWordRangeAtPosition(doc, position);
    const token = tokenRange ? doc.getText(tokenRange) : match.name;
    const isBareToken = !token.includes(".");
    const tokenForResolve = isBareToken ? token : (String(match.name ?? token).trim() || token);
    const resolved = resolveColumnAtOffset({
      parsed,
      offset,
      columnName: tokenForResolve,
      tokenText: tokenForResolve,
      tablesByName: deps.tablesByName,
      tableTypesByName: deps.tableTypesByName,
      localDefsByName,
      resolverOptions: { allowQualifiedSchemaLookup: !isBareToken }
    });
    if (resolved.status === "resolved" && resolved.owner?.ownerName && resolved.owner?.column) {
      const colName = String(resolved.owner.column?.name ?? resolved.owner.column?.rawName ?? "");
      const locs = deps.findColumnInTable(normalizeName(resolved.owner.ownerName), colName);
      if (locs.length > 0) {
        return locs;
      }
      if (normalizeName(String(resolved.owner.kindLabel ?? "")).includes("derived")) {
        const derivedLocs = deps.findDerivedAliasProjectedColumnRange(
          doc,
          parsed,
          offset,
          normalizeName(resolved.owner.ownerName),
          normalizeName(colName)
        );
        if (derivedLocs && derivedLocs.length > 0) {
          return derivedLocs;
        }
      }
    }
  }

  return null;
}

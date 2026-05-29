import type { Location, Position } from "vscode-languageserver/node";
import type { TextDocument } from "vscode-languageserver-textdocument";
import { getWordRangeAtPosition, isSqlKeyword, normalizeName } from "./text-utils";
import { resolveBareColumnAtOffset } from "./column-resolution";
import type { ParseResult } from "./sql-parser";
import type { ReferenceDef } from "./definitions";
import type { RefsProviderDeps } from "./provider-types";

export function isAmbiguousBareColumnAtPositionProvider(
  doc: TextDocument,
  position: Position,
  parsed: ParseResult | null,
  deps: RefsProviderDeps
): boolean {
  const range = getWordRangeAtPosition(doc, position);
  if (!range) { return false; }

  const rawWord = doc.getText(range).trim();
  if (!rawWord || rawWord.includes(".") || rawWord.startsWith("@")) {
    return false;
  }

  const colNorm = normalizeName(rawWord);
  if (!colNorm || isSqlKeyword(colNorm)) {
    return false;
  }

  const offset = doc.offsetAt(position);
  const normUri = deps.toNormUri(doc.uri);
  const localDefsByName = new Map<string, any>();
  for (const def of deps.definitions.get(normUri) ?? []) {
    localDefsByName.set(normalizeName(def.name), def);
  }
  const resolved = resolveBareColumnAtOffset({
    parsed,
    offset,
    columnName: rawWord,
    tablesByName: deps.tablesByName,
    tableTypesByName: deps.tableTypesByName,
    localDefsByName
  });
  return resolved.status === "ambiguous";
}

export function findReferencesForWordProvider(
  rawWord: string,
  doc: TextDocument,
  deps: RefsProviderDeps,
  position?: Position
): Location[] {
  if (!rawWord || rawWord.trim().length === 0) { return []; }
  const rawNorm = normalizeName(rawWord);
  if (isSqlKeyword(rawNorm)) { return []; }

  const normUri = deps.toNormUri(doc.uri);
  const results: Location[] = [];
  const seen = new Set<string>();

  const pushLocFromRef = (r: ReferenceDef) => {
    const key = `${r.uri}:${r.line}:${r.start}:${r.end}`;
    if (seen.has(key)) { return; }
    seen.add(key);
    results.push({
      uri: r.uri,
      range: {
        start: { line: r.line, character: r.start },
        end: { line: r.line, character: r.end }
      }
    });
  };

  const match = position
    ? deps.findReferenceAtPosition(normUri, position.line, position.character)
    : undefined;
  const parsed = position ? deps.getParsedDocument(doc) : null;
  const offset = position ? doc.offsetAt(position) : -1;

  if (match) {
    const byUri = deps.referencesIndex.get(match.name);
    const localArr = byUri?.get(normUri) ?? [];
    if (localArr.length > 0) {
      for (const r of localArr) {
        pushLocFromRef(r);
      }
    } else if (byUri) {
      for (const arr of byUri.values()) {
        for (const r of arr) {
          pushLocFromRef(r);
        }
      }
    }
    if (match.kind === "table") {
      const defs = deps.definitions.get(normalizeName(match.name));
      if (defs) {
        for (const d of defs) {
          pushLocFromRef({
            name: d.name,
            uri: d.uri,
            line: d.line,
            start: 0,
            end: d.rawName.length,
            kind: "table"
          });
        }
      }
    }
    return results;
  }

  const isBareColumnToken = !rawWord.includes(".") && !rawWord.startsWith("@") && !isSqlKeyword(rawNorm);
  if (isBareColumnToken && position) {
    const scopeAtPos = parsed?.scope?.root?.findInnermost(offset) ?? parsed?.scope?.root;
    const localDefsByName = new Map<string, any>();
    for (const def of deps.definitions.get(normUri) ?? []) {
      localDefsByName.set(normalizeName(def.name), def);
    }
    const stmtOwner = deps.findStatementLocalColumnOwner(
      deps.getCurrentStatement(doc, position),
      rawWord,
      scopeAtPos,
      parsed,
      offset,
      localDefsByName
    );
    if (stmtOwner?.ownerName) {
      const ownerNorm = normalizeName(stmtOwner.ownerName);
      const colNorm = normalizeName(rawWord);
      const key = `${ownerNorm}.${colNorm}`;
      const byUri = deps.referencesIndex.get(key);
      const localArr = byUri?.get(normUri) ?? [];
      if (localArr.length > 0) {
        for (const r of localArr) {
          pushLocFromRef(r);
        }
        return results;
      }
      if (byUri) {
        for (const arr of byUri.values()) {
          for (const r of arr) {
            pushLocFromRef(r);
          }
        }
        return results;
      }
    }
    return results;
  }

  return results;
}

/**
 * Find-References logic extracted from server.ts.
 * Testable without the LSP connection.
 */
import { Location, Position } from "vscode-languageserver/node";
import { TextDocument } from "vscode-languageserver-textdocument";
import { normalizeName, getWordRangeAtPosition, isSqlKeyword } from "./text-utils";
import {
  definitions, referencesIndex, findReferenceAtPosition, type ReferenceDef
} from "./definitions";
import type { ParseResult } from "./sql-parser";
import {
  toNormUri, getResolutionSourceColumns, findStatementLocalColumnOwner,
  computeCurrentStatement, getDefinitionReferenceKeys, isReferenceHiddenFromObjectUsage,
  isAmbiguousBareColumnAtPosition
} from "./sql-helpers";

function pushLocFromRef(r: ReferenceDef, results: Location[], seen: Set<string>): void {
  if (isReferenceHiddenFromObjectUsage(r)) { return; }
  const key = `${r.uri}:${r.line}:${r.start}:${r.end}`;
  if (seen.has(key)) { return; }
  seen.add(key);
  results.push({ uri: r.uri, range: { start: { line: r.line, character: r.start }, end: { line: r.line, character: r.end } } });
}

/**
 * Find all references to the word at `pos` in `doc`.
 * Accepts an already-parsed document so callers (including tests) can pass
 * `parseSql(text)` directly instead of going through the document cache.
 */
export function computeReferences(
  doc: TextDocument,
  pos: Position,
  parsed: ParseResult | null
): Location[] {
  if (isAmbiguousBareColumnAtPosition(doc, pos, parsed)) { return []; }
  const range = getWordRangeAtPosition(doc, pos);
  if (!range) { return []; }
  const rawWord = doc.getText(range);
  return computeReferencesForWord(rawWord, doc, pos, parsed);
}

/**
 * Core reference-lookup logic — separated so it can be called with an explicit
 * `parsed` argument from tests (avoiding the document-cache call).
 */
export function computeReferencesForWord(
  rawWord: string,
  doc: TextDocument,
  pos: Position | undefined,
  parsed: ParseResult | null
): Location[] {
  if (!rawWord || rawWord.trim().length === 0) { return []; }
  const rawNorm = normalizeName(rawWord);
  if (isSqlKeyword(rawNorm)) { return []; }

  const normUri = toNormUri(doc.uri);
  const results: Location[] = [];
  const seen = new Set<string>();
  const offset = pos ? doc.offsetAt(pos) : -1;

  const match = pos ? findReferenceAtPosition(normUri, pos.line, pos.character) : undefined;

  if (match) {
    const keys = match.kind === "table" ? getDefinitionReferenceKeys(match.name) : [match.name];

    if (match.kind === "table") {
      for (const key of keys) {
        const byUri = referencesIndex.get(key);
        if (!byUri) { continue; }
        for (const arr of byUri.values()) { for (const r of arr) { pushLocFromRef(r, results, seen); } }
      }
      const defs = definitions.get(normalizeName(match.name));
      if (defs) {
        for (const d of defs) {
          pushLocFromRef({ name: d.name, uri: d.uri, line: d.line, start: 0, end: d.rawName.length, kind: "table" }, results, seen);
        }
      }
    } else {
      const byUri = referencesIndex.get(match.name);
      const localArr = byUri?.get(normUri) ?? [];
      if (localArr.length > 0) {
        for (const r of localArr) { pushLocFromRef(r, results, seen); }
      } else if (byUri) {
        for (const arr of byUri.values()) { for (const r of arr) { pushLocFromRef(r, results, seen); } }
      }
    }
  } else {
    const isBareColumnToken = !rawWord.includes(".") && !rawWord.startsWith("@") && !isSqlKeyword(rawNorm);
    if (parsed && offset >= 0) {
      const matchedResolution = parsed.columns?.resolutions?.find((r: any) => {
        const s = Number(r?.location?.start);
        const e = Number(r?.location?.end);
        return Number.isFinite(s) && Number.isFinite(e) && offset >= s && offset <= e;
      });
      if (matchedResolution) {
        const sources = getResolutionSourceColumns(matchedResolution);
        for (const src of sources) {
          const key = `${src.table}.${src.column}`;
          const byUri = referencesIndex.get(key);
          const localArr = byUri?.get(normUri) ?? [];
          if (localArr.length > 0) { for (const r of localArr) { pushLocFromRef(r, results, seen); } return results; }
          if (byUri) { for (const arr of byUri.values()) { for (const r of arr) { pushLocFromRef(r, results, seen); } } return results; }
        }
      }
    }

    if (isBareColumnToken && pos && parsed && offset >= 0) {
      const scopeAtPos = parsed.scope?.root?.findInnermost(offset) ?? parsed.scope?.root;
      const localDefsByName = new Map<string, any>();
      for (const def of definitions.get(normUri) ?? []) {
        localDefsByName.set(normalizeName(def.name), def);
      }
      const text = doc.getText();
      const fallbackLine = doc.getText({ start: { line: pos.line, character: 0 }, end: { line: pos.line, character: Number.MAX_VALUE } });
      const stmtText = computeCurrentStatement(text, offset, parsed, fallbackLine);
      const stmtOwner = findStatementLocalColumnOwner(stmtText, rawWord, scopeAtPos, parsed, offset, localDefsByName);
      if (stmtOwner?.ownerName) {
        const ownerNorm = normalizeName(stmtOwner.ownerName);
        const colNorm = normalizeName(rawWord);
        const key = `${ownerNorm}.${colNorm}`;
        const byUri = referencesIndex.get(key);
        const localArr = byUri?.get(normUri) ?? [];
        if (localArr.length > 0) { for (const r of localArr) { pushLocFromRef(r, results, seen); } return results; }
        if (byUri) { for (const arr of byUri.values()) { for (const r of arr) { pushLocFromRef(r, results, seen); } } return results; }
      }
    }
  }

  return results;
}

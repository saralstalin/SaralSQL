import { TextDocument } from "vscode-languageserver-textdocument";
import { Position } from "vscode-languageserver";
import {
  normalizeName,
  extractAliases,   
} from "./text-utils";

// Types
export type LocalTable = { name: string; columns: Array<{ name: string; type?: string }> };
export type LocalTableMap = Map<string, LocalTable>; // key: normalized-name-lower

// ---------- Core helpers ----------

// Absolute offset helper
export function getOffsetForPosition(doc: TextDocument, pos: Position): number {
  return typeof (doc as any).offsetAt === "function" ? (doc as any).offsetAt(pos) : undefined as any;
}

// Find last "CREATE [OR ALTER] PROCEDURE" index <= pos
export function findEnclosingCreateProcedureIndex(fullText: string, absOffset: number): number {
  try {
    const createRe = /create\s+(?:or\s+alter\s+)?procedure\b/ig;
    let match: RegExpExecArray | null;
    let lastIndex = -1;
    while ((match = createRe.exec(fullText))) {
      if (match.index <= absOffset) {
        lastIndex = match.index;
      } else {
        break;
      }
    }
    return lastIndex;
  } catch {
    return -1;
  }
}

// Header: from CREATE...PROCEDURE to AS|BEGIN (not inclusive)
export function getEnclosingProcedureHeader(fullText: string, absOffset: number): string | null {
  const startIdx = findEnclosingCreateProcedureIndex(fullText, absOffset);
  if (startIdx === -1) { return null; }
  const afterCreate = fullText.slice(startIdx);
  const endRe = /\b(as|begin)\b/i;
  const endMatch = endRe.exec(afterCreate);
  if (!endMatch) {
    return afterCreate.slice(0, 2000);
  }
  return afterCreate.slice(0, endMatch.index);
}

// Body: heuristic slice after AS|BEGIN until GO or first END or truncated length
export function getEnclosingProcedureBody(fullText: string, absOffset: number): string | null {
  const startIdx = findEnclosingCreateProcedureIndex(fullText, absOffset);
  if (startIdx === -1) { return null; }
  const afterCreate = fullText.slice(startIdx);
  const asBeginRe = /\b(as|begin)\b/i;
  const mm = asBeginRe.exec(afterCreate);
  if (!mm) { return afterCreate.slice(0, 10000); }
  const bodyStart = mm.index + mm[0].length;
  const candidate = afterCreate.slice(bodyStart);

  const goRe = /^\s*GO\s*$/gim;
  goRe.lastIndex = 0;
  const g = goRe.exec(candidate);
  if (g) { return candidate.slice(0, g.index); }
  const endIdx = candidate.search(/\bend\b/i);
  if (endIdx !== -1) { return candidate.slice(0, endIdx); }
  return candidate.slice(0, 20000);
}

// ---------- Parameter extraction ----------
const headerParamRe = /(@[A-Za-z0-9_]+)\s+([^,\r\n]+?)(?=\s*(?:,|AS\b|BEGIN\b|$))/ig;
export function extractParamsFromHeader(headerText: string | null): Map<string,string> {
  const map = new Map<string,string>();
  if (!headerText) { return map; }
  let m: RegExpExecArray | null;
  while ((m = headerParamRe.exec(headerText))) {
    const name = m[1];
    let type = (m[2] || "").trim();
    type = type.replace(/\s+/g, " ").replace(/,\s*$/, "");
    map.set(name.toLowerCase(), type);
  }
  return map;
}

// DECLARE variable extraction (multi-declare supported)
export function extractDeclareVarsFromText(text: string | null): Map<string,string> {
  const map = new Map<string,string>();
  if (!text) { return map; }
  const declareRe = /\bdeclare\b\s+([^;\r\n]+)/ig;
  let m: RegExpExecArray | null;
  while ((m = declareRe.exec(text))) {
    const declList = m[1];
    const parts: string[] = [];
    let cur = "";
    let depth = 0;
    for (let i = 0; i < declList.length; i++) {
      const ch = declList[i];
      if (ch === "(") { depth++; cur += ch; continue; }
      if (ch === ")") { if (depth>0) {depth--;} cur += ch; continue; }
      if (ch === "," && depth === 0) { parts.push(cur); cur = ""; continue; }
      cur += ch;
    }
    if (cur.trim() !== "") { parts.push(cur); }
    for (const pRaw of parts) {
      const p = pRaw.trim();
      const nmMatch = /^(@[A-Za-z0-9_]+)\s+(.+)$/is.exec(p);
      if (nmMatch) {
        const name = nmMatch[1];
        let type = nmMatch[2].trim();
        type = type.replace(/\s+$/g, "").replace(/,\s*$/g, "");
        type = type.replace(/\s+/g, " ");
        map.set(name.toLowerCase(), type);
      }
    }
  }
  return map;
}

// ---------- Local table definitions (table vars, temp tables, learn from insert/select into) ----------
function splitTopLevelCommas(s: string) {
  const parts: string[] = [];
  let cur = "";
  let depth = 0;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === "(") { depth++; cur += ch; continue; }
    if (ch === ")") { if (depth>0) {depth--;} cur += ch; continue; }
    if (ch === "," && depth === 0) { parts.push(cur); cur = ""; continue; }
    cur += ch;
  }
  if (cur.trim() !== "") {parts.push(cur);}
  return parts.map(p => p.trim()).filter(Boolean);
}

export function extractLocalTableDefsFromText(bodyText: string | null): LocalTableMap {
  const map = new Map<string, LocalTable>();
  if (!bodyText) { return map; }
  try {
    let m: RegExpExecArray | null;
    // DECLARE @tv TABLE (...)
    const declareTableRe = /\bdeclare\s+(@[A-Za-z0-9_]+)\s+table\s*\(([\s\S]*?)\)\s*(?:;|$)/ig;
    while ((m = declareTableRe.exec(bodyText))) {
      const varName = m[1];
      const colsRaw = m[2];
      const colsParts = splitTopLevelCommas(colsRaw);
      const cols: Array<{ name: string; type?: string }> = [];
      for (const part of colsParts) {
        const nameMatch = /^\s*(?:\[([^\]]+)\]|"([^"]+)"|`([^`]+)`|([A-Za-z0-9_]+))/i.exec(part);
        if (!nameMatch) { continue; }
        const colName = nameMatch[1] || nameMatch[2] || nameMatch[3] || nameMatch[4];
        let rest = part.slice((nameMatch[0] || "").length).trim();
        rest = rest.replace(/\bPRIMARY\s+KEY\b/ig, "")
                   .replace(/\bNOT\s+NULL\b/ig, "")
                   .replace(/\bNULL\b/ig, "")
                   .replace(/\bIDENTITY\s*\([^\)]*\)/ig, "")
                   .trim();
        const colType = rest ? rest.split(/\s+/).slice(0,3).join(" ") : undefined;
        cols.push({ name: colName, type: colType });
      }
      map.set(varName.toLowerCase(), { name: varName, columns: cols });
    }

    // CREATE TABLE #tmp (...)
    const createTempRe = /\bcreate\s+table\s+(#?[A-Za-z0-9_]+)\s*\(([\s\S]*?)\)\s*(?:;|$)/ig;
    while ((m = createTempRe.exec(bodyText))) {
      const tblName = m[1];
      const colsRaw = m[2];
      const colsParts = splitTopLevelCommas(colsRaw);
      const cols: Array<{ name: string; type?: string }> = [];
      for (const part of colsParts) {
        const nameMatch = /^\s*(?:\[([^\]]+)\]|"([^"]+)"|`([^`]+)`|([A-Za-z0-9_]+))/i.exec(part);
        if (!nameMatch) { continue; }
        const colName = nameMatch[1] || nameMatch[2] || nameMatch[3] || nameMatch[4];
        let rest = part.slice((nameMatch[0] || "").length).trim();
        rest = rest.replace(/\bPRIMARY\s+KEY\b/ig, "")
                   .replace(/\bNOT\s+NULL\b/ig, "")
                   .replace(/\bNULL\b/ig, "")
                   .replace(/\bIDENTITY\s*\([^\)]*\)/ig, "")
                   .trim();
        const colType = rest ? rest.split(/\s+/).slice(0,3).join(" ") : undefined;
        cols.push({ name: colName, type: colType });
      }
      map.set(tblName.toLowerCase(), { name: tblName, columns: cols });
    }

    // INSERT INTO #tmp (col1, col2, ...)
    const insertColsRe = /\binsert\s+into\s+(#?[A-Za-z0-9_]+)\s*\(([^)]+)\)/ig;
    while ((m = insertColsRe.exec(bodyText))) {
      const tblName = m[1];
      const colsList = m[2];
      const colNames = colsList.split(",").map(s => s.trim().replace(/^\[|\]$/g, "").replace(/^"|"$/g, "").replace(/^`|`$/g, "")).filter(Boolean);
      if (colNames.length > 0) {
        const key = tblName.toLowerCase();
        const existing = map.get(key);
        if (!existing) {
          map.set(key, { name: tblName, columns: colNames.map(n => ({ name: n })) });
        } else {
          const existingNames = new Set(existing.columns.map(c => c.name.toLowerCase()));
          for (const n of colNames) {
            if (!existingNames.has(n.toLowerCase())) { existing.columns.push({ name: n }); }
          }
        }
      }
    }

    // SELECT ... INTO #tmp
    const selectIntoRe = /\bselect\s+([\s\S]*?)\binto\s+(#?[A-Za-z0-9_]+)\b/ig;
    while ((m = selectIntoRe.exec(bodyText))) {
      const selectList = m[1];
      const tblName = m[2];
      const parts = splitTopLevelCommas(selectList);
      const colNames: string[] = [];
      for (const p of parts) {
        const asMatch = /\bas\s+(?:\[([^\]]+)\]|"([^"]+)"|`([^`]+)`|([A-Za-z0-9_]+))\b/i.exec(p);
        if (asMatch) {
          const name = asMatch[1] || asMatch[2] || asMatch[3] || asMatch[4];
          colNames.push(name);
          continue;
        }
        const endIdent = /(?:\[([^\]]+)\]|"([^"]+)"|`([^`]+)`|([A-Za-z0-9_]+))\s*$/i.exec(p);
        if (endIdent) {
          const name = endIdent[1] || endIdent[2] || endIdent[3] || endIdent[4];
          colNames.push(name);
        }
      }
      if (colNames.length > 0) {
        const key = tblName.toLowerCase();
        const existing = map.get(key);
        if (!existing) {
          map.set(key, { name: tblName, columns: colNames.map(n => ({ name: n })) });
        } else {
          const existingNames = new Set(existing.columns.map(c => c.name.toLowerCase()));
          for (const n of colNames) {
            if (!existingNames.has(n.toLowerCase())) { existing.columns.push({ name: n }); }
          }
        }
      }
    }
  } catch {
    // best-effort only
  }
  return map;
}

// ---------- Candidate tables from a statement ----------
export function collectCandidateTablesFromStatement(stmtText: string): Set<string> {
  const candidateTables = new Set<string>();
  try {
    // UPDATE (allow @ / #)
    const updateRe = /\bupdate\s+([@#]?[a-zA-Z0-9_\[\]\.]+)/i;
    const upd = updateRe.exec(stmtText);
    if (upd && upd[1]) { candidateTables.add(normalizeName(upd[1])); }

    // INSERT (allow optional INTO and @ / # temp or table vars)
    const insertRe = /\binsert\s+(?:into\s+)?([@#]?[a-zA-Z0-9_\[\]\.]+)/i;
    const ins = insertRe.exec(stmtText);
    if (ins && ins[1]) { candidateTables.add(normalizeName(ins[1])); }

    // DELETE FROM (allow @ / #)
    const deleteRe = /\bdelete\s+from\s+([@#]?[a-zA-Z0-9_\[\]\.]+)/i;
    const del = deleteRe.exec(stmtText);
    if (del && del[1]) { candidateTables.add(normalizeName(del[1])); }

    // FROM / JOIN (allow optional alias; capture table token possibly prefixed with @/#)
    const fromJoinRe = /\b(from|join)\s+([@#]?[a-zA-Z0-9_\[\]\.]+)(?:\s+(?:as\s+)?([a-zA-Z0-9_\[\]]+))?/gi;
    let m: RegExpExecArray | null;
    while ((m = fromJoinRe.exec(stmtText))) {
      const tableTok = m[2];
      candidateTables.add(normalizeName(tableTok));
    }

  } catch {
    // ignore
  }
  return candidateTables;
}

// ---------- Alias resolution (map alias token to actual target using extractAliases) ----------
export function resolveAliasTarget(stmtText: string, aliasToken: string): string | null {
  try {
    const aliases = extractAliases(stmtText || "");
    const key = aliasToken.replace(/^\[|\]$/g, "").toLowerCase();
    const mapped = aliases.get(key);
    if (mapped) { return String(mapped); }
    return null;
  } catch {
    return null;
  }
}

// ---------- Combined builders used by hover/validate ----------

export function buildParamMapForDocAtPos(doc: TextDocument, pos: Position): Map<string,string> {
  const abs = getOffsetForPosition(doc, pos);
  const full = doc.getText();
  const header = getEnclosingProcedureHeader(full, abs);
  const body = getEnclosingProcedureBody(full, abs);
  const m = extractParamsFromHeader(header);
  const dm = extractDeclareVarsFromText(body);
  for (const [k,v] of dm.entries()) {
    if (!m.has(k)) {m.set(k, v);}
  }
  return m;
}

export function buildLocalTableMapForDocAtPos(doc: TextDocument, pos: Position): LocalTableMap {
  const abs = getOffsetForPosition(doc, pos);
  const full = doc.getText();
  const body = getEnclosingProcedureBody(full, abs);
  return extractLocalTableDefsFromText(body);
}


/**
 * Normalize a table key for maps while preserving @/# prefix for locals.
 * e.g. "@MyTV" -> "@mytv", "#Tmp" -> "#tmp", "dbo.MyTbl" -> "mytbl" (strip dbo.)
 */

// ---------- Local table checks (explicit) ----------
/**
 * Returns true **only** if the token is a local table token (starts with @ or #)
 * AND the provided localMap contains a definition for it.
 *
 * Use this instead of `localMap.has(normalizeTableKey(...))` when you want to be
 * sure only true "local" tables (table variables / temp tables) are treated as local.
 *
 * Example:
 *   const localMap = buildLocalTableMapForDocAtPos(doc, pos);
 *   if (isTokenLocalTable(' @tv', localMap)) { ... } // true only if @tv exists
 */
export function isTokenLocalTable(token: string | null | undefined, localMap?: LocalTableMap | null): boolean {
  if (!token) {return false;}
  // trim quotes/brackets but keep leading @ / #
  const cleaned = String(token).trim().replace(/^\[|\]$/g, "").replace(/^"|"$/g, "").replace(/`/g, "");
  if (!cleaned) {return false;}
  if (!(cleaned.startsWith("@") || cleaned.startsWith("#"))) {
    // NOT a local table token by prefix, so false regardless of map contents
    return false;
  }
  const key = cleaned.toLowerCase();
  return Boolean(localMap && localMap.has(key));
}
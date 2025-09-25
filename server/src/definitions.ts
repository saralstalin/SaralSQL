
import { Location } from "vscode-languageserver/node";
import {
    normalizeName
    , stripComments
    , getLineStarts
    , offsetToPosition
    , parseColumnsFromCreateBlock
    , extractAliases
    , isSqlKeyword
    , getCurrentStatement
} from "./text-utils";
import * as url from "url";
import { isAstPoolReady, parseSqlWithWorker } from "./parser-pool";
import { resolveColumnFromAst, walkAst, extractColumnName } from "./ast-utils";
import { collectCandidateTablesFromStatement } from "./sql-scope-utils";

export interface ColumnDef {
    name: string;
    rawName: string;
    type?: string;
    line: number;
    start?: number;
    end?: number;
}

export interface SymbolDef {
    name: string;
    rawName: string;
    uri: string;
    line: number;
    columns?: ColumnDef[],
    kind?: string;
}

export interface ReferenceDef {
    name: string; // normalized table or column name
    uri: string;
    line: number;
    start: number;
    end: number;
    kind: "table" | "column" | "parameter";
}

export const columnsByTable = new Map<string, Set<string>>();
export const aliasesByUri = new Map<string, Map<string, string>>();
export const definitions = new Map<string, SymbolDef[]>();
export const referencesIndex = new Map<string, Map<string, ReferenceDef[]>>();
export const tablesByName = new Map<string, SymbolDef>();
export const tableTypesByName = new Map<string, SymbolDef>();

const MAX_FILE_SIZE_BYTES = 12 * 1024;   // 12 KB (skip larger files)
const MAX_REFS_PER_FILE = 5000;         // cap number of reference objects created per file
const DEPLOY_BLOCK_SUBSTRING = "deploy"; // block any path that contains this substring

let isIndexReady = false;

export function setIndexReady() {
    isIndexReady = true;
}

export function getIndexReady(): boolean {
    return isIndexReady;
}

export function setRefsForFile(name: string, uri: string, refs: ReferenceDef[]) {
    let byUri = referencesIndex.get(name);
    if (!byUri) { byUri = new Map(); referencesIndex.set(name, byUri); }
    byUri.set(uri, refs);
}

export function deleteRefsForFile(uri: string) {
    for (const [, byUri] of referencesIndex) { byUri.delete(uri); }
    for (const [name, byUri] of Array.from(referencesIndex.entries())) {
        if (byUri.size === 0) { referencesIndex.delete(name); }
    }
}

export function getRefs(name: string): ReferenceDef[] {
    const byUri = referencesIndex.get(name);
    if (!byUri) { return []; }
    const out: ReferenceDef[] = [];
    for (const arr of byUri.values()) {
        out.push(...arr);
    }
    return out;
}

export function findColumnInTable(tableName: string, colName: string): Location[] {
    const results: Location[] = [];
    for (const defs of definitions.values()) {
        for (const def of defs) {
            if (normalizeName(def.name) === normalizeName(tableName) && def.columns) {
                for (const col of def.columns) {
                    if (col.name === colName) {
                        const startChar = typeof (col as any).start === "number" ? (col as any).start : 0;
                        const endChar = typeof (col as any).end === "number" ? (col as any).end : 200;
                        results.push({
                            uri: def.uri,
                            range: {
                                start: { line: col.line, character: startChar },
                                end: { line: col.line, character: endChar }
                            }
                        });
                    }
                }
            }
        }
    }
    return results;
}

export function findTableOrColumn(word: string): Location[] {
    const results: Location[] = [];
    for (const defs of definitions.values()) {
        for (const def of defs) {
            if (normalizeName(def.name) === word) {
                results.push({
                    uri: def.uri,
                    range: {
                        start: { line: def.line, character: 0 },
                        end: { line: def.line, character: 200 }
                    }
                });
            }
            if (def.columns) {
                for (const col of def.columns) {
                    if (col.name === word) {
                        results.push({
                            uri: def.uri,
                            range: {
                                start: { line: col.line, character: 0 },
                                end: { line: col.line, character: 200 }
                            }
                        });
                    }
                }
            }
        }
    }
    return results;
}

// ---------- Indexing ----------
export function indexText(uri: string, text: string): void {
    const normUri = uri.startsWith("file://") ? uri : url.pathToFileURL(uri).toString();

    if (normUri.toLowerCase().includes(`/${DEPLOY_BLOCK_SUBSTRING}`) || normUri.toLowerCase().includes(`\\${DEPLOY_BLOCK_SUBSTRING}`)) {
        return;
    }

    if (!normUri.toLowerCase().endsWith(".sql")) {
        return;
    }

    // 2) Skip files that are larger than the configured threshold (avoid heavy files)
    const byteLen = Buffer.byteLength(text || "", "utf8");
    if (byteLen > MAX_FILE_SIZE_BYTES) {
        return;
    }

    // Reset any old defs/refs for this file (purge stale def caches too)
    const oldDefs = definitions.get(normUri) || [];
    for (const d of oldDefs) { tablesByName.delete(d.name); columnsByTable.delete(d.name); }
    definitions.delete(normUri);
    deleteRefsForFile(normUri);

    const defs: SymbolDef[] = [];
    const lines = text.split(/\r?\n/);
    const cleanLines = lines.map(stripComments); // <-- compute once
    const localRefs: ReferenceDef[] = [];
    const cleanText = cleanLines.join("\n");

    // Precompile regexes once (reset lastIndex before each use)
    const tableRegexG = /\b(from|join|update|into)\s+([a-zA-Z0-9_\[\]\.]+)/gi;
    const aliasColRegexG = /([a-zA-Z0-9_]+)\.(\[[^\]]+\]|[a-zA-Z_][a-zA-Z0-9_]*)/g;
    const bareColRegexG = /\[([a-zA-Z0-9_ ]+)\]|(?<!@)\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
    const fromJoinRegexG = /\b(from|join)\s+([a-zA-Z0-9_\[\]\.]+)/gi;
    const procHeaderRegex = /create\s+(?:or\s+alter\s+)?(procedure|proc|function)\s+([\[\]\w\."']+(?:\.[\[\]\w\."']+)*)\s*\(([\s\S]*?)\)\s*(as|begin)/ig;

    // parse procedure/function parameters into localRefs (unchanged)
    let m: RegExpExecArray | null;
    while ((m = procHeaderRegex.exec(cleanText)) !== null) {
        const wholeMatch = m[0];
        const paramsBlock = m[3];
        const matchIndex = m.index; // absolute offset of start of wholeMatch in clean

        const openParenInMatch = wholeMatch.indexOf('(');
        const paramsStartAbs = (openParenInMatch >= 0)
            ? (matchIndex + openParenInMatch + 1)
            : (matchIndex + wholeMatch.indexOf(paramsBlock));

        const paramTokenRegex = /@([A-Za-z_][A-Za-z0-9_]*)\b/g;
        let pm: RegExpExecArray | null;
        while ((pm = paramTokenRegex.exec(paramsBlock)) !== null) {
            const raw = '@' + pm[1];
            const absOffset = paramsStartAbs + pm.index;
            const lineStarts = getLineStarts(cleanText);
            const { line, character } = offsetToPosition(absOffset, lineStarts);
            localRefs.push({
                name: raw,
                uri: normUri,
                line,
                start: character,
                end: character + raw.length,
                kind: "parameter"
            });
        }
    }

    // Cache for statement scopes per line (avoids O(N²))
    type StmtScope = {
        text: string;
        tablesInScope: string[];
        stmtAliases?: Map<string, string>;
    };
    const stmtScopeByLine = new Map<number, StmtScope>();

    // --- scan for definitions ---
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        const match = /^\s*CREATE\s+(PROCEDURE|FUNCTION|VIEW|TABLE|TYPE)\s+([a-zA-Z0-9_\[\]\.]+)/i.exec(line);
        if (!match) { continue; }

        const kind = match[1].toUpperCase();
        const rawName = match[2];
        const norm = normalizeName(rawName);

        const sym: SymbolDef = { name: norm, rawName, uri: normUri, line: i, kind };

        if (kind === "TABLE" || kind === "VIEW" || (kind === "TYPE" && /\bAS\s+TABLE\b/i.test(line))) {
            // Build block from current line and parse columns (linear helper)
            const createBlock = lines.slice(i).join("\n");
            let cols = parseColumnsFromCreateBlock(createBlock, 0);
            console.debug(`[indexText] ${rawName} parseColumnsFromCreateBlock -> ${cols?.length ?? 0}`);

            if (kind === "VIEW") {
                try {
                    const viewCols = parseColumnsFromCreateView(createBlock, i);
                    console.debug(`[indexText] parseColumnsFromCreateView -> ${Array.isArray(viewCols) ? viewCols.length : "?"}`);

                    if (viewCols && viewCols.length) {
                        cols = Array.isArray(cols) ? cols : [];

                        // map into the exact shape cols expects (start/end guaranteed numbers)
                        const mapped = viewCols.map(v => {
                            const raw = String(v.rawName ?? v.name ?? "")
                                .replace(/^(?:\s*(?:\[[^\]]+\]|"[^"]+"|`[^`]+`|[A-Za-z_][\w@#]*)\s*\.)+/g, "") // strip leading qualifiers like d. or [dbo].
                                .replace(/^[\[\]`"]|[\[\]`"]$/g, "") // strip surrounding brackets/quotes from final token
                                .trim();
                            const normalizedName = normalizeName(raw);
                            const lineNum = (typeof v.line === "number") ? v.line : i;
                            const startNum = (typeof v.start === "number") ? v.start : 0;
                            const endNum = (typeof v.end === "number") ? v.end : (raw.length);

                            return {
                                name: normalizedName,
                                rawName: raw,
                                type: v.type,
                                line: lineNum,
                                start: startNum,
                                end: endNum
                            } as { name: string; rawName: string; type?: string; line: number; start: number; end: number };
                        });

                        cols.push(...mapped);
                    }
                } catch (e) {
                    console.debug("[indexText] parseColumnsFromCreateView failed:", e);
                }
            }

            // Attempt optional AST parse for this CREATE block to discover any additional columns (pool-only)
            if (kind === "TABLE" || (kind === "TYPE" && /\bAS\s+TABLE\b/i.test(line))) {
                try {
                    if (!isAstPoolReady()) {
                        //Nothing
                    } else {
                        // Fire-and-forget parse with short timeout so we don't block indexing
                        parseSqlWithWorker(createBlock, { database: "mssql" }, 800)
                            .then((ast) => {
                                try {
                                    if (!ast) { return; }
                                    const astColsRaw: string[] = [];
                                    if (ast && (ast.type === "create" || (Array.isArray(ast) && ast.length && ast[0].type === "create"))) {
                                        const node = Array.isArray(ast) ? ast[0] : ast;
                                        const defs = node.create_definitions || node.createDefinitions || [];
                                        for (const d of defs) {
                                            try {
                                                const cname = (d.column && (d.column.column || d.column.name)) || d.name || d.field;
                                                if (cname) { astColsRaw.push(String(cname).trim()); }
                                            } catch { /* ignore per-column errors */ }
                                        }
                                    }

                                    if (astColsRaw.length) {
                                        // prepare sets for dedupe
                                        const existingCols = new Set<string>(cols.map(c => normalizeName(c.name)));
                                        const existingSymCols = new Set<string>((sym.columns || []).map((c: any) => normalizeName((c.rawName || c.name || "").replace(/^\[|\]$/g, ""))));
                                        const set = columnsByTable.get(norm) || new Set<string>();

                                        // collect new local refs that we will add (deduped by pos)
                                        const newLocalRefs: ReferenceDef[] = [];

                                        for (const rawAc of astColsRaw) {
                                            const acNorm = normalizeName(rawAc);
                                            if (existingCols.has(acNorm) || existingSymCols.has(acNorm) || set.has(acNorm)) {
                                                continue;
                                            }

                                            // Add to cols list (keeps downstream logic compatible)
                                            cols.push({ name: acNorm, rawName: rawAc, line: i /* fallback */, start: 0, end: rawAc.length });

                                            // Add to per-table set
                                            set.add(acNorm);

                                            // Add to sym.columns, avoiding duplicates
                                            if (!sym.columns) { sym.columns = []; }
                                            const alreadyInSym = (sym.columns as any[]).some((c: any) => normalizeName((c.rawName || c.name || "").replace(/^\[|\]$/g, "")) === acNorm);
                                            if (!alreadyInSym) {
                                                (sym.columns as any[]).push({
                                                    name: acNorm,
                                                    rawName: rawAc,
                                                    line: i + 1,
                                                    start: 0,
                                                    end: rawAc.length
                                                });
                                            }

                                            // prepare local ref
                                            const newRef: ReferenceDef = {
                                                name: `${norm}.${acNorm}`,
                                                uri: normUri,
                                                line: i + 1,
                                                start: 0,
                                                end: rawAc.length,
                                                kind: "column"
                                            };
                                            const posKey = `${newRef.line}:${newRef.start}:${newRef.end}`;
                                            if (!newLocalRefs.some(r => `${r.line}:${r.start}:${r.end}` === posKey)) {
                                                newLocalRefs.push(newRef);
                                            }
                                        }

                                        // persist set back
                                        columnsByTable.set(norm, set);

                                        // merge newLocalRefs into the per-file references (dedupe by position)
                                        for (const ref of newLocalRefs) {
                                            const key = ref.name;
                                            const existingForKey = getRefs(key).filter(r => r.uri === normUri);

                                            const mergedByPos = new Map<string, ReferenceDef>();
                                            for (const r of existingForKey) {
                                                mergedByPos.set(`${r.line}:${r.start}:${r.end}`, r);
                                            }
                                            mergedByPos.set(`${ref.line}:${ref.start}:${ref.end}`, ref);

                                            const merged = Array.from(mergedByPos.values());
                                            setRefsForFile(key, normUri, merged);
                                        }
                                    }
                                } catch (mergeErr) {
                                }
                            })
                            .catch((pErr) => {
                            });
                    }
                } catch (e) {
                }
            }

            // Keep columnsByTable in sync (normalized)
            const set = new Set<string>();
            for (const c of cols) { set.add(normalizeName(c.name)); }
            columnsByTable.set(norm, set);

            // Definitions for columns with correct absolute positions
            (sym as any).columns = cols.map(c => {
                // c.line is already absolute (0-based) because parseColumnsFromCreateView was passed `i`
                const fileLine = c.line;
                const sourceLineText = lines[fileLine] || "";

                // improved start detection: try rawName, then name, then rawName without qualifier, then first non-space
                let startCol = -1;
                if (c.rawName) { startCol = sourceLineText.indexOf(String(c.rawName)); }
                if (startCol < 0 && c.name) { startCol = sourceLineText.indexOf(String(c.name)); }
                if (startCol < 0 && c.rawName) {
                    const noQual = String(c.rawName).replace(/^[^.]+\./, "");
                    startCol = sourceLineText.indexOf(noQual);
                }
                if (startCol < 0) { startCol = Math.max(0, sourceLineText.search(/\S/)); }

                return {
                    name: c.name,
                    rawName: c.rawName,
                    type: c.type,
                    line: fileLine,
                    start: startCol,
                    end: startCol + (c.rawName ? String(c.rawName).length : String(c.name).length)
                };
            }) as any;

            // Local column refs (definition locations)
            for (const c of cols) {
                const fileLine = c.line;
                const sourceLineText = lines[fileLine] || "";

                let startCol = -1;
                if (c.rawName) { startCol = sourceLineText.indexOf(String(c.rawName)); }
                if (startCol < 0 && c.name) { startCol = sourceLineText.indexOf(String(c.name)); }
                if (startCol < 0 && c.rawName) {
                    const noQual = String(c.rawName).replace(/^[^.]+\./, "");
                    startCol = sourceLineText.indexOf(noQual);
                }
                if (startCol < 0) { startCol = Math.max(0, sourceLineText.search(/\S/)); }

                localRefs.push({
                    name: `${norm}.${c.name}`,
                    uri: normUri,
                    line: fileLine,
                    start: startCol,
                    end: startCol + (c.rawName ? String(c.rawName).length : String(c.name).length),
                    kind: "column"
                });
            }

        } // end if table/view/type

        defs.push(sym);

        // Definition ref for the object name on its CREATE line
        const idx = line.toLowerCase().indexOf(rawName.toLowerCase());
        if (idx >= 0) {
            localRefs.push({
                name: norm,
                uri: normUri,
                line: i,
                start: idx,
                end: idx + rawName.length,
                kind: "table"
            });
        }
    } // end for definitions

    // Aliases computed once and reused
    const aliases = extractAliases(text);

    // --- scan for usages (tables, alias columns, bare columns) ---
    for (let i = 0; i < lines.length; i++) {
        const clean = cleanLines[i];

        // table usages
        tableRegexG.lastIndex = 0;
        for (let m = tableRegexG.exec(clean); m; m = tableRegexG.exec(clean)) {
            const raw = m[2];
            const tnorm = normalizeName(raw);

            const keywordIndex = m.index;
            const tableIndex = keywordIndex + m[0].indexOf(raw);

            localRefs.push({
                name: tnorm,
                uri: normUri,
                line: i,
                start: tableIndex,
                end: tableIndex + raw.length,
                kind: "table"
            });
        }

        // alias.column usages
        aliasColRegexG.lastIndex = 0;
        for (let m2 = aliasColRegexG.exec(clean); m2; m2 = aliasColRegexG.exec(clean)) {
            const alias = m2[1].toLowerCase();
            const col = normalizeName(m2[2].replace(/[\[\]]/g, ""));

            // prefer statement-scoped aliases (fall back to file-level aliases map)
            const stmt = stmtScopeByLine.get(i);
            const stmtAliases = stmt?.stmtAliases;
            let table = stmtAliases?.get(alias) || aliases.get(alias);

            if (!table) { continue; }

            // if alias maps to a subquery token (your extractAliases might mark it specially), skip mapping
            if (typeof table === "string" && table.toLowerCase() === "__subquery__") {
                // do not create a table-column ref for subquery aliases
                continue;
            }

            const key = `${normalizeName(table)}.${col}`;
            localRefs.push({
                name: key,
                uri: normUri,
                line: i,
                start: m2.index,
                end: m2.index + m2[0].length,
                kind: "column"
            });

        }

        // unaliased column usages (allow [Column])
        bareColRegexG.lastIndex = 0;
        for (let m3 = bareColRegexG.exec(clean); m3; m3 = bareColRegexG.exec(clean)) {
            const tokenRaw = (m3[1] || m3[2] || "");
            const col = normalizeName(tokenRaw.replace(/[\[\]]/g, ""));
            if (isSqlKeyword(col)) { continue; }

            // skip if part of alias.column
            const tokenStart = m3.index;
            const tokenEnd = m3.index + (m3[0] ? m3[0].length : tokenRaw.length);
            const beforeChar = clean.charAt(Math.max(0, tokenStart - 1));
            const afterChar = clean.charAt(tokenEnd);
            if (beforeChar === "." || afterChar === "." || beforeChar === "@") { continue; }

            // Get (cached) statement text and tables-in-scope for this line
            let scope = stmtScopeByLine.get(i);
            if (!scope) {
                const stmtText = getCurrentStatement(
                    { getText: () => text } as any,
                    { line: i, character: 0 }
                );

                // Use robust helper to collect candidate tables for this statement.
                let tablesInScopeArr: string[] = [];
                try {
                    const maybe = collectCandidateTablesFromStatement(stmtText);
                    if (Array.isArray(maybe)) {
                        tablesInScopeArr = maybe;
                    } else if (maybe instanceof Set) {
                        tablesInScopeArr = Array.from(maybe);
                    } else if (maybe) {
                        try {
                            tablesInScopeArr = Array.from(maybe as Iterable<string>);
                        } catch {
                            tablesInScopeArr = [];
                        }
                    } else {
                        tablesInScopeArr = [];
                    }
                } catch (e) {
                    // fallback to previous simple regex if helper fails
                    const tmp: string[] = [];
                    fromJoinRegexG.lastIndex = 0;
                    for (let fm = fromJoinRegexG.exec(stmtText); fm; fm = fromJoinRegexG.exec(stmtText)) {
                        tmp.push(normalizeName(fm[2]));
                    }
                    tablesInScopeArr = tmp;
                }

                // Compute statement-scoped aliases from the statement text (important!)
                let stmtAliasesMap: Map<string, string>;
                try {
                    stmtAliasesMap = extractAliases(stmtText) || new Map();
                } catch (e) {
                    stmtAliasesMap = new Map();
                }

                const newScope: StmtScope = {
                    text: stmtText,
                    tablesInScope: tablesInScopeArr,
                    stmtAliases: stmtAliasesMap
                };

                scope = newScope;
                stmtScopeByLine.set(i, scope);
            }

            // --- AST disambiguation for UPDATE/INSERT/DELETE ---
            try {
                if (isAstPoolReady()) {
                    parseSqlWithWorker(scope.text, { database: "transactsql" }, 700)
                        .then((ast) => {
                            if (!ast) { return; }
                            let targetTable: string | null = null;
                            if (ast.type === "update" || ast.type === "insert" || ast.type === "delete") {
                                targetTable = ast.table?.[0]?.table || null;
                            }
                            if (targetTable) {
                                const resolvedNorm = normalizeName(targetTable);
                                const resolvedKey = `${resolvedNorm}.${col}`;
                                const existingForResolved = (referencesIndex.get(resolvedKey)?.get(normUri)) || [];
                                const hasRef = existingForResolved.some(r => r.line === i && r.start === tokenStart && r.end === tokenEnd);
                                if (!hasRef) {
                                    const newRef: ReferenceDef = {
                                        name: resolvedKey,
                                        uri: normUri,
                                        line: i,
                                        start: tokenStart,
                                        end: tokenEnd,
                                        kind: "column"
                                    };
                                    const mergedByPos = new Map<string, ReferenceDef>();
                                    for (const r of existingForResolved) {
                                        mergedByPos.set(`${r.line}:${r.start}:${r.end}`, r);
                                    }
                                    mergedByPos.set(`${newRef.line}:${newRef.start}:${newRef.end}`, newRef);
                                    setRefsForFile(resolvedKey, normUri, Array.from(mergedByPos.values()));
                                }
                            }
                        })
                        .catch(() => { });
                }
            } catch { }

            const { tablesInScope } = scope;
            if (tablesInScope.length === 0) { continue; } // fast skip

            const candidateTables: string[] = [];
            for (const t of tablesInScope) {
                if (columnsByTable.get(t)?.has(col)) { candidateTables.push(t); }
            }

            if (candidateTables.length === 1) {
                localRefs.push({
                    name: `${candidateTables[0]}.${col}`,
                    uri: normUri,
                    line: i,
                    start: m3.index,
                    end: m3.index + tokenRaw.length,
                    kind: "column"
                });
            } else if (candidateTables.length > 1) {
                // keep current immediate behavior (add multiple candidate refs)
                for (const t of candidateTables) {
                    localRefs.push({
                        name: `${t}.${col}`,
                        uri: normUri,
                        line: i,
                        start: m3.index,
                        end: m3.index + tokenRaw.length,
                        kind: "column"
                    });
                }

                // capture values for the async closure
                const stmtText = scope.text;
                const tokenStart = m3.index;
                const tokenEnd = m3.index + tokenRaw.length;
                const localCandidateTables = [...candidateTables];

                // Attempt AST-based disambiguation asynchronously (pool-only)
                try {
                    if (!isAstPoolReady()) {
                    } else {
                        parseSqlWithWorker(stmtText, { database: "mssql" }, 700)
                            .then((ast) => {
                                try {
                                    const resolved = resolveColumnFromAst(ast, col);
                                    if (!resolved) { return; }

                                    let resolvedNorm = normalizeName(resolved);

                                    // resolved might be an alias name; map via statement aliases if needed
                                    const stmtAliases = extractAliases(stmtText);
                                    const mapped = stmtAliases.get(resolved.toLowerCase());
                                    if (mapped) { resolvedNorm = normalizeName(mapped); }

                                    // only act if AST resolved to one of our candidate tables
                                    if (!localCandidateTables.includes(resolvedNorm)) { return; }

                                    const resolvedKey = `${resolvedNorm}.${col}`;

                                    // ensure resolved key has a ref for this file/position
                                    const existingForResolved = (referencesIndex.get(resolvedKey)?.get(normUri)) || [];
                                    const hasRef = existingForResolved.some(r => r.line === i && r.start === tokenStart && r.end === tokenEnd);
                                    if (!hasRef) {
                                        const newRef = { name: resolvedKey, uri: normUri, line: i, start: tokenStart, end: tokenEnd, kind: "column" } as ReferenceDef;
                                        // merge with any other refs for this uri (preserve others) and dedupe by pos
                                        const mergedByPos = new Map<string, ReferenceDef>();
                                        for (const r of existingForResolved) {
                                            mergedByPos.set(`${r.line}:${r.start}:${r.end}`, r);
                                        }
                                        mergedByPos.set(`${newRef.line}:${newRef.start}:${newRef.end}`, newRef);
                                        setRefsForFile(resolvedKey, normUri, Array.from(mergedByPos.values()));
                                    }

                                    // remove the previously-added refs for other candidate tables at this exact position
                                    for (const t of localCandidateTables) {
                                        if (t === resolvedNorm) { continue; }
                                        const key = `${t}.${col}`;
                                        const byUri = referencesIndex.get(key);
                                        const arrForThis = byUri?.get(normUri) || [];
                                        const filtered = arrForThis.filter(r => !(r.line === i && r.start === tokenStart && r.end === tokenEnd));
                                        setRefsForFile(key, normUri, filtered);
                                    }

                                    // safeLog(`[AST disambiguation] ${col} → ${resolvedNorm} in ${normUri}`);
                                } catch (e) {
                                    //safeLog('[AST disambiguation] merge failed: ' + String(e));
                                }
                            })
                            .catch((pErr) => {
                                // safeLog('[AST disambiguation] parse failed: ' + String(pErr));
                            });
                    }
                } catch (e) {
                    //safeLog('[indexText][AST disambiguation] unexpected: ' + String(e));
                }
            }
        }
    }

    // Save definitions
    definitions.set(normUri, defs);

    // De-dupe local refs and persist per-file by name
    const byName = new Map<string, ReferenceDef[]>();
    for (const ref of localRefs) { const arr = byName.get(ref.name) || []; arr.push(ref); byName.set(ref.name, arr); }
    for (const [name, arr] of byName) {
        const seen = new Set<string>();
        const dedup = arr.filter(r => { const k = `${r.line}:${r.start}:${r.end}`; if (seen.has(k)) { return false; } seen.add(k); return true; });
        setRefsForFile(name, normUri, dedup);
    }

    // Save aliases (reuse the map we already computed)
    aliasesByUri.set(normUri, aliases);

    // Index tables by name for quick definition lookup
    for (const def of defs) {
        if (!def.columns || !Array.isArray(def.columns) || def.columns.length === 0) { continue; }
        const kindNorm = (def.kind || "").toUpperCase();
        if (kindNorm === "TABLE" || kindNorm === "VIEW") {
            tablesByName.set(def.name, def);
        }
        else if (kindNorm === "TYPE") {
            // This is a table-type (CREATE TYPE ... AS TABLE) that we parsed with columns.
            // Store it separately so hover can show "Table Type".
            tableTypesByName.set(def.name, def);
        }
    }
}



export function parseColumnsFromCreateView(viewText: string, fileStartLine = 0): ColumnDef[] {
    const t = viewText;
    const lower = t.toLowerCase();

    // find "create" then the next "view"
    const createIdx = lower.indexOf("create");
    const viewIdx = lower.indexOf("view", createIdx === -1 ? 0 : createIdx);
    if (viewIdx === -1) { return []; }

    // find "AS" token (word boundary, robust to newlines)
    const afterView = t.substring(viewIdx + "view".length);
    const asMatch = /\bas\b/i.exec(afterView);
    const asIdx = asMatch ? viewIdx + "view".length + asMatch.index : -1;

    // find SELECT token after AS (or after view if no AS)
    const selectSearchStart = asIdx !== -1 ? asIdx + 2 : viewIdx + "view".length;
    const afterSelectSearch = t.substring(selectSearchStart);
    const selectMatch = /\bselect\b/i.exec(afterSelectSearch);
    if (!selectMatch) { return []; }
    const selectIdx = selectSearchStart + selectMatch.index;

    // scan forward to find the top-level FROM (respecting parentheses, quotes, bracket identifiers)
    let i = selectIdx + "select".length;
    let paren = 0;
    let inSingle = false;
    let inDouble = false;
    let inBracket = false;
    let fromIdx = -1;
    const isWordChar = (c: string) => /[a-zA-Z0-9_]/.test(c);

    while (i < t.length) {
        const ch = t[i];

        // handle quoting/brackets
        if (!inDouble && ch === "'" && !inBracket) {
            if (inSingle && t[i + 1] === "'") { i += 2; continue; }
            inSingle = !inSingle; i++; continue;
        }
        if (!inSingle && ch === '"' && !inBracket) {
            if (inDouble && t[i + 1] === '"') { i += 2; continue; }
            inDouble = !inDouble; i++; continue;
        }
        if (!inSingle && !inDouble) {
            if (ch === "[") { inBracket = true; i++; continue; }
            if (ch === "]") { inBracket = false; i++; continue; }
        }
        if (inSingle || inDouble || inBracket) { i++; continue; }

        // parentheses
        if (ch === "(") { paren++; i++; continue; }
        if (ch === ")") { if (paren > 0) { paren--; } i++; continue; }

        // potential top-level 'from'
        if (paren === 0) {
            // check for 'from' word at i
            const maybe = lower.substr(i, 4);
            if (maybe === "from") {
                const before = i - 1 >= 0 ? lower[i - 1] : " ";
                const after = lower[i + 4] || " ";
                if (!isWordChar(before) && !isWordChar(after)) {
                    fromIdx = i;
                    break;
                }
            }
        }

        i++;
    }

    // fallback: use regex word-boundary search for "from" after select
    if (fromIdx === -1) {
        const afterSelect = t.substring(selectIdx + "select".length);
        const fallback = /\bfrom\b/i.exec(afterSelect);
        fromIdx = fallback ? selectIdx + "select".length + fallback.index : t.length;
    }

    const projStart = selectIdx + "select".length;
    const projText = t.substring(projStart, fromIdx);

    // split top-level commas (respect quotes/brackets/parens)
    const segments: { text: string; relStart: number; relEnd: number }[] = [];
    let j = 0;
    let segStart = 0;
    paren = 0; inSingle = false; inDouble = false; inBracket = false;
    while (j < projText.length) {
        const ch = projText[j];

        if (!inDouble && ch === "'" && !inBracket) {
            if (inSingle && projText[j + 1] === "'") { j += 2; continue; }
            inSingle = !inSingle; j++; continue;
        }
        if (!inSingle && ch === '"' && !inBracket) {
            if (inDouble && projText[j + 1] === '"') { j += 2; continue; }
            inDouble = !inDouble; j++; continue;
        }
        if (!inSingle && !inDouble) {
            if (ch === "[") { inBracket = true; j++; continue; }
            if (ch === "]") { inBracket = false; j++; continue; }
        }
        if (inSingle || inDouble || inBracket) { j++; continue; }

        if (ch === "(") { paren++; j++; continue; }
        if (ch === ")") { if (paren > 0) { paren--; } j++; continue; }

        if (paren === 0 && ch === ",") {
            const seg = projText.substring(segStart, j).trim();
            if (seg.length > 0) { segments.push({ text: seg, relStart: segStart, relEnd: j }); }
            segStart = j + 1;
        }
        j++;
    }
    const last = projText.substring(segStart).trim();
    if (last.length > 0) { segments.push({ text: last, relStart: segStart, relEnd: projText.length }); }

    // produce ColumnDef[]
    const lineStarts = getLineStarts(t);
    const out: ColumnDef[] = [];
    for (const seg of segments) {
        const rawSegment = seg.text;
        const cleaned = rawSegment.replace(/\/\*[\s\S]*?\*\//g, " ").replace(/--.*$/m, "").trim();

        let alias: string | null = null;
        let exprPart = cleaned;

        const asMatch2 = cleaned.match(/\s+as\s+([\[\]`"]?[@A-Za-z_][\w@#]*[\]\"]?)/i);
        if (asMatch2) {
            alias = asMatch2[1].trim();
            exprPart = cleaned.substring(0, asMatch2.index).trim();
        } else {
            const tokens = cleaned.split(/\s+/);
            if (tokens.length >= 2) {
                const last = tokens[tokens.length - 1];
                if (/^[\[\]`"]?[@A-Za-z_][\w@#]*[\]\"]?$/.test(last) && !last.includes(".") && !last.includes("(")) {
                    alias = last;
                    exprPart = tokens.slice(0, -1).join(" ");
                }
            }
        }

        const rawName = alias ?? exprPart;
        const segAbsStart = projStart + seg.relStart;

        let foundOffsetInSeg = -1;
        if (alias) {
            const idx = projText.indexOf(alias, seg.relStart);
            if (idx >= 0) { foundOffsetInSeg = idx; }
        }
        if (foundOffsetInSeg === -1) {
            const idx2 = projText.indexOf(rawName, seg.relStart);
            if (idx2 >= 0) { foundOffsetInSeg = idx2; }
        }
        if (foundOffsetInSeg === -1) { foundOffsetInSeg = seg.relStart; }

        const absStart = projStart + foundOffsetInSeg;
        const absEnd = absStart + String(rawName).length;
        const posStart = offsetToPosition(absStart, lineStarts);

        const normalized = String(rawName).replace(/^[\[\]`"]|[\[\]`"]$/g, "");

        out.push({
            name: normalized,
            rawName: String(rawName),
            line: posStart.line + fileStartLine,
            start: absStart,
            end: absEnd,
        });
    }

    return out;
}

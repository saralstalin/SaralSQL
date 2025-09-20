
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
    start?: number; // ← add
    end?: number;   // ← add
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

    let m: RegExpExecArray | null;
    while ((m = procHeaderRegex.exec(cleanText)) !== null) {
        const wholeMatch = m[0];
        const paramsBlock = m[3];
        const matchIndex = m.index; // absolute offset of start of wholeMatch in clean

        // safer: find the '(' inside the wholeMatch and compute paramsStartAbs from that
        const openParenInMatch = wholeMatch.indexOf('(');
        // fallback: if something odd happens, fall back to paramsBlock index inside wholeMatch
        const paramsStartAbs = (openParenInMatch >= 0)
            ? (matchIndex + openParenInMatch + 1)                 // right after '('
            : (matchIndex + wholeMatch.indexOf(paramsBlock));    // less likely path

        const paramTokenRegex = /@([A-Za-z_][A-Za-z0-9_]*)\b/g;
        let pm: RegExpExecArray | null;
        while ((pm = paramTokenRegex.exec(paramsBlock)) !== null) {
            const raw = '@' + pm[1];                 // token text
            const absOffset = paramsStartAbs + pm.index; // pm.index is relative to paramsBlock
            const lineStarts = getLineStarts(cleanText);
            const { line, character } = offsetToPosition(absOffset, lineStarts); // your helper
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

            // If this is a VIEW and the parenthesis-style extractor returned nothing,
            // attempt to extract projected columns from the defining SELECT using AST.
            if (kind === "VIEW" && (!cols || cols.length === 0)) {
                try {
                    if (isAstPoolReady()) {
                        // fire-and-forget (keep indexText synchronous)
                        parseColumnsFromCreateView(createBlock, i)
                            .then((viewCols) => {
                                if (viewCols && viewCols.length) {
                                    // ensure cols is an array we can mutate
                                    cols = Array.isArray(cols) ? cols : [];
                                    // PUSH the returned columns (correct spread and identifier)
                                    cols.push(...viewCols);
                                    // also update columnsByTable & sym.columns so other async logic
                                    // + hover/completions eventually see them (optional)
                                    try {
                                        const normSet = columnsByTable.get(norm) || new Set<string>();
                                        for (const vc of viewCols) {
                                            const n = normalizeName(vc.name);
                                            if (!normSet.has(n)) {
                                                normSet.add(n);
                                            }
                                            if (!sym.columns) { sym.columns = []; }
                                            const already = (sym.columns as any[]).some((c: any) =>
                                                normalizeName((c.rawName || c.name || "").replace(/^\[|\]$/g, "")) === n
                                            );
                                            if (!already) {
                                                (sym.columns as any[]).push({
                                                    name: n,
                                                    rawName: vc.rawName || vc.name,
                                                    line: i + (vc.line || 0) + 1,
                                                    start: vc.start || 0,
                                                    end: (vc.end && vc.end > 0) ? vc.end : (String(vc.rawName || vc.name || "").length)
                                                });
                                            }
                                        }
                                        columnsByTable.set(norm, normSet);
                                    } catch {
                                        // non-fatal; keep indexing resilient
                                    }
                                }
                            })
                            .catch(() => {
                                /* ignore parse errors */
                            });
                    }
                } catch {
                    // keep indexer resilient on AST errors — swallow
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
                                            cols.push({ name: acNorm, rawName: rawAc, line: 0, start: 0, end: rawAc.length });

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

                                            // mergedByPos will dedupe by line:start:end
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
                const fileLine = i + c.line + 1;   // correct offset: skip CREATE line
                const sourceLineText = lines[fileLine] || "";

                let startCol = sourceLineText.indexOf(c.rawName);
                if (startCol < 0) { startCol = sourceLineText.indexOf(c.name); }
                if (startCol < 0) { startCol = Math.max(0, sourceLineText.search(/\S/)); }

                return {
                    name: c.name,
                    rawName: c.rawName,
                    type: c.type,
                    line: fileLine,
                    start: startCol,
                    end: startCol + c.rawName.length
                };
            }) as any;

            // Local column refs (definition locations)
            for (const c of cols) {
                const fileLine = i + c.line + 1;
                const sourceLineText = lines[fileLine] || "";

                let startCol = sourceLineText.indexOf(c.rawName);
                if (startCol < 0) { startCol = sourceLineText.indexOf(c.name); }
                if (startCol < 0) { startCol = Math.max(0, sourceLineText.search(/\S/)); }

                localRefs.push({
                    name: `${norm}.${c.name}`,
                    uri: normUri,
                    line: fileLine,
                    start: startCol,
                    end: startCol + c.rawName.length,
                    kind: "column"
                });
            }
        }

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
    }

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
                // collectCandidateTablesFromStatement may return a Set<string> or string[]; normalize to string[]
                let tablesInScopeArr: string[] = [];
                try {
                    const maybe = collectCandidateTablesFromStatement(stmtText);
                    if (Array.isArray(maybe)) {
                        tablesInScopeArr = maybe;
                    } else if (maybe instanceof Set) {
                        tablesInScopeArr = Array.from(maybe);
                    } else if (maybe) {
                        // fallback: try to coerce iterable
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

                // Now build strongly-typed scope object
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


export async function parseColumnsFromCreateView(blockText: string, startLine: number) {
    // Try parse with AST worker (timeout small so indexer stays snappy)
    const ast = await parseSqlWithWorker(blockText, { database: "mssql" }, 800).catch(() => null);
    if (!ast) { return []; }

    const colsOut: Array<{ name: string; rawName: string; type?: string; line: number; start: number; end: number }> = [];

    // find the first SELECT node in the AST (the view's defining select)
    let foundSelect: any = null;
    const roots = Array.isArray(ast) ? ast : [ast];
    for (const root of roots) {
        walkAst(root, (n: any) => {
            if (foundSelect) { return; }
            if (!n || typeof n !== "object") { return; }
            if (n.type === "select" || n.ast === "select") {
                foundSelect = n;
            }
        });
        if (foundSelect) { break; }
    }

    if (!foundSelect) { return []; }

    // node.columns or node.columns/ node.columns may be the list depending on parser output
    const colNodes = (foundSelect.columns || foundSelect.columns || foundSelect.fields || []) as any[];

    let lineGuess = startLine; // we don't have precise positions for each expr easily; use startLine as fallback
    for (const c of colNodes) {
        if (!c) { continue; }
        // prefer alias / AS name
        let alias = (c.as || c.alias || (c.as && c.as.value) || (c.alias && c.alias.value));
        if (alias && typeof alias === "object") { alias = alias.value || alias.name; }
        if (alias && typeof alias === "string") {
            const n = normalizeName(alias);
            colsOut.push({ name: n, rawName: alias, line: lineGuess, start: 0, end: 0 });
            continue;
        }

        // no explicit alias → try to extract a referenced column name (e.g. d.DepartmentId)
        // column node may be `c.expr` or `c` itself
        const candidate = extractColumnName(c.expr ?? c);
        if (candidate) {
            colsOut.push({ name: candidate, rawName: candidate, line: lineGuess, start: 0, end: 0 });
            continue;
        }

        // fallback: try to stringify expression minimally (strip whitespace) as rawName
        try {
            const raw = JSON.stringify(c).slice(0, 80);
            colsOut.push({ name: normalizeName(raw), rawName: raw, line: lineGuess, start: 0, end: 0 });
        } catch (e) {
            // skip
        }
    }

    return colsOut;
}



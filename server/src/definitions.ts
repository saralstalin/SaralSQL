import { Location } from "vscode-languageserver/node";
import {
    normalizeName
    , getLineStarts
    , offsetToPosition
    , isSqlKeyword
} from "./text-utils";
import * as url from "url";
import { walkAst, resolveAliasTableName, resolveSymbolCaseInsensitive } from "./ast-utils";
import { parseSql, type ParseResult } from "./sql-parser";
import { extractReferences, type ASTNode, type BinaryExpression, type Expression, type ExtractedReferenceContext, type InsertNode, type Statement, type TableReference, type UpdateNode, type VariableNode } from "@saralsql/tsql-parser";

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
    columns?: ColumnDef[];
    kind?: string;
}

export interface ReferenceDef {
    name: string; // normalized table or column name
    uri: string;
    line: number;
    start: number;
    end: number;
    kind: "table" | "column" | "parameter";
    context?: ExtractedReferenceContext | "create-definition";
    validateSchema?: boolean;
}

export const columnsByTable = new Map<string, Set<string>>();
export const aliasesByUri = new Map<string, Map<string, string>>();
export const definitions = new Map<string, SymbolDef[]>();
export const referencesIndex = new Map<string, Map<string, ReferenceDef[]>>();
const referencesByUri = new Map<string, Map<number, ReferenceDef[]>>();
export const tablesByName = new Map<string, SymbolDef>();
export const tableTypesByName = new Map<string, SymbolDef>();
export const tempTablesByUri = new Map<string, Map<string, { columns: Set<string>; declaredAt: number }>>();

const MAX_FILE_SIZE_BYTES = 250 * 1024;  // 250 KB (skip larger files)
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
    removeRefsFromUriIndex(name, uri);
    byUri.set(uri, refs);
    addRefsToUriIndex(uri, refs);
}

export function deleteRefsForFile(uri: string) {
    for (const [, byUri] of referencesIndex) { byUri.delete(uri); }
    for (const [name, byUri] of Array.from(referencesIndex.entries())) {
        if (byUri.size === 0) { referencesIndex.delete(name); }
    }
    referencesByUri.delete(uri);
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

export function findReferenceAtPosition(uri: string, line: number, character: number): ReferenceDef | undefined {
    const refsByLine = referencesByUri.get(uri);
    const refs = refsByLine?.get(line);
    if (!refs) {
        return undefined;
    }

    let best: ReferenceDef | undefined;
    for (const ref of refs) {
        if (character < ref.start || character > ref.end) {
            continue;
        }

        if (!best || (ref.end - ref.start) < (best.end - best.start)) {
            best = ref;
        }
    }

    return best;
}

export function getReferencesForUri(uri: string): ReferenceDef[] {
    const refsByLine = referencesByUri.get(uri);
    if (!refsByLine) {
        return [];
    }

    const refs: ReferenceDef[] = [];
    for (const lineRefs of refsByLine.values()) {
        refs.push(...lineRefs);
    }

    return refs;
}

function removeRefsFromUriIndex(name: string, uri: string): void {
    const existing = referencesIndex.get(name)?.get(uri);
    if (!existing || existing.length === 0) {
        return;
    }

    const refsByLine = referencesByUri.get(uri);
    if (!refsByLine) {
        return;
    }

    for (const ref of existing) {
        const refs = refsByLine.get(ref.line);
        if (!refs) {
            continue;
        }

        const filtered = refs.filter(r => !(r.name === ref.name && r.start === ref.start && r.end === ref.end && r.kind === ref.kind));
        if (filtered.length > 0) {
            refsByLine.set(ref.line, filtered);
        } else {
            refsByLine.delete(ref.line);
        }
    }

    if (refsByLine.size === 0) {
        referencesByUri.delete(uri);
    }
}

function addRefsToUriIndex(uri: string, refs: ReferenceDef[]): void {
    if (refs.length === 0) {
        return;
    }

    let refsByLine = referencesByUri.get(uri);
    if (!refsByLine) {
        refsByLine = new Map();
        referencesByUri.set(uri, refsByLine);
    }

    for (const ref of refs) {
        const refsForLine = refsByLine.get(ref.line) ?? [];
        refsForLine.push(ref);
        refsByLine.set(ref.line, refsForLine);
    }
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

function normalizeIndexUri(rawUri: string): string {
    try {
        let uri = rawUri;
        if (!uri.startsWith("file://")) {
            uri = url.pathToFileURL(uri).toString();
        }

        const prefix = "file:///";
        if (!uri.toLowerCase().startsWith(prefix)) {
            return uri;
        }

        const pathPart = decodeURIComponent(uri.substring(prefix.length));
        const normalizedPath = pathPart
            .replace(/\\/g, "/")
            .replace(/^([A-Za-z]):\//, (_m, drive) => `${String(drive).toLowerCase()}:/`);

        return prefix + encodeURI(normalizedPath);
    } catch {
        return rawUri;
    }
}

function getParserColumns(node: any, text: string, lineStarts: number[]): ColumnDef[] {
    const kind = String(node.objectType ?? "").toUpperCase();

    if (kind === "TABLE" || (kind === "TYPE" && node.isTableType)) {
        return (node.columns ?? [])
            .map((col: any) => parserColumnDefinition(col, text, lineStarts))
            .filter(Boolean) as ColumnDef[];
    }

    if (kind === "VIEW" && Array.isArray(node.body?.columns)) {
        return node.body.columns
            .filter((col: any) => !col?.wildcard)
            .map((col: any) => parserViewColumnDefinition(col, text, lineStarts))
            .filter(Boolean) as ColumnDef[];
    }

    return [];
}

function parserColumnDefinition(col: any, text: string, lineStarts: number[]): ColumnDef | null {
    const rawName = String(col?.name ?? "").trim();
    if (!rawName) {
        return null;
    }

    const startOffset = typeof col.start === "number" ? col.start : text.indexOf(rawName);
    const endOffset = typeof col.end === "number" ? col.end : startOffset + rawName.length;
    const pos = offsetToPosition(Math.max(0, startOffset), lineStarts);

    return {
        name: normalizeName(rawName),
        rawName,
        type: col.dataType ? String(col.dataType) : undefined,
        line: pos.line,
        start: pos.character,
        end: pos.character + Math.max(rawName.length, endOffset - startOffset)
    };
}

function parserViewColumnDefinition(col: any, text: string, lineStarts: number[]): ColumnDef | null {
    const rawName = String(col?.outputName ?? col?.alias ?? col?.sourceName ?? "").trim();
    if (!rawName) {
        return null;
    }

    const colStart = typeof col.start === "number" ? col.start : 0;
    const colEnd = typeof col.end === "number" ? col.end : colStart + rawName.length;
    const columnText = text.slice(colStart, colEnd);
    const rawIndex = columnText.toLowerCase().lastIndexOf(rawName.toLowerCase());
    const startOffset = rawIndex >= 0 ? colStart + rawIndex : colStart;
    const pos = offsetToPosition(startOffset, lineStarts);

    return {
        name: normalizeName(rawName),
        rawName,
        line: pos.line,
        start: pos.character,
        end: pos.character + rawName.length
    };
}

// ---------- Indexing ----------
export function indexText(uri: string, text: string): void {
    const normUri = normalizeIndexUri(uri);

    if (normUri.toLowerCase().includes(`/${DEPLOY_BLOCK_SUBSTRING}`) || normUri.toLowerCase().includes(`\\${DEPLOY_BLOCK_SUBSTRING}`)) {
        return;
    }

    if (!normUri.toLowerCase().endsWith(".sql")) {
        return;
    }

    const byteLen = Buffer.byteLength(text || "", "utf8");
    if (byteLen > MAX_FILE_SIZE_BYTES) {
        return;
    }

    const oldDefs = definitions.get(normUri) || [];
    for (const d of oldDefs) { 
        tablesByName.delete(d.name); 
        columnsByTable.delete(d.name); 
        tableTypesByName.delete(d.name);
    }
    definitions.delete(normUri);
    tempTablesByUri.delete(normUri);
    deleteRefsForFile(normUri);

    const parsed: ParseResult | null = parseSql(text);
    if (!parsed || !parsed.ast) {
        return;
    }

    const lineStarts = getLineStarts(text);
    const defs: SymbolDef[] = [];
    const localRefs: ReferenceDef[] = [];

    // 1. Walk AST to find CREATE definitions (TABLE, VIEW, PROCEDURE, FUNCTION, TYPE)
    walkAst(parsed.ast, (node: any) => {
        if (!node || node.type !== "CreateStatement" || !node.name) {
            return;
        }

        const kind = String(node.objectType ?? "").toUpperCase();
        if (!["TABLE", "VIEW", "TYPE", "PROCEDURE", "FUNCTION"].includes(kind)) {
            return;
        }

        const rawName = String(node.name);
        const norm = normalizeName(rawName);

        const nameStart = typeof node.nameNode?.start === "number" ? node.nameNode.start : node.start ?? 0;
        const nameEnd = typeof node.nameNode?.end === "number" ? node.nameNode.end : nameStart + rawName.length;
        const namePos = offsetToPosition(nameStart, lineStarts);

        const sym: SymbolDef = {
            name: norm,
            rawName,
            uri: normUri,
            line: namePos.line,
            kind
        };

        localRefs.push({
            name: norm,
            uri: normUri,
            line: namePos.line,
            start: namePos.character,
            end: namePos.character + Math.max(1, nameEnd - nameStart),
            kind: "table",
            context: "create-definition",
            validateSchema: kind === "TABLE" || kind === "VIEW" || (kind === "TYPE" && Boolean(node.isTableType))
        });

        const columns = getParserColumns(node, text, lineStarts);
        if (columns.length > 0) {
            sym.columns = columns;
            if (!norm.startsWith("#")) {
                columnsByTable.set(norm, new Set(columns.map(col => normalizeName(col.name))));
            }

            for (const col of columns) {
                localRefs.push({
                    name: `${norm}.${col.name}`,
                    uri: normUri,
                    line: col.line,
                    start: col.start ?? 0,
                    end: col.end ?? ((col.start ?? 0) + col.rawName.length),
                    kind: "column"
                });
            }
        }

        defs.push(sym);
    });

    indexVariablesFromScope(parsed.scope?.root, normUri, lineStarts, localRefs);
    indexVariableDataTypeReferencesFromScope(parsed.scope?.root, text, normUri, lineStarts, localRefs);
    indexVariableReferencesFromAst(parsed.ast, normUri, lineStarts, localRefs);
    indexAliasReferencesFromAst(parsed.ast, text, normUri, lineStarts, localRefs);
    indexSelectIntoTempTablesFromAst(parsed.ast, normUri);
    indexInsertColumnReferencesFromAst(parsed.ast, normUri, lineStarts, localRefs);
    indexUpdateAssignmentColumnsFromAst(parsed.ast, parsed.scope?.root, normUri, lineStarts, localRefs);
    indexTableVariableColumnReferencesFromAst(parsed.ast, parsed.scope?.root, normUri, lineStarts, localRefs);
    indexQualifiedColumnReferencesFromAst(parsed.ast, parsed.scope?.root, normUri, lineStarts, localRefs);

    // 2. Extract and resolve all references using extractReferences and column resolutions
    const references = extractReferences(parsed.ast);
    const resolutions = parsed.columns?.resolutions ?? [];

    for (const ref of references) {
        const refPos = offsetToPosition(ref.location.start, lineStarts);
        const startChar = ref.location.start - lineStarts[refPos.line];
        const endChar = ref.location.end - lineStarts[refPos.line];

        if (ref.kind === "table") {
            localRefs.push({
                name: normalizeName(ref.name),
                uri: normUri,
                line: refPos.line,
                start: startChar,
                end: endChar,
                kind: "table",
                context: ref.context,
                validateSchema: ref.context !== "execute-target"
            });
        } else if (ref.kind === "variable") {
            localRefs.push({
                name: ref.name.toLowerCase(),
                uri: normUri,
                line: refPos.line,
                start: startChar,
                end: endChar,
                kind: "parameter"
            });
        } else if (ref.kind === "column" || ref.kind === "unknown") {
            const colName = ref.name.split('.').pop()!;
            const colNameNorm = normalizeName(colName);
            if (isSqlKeyword(colNameNorm)) {
                continue;
            }

            const parts = ref.name.split('.');
            const scopeAtPos = parsed.scope?.root?.findInnermost(ref.location.start);
            const visibleSymbols = scopeAtPos ? scopeAtPos.getVisibleSymbols() : [];

            const matchedResolution = resolutions.find((r: any) => r.location?.start === ref.location.start);
            let resolved = false;

            if (matchedResolution && matchedResolution.inputs) {
                for (const input of matchedResolution.inputs) {
                    if (input.kind === "column" && input.source) {
                        localRefs.push({
                            name: `${normalizeName(input.source)}.${normalizeName(input.name.split('.').pop()!)}`,
                            uri: normUri,
                            line: refPos.line,
                            start: startChar,
                            end: endChar,
                            kind: "column",
                            validateSchema: tablesByName.has(normalizeName(String(input.source))) || tableTypesByName.has(normalizeName(String(input.source)))
                        });
                        resolved = true;
                    } else if (input.kind === "variable") {
                        localRefs.push({
                            name: input.name.toLowerCase(),
                            uri: normUri,
                            line: refPos.line,
                            start: startChar,
                            end: endChar,
                            kind: "parameter"
                        });
                        resolved = true;
                    }
                }
            }

                if (!resolved) {
                    // Scope-based fallback resolution
                    if (parts.length === 2) {
                    const aliasOrTable = parts[0].trim();
                    const col = parts[1].trim();
                    const aliasOrTableNorm = normalizeName(aliasOrTable);
                    const colNorm = normalizeName(col);

                    let resolvedTable: string | null = null;
                    for (const sym of visibleSymbols) {
                        if (sym.kind === "Alias" && normalizeName(sym.name) === aliasOrTableNorm) {
                            resolvedTable = resolveAliasTableName(sym) ?? null;
                            break;
                        } else if ((sym.kind === "Table" || sym.kind === "TempTable" || sym.kind === "CTE") && normalizeName(sym.name) === aliasOrTableNorm) {
                            resolvedTable = sym.name;
                            break;
                        }
                    }

                    let resolvedFromFileAlias = false;
                    if (!resolvedTable) {
                        const fileAliases = aliasesByUri.get(normUri);
                        const matchedTable = fileAliases?.get(aliasOrTableNorm);
                        if (matchedTable && matchedTable.toLowerCase() !== "__subquery__") {
                            resolvedTable = matchedTable;
                            resolvedFromFileAlias = true;
                        }
                    }

                    if (resolvedTable) {
                        const resolvedTableNorm = normalizeName(resolvedTable);
                        localRefs.push({
                            name: `${resolvedTableNorm}.${colNorm}`,
                            uri: normUri,
                            line: refPos.line,
                            start: startChar,
                            end: endChar,
                            kind: "column",
                            validateSchema: !resolvedFromFileAlias && !resolvedTableNorm.startsWith("#")
                        });
                    }
                } else if (parts.length === 1) {
                    const colNorm = normalizeName(parts[0]);
                    const candidateTables = collectBareColumnCandidateTablesIncremental(scopeAtPos, colNorm);

                    if (candidateTables.length === 0) {
                        const updateTarget = resolveUpdateTargetTableAtOffsetFromAst(parsed.ast, parsed.scope?.root, ref.location.start);
                        if (updateTarget) {
                            candidateTables.push(normalizeName(updateTarget));
                        }
                    }

                    if (candidateTables.length > 0) {
                        for (const t of candidateTables) {
                            localRefs.push({
                                name: `${t}.${colNorm}`,
                                uri: normUri,
                                line: refPos.line,
                                start: startChar,
                                end: endChar,
                                kind: "column"
                            });
                        }
                    }
                }
            }
        }
    }

    definitions.set(normUri, defs);

    const byName = new Map<string, ReferenceDef[]>();
    for (const ref of localRefs) { const arr = byName.get(ref.name) || []; arr.push(ref); byName.set(ref.name, arr); }
    for (const [name, arr] of byName) {
        const seen = new Set<string>();
        const dedup = arr.filter(r => { const k = `${r.line}:${r.start}:${r.end}`; if (seen.has(k)) { return false; } seen.add(k); return true; });
        setRefsForFile(name, normUri, dedup);
    }

    const parsedAliases = new Map<string, string>();
    if (parsed.scope?.root) {
        const visitScope = (scope: any) => {
            const symbols = typeof scope.getOwnSymbols === "function" ? scope.getOwnSymbols() : Object.values(scope.symbols ?? {});
            for (const sym of symbols) {
                if (sym.kind === "Alias" && sym.location?.table) {
                    const tableName = resolveAliasTableName(sym);
                    if (tableName) {
                        parsedAliases.set(normalizeName(sym.name), normalizeName(tableName));
                    }
                }
            }
            const children = typeof scope.getChildren === "function" ? scope.getChildren() : (scope.children ?? []);
            for (const child of children) {
                visitScope(child);
            }
        };
        visitScope(parsed.scope.root);
    }
    aliasesByUri.set(normUri, parsedAliases);

    for (const def of defs) {
        if (!def.columns || !Array.isArray(def.columns) || def.columns.length === 0) { continue; }
        const kindNorm = (def.kind || "").toUpperCase();
        if ((kindNorm === "TABLE" || kindNorm === "VIEW") && !def.name.startsWith("#")) {
            tablesByName.set(def.name, def);
        } else if (kindNorm === "TYPE") {
            tableTypesByName.set(def.name, def);
        }
    }
}

function resolveUpdateTargetTableAtOffsetFromAst(
    ast: ParseResult["ast"] | null,
    rootScope: any,
    offset: number
): string | null {
    if (!ast || !rootScope || typeof offset !== "number") {
        return null;
    }

    let best: any = null;
    walkAst(ast, (node: any) => {
        if (!node || node.type !== "UpdateStatement") {
            return;
        }
        if (typeof node.start !== "number" || typeof node.end !== "number") {
            return;
        }
        if (offset < node.start || offset > node.end) {
            return;
        }
        if (!best || (node.end - node.start) < (best.end - best.start)) {
            best = node;
        }
    });

    if (!best) {
        return null;
    }

    const resolved = resolveUpdateTargetTable(best as UpdateNode, rootScope);
    return resolved ? normalizeName(resolved) : null;
}

function collectBareColumnCandidateTablesIncremental(scopeAtPos: any, colNorm: string): string[] {
    const out: string[] = [];
    let scope = scopeAtPos;

    while (scope) {
        const symbols = typeof scope.getOwnSymbols === "function"
            ? scope.getOwnSymbols()
            : Object.values(scope.symbols ?? {});

        const level = new Set<string>();
        for (const sym of symbols) {
            if (sym.kind === "Table" || sym.kind === "TempTable" || sym.kind === "CTE") {
                const symNorm = normalizeName(sym.name);
                if (columnsByTable.get(symNorm)?.has(colNorm)) {
                    level.add(symNorm);
                }
                continue;
            }

            if (sym.kind === "Alias") {
                const tblName = resolveAliasTableName(sym);
                if (!tblName) {
                    continue;
                }
                const tblNorm = normalizeName(tblName);
                if (columnsByTable.get(tblNorm)?.has(colNorm)) {
                    level.add(tblNorm);
                }
            }
        }

        if (level.size > 0) {
            out.push(...Array.from(level.values()));
            return out;
        }

        if (String(scope.name ?? "").toLowerCase() === "subquery") {
            return out;
        }

        scope = scope.parent ?? null;
    }

    return out;
}

function indexSelectIntoTempTablesFromAst(
    ast: ParseResult["ast"] | null,
    normUri: string
): void {
    const tempTables = new Map<string, { columns: Set<string>; declaredAt: number }>();

    walkAst(ast, (node: any) => {
        if (!node || node.type !== "SelectStatement" || !node.into || !Array.isArray(node.columns)) {
            return;
        }

        const intoName = normalizeName(String(node.into?.name ?? ""));
        if (!intoName.startsWith("#")) {
            return;
        }

        const columns = new Set<string>();
        for (const col of node.columns) {
            const projected = extractSelectProjectedColumnName(col);
            if (!projected) {
                continue;
            }
            columns.add(projected);
        }

        if (columns.size === 0) {
            return;
        }

        const declaredAt = typeof node.into?.start === "number" ? node.into.start : 0;
        tempTables.set(intoName, { columns, declaredAt });
    });

    if (tempTables.size > 0) {
        tempTablesByUri.set(normUri, tempTables);
    }
}

function extractSelectProjectedColumnName(col: any): string | null {
    const alias = normalizeName(String(col?.alias ?? col?.name ?? ""));
    if (alias) {
        return alias;
    }

    const expr = col?.expression;
    if (!expr) {
        return null;
    }

    if (expr.type === "Identifier") {
        const raw = Array.isArray(expr.parts) && expr.parts.length > 0
            ? String(expr.parts[expr.parts.length - 1] ?? "")
            : String(expr.name ?? "");
        const norm = normalizeName(raw);
        return norm || null;
    }

    return null;
}

function indexQualifiedColumnReferencesFromAst(
    ast: ParseResult["ast"] | null,
    rootScope: any,
    normUri: string,
    lineStarts: number[],
    localRefs: ReferenceDef[]
): void {
    if (!rootScope) {
        return;
    }

    walkAst(ast, (node: any) => {
        if (
            !node ||
            node.type !== "Identifier" ||
            !Array.isArray(node.parts) ||
            node.parts.length !== 2 ||
            typeof node.start !== "number"
        ) {
            return;
        }

        const qualifier = normalizeName(node.parts[0]);
        const columnName = normalizeName(node.parts[1]);
        if (!qualifier || !columnName) {
            return;
        }

        const scopeAtPos = rootScope.findInnermost?.(node.start) ?? rootScope;
        const sym = resolveSymbolCaseInsensitive(scopeAtPos, qualifier);
        if (sym?.kind !== "Alias") {
            return;
        }

        const tableName = resolveAliasTableName(sym);
        if (!tableName) {
            return;
        }

        const normalizedTableName = tableName ? normalizeName(tableName) : "";
        const isSchemaValidTableAlias = Boolean(normalizedTableName) && (tablesByName.has(normalizedTableName) || tableTypesByName.has(normalizedTableName));

        const pos = offsetToPosition(node.start, lineStarts);
        localRefs.push({
            name: `${normalizeName(tableName)}.${columnName}`,
            uri: normUri,
            line: pos.line,
            start: node.start - lineStarts[pos.line],
            end: (node.end ?? node.start + String(node.name).length) - lineStarts[pos.line],
            kind: "column",
            validateSchema: isSchemaValidTableAlias
        });
    });
}

function indexInsertColumnReferencesFromAst(
    ast: ParseResult["ast"] | null,
    normUri: string,
    lineStarts: number[],
    localRefs: ReferenceDef[]
): void {
    walkAst(ast, (node: any) => {
        if (!node || node.type !== "InsertStatement" || !node.table || !Array.isArray(node.columnNodes)) {
            return;
        }

        const tableName = normalizeName(node.table.name ?? node.table);
        if (!tableName) {
            return;
        }

        for (const col of node.columnNodes) {
            const colName = normalizeName(col?.name ?? "");
            if (!colName || typeof col.start !== "number") {
                continue;
            }

            const pos = offsetToPosition(col.start, lineStarts);
            localRefs.push({
                name: `${tableName}.${colName}`,
                uri: normUri,
                line: pos.line,
                start: col.start - lineStarts[pos.line],
                end: (col.end ?? col.start + colName.length) - lineStarts[pos.line],
                kind: "column"
            });
        }
    });
}

function indexUpdateAssignmentColumnsFromAst(
    ast: ParseResult["ast"] | null,
    rootScope: any,
    normUri: string,
    lineStarts: number[],
    localRefs: ReferenceDef[]
): void {
    if (!rootScope) {
        return;
    }

    walkAst(ast, (node: any) => {
        if (!node || node.type !== "UpdateStatement" || !Array.isArray(node.assignments) || node.assignments.length === 0) {
            return;
        }

        const targetTable = resolveUpdateTargetTable(node, rootScope);
        if (!targetTable) {
            return;
        }

        const targetNorm = normalizeName(targetTable);
        for (const assignment of node.assignments) {
            const colNode = assignment?.columnNode;
            if (!colNode || typeof colNode.start !== "number") {
                continue;
            }

            const colParts = Array.isArray(colNode.parts) ? colNode.parts : [String(colNode.name ?? "")];
            const rawCol = String(colParts[colParts.length - 1] ?? "").trim();
            const colName = normalizeName(rawCol);
            if (!colName) {
                continue;
            }

            const pos = offsetToPosition(colNode.start, lineStarts);
            localRefs.push({
                name: `${targetNorm}.${colName}`,
                uri: normUri,
                line: pos.line,
                start: colNode.start - lineStarts[pos.line],
                end: (colNode.end ?? (colNode.start + rawCol.length)) - lineStarts[pos.line],
                kind: "column"
            });
        }
    });
}

function resolveUpdateTargetTable(updateNode: UpdateNode, rootScope: any): string | undefined {
    const targetExpr = updateNode?.target as any;
    const targetParts = Array.isArray(targetExpr?.parts) ? targetExpr.parts : [];
    const targetName = String(targetExpr?.name ?? targetParts.join(".") ?? "").trim();
    if (!targetName) {
        return undefined;
    }

    const targetNorm = normalizeName(targetName);
    const scopeAtPos = rootScope.findInnermost?.(targetExpr?.start) ?? rootScope;
    const targetSym = resolveSymbolCaseInsensitive(scopeAtPos, targetNorm);

    if (targetSym?.kind === "Alias") {
        const resolvedAlias = resolveAliasTableName(targetSym);
        if (resolvedAlias) {
            return resolvedAlias;
        }
    }

    if (targetSym && (targetSym.kind === "Table" || targetSym.kind === "TempTable" || targetSym.kind === "CTE")) {
        return String(targetSym.name ?? "");
    }

    if (targetParts.length > 1) {
        return targetName;
    }

    if (Array.isArray(updateNode.from)) {
        for (const source of updateNode.from) {
            const alias = normalizeName(String(source?.alias ?? ""));
            if (alias && alias === targetNorm) {
                const tableName = resolveTableNameFromNode(source?.table);
                if (tableName) {
                    return tableName;
                }
            }
        }
    }

    return targetName;
}

function indexTableVariableColumnReferencesFromAst(
    ast: ParseResult["ast"] | null,
    rootScope: any,
    normUri: string,
    lineStarts: number[],
    localRefs: ReferenceDef[]
): void {
    if (!rootScope) {
        return;
    }

    walkAst(ast, (node: any) => {
        if (
            !node ||
            node.type !== "BinaryExpression" ||
            node.operator !== "." ||
            node.left?.type !== "Variable" ||
            node.right?.type !== "Identifier" ||
            typeof node.right.start !== "number"
        ) {
            return;
        }

        const variableName = String(node.left.name ?? "").toLowerCase();
        const columnName = normalizeName(node.right.name ?? "");
        if (!variableName || !columnName) {
            return;
        }

        const scopeAtPos = rootScope.findInnermost?.(node.left.start) ?? rootScope;
        const sym = resolveSymbolCaseInsensitive(scopeAtPos, variableName);
        if (!sym?.dataType && !Array.isArray(sym?.columns)) {
            return;
        }

        const pos = offsetToPosition(node.right.start, lineStarts);
        localRefs.push({
            name: `${variableName}.${columnName}`,
            uri: normUri,
            line: pos.line,
            start: node.right.start - lineStarts[pos.line],
            end: (node.right.end ?? node.right.start + columnName.length) - lineStarts[pos.line],
            kind: "column"
        });
    });
}


function indexAliasReferencesFromAst(
    ast: ParseResult["ast"] | null,
    text: string,
    normUri: string,
    lineStarts: number[],
    localRefs: ReferenceDef[]
): void {
    walkAst(ast, (node: any) => {
        if (!node || typeof node !== "object" || !node.alias || !node.table) {
            return;
        }

        const tableName = resolveTableNameFromNode(node.table);
        const aliasName = String(node.alias ?? "");
        if (!tableName || !aliasName) {
            return;
        }

        const aliasRange = findAliasRange(node, aliasName, text);
        if (!aliasRange) {
            return;
        }

        const pos = offsetToPosition(aliasRange.start, lineStarts);
        localRefs.push({
            name: normalizeName(tableName),
            uri: normUri,
            line: pos.line,
            start: aliasRange.start - lineStarts[pos.line],
            end: aliasRange.end - lineStarts[pos.line],
            kind: "table",
            validateSchema: false
        });
    });
}

function resolveTableNameFromNode(tableNode: Expression | TableReference | string | null | undefined): string | undefined {
    if (!tableNode) {
        return undefined;
    }

    if (typeof tableNode === "string") {
        return tableNode;
    }

    const node = tableNode as any;
    if (typeof node?.name === "string") {
        return node.name;
    }

    if (node.type === "Identifier" && typeof node.name === "string") {
        return node.name;
    }

    return undefined;
}

function findAliasRange(node: { table?: { end?: number }; start?: number; end?: number }, aliasName: string, text: string): { start: number; end: number } | null {
    const searchStart = typeof node.table?.end === "number" ? node.table.end : node.start ?? 0;
    const searchEnd = typeof node.end === "number" ? node.end : Math.min(text.length, searchStart + 200);
    if (searchEnd <= searchStart) {
        return null;
    }

    const segment = text.slice(searchStart, searchEnd);
    const wanted = normalizeName(aliasName);

    const skipWs = (i: number) => {
        while (i < segment.length && /\s/.test(segment[i])) {
            i++;
        }
        return i;
    };

    const readIdentifier = (i: number): { start: number; end: number; value: string } | null => {
        if (i >= segment.length) {
            return null;
        }

        const ch = segment[i];
        if (ch === "[") {
            const close = segment.indexOf("]", i + 1);
            if (close < 0) {
                return null;
            }
            return { start: i, end: close + 1, value: segment.slice(i + 1, close) };
        }

        if (ch === "\"" || ch === "`") {
            const close = segment.indexOf(ch, i + 1);
            if (close < 0) {
                return null;
            }
            return { start: i, end: close + 1, value: segment.slice(i + 1, close) };
        }

        if (!/[A-Za-z_#@]/.test(ch)) {
            return null;
        }

        let j = i + 1;
        while (j < segment.length && /[A-Za-z0-9_$#@]/.test(segment[j])) {
            j++;
        }
        return { start: i, end: j, value: segment.slice(i, j) };
    };

    let i = skipWs(0);
    const first = readIdentifier(i);
    if (!first) {
        return null;
    }

    const firstNorm = normalizeName(first.value);
    if (firstNorm === "as") {
        i = skipWs(first.end);
        const second = readIdentifier(i);
        if (!second) {
            return null;
        }
        if (normalizeName(second.value) !== wanted) {
            return null;
        }
        return {
            start: searchStart + second.start,
            end: searchStart + second.end
        };
    }

    if (firstNorm !== wanted) {
        return null;
    }

    return {
        start: searchStart + first.start,
        end: searchStart + first.end
    };
}

function indexVariablesFromScope(
    rootScope: any,
    normUri: string,
    lineStarts: number[],
    localRefs: ReferenceDef[]
): void {
    if (!rootScope) {
        return;
    }

    const visitScope = (scope: any) => {
        const symbols = typeof scope.getOwnSymbols === "function"
            ? scope.getOwnSymbols()
            : Object.values(scope.symbols ?? {});

        for (const sym of symbols) {
            if (sym.kind !== "Parameter" && sym.kind !== "Variable") {
                continue;
            }

            const name = String(sym.name ?? "");
            const start = typeof sym.location?.start === "number" ? sym.location.start : undefined;
            if (!name || start === undefined) {
                continue;
            }

            addVariableRef(name, start, start + name.length, normUri, lineStarts, localRefs);
        }

        const children = typeof scope.getChildren === "function"
            ? scope.getChildren()
            : (scope.children ?? []);

        for (const child of children) {
            visitScope(child);
        }
    };

    visitScope(rootScope);
}

function indexVariableDataTypeReferencesFromScope(
    rootScope: any,
    text: string,
    normUri: string,
    lineStarts: number[],
    localRefs: ReferenceDef[]
): void {
    if (!rootScope) {
        return;
    }

    const visitScope = (scope: any) => {
        const symbols = typeof scope.getOwnSymbols === "function"
            ? scope.getOwnSymbols()
            : Object.values(scope.symbols ?? {});

        for (const sym of symbols) {
            if (sym.kind !== "Parameter" && sym.kind !== "Variable") {
                continue;
            }

            const dataTypeRaw = String(sym.dataType ?? "").trim();
            const dataTypeNorm = normalizeName(dataTypeRaw);
            if (!dataTypeNorm || isLikelyBuiltInSqlType(dataTypeNorm)) {
                continue;
            }

            const loc = sym.location;
            const start = Number(loc?.start);
            const end = Number(loc?.end);
            if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
                continue;
            }

            const snippet = text.slice(start, end);
            const range = findTypeNameRangeInSnippet(snippet, dataTypeRaw);
            if (!range) {
                continue;
            }

            const absStart = start + range.start;
            const absEnd = start + range.end;
            const pos = offsetToPosition(absStart, lineStarts);
            localRefs.push({
                name: dataTypeNorm,
                uri: normUri,
                line: pos.line,
                start: absStart - lineStarts[pos.line],
                end: absEnd - lineStarts[pos.line],
                kind: "table",
                context: "execute-target",
                validateSchema: false
            });
        }

        const children = typeof scope.getChildren === "function"
            ? scope.getChildren()
            : (scope.children ?? []);

        for (const child of children) {
            visitScope(child);
        }
    };

    visitScope(rootScope);
}

function findTypeNameRangeInSnippet(snippet: string, dataTypeRaw: string): { start: number; end: number } | null {
    const escaped = dataTypeRaw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const match = new RegExp(`\\b${escaped}\\b`, "i").exec(snippet);
    if (!match || typeof match.index !== "number") {
        return null;
    }

    return { start: match.index, end: match.index + match[0].length };
}

function isLikelyBuiltInSqlType(dataTypeNorm: string): boolean {
    const base = dataTypeNorm.split("(")[0]?.trim() ?? dataTypeNorm;
    const builtins = new Set([
        "int", "bigint", "smallint", "tinyint", "bit",
        "decimal", "numeric", "money", "smallmoney",
        "float", "real",
        "date", "datetime", "datetime2", "smalldatetime", "time", "datetimeoffset",
        "char", "varchar", "text", "nchar", "nvarchar", "ntext",
        "binary", "varbinary", "image",
        "uniqueidentifier", "xml", "sql_variant",
        "rowversion", "timestamp", "hierarchyid", "geography", "geometry", "json"
    ]);

    return builtins.has(base);
}

function indexVariableReferencesFromAst(
    ast: any,
    normUri: string,
    lineStarts: number[],
    localRefs: ReferenceDef[]
): void {
    walkAst(ast, (node: any) => {
        if (!node || typeof node !== "object") {
            return;
        }

        if (node.type === "Variable" && node.name && typeof node.start === "number") {
            addVariableRef(node.name, node.start, node.end ?? node.start + String(node.name).length, normUri, lineStarts, localRefs);
        }

        if (node.type === "SetStatement" && node.variable && typeof node.variableStart === "number") {
            const name = String(node.variable);
            if (name.startsWith("@")) {
                addVariableRef(name, node.variableStart, node.variableEnd ?? node.variableStart + name.length, normUri, lineStarts, localRefs);
            }
        }
    });
}

function addVariableRef(
    rawName: string,
    startOffset: number,
    endOffset: number,
    normUri: string,
    lineStarts: number[],
    localRefs: ReferenceDef[]
): void {
    const pos = offsetToPosition(startOffset, lineStarts);

    localRefs.push({
        name: rawName.toLowerCase(),
        uri: normUri,
        line: pos.line,
        start: startOffset - lineStarts[pos.line],
        end: endOffset - lineStarts[pos.line],
        kind: "parameter"
    });
}

export function parseColumnsFromCreateView(viewText: string, fileStartLine = 0): ColumnDef[] {
    const parsed = parseSql(viewText);
    if (!parsed || !parsed.ast) { return []; }
    const lineStarts = getLineStarts(viewText);
    const out: ColumnDef[] = [];
    walkAst(parsed.ast, (node: any) => {
        if (node.type === "CreateStatement" && node.objectType === "VIEW") {
            const cols = getParserColumns(node, viewText, lineStarts);
            for (const col of cols) {
                col.line += fileStartLine;
                out.push(col);
            }
        }
    });
    return out;
}

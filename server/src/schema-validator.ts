/**
 * Schema diagnostic computation extracted from server.ts.
 * Returns the schema diagnostics array without sending — testable without the LSP connection.
 */
import { Diagnostic, DiagnosticSeverity, Range } from "vscode-languageserver/node";
import { TextDocument } from "vscode-languageserver-textdocument";
import * as url from "url";
import * as path from "path";
import * as fs from "fs";
import { normalizeName, getLineStarts, offsetToPosition } from "./text-utils";
import {
  tablesByName, tableTypesByName, columnsByTable,
  tempTablesByUri, aliasesByUri, definitions, getReferencesForUri,
  type ReferenceDef
} from "./definitions";
import type { ParseResult } from "./sql-parser";
import { SARAL_DIAGNOSTIC_CODES, shouldSuppressDiagnosticCode, resolveDiagnosticSeverity,
  collectAmbiguousColumnDiagnostics, collectReadableBareColumnDiagnostics, collectStringComparisonDiagnostics
} from "./diagnostic-helpers";
import { resolveSymbolCaseInsensitive, resolveAliasTableName, getDisplaySymbolName, getCteColumns } from "./ast-utils";
import { toNormUri } from "./sql-helpers";

export interface SchemaValidatorOptions {
  disabledDiagnosticCodes: Set<string>;
  diagnosticSeverityOverrides: Map<string, DiagnosticSeverity>;
  hasSqlProjInWorkspace: boolean;
  sqlProjStrictBuildMembership: boolean;
  schemaDiagnosticCodes: Set<string>;
}

export function computeSchemaDiagnostics(
  doc: TextDocument,
  text: string,
  lineStarts: number[],
  parsed: ParseResult | null,
  normDocUri: string,
  options: SchemaValidatorOptions
): Diagnostic[] {
  const diagnostics: Diagnostic[] = [];
  const {
    disabledDiagnosticCodes,
    diagnosticSeverityOverrides,
    hasSqlProjInWorkspace,
    sqlProjStrictBuildMembership,
  } = options;

  /* connection.console.log("[validate] entered schema block") */;

  try {
    const normDocUri = toNormUri(doc.uri);
    const fileAliases = aliasesByUri.get(normDocUri);
    const refsForDoc = getReferencesForUri(normDocUri);
    const localDefs = definitions.get(normDocUri) ?? [];
    const localDefsByName = new Map<string, any>();
    for (const def of localDefs) {
      localDefsByName.set(normalizeName(def.name), def);
    }

    // When no schema has been indexed at all (no CREATE TABLE/VIEW in workspace),
    // suppress table/column existence checks — they would fire on every reference.
    // Readability (LSP004) and ambiguity (LSP003) diagnostics are scope-based and
    // remain active regardless of schema availability.
    const hasIndexedSchema = tablesByName.size > 0 || tableTypesByName.size > 0 || localDefsByName.size > 0;
    const cteNames = new Set<string>();
    const seenTables = new Set<string>();
    const reportedMissingTables = new Set<string>();
    const seenColumns = new Set<string>();
    const diagnosticTextCache = new Map<string, string>();
    const tableRefsByName = new Map<string, ReferenceDef[]>();
    const propertyAccessRanges = (
      Array.isArray(parsed?.columns?.propertyAccesses)
        ? parsed.columns.propertyAccesses
          .map((p: any) => ({
            start: Number(p?.location?.start),
            end: Number(p?.location?.end)
          }))
          .filter((r: { start: number; end: number }) => Number.isFinite(r.start) && Number.isFinite(r.end))
        : []
    );

    function makeOffsetRange(start: number, end: number): Range {
      const startPos = offsetToPosition(start, lineStarts);
      const endPos = offsetToPosition(end, lineStarts);

      return {
        start: {
          line: startPos.line,
          character: startPos.character
        },
        end: {
          line: endPos.line,
          character: endPos.character
        }
      };
    }

    function addUnknownTable(name: string, line: number, start: number, end: number, isFallback = false) {
      addUnknownTableAt(name, line, start, line, end, isFallback);
    }

    function addUnknownTableAt(name: string, startLine: number, startChar: number, endLine: number, endChar: number, isFallback = false) {
      if (!name) {return;}
      if (!hasIndexedSchema) {return;}
      if (shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.UnknownTable, disabledDiagnosticCodes)) {return;}

      const clean = normalizeName(name);
      
      if (isFallback && reportedMissingTables.has(clean)) {
          return;
      }

      const key = isFallback ? `fallback:${clean}` : `${clean}:${startLine}:${startChar}`;
      if (seenTables.has(key)) {return;}
      seenTables.add(key);
      reportedMissingTables.add(clean);

      const refOffset = (lineStarts[startLine] ?? 0) + startChar;
      if (resolveCteSymbolAtOffset(clean, refOffset)) {return;}
      if (cteNames.has(clean)) {return;}

      if (clean.startsWith("#") || clean.startsWith("@")) {return;}
      if (isSystemTableReference(clean)) {return;}
      if (tableExistsInContext(clean)) {return;}

      let displayTableName = name;
      const rangeText = getTextAtRange(startLine, startChar, endLine, endChar);
      if (rangeText) {
        const rangeClean = normalizeName(rangeText);
        if (rangeClean === clean || rangeClean.endsWith("." + clean)) {
          displayTableName = rangeText;
        }
      }

      diagnostics.push({
        code: SARAL_DIAGNOSTIC_CODES.UnknownTable,
        severity: resolveDiagnosticSeverity(SARAL_DIAGNOSTIC_CODES.UnknownTable, DiagnosticSeverity.Error, diagnosticSeverityOverrides),
        range: {
          start: { line: startLine, character: startChar },
          end: { line: endLine, character: endChar }
        },
        message: `Unknown table '${displayTableName}'`,
        source: "SaralSQL"
      });
    }

    // T-SQL disallows referencing a table by its original name once it has been
    // aliased within the same statement — only the alias is a valid qualifier from
    // that point on. addUnknownTable wouldn't fire here because the table genuinely
    // exists; this is a distinct "wrong qualifier in this scope" condition.
    function addShadowedByAliasTable(name: string, aliasName: string, line: number, start: number, end: number) {
      if (shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.UnknownTable, disabledDiagnosticCodes)) { return; }
      const key = `shadowed:${normalizeName(name)}:${line}:${start}`;
      if (seenTables.has(key)) { return; }
      seenTables.add(key);

      diagnostics.push({
        code: SARAL_DIAGNOSTIC_CODES.UnknownTable,
        severity: resolveDiagnosticSeverity(SARAL_DIAGNOSTIC_CODES.UnknownTable, DiagnosticSeverity.Error, diagnosticSeverityOverrides),
        range: {
          start: { line, character: start },
          end: { line, character: end }
        },
        message: `Table '${name}' has been aliased as '${aliasName}' in this statement; use '${aliasName}' instead of the table name`,
        source: "SaralSQL"
      });
    }

    function addWrongColumn(
      table: string,
      column: string,
      line: number,
      start: number,
      end: number,
      tableDisplay?: string
    ) {
      if (!hasIndexedSchema) {return;}
      if (shouldSuppressDiagnosticCode(SARAL_DIAGNOSTIC_CODES.UnknownColumn, disabledDiagnosticCodes)) {return;}

      const key = `${column}:${line}:${start}:${end}`;
      if (seenColumns.has(key)) {return;}
      seenColumns.add(key);

      diagnostics.push({
        code: SARAL_DIAGNOSTIC_CODES.UnknownColumn,
        severity: resolveDiagnosticSeverity(SARAL_DIAGNOSTIC_CODES.UnknownColumn, DiagnosticSeverity.Error, diagnosticSeverityOverrides),
        range: {
          start: { line, character: start },
          end: { line, character: end }
        },
        message: `Column '${getTextAtRange(line, start, line, end) || column}' not found in table '${tableDisplay ?? table}'`,
        source: "SaralSQL"
      });
    }

    function getDisplayTableName(table: string): string {
      const norm = normalizeName(table);
      const stripped = norm.replace(/^dbo\./, "");
      const def = localDefsByName.get(norm) || localDefsByName.get(stripped) || tablesByName.get(norm) || tableTypesByName.get(norm) || tablesByName.get(stripped) || tableTypesByName.get(stripped);
      return def?.rawName ?? table;
    }

    function tableExistsInContext(tableName: string): boolean {
      const norm = normalizeName(tableName);
      if (isOutputPseudoTableReference(norm)) { return true; }
      if (isSystemTableReference(norm)) { return true; }
      if (localDefsByName.has(norm)) { return true; }
      const stripped = norm.replace(/^dbo\./, "");
      if (localDefsByName.has(stripped)) { return true; }
      return tableExists(tableName);
    }

    function columnExistsInContext(tableName: string, columnName: string): boolean {
      const norm = normalizeName(tableName);
      if (isOutputPseudoTableReference(norm)) { return true; }
      if (isSystemTableReference(norm)) { return true; }
      const stripped = norm.replace(/^dbo\./, "");
      const localDef = localDefsByName.get(norm) || localDefsByName.get(stripped);
      const targetCol = normalizeName(columnName);
      if (localDef?.columns?.some((c: any) => normalizeName(c.name ?? c.rawName) === targetCol)) {
        return true;
      }
      return columnExists(tableName, columnName);
    }

    function getSchemaEquivalentTableRefCandidates(tableName: string): ReferenceDef[] {
      const norm = normalizeName(tableName);
      const stripped = norm.replace(/^dbo\./, "");
      const dboPrefixed = stripped ? `dbo.${stripped}` : norm;
      const candidates = [
        ...(tableRefsByName.get(norm) ?? []),
        ...(tableRefsByName.get(stripped) ?? []),
        ...(tableRefsByName.get(dboPrefixed) ?? [])
      ];
      return candidates;
    }

    function getTextAtRange(startLine: number, startChar: number, endLine: number, endChar: number): string {
      const key = `${startLine}:${startChar}:${endLine}:${endChar}`;
      const cached = diagnosticTextCache.get(key);
      if (cached !== undefined) {
        return cached;
      }

      const value = doc.getText({
        start: { line: startLine, character: startChar },
        end: { line: endLine, character: endChar }
      }).trim();
      diagnosticTextCache.set(key, value);
      return value;
    }

    function isAliasMutationTarget(ref: ReferenceDef): boolean {
      if (ref.context !== "update-target" && ref.context !== "delete-target") {
        return false;
      }

      return Boolean(fileAliases?.has(normalizeName(ref.name)));
    }

    function resolveTempTableSymbolAtOffset(tableName: string, offset: number): any | null {
      if (!tableName.startsWith("#")) {
        return null;
      }

      if (parsed?.scope?.root) {
        const scopeAtPos = parsed.scope.root.findInnermost?.(offset) ?? parsed.scope.root;
        const sym = resolveSymbolCaseInsensitive(scopeAtPos, tableName);
        if (sym && (sym.kind === "TempTable" || (sym.kind === "Table" && String(sym.name ?? "").startsWith("#")))) {
          return sym;
        }
      }

      const fallback = tempTablesByUri.get(normDocUri)?.get(normalizeName(tableName));
      if (fallback && offset >= fallback.declaredAt) {
        return {
          kind: "TempTable",
          name: normalizeName(tableName),
          columns: Array.from(fallback.columns.values()).map((c) => ({ name: c, rawName: c }))
        };
      }

      return null;
    }

    function resolveCteSymbolAtOffset(tableName: string, offset: number): any | null {
      if (!parsed?.scope?.root) {
        return null;
      }

      const scopeAtPos = parsed.scope.root.findInnermost?.(offset) ?? parsed.scope.root;
      const sym = resolveSymbolCaseInsensitive(scopeAtPos, tableName);
      return sym?.kind === "CTE" ? sym : null;
    }

    function tempTableSymbolHasColumn(sym: any, columnName: string): boolean {
      if (!sym || !Array.isArray(sym.columns)) {
        return false;
      }

      const target = normalizeName(columnName);
      return sym.columns.some((c: any) => normalizeName(String(c?.rawName ?? c?.name ?? c)) === target);
    }

    function getDerivedAliasProjectedColumns(aliasNorm: string, aliasSym?: any): Set<string> {
      const out = new Set<string>();

      // When the alias symbol carries an inline column list (e.g. USING (VALUES…) AS source (col1, col2)
      // as populated by the parser for VALUES-based MERGE sources), that list is authoritative and
      // avoids mis-matching against a same-named alias from a different batch/statement.
      if (Array.isArray(aliasSym?.columns) && aliasSym.columns.length > 0) {
        for (const c of aliasSym.columns) {
          const n = normalizeName(String(c?.rawName ?? c?.name ?? c));
          if (n && n !== "expression" && n !== "*") {
            out.add(n);
          }
        }
        if (out.size > 0) {
          return out;
        }
      }

      const sources = Array.isArray(parsed?.lineage?.sources) ? parsed.lineage.sources : [];
      const isMeaningfulProjectionName = (value: string): boolean => {
        const n = normalizeName(value);
        return Boolean(n && n !== "expression" && n !== "*");
      };
      const collectQueryProjectionColumns = (query: any): any[] => {
        if (!query || typeof query !== "object") {
          return [];
        }
        if (Array.isArray(query.columns) && query.columns.length > 0) {
          return query.columns;
        }
        if (query.type === "SetOperator") {
          const left = collectQueryProjectionColumns(query.left);
          if (left.length > 0) {
            return left;
          }
          return collectQueryProjectionColumns(query.right);
        }
        return [];
      };

      const addColumnsFromSourceAlias = (sourceAlias: string): void => {
        const targetAlias = normalizeName(sourceAlias);
        if (!targetAlias) {
          return;
        }

        for (const src of sources) {
          const srcAlias = normalizeName(String(src?.alias ?? src?.name ?? ""));
          if (srcAlias !== targetAlias) {
            continue;
          }
          const candidates = [
            normalizeName(String(src?.baseName ?? "")),
            normalizeName(String(src?.name ?? ""))
          ].filter(Boolean);
          for (const candidate of candidates) {
            const stripped = candidate.replace(/^dbo\./, "");
            const def = localDefsByName.get(candidate)
              || localDefsByName.get(stripped)
              || tablesByName.get(candidate)
              || tablesByName.get(stripped)
              || tableTypesByName.get(candidate)
              || tableTypesByName.get(stripped);
            if (!def || !Array.isArray(def.columns)) {
              continue;
            }
            for (const c of def.columns) {
              const n = normalizeName(String(c?.rawName ?? c?.name ?? ""));
              if (n) {
                out.add(n);
              }
            }
          }
        }
      };

      for (const src of sources) {
        const srcAlias = normalizeName(String(src?.alias ?? src?.name ?? ""));
        if (!srcAlias || srcAlias !== aliasNorm || !Array.isArray(src?.projection)) {
          continue;
        }
        for (const p of src.projection) {
          const n = normalizeName(String(p?.name ?? ""));
          if (n && n !== "*") {
            out.add(n);
          }
        }
      }

      // Read derived subquery select-list projection names directly from AST shape.
      // This catches cases where lineage projection emits generic placeholders.
      const queryCols = collectQueryProjectionColumns(aliasSym?.location?.table?.query);
      if (Array.isArray(queryCols)) {
        for (const col of queryCols) {
          if (!col || col?.wildcard === true) {
            continue;
          }

          const directCandidates = [
            String(col?.alias ?? "").trim(),
            String(col?.outputName ?? "").trim(),
            String(col?.sourceName ?? "").trim()
          ].filter(Boolean);

          let projected = directCandidates.find((v) => isMeaningfulProjectionName(v)) ?? "";

          if (!projected) {
            const expr = col?.expression;
            if (expr?.type === "Identifier") {
              if (Array.isArray(expr.parts) && expr.parts.length > 0) {
                projected = String(expr.parts[expr.parts.length - 1] ?? "").trim();
              } else {
                projected = String(expr.name ?? "").trim();
              }
            } else if (expr?.type === "MemberExpression") {
              projected = String(expr.property ?? "").trim();
            }
          }

          const n = normalizeName(projected);
          if (isMeaningfulProjectionName(projected)) {
            out.add(n);
          }
        }
      }

      // Expand wildcard projection parts in derived aliases, e.g. SELECT d.*, e.Address ... AS s
      if (Array.isArray(queryCols)) {
        for (const col of queryCols) {
          if (col?.type !== "Column" || col?.wildcard !== true || col?.expression?.type !== "WildcardExpression") {
            continue;
          }
          const prefix = normalizeName(String(col?.expression?.tablePrefix?.name ?? ""));
          if (!prefix) {
            continue;
          }
          for (const src of sources) {
            const srcName = normalizeName(String(src?.alias ?? src?.name ?? ""));
            if (srcName !== prefix || !Array.isArray(src?.projection)) {
              continue;
            }
            for (const p of src.projection) {
              const n = normalizeName(String(p?.name ?? ""));
              if (n && n !== "*" && n !== "expression") {
                out.add(n);
              }
            }
          }
          // If parser projection for that source alias is empty or wildcard-only, hydrate from indexed schema.
          addColumnsFromSourceAlias(prefix);
        }
      }

      return out;
    }

    function getAliasCandidatesAtOffset(scopeAtPos: any, aliasNorm: string): any[] {
      if (!scopeAtPos || !aliasNorm) {
        return [];
      }
      const visible = typeof scopeAtPos.getVisibleSymbols === "function"
        ? scopeAtPos.getVisibleSymbols()
        : Object.values(scopeAtPos.symbols ?? {});
      return (visible as any[])
        .filter((s: any) => s?.kind === "Alias" && normalizeName(String(s?.name ?? "")) === aliasNorm)
        .filter((s: any) => {
          if (Array.isArray(s?.columns) && s.columns.length > 0) {
            return true;
          }
          const aliasTarget = normalizeName(resolveAliasTableName(s) ?? "");
          return Boolean(aliasTarget);
        });
    }

    function collectCteNames(scope: any): void {
      const symbols = typeof scope?.getOwnSymbols === "function"
        ? scope.getOwnSymbols()
        : Object.values(scope?.symbols ?? {});

      for (const sym of symbols) {
        if (sym?.kind === "CTE") {
          cteNames.add(normalizeName(sym.name));
        }
      }

      const children = typeof scope?.getChildren === "function"
        ? scope.getChildren()
        : (scope?.children ?? []);

      for (const child of children) {
        collectCteNames(child);
      }
    }

    collectCteNames(parsed!.scope!.root);
    for (const ref of refsForDoc) {
      if (ref.kind === "table") {
        const key = normalizeName(ref.name);
        const bucket = tableRefsByName.get(key) ?? [];
        bucket.push(ref);
        tableRefsByName.set(key, bucket);
      }
    }

    for (const ref of refsForDoc) {
      if (ref.kind === "table") {
        if (ref.validateSchema === false) {
          continue;
        }
        if (ref.context === "create-definition") {
          continue;
        }
        if (isAliasMutationTarget(ref)) {
          continue;
        }

        if (cteNames.has(normalizeName(ref.name))) {
          continue;
        }

        const refOffset = (lineStarts[ref.line] ?? 0) + ref.start;

        if (ref.context === "shadowed-by-alias") {
          const scopeAtPos = parsed?.scope?.root?.findInnermost?.(refOffset) ?? parsed?.scope?.root;
          const visible = typeof scopeAtPos?.getVisibleSymbols === "function" ? scopeAtPos.getVisibleSymbols() : [];
          const shadowingAlias = (visible as any[]).find((sym: any) =>
            sym?.kind === "Alias" && normalizeName(resolveAliasTableName(sym) ?? "") === normalizeName(ref.name)
          );
          if (shadowingAlias) {
            addShadowedByAliasTable(getDisplayTableName(ref.name), String(shadowingAlias.name ?? ""), ref.line, ref.start, ref.end);
          }
          continue;
        }

        if (fileAliases?.has(normalizeName(ref.name))) {
          continue;
        }
        const scopeAtPos = parsed?.scope?.root?.findInnermost?.(refOffset) ?? parsed?.scope?.root;
        const aliasCandidates = getAliasCandidatesAtOffset(scopeAtPos, normalizeName(ref.name));
        if (aliasCandidates.length > 0) {
          continue;
        }
        if (resolveCteSymbolAtOffset(ref.name, refOffset)) {
          continue;
        }
        if (resolveTempTableSymbolAtOffset(ref.name, refOffset)) {
          continue;
        }

        addUnknownTable(ref.name, ref.line, ref.start, ref.end);
        continue;
      }

      if (ref.kind !== "column") {
        continue;
      }

      const lastDot = ref.name.lastIndexOf(".");
      if (lastDot <= 0) {
        if (ref.validateSchema === false) {
          continue;
        }
        continue;
      }

      const table = normalizeName(ref.name.slice(0, lastDot));
      const column = normalizeName(ref.name.slice(lastDot + 1));
      const refOffset = (lineStarts[ref.line] ?? 0) + ref.start;

      // Validate local derived/alias sources (e.g., subquery alias "s") even when ref.validateSchema is false.
      const scopeAtPos = parsed?.scope?.root?.findInnermost?.(refOffset) ?? parsed?.scope?.root;
      const scopedSym = resolveSymbolCaseInsensitive(scopeAtPos, table);
      const aliasCandidates = getAliasCandidatesAtOffset(scopeAtPos, table);
      if (aliasCandidates.length > 0) {
        const withColumns = aliasCandidates
          .map((sym: any) => {
            const explicitCols = Array.isArray(sym.columns) ? sym.columns : [];
            const explicitHas = explicitCols.some((c: any) => normalizeName(String(c?.rawName ?? c?.name ?? c)) === column);
            const aliasTarget = normalizeName(resolveAliasTableName(sym) ?? "");
            const aliasTargetStripped = aliasTarget.replace(/^dbo\./, "");
            const aliasTargetDef = aliasTarget
              ? (localDefsByName.get(aliasTarget)
                || localDefsByName.get(aliasTargetStripped)
                || tablesByName.get(aliasTarget)
                || tablesByName.get(aliasTargetStripped)
                || tableTypesByName.get(aliasTarget)
                || tableTypesByName.get(aliasTargetStripped))
              : null;
            const targetHas = Array.isArray(aliasTargetDef?.columns)
              ? aliasTargetDef.columns.some((c: any) => normalizeName(String(c?.rawName ?? c?.name ?? c)) === column)
              : false;
            const projected = getDerivedAliasProjectedColumns(table, sym);
            const projectedHas = projected.has(column);
            const hasTargetKnowledge = Array.isArray(aliasTargetDef?.columns);
            const hasProjectedKnowledge = projected.size > 0;
            const hasExplicitKnowledge = explicitCols.length > 0;
            return { sym, aliasTarget, explicitHas, targetHas, projectedHas, hasTargetKnowledge, hasProjectedKnowledge, hasExplicitKnowledge };
          });

        if (withColumns.some(x =>
          (x.hasTargetKnowledge && x.targetHas)
          || (!x.hasTargetKnowledge && x.hasProjectedKnowledge && x.projectedHas)
          || (!x.hasTargetKnowledge && !x.hasProjectedKnowledge && x.explicitHas)
        )) {
          continue;
        }

        // Emit missing column only when at least one alias candidate has known projection shape.
        const known = withColumns.find(x => x.hasTargetKnowledge || x.hasProjectedKnowledge || x.hasExplicitKnowledge);
        if (known) {
          const display = known.hasTargetKnowledge && known.aliasTarget
            ? getDisplayTableName(known.aliasTarget)
            : getDisplaySymbolName(known.sym) || table;
          addWrongColumn(table, column, ref.line, ref.start, ref.end, display);
          continue;
        }
      }

      if (scopedSym?.kind === "Alias") {
        const projected = getDerivedAliasProjectedColumns(table, scopedSym);
        if (projected.size > 0) {
          if (!projected.has(column)) {
            addWrongColumn(table, column, ref.line, ref.start, ref.end, getDisplaySymbolName(scopedSym) || table);
          }
          continue;
        }

        const aliasTarget = normalizeName(resolveAliasTableName(scopedSym) ?? "");
        const aliasTargetStripped = aliasTarget.replace(/^dbo\./, "");
        const aliasTargetDef = aliasTarget
          ? (localDefsByName.get(aliasTarget)
            || localDefsByName.get(aliasTargetStripped)
            || tablesByName.get(aliasTarget)
            || tablesByName.get(aliasTargetStripped)
            || tableTypesByName.get(aliasTarget)
            || tableTypesByName.get(aliasTargetStripped))
          : null;
        if (Array.isArray(aliasTargetDef?.columns)) {
          const hasTargetCol = aliasTargetDef.columns.some((c: any) => normalizeName(String(c?.rawName ?? c?.name ?? c)) === column);
          if (!hasTargetCol) {
            addWrongColumn(table, column, ref.line, ref.start, ref.end, getDisplayTableName(aliasTarget));
          }
          continue;
        }

        if (Array.isArray(scopedSym.columns) && scopedSym.columns.length > 0) {
          const hasLocalCol = scopedSym.columns.some((c: any) => normalizeName(String(c?.rawName ?? c?.name ?? c)) === column);
          if (!hasLocalCol) {
            addWrongColumn(table, column, ref.line, ref.start, ref.end, getDisplaySymbolName(scopedSym) || table);
          }
          continue;
        }
      }
      if (scopedSym?.kind === "CTE") {
        const cteCols = getCteColumns(scopedSym);
        const hasLocalCol = cteCols.some((c: any) => normalizeName(String(c?.rawName ?? c?.name ?? c)) === column);
        if (!hasLocalCol) {
          addWrongColumn(table, column, ref.line, ref.start, ref.end, getDisplaySymbolName(scopedSym) || table);
        }
        continue;
      }

      if (ref.validateSchema === false) {
        continue;
      }

      if (propertyAccessRanges.some((r: { start: number; end: number }) => refOffset >= r.start && refOffset <= r.end)) {
        continue;
      }
      const tempTableSym = resolveTempTableSymbolAtOffset(table, refOffset);
      if (tempTableSym) {
        if (!tempTableSymbolHasColumn(tempTableSym, column)) {
          addWrongColumn(table, column, ref.line, ref.start, ref.end, getDisplayTableName(table));
        }
        continue;
      }

      const matchedResolution = parsed?.columns?.resolutions?.find((r: any) => r.location?.start === refOffset);
      if (matchedResolution?.inputs) {
        let hasResolvedColumnInput = false;
        let hasValidatedResolvedInput = false;
        for (const input of matchedResolution.inputs) {
          if (input?.kind !== "column" || !input?.source || !input?.name) {
            continue;
          }
          hasResolvedColumnInput = true;
          const srcTable = normalizeName(String(input.source));
          const srcCol = normalizeName(String(input.name).split(".").pop() ?? "");
          if (!srcTable || !srcCol) {
            continue;
          }
          if (String(input.sourceKind ?? "table") !== "table") {
            hasValidatedResolvedInput = true;
            break;
          }
          if (tableExistsInContext(srcTable) && columnExistsInContext(srcTable, srcCol)) {
            hasValidatedResolvedInput = true;
            break;
          }
        }
        if (hasResolvedColumnInput) {
          if (hasValidatedResolvedInput) {
            continue;
          }
          
          // The parser resolved the column, but it failed schema validation.
          // Emit the exact error so the user sees the typo!
          for (const input of matchedResolution.inputs) {
            if (input?.kind !== "column" || !input?.source || !input?.name) {continue;};
            const srcTable = normalizeName(String(input.source));
            const srcCol = normalizeName(String(input.name).split(".").pop() ?? "");
            if (!srcTable || !srcCol) {continue;};
            if (String(input.sourceKind ?? "table") !== "table") {continue;};
            
            if (!tableExistsInContext(srcTable)) {
              const tableRef = getSchemaEquivalentTableRefCandidates(srcTable)
                .find((r) => r.context === "insert-target" && r.validateSchema !== false)
                ?? getSchemaEquivalentTableRefCandidates(srcTable).find((r) => r.validateSchema !== false);
              if (tableRef) {
                addUnknownTable(tableRef.name, tableRef.line, tableRef.start, tableRef.end);
              } else {
                addUnknownTable(srcTable, ref.line, ref.start, ref.end, true);
              }
            } else if (!columnExistsInContext(srcTable, srcCol)) {
              addWrongColumn(srcTable, srcCol, ref.line, ref.start, ref.end, getDisplayTableName(srcTable));
            }
          }
          continue;
        }
      }

      if (!table || table.startsWith("@")) {
        continue;
      }

      if (cteNames.has(table)) {
        continue;
      }

      if (isSystemTableReference(table)) {
        continue;
      }

      if (!tableExistsInContext(table)) {
        const tableRef = getSchemaEquivalentTableRefCandidates(table)
          .find((r) => r.context === "insert-target" && r.validateSchema !== false)
          ?? getSchemaEquivalentTableRefCandidates(table).find((r) => r.validateSchema !== false);
        if (tableRef) {
          addUnknownTable(tableRef.name, tableRef.line, tableRef.start, tableRef.end);
        } else {
          addUnknownTable(table, ref.line, ref.start, ref.end, true);
        }
        continue;
      }

      if (!columnExistsInContext(table, column)) {
        addWrongColumn(table, column, ref.line, ref.start, ref.end, getDisplayTableName(table));
      }
    }

    function visitScope(scope: any) {
      for (const sym of Object.values(scope.symbols ?? {}) as any[]) {
        if (sym.kind === "Alias" && sym.location?.table) {
          const t = sym.location.table;
          const tableNodeType = String(t.type ?? "");
          const isTableLikeNode =
            tableNodeType === "Identifier" ||
            tableNodeType === "MemberExpression";

          if (!isTableLikeNode) {
            continue;
          }

          if (
            typeof t.start === "number" &&
            typeof t.end === "number"
          ) {
            const range = makeOffsetRange(t.start, t.end);
            addUnknownTableAt(t.name, range.start.line, range.start.character, range.end.line, range.end.character);
          }
        }

        if (sym.kind === "Table" && sym.location?.nameNode) {
          const n = sym.location.nameNode;

          if (
            typeof n.start === "number" &&
            typeof n.end === "number"
          ) {
            const range = makeOffsetRange(n.start, n.end);
            addUnknownTableAt(n.name, range.start.line, range.start.character, range.end.line, range.end.character);
          }
        }
      }

      for (const child of scope.children ?? []) {
        visitScope(child);
      }
    }

    visitScope(parsed!.scope!.root);

    for (const diag of collectAmbiguousColumnDiagnostics(parsed, lineStarts, tablesByName, tableTypesByName, "SaralSQL", diagnosticSeverityOverrides)) {
      if (!shouldSuppressDiagnosticCode(String((diag as any).code ?? ""), disabledDiagnosticCodes)) {
        diagnostics.push(diag);
      }
    }

    for (const diag of collectReadableBareColumnDiagnostics(parsed, lineStarts, tablesByName, tableTypesByName, "SaralSQL", diagnosticSeverityOverrides)) {
      if (!shouldSuppressDiagnosticCode(String((diag as any).code ?? ""), disabledDiagnosticCodes)) {
        diagnostics.push(diag);
      }
    }

    for (const diag of collectStringComparisonDiagnostics(parsed, lineStarts, tablesByName, tableTypesByName, "SaralSQL", diagnosticSeverityOverrides)) {
      if (!shouldSuppressDiagnosticCode(String((diag as any).code ?? ""), disabledDiagnosticCodes)) {
        diagnostics.push(diag);
      }
    }
  } catch (e) {
    /* safeLog */ console.error(`[validate] Schema validation failed: ${String(e)}`);
  }

  return diagnostics;
}

// ── Standalone helpers used by computeSchemaDiagnostics ──────────────────────

function tableExists(tableName: string): boolean {
  const norm = normalizeName(tableName);
  if (isOutputPseudoTableReference(norm)) { return true; }
  if (isSystemTableReference(norm)) { return true; }

  if (tablesByName.has(norm) || tableTypesByName.has(norm)) { return true; }
  if (columnsByTable.has(norm)) { return true; }

  const stripped = norm.replace(/^dbo\./, "");
  if (tablesByName.has(stripped) || tableTypesByName.has(stripped)) { return true; }
  if (columnsByTable.has(stripped)) { return true; }

  return false;
}

function columnExists(tableName: string, columnName: string): boolean {
  const norm = normalizeName(tableName);
  if (isOutputPseudoTableReference(norm)) { return true; }
  if (isSystemTableReference(norm)) { return true; }
  const column = normalizeName(columnName);
  const direct = columnsByTable.get(norm);
  if (direct?.has(column)) { return true; }

  const stripped = norm.replace(/^dbo\./, "");
  const strippedCols = columnsByTable.get(stripped);
  if (strippedCols?.has(column)) { return true; }

  return false;
}

function isSystemTableReference(tableName: string): boolean {
  const norm = normalizeName(tableName);

  if (!norm) {
    return false;
  }

  if (norm.startsWith("sys.") || norm.startsWith("information_schema.")) {
    return true;
  }

  if (norm.includes(".sys.") || norm.includes(".information_schema.")) {
    return true;
  }

  const systemObjectPrefixes = [
    "sysdm_",
    "sys.dm_",
    "sysall_",
    "sysobjects",
    "sysindexes",
    "syscolumns",
    "systypes"
  ];

  return systemObjectPrefixes.some(prefix => norm.startsWith(prefix));
}

function isOutputPseudoTableReference(tableName: string): boolean {
  const norm = normalizeName(tableName);
  return norm === "inserted" || norm === "deleted";
}

function resolveExistingSqlCmdInclude(docUri: string, message: string): string | null {
  const includeMatch = /SQLCMD include was not resolved:\s*(.+?)\.\s*$/i.exec(String(message ?? "").trim());
  if (!includeMatch) {
    return null;
  }

  let includeRaw = String(includeMatch[1] ?? "").trim();
  if (!includeRaw || includeRaw === "<empty>") {
    return null;
  }
  includeRaw = includeRaw.replace(/^["']|["']$/g, "");

  try {
    const docFsPath = url.fileURLToPath(docUri);
    const baseDir = path.dirname(docFsPath);
    const resolved = path.isAbsolute(includeRaw) ? includeRaw : path.resolve(baseDir, includeRaw);
    return fs.existsSync(resolved) ? resolved : null;
  } catch {
    return null;
  }
}

function mapSeverity(severity: unknown, code?: string): DiagnosticSeverity {
  const s = String(severity ?? "").toLowerCase();
  let mapped: DiagnosticSeverity;

  switch (s) {
    case "error":
      mapped = DiagnosticSeverity.Error;
      break;

    case "warning":
    case "warn":
      mapped = DiagnosticSeverity.Warning;
      break;

    case "info":
    case "information":
      mapped = DiagnosticSeverity.Information;
      break;

    case "hint":
      mapped = DiagnosticSeverity.Hint;
      break;

    default:
      mapped = DiagnosticSeverity.Error;
      break;
  }

  return mapped;
}

import { CodeAction, CodeActionKind, Diagnostic, DiagnosticSeverity, TextEdit } from "vscode-languageserver/node";
import { isSqlKeyword, normalizeName, offsetToPosition } from "./text-utils";
import { getCteColumns, getDisplaySymbolName, normalizeAstTableName, resolveAliasTableName, resolveSymbolCaseInsensitive } from "./ast-utils";
import { extractReferences } from "@saralsql/tsql-parser";
import { resolveBareColumnAtOffset } from "./column-resolution";

export const SARAL_DIAGNOSTIC_CODES = {
  UnknownTable: "LSP001",
  UnknownColumn: "LSP002",
  AmbiguousColumn: "LSP003",
  ReadabilityQualifyColumn: "LSP004",
  StringComparison: "LSP005"
} as const;

type DiagnosticSettingSpec = {
  enabledPath: string;
  severityPath: string;
  code: string;
  fallbackSeverity: DiagnosticSeverity;
};

const DIAGNOSTIC_SETTING_SPECS: DiagnosticSettingSpec[] = [
  { enabledPath: "diagnostics.unknownTable", severityPath: "diagnostics.unknownTableSeverity", code: SARAL_DIAGNOSTIC_CODES.UnknownTable, fallbackSeverity: DiagnosticSeverity.Error },
  { enabledPath: "diagnostics.unknownColumn", severityPath: "diagnostics.unknownColumnSeverity", code: SARAL_DIAGNOSTIC_CODES.UnknownColumn, fallbackSeverity: DiagnosticSeverity.Error },
  { enabledPath: "diagnostics.ambiguousColumn", severityPath: "diagnostics.ambiguousColumnSeverity", code: SARAL_DIAGNOSTIC_CODES.AmbiguousColumn, fallbackSeverity: DiagnosticSeverity.Warning },
  { enabledPath: "diagnostics.readabilityHint", severityPath: "diagnostics.readabilityHintSeverity", code: SARAL_DIAGNOSTIC_CODES.ReadabilityQualifyColumn, fallbackSeverity: DiagnosticSeverity.Information },
  { enabledPath: "diagnostics.stringComparison", severityPath: "diagnostics.stringComparisonSeverity", code: SARAL_DIAGNOSTIC_CODES.StringComparison, fallbackSeverity: DiagnosticSeverity.Warning },
  { enabledPath: "diagnostics.unnamedKeyConstraint", severityPath: "diagnostics.unnamedKeyConstraintSeverity", code: "DDL002", fallbackSeverity: DiagnosticSeverity.Warning },
  { enabledPath: "diagnostics.unnamedDefaultConstraint", severityPath: "diagnostics.unnamedDefaultConstraintSeverity", code: "DDL003", fallbackSeverity: DiagnosticSeverity.Warning },
  { enabledPath: "diagnostics.undeclaredVariable", severityPath: "diagnostics.undeclaredVariableSeverity", code: "VAR001", fallbackSeverity: DiagnosticSeverity.Error },
  { enabledPath: "diagnostics.unusedVariable", severityPath: "diagnostics.unusedVariableSeverity", code: "VAR002", fallbackSeverity: DiagnosticSeverity.Warning },
  { enabledPath: "diagnostics.unusedParameter", severityPath: "diagnostics.unusedParameterSeverity", code: "VAR003", fallbackSeverity: DiagnosticSeverity.Warning },
  { enabledPath: "diagnostics.variableUsedBeforeSet", severityPath: "diagnostics.variableUsedBeforeSetSeverity", code: "VAR004", fallbackSeverity: DiagnosticSeverity.Warning },
  { enabledPath: "diagnostics.selfComparison", severityPath: "diagnostics.selfComparisonSeverity", code: "LOG001", fallbackSeverity: DiagnosticSeverity.Warning },
  { enabledPath: "diagnostics.updateWithoutWhere", severityPath: "diagnostics.updateWithoutWhereSeverity", code: "DML001", fallbackSeverity: DiagnosticSeverity.Warning },
  { enabledPath: "diagnostics.deleteWithoutWhere", severityPath: "diagnostics.deleteWithoutWhereSeverity", code: "DML002", fallbackSeverity: DiagnosticSeverity.Warning },
  { enabledPath: "diagnostics.insertWithoutColumnList", severityPath: "diagnostics.insertWithoutColumnListSeverity", code: "DML003", fallbackSeverity: DiagnosticSeverity.Warning },
  { enabledPath: "diagnostics.updateTargetNoLock", severityPath: "diagnostics.updateTargetNoLockSeverity", code: "DML004", fallbackSeverity: DiagnosticSeverity.Error }
];

function readBooleanSetting(settings: any, path: string): boolean | undefined {
  const parts = path.split(".");
  let current = settings;

  for (const part of parts) {
    if (!current || typeof current !== "object") {
      return undefined;
    }
    current = current[part];
  }

  return typeof current === "boolean" ? current : undefined;
}

export function normalizeSaralSqlSettings(settings: any): any {
  return settings?.saralsql && typeof settings.saralsql === "object"
    ? settings.saralsql
    : settings;
}

function readStringSetting(settings: any, path: string): string | undefined {
  const parts = path.split(".");
  let current = settings;

  for (const part of parts) {
    if (!current || typeof current !== "object") {
      return undefined;
    }
    current = current[part];
  }

  return typeof current === "string" ? current : undefined;
}

function addIfDisabled(disabled: Set<string>, enabled: boolean | undefined, code: string): void {
  if (enabled === false) {
    disabled.add(code);
  }
}

function parseDiagnosticSeveritySetting(value: string | undefined): DiagnosticSeverity | undefined {
  switch (String(value ?? "").trim().toLowerCase()) {
    case "error":
      return DiagnosticSeverity.Error;
    case "warning":
    case "warn":
      return DiagnosticSeverity.Warning;
    case "information":
    case "info":
      return DiagnosticSeverity.Information;
    case "hint":
      return DiagnosticSeverity.Hint;
    default:
      return undefined;
  }
}

export function buildDisabledDiagnosticCodes(settings: any): Set<string> {
  settings = normalizeSaralSqlSettings(settings);
  const disabled = new Set<string>();

  const rawDisabled = settings?.disabledDiagnostics ?? [];
  const disabledList = Array.isArray(rawDisabled) ? rawDisabled : [rawDisabled];
  for (const value of disabledList) {
    const code = String(value ?? "").trim().toUpperCase();
    if (code) {
      disabled.add(code);
    }
  }

  for (const spec of DIAGNOSTIC_SETTING_SPECS) {
    addIfDisabled(disabled, readBooleanSetting(settings, spec.enabledPath), spec.code);
  }

  return disabled;
}

export function buildDiagnosticSeverityOverrides(settings: any): Map<string, DiagnosticSeverity> {
  settings = normalizeSaralSqlSettings(settings);
  const overrides = new Map<string, DiagnosticSeverity>();

  for (const spec of DIAGNOSTIC_SETTING_SPECS) {
    const severity = parseDiagnosticSeveritySetting(readStringSetting(settings, spec.severityPath));
    if (severity !== undefined) {
      overrides.set(spec.code, severity);
      continue;
    }

    overrides.set(spec.code, spec.fallbackSeverity);
  }

  return overrides;
}

export function shouldSuppressDiagnosticCode(code: string | undefined, disabledCodes: Set<string>): boolean {
  if (!code || disabledCodes.size === 0) {
    return false;
  }

  return disabledCodes.has(String(code).trim().toUpperCase());
}

export function resolveDiagnosticSeverity(
  code: string | undefined,
  fallback: DiagnosticSeverity,
  severityOverrides: Map<string, DiagnosticSeverity>
): DiagnosticSeverity {
  if (!code || severityOverrides.size === 0) {
    return fallback;
  }

  return severityOverrides.get(String(code).trim().toUpperCase()) ?? fallback;
}

export function hasBlockingParseIssues(parsed: any, parserIssues: any[]): boolean {
  if (!parsed?.ast) {
    return true;
  }

  return (parserIssues ?? []).some((issue: any) => {
    const sev = String(issue?.severity ?? "").toLowerCase();
    const code = String(issue?.code ?? "").toUpperCase();
    return sev === "error" && code !== "PARSE_STUCK";
  });
}

export function collectAmbiguousColumnDiagnostics(
  parsed: any,
  lineStarts: number[],
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>,
  source = "SaralSQL",
  severityOverrides = new Map<string, DiagnosticSeverity>()
): Diagnostic[] {
  if (!parsed?.ast || !parsed?.scope?.root) {
    return [];
  }

  const diagnostics: Diagnostic[] = [];
  const seen = new Set<string>();
  const refs = extractReferences(parsed.ast) ?? [];
  const resolutions = parsed.columns?.resolutions ?? [];
  const orderByAliasStarts = collectOrderByAliasStarts(parsed.ast);
  const orderByDuplicateAliasStarts = collectOrderByDuplicateAliasStarts(parsed.ast);
  const outputPseudoColumnStarts = collectOutputPseudoColumnStarts(parsed.ast);
  const readScopeRanges = collectReadScopeRanges(parsed);

  for (const ref of refs) {
    if (!ref || (ref.kind !== "column" && ref.kind !== "unknown")) {
      continue;
    }
    if (!ref.location || typeof ref.location.start !== "number") {
      continue;
    }

    const name = String(ref.name ?? "");
    if (!name || name.includes(".") || name.startsWith("@") || isSqlKeyword(normalizeName(name))) {
      continue;
    }
    const colNorm = normalizeName(name);
    const containingSelect = findContainingSelectAtOffset(parsed?.ast, ref.location.start);
    if (containingSelect && (!Array.isArray(containingSelect.from) || containingSelect.from.length === 0)) {
      continue;
    }
    if (outputPseudoColumnStarts.has(ref.location.start)) {
      continue;
    }
    if (orderByAliasStarts.has(ref.location.start)) {
      continue;
    }
    if (orderByDuplicateAliasStarts.has(ref.location.start)) {
      const startPos = offsetToPosition(ref.location.start, lineStarts);
      const endPos = offsetToPosition(ref.location.end ?? ref.location.start, lineStarts);
      const key = `${startPos.line}:${startPos.character}:${colNorm}`;
      if (!seen.has(key)) {
        seen.add(key);
        diagnostics.push({
          code: SARAL_DIAGNOSTIC_CODES.AmbiguousColumn,
          severity: resolveDiagnosticSeverity(SARAL_DIAGNOSTIC_CODES.AmbiguousColumn, DiagnosticSeverity.Warning, severityOverrides),
          range: {
            start: { line: startPos.line, character: startPos.character },
            end: { line: endPos.line, character: endPos.character }
          },
          message: `Ambiguous column '${name}' could refer to multiple sources`,
          source
        });
      }
      continue;
    }

    const resolved = resolveBareColumnAtOffset({
      parsed,
      offset: ref.location.start,
      columnName: name,
      tablesByName,
      tableTypesByName
    });
    if (hasSingleSelectSourceAtOffset(parsed?.ast, ref.location.start)) {
      continue;
    }
    if (hasSingleSelectVariableSourceAtOffset(parsed?.ast, ref.location.start)) {
      continue;
    }
    const readSourceCount = getReadScopeSourceCountAtOffset(readScopeRanges, ref.location.start);
    if (readSourceCount === 1) {
      continue;
    }

    if (resolved.status === "ambiguous") {
      const startPos = offsetToPosition(ref.location.start, lineStarts);
      const endPos = offsetToPosition(ref.location.end ?? ref.location.start, lineStarts);
      const key = `${startPos.line}:${startPos.character}:${colNorm}`;
      if (!seen.has(key)) {
        seen.add(key);
        diagnostics.push({
          code: SARAL_DIAGNOSTIC_CODES.AmbiguousColumn,
          severity: resolveDiagnosticSeverity(SARAL_DIAGNOSTIC_CODES.AmbiguousColumn, DiagnosticSeverity.Warning, severityOverrides),
          range: {
            start: { line: startPos.line, character: startPos.character },
            end: { line: endPos.line, character: endPos.character }
          },
          message: `Ambiguous column '${name}' could refer to multiple sources`,
          source
        });
      }
      continue;
    }

    if (resolved.status === "resolved") {
      continue;
    }

    if (isBareColumnInMutationStatementAtOffset(parsed?.ast, ref.location.start)) {
      continue;
    }

    // Do not emit ambiguity from parser input-count fallback.
    // Ambiguity must be rooted in scope-visible competing owners only.
    continue;
  }

  return diagnostics;
}

function isBareColumnInMutationStatementAtOffset(ast: any, offset: number): boolean {
  if (!ast || typeof offset !== "number") {
    return false;
  }

  const body = Array.isArray(ast?.body) ? ast.body : [];
  for (const stmt of body) {
    const start = Number(stmt?.start);
    const end = Number(stmt?.end);
    if (!Number.isFinite(start) || !Number.isFinite(end) || offset < start || offset > end) {
      continue;
    }

    if (stmt?.type === "UpdateStatement" || stmt?.type === "DeleteStatement") {
      return true;
    }
  }

  return false;
}

function hasSingleSelectSourceAtOffset(ast: any, offset: number): boolean {
  const best = findContainingSelectAtOffset(ast, offset);
  if (!best) {
    return false;
  }
  if (!Array.isArray(best.from) || best.from.length === 0) {
    return false;
  }

  let sourceCount = 0;
  for (const tableRef of best.from) {
    sourceCount += 1;
    if (Array.isArray(tableRef?.joins)) {
      sourceCount += tableRef.joins.length;
    }
  }

  return sourceCount === 1;
}

function hasSingleSelectVariableSourceAtOffset(ast: any, offset: number): boolean {
  const best = findContainingSelectAtOffset(ast, offset);
  if (!best || !Array.isArray(best.from) || best.from.length !== 1) {
    return false;
  }
  const tableName = String(best.from[0]?.table?.name ?? "");
  return tableName.startsWith("@") || tableName.startsWith("#");
}

function findContainingSelectAtOffset(ast: any, offset: number): any | null {
  if (!ast || typeof offset !== "number") {
    return null;
  }
  let best: any = null;
  const visit = (node: any): void => {
    if (!node || typeof node !== "object") {
      return;
    }
    if (node.type === "SelectStatement" && typeof node.start === "number" && typeof node.end === "number") {
      if (offset >= node.start && offset <= node.end) {
        if (!best || (node.end - node.start) < (best.end - best.start)) {
          best = node;
        }
      }
    }
    if (Array.isArray(node)) {
      for (const item of node) {
        visit(item);
      }
      return;
    }
    for (const value of Object.values(node)) {
      if (value && typeof value === "object") {
        visit(value);
      }
    }
  };
  visit(ast);
  return best;
}

function collectReadScopeRanges(parsed: any): Array<{ start: number; end: number; sourceCount: number }> {
  const out: Array<{ start: number; end: number; sourceCount: number }> = [];
  const scopes = Array.isArray(parsed?.lineage?.readScopes) ? parsed.lineage.readScopes : [];
  for (const scope of scopes) {
    const start = Number(scope?.location?.start);
    const end = Number(scope?.location?.end);
    if (!Number.isFinite(start) || !Number.isFinite(end)) {
      continue;
    }
    const sourceCount = Array.isArray(scope?.sources) ? scope.sources.length : 0;
    out.push({ start, end, sourceCount });
  }
  return out;
}

function getReadScopeSourceCountAtOffset(
  ranges: Array<{ start: number; end: number; sourceCount: number }>,
  offset: number
): number | null {
  let best: { start: number; end: number; sourceCount: number } | null = null;
  for (const r of ranges) {
    if (offset < r.start || offset > r.end) {
      continue;
    }
    if (!best || (r.end - r.start) < (best.end - best.start)) {
      best = r;
    }
  }
  return best?.sourceCount ?? null;
}

function collectOrderByAliasStarts(ast: any): Set<number> {
  const starts = new Set<number>();

  const visit = (node: any): void => {
    if (!node || typeof node !== "object") {
      return;
    }

    if (node.type === "SelectStatement" && Array.isArray(node.columns) && Array.isArray(node.orderBy)) {
      const aliasCounts = new Map<string, number>();
      for (const col of node.columns) {
        const alias = normalizeName(String(col?.alias ?? col?.outputName ?? ""));
        if (alias) {
          aliasCounts.set(alias, (aliasCounts.get(alias) ?? 0) + 1);
        }
      }

      if (aliasCounts.size > 0) {
        for (const order of node.orderBy) {
          const expr = order?.expression;
          if (expr?.type !== "Identifier") {
            continue;
          }

          const raw = Array.isArray(expr.parts) && expr.parts.length > 0
            ? String(expr.parts[expr.parts.length - 1] ?? "")
            : String(expr.name ?? "");
          const norm = normalizeName(raw);
          if (norm && (aliasCounts.get(norm) ?? 0) === 1 && typeof expr.start === "number") {
            starts.add(expr.start);
          }
        }
      }
    }

    if (Array.isArray(node)) {
      for (const item of node) {
        visit(item);
      }
      return;
    }

    for (const value of Object.values(node)) {
      if (value && typeof value === "object") {
        visit(value);
      }
    }
  };

  visit(ast);
  return starts;
}

function collectOrderByDuplicateAliasStarts(ast: any): Set<number> {
  const starts = new Set<number>();

  const visit = (node: any): void => {
    if (!node || typeof node !== "object") {
      return;
    }

    if (node.type === "SelectStatement" && Array.isArray(node.columns) && Array.isArray(node.orderBy)) {
      const aliasCounts = new Map<string, number>();
      for (const col of node.columns) {
        const alias = normalizeName(String(col?.alias ?? col?.outputName ?? ""));
        if (alias) {
          aliasCounts.set(alias, (aliasCounts.get(alias) ?? 0) + 1);
        }
      }

      if (aliasCounts.size > 0) {
        for (const order of node.orderBy) {
          const expr = order?.expression;
          if (expr?.type !== "Identifier") {
            continue;
          }

          const raw = Array.isArray(expr.parts) && expr.parts.length > 0
            ? String(expr.parts[expr.parts.length - 1] ?? "")
            : String(expr.name ?? "");
          const norm = normalizeName(raw);
          if (norm && (aliasCounts.get(norm) ?? 0) > 1 && typeof expr.start === "number") {
            starts.add(expr.start);
          }
        }
      }
    }

    if (Array.isArray(node)) {
      for (const item of node) {
        visit(item);
      }
      return;
    }

    for (const value of Object.values(node)) {
      if (value && typeof value === "object") {
        visit(value);
      }
    }
  };

  visit(ast);
  return starts;
}

function collectQualifiedIdentifierStarts(ast: any): Set<number> {
  const starts = new Set<number>();

  const visit = (node: any): void => {
    if (!node || typeof node !== "object") {
      return;
    }

    if (node.type === "Identifier" && Array.isArray(node.parts) && node.parts.length > 1 && typeof node.start === "number") {
      starts.add(node.start);
    }

    if (Array.isArray(node)) {
      for (const item of node) {
        visit(item);
      }
      return;
    }

    for (const value of Object.values(node)) {
      if (value && typeof value === "object") {
        visit(value);
      }
    }
  };

  visit(ast);
  return starts;
}

export function collectReadableBareColumnDiagnostics(
  parsed: any,
  lineStarts: number[],
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>,
  source = "SaralSQL",
  severityOverrides = new Map<string, DiagnosticSeverity>()
): Diagnostic[] {
  if (!parsed?.ast || !parsed?.scope?.root) {
    return [];
  }

  const diagnostics: Diagnostic[] = [];
  const seen = new Set<string>();
  const refs = extractReferences(parsed.ast) ?? [];
  const qualifiedIdentifierStarts = collectQualifiedIdentifierStarts(parsed.ast);
  const outputPseudoColumnStarts = collectOutputPseudoColumnStarts(parsed.ast);

  for (const ref of refs) {
    if (!ref || (ref.kind !== "column" && ref.kind !== "unknown")) {
      continue;
    }
    if (!ref.location || typeof ref.location.start !== "number") {
      continue;
    }

    const name = String(ref.name ?? "");
    if (!name || name.includes(".") || name.startsWith("@") || isSqlKeyword(normalizeName(name))) {
      continue;
    }
    if (qualifiedIdentifierStarts.has(ref.location.start)) {
      continue;
    }
    if (outputPseudoColumnStarts.has(ref.location.start)) {
      continue;
    }

    const scopeAtPos = parsed.scope.root.findInnermost?.(ref.location.start) ?? parsed.scope.root;
    const colNorm = normalizeName(name);
    const resolved = resolveBareColumnAtOffset({
      parsed,
      offset: ref.location.start,
      columnName: name,
      tablesByName,
      tableTypesByName,
      scopeAtPos
    });
    if (resolved.status !== "resolved") {
      continue;
    }
    const matches: Array<{ alias: string; displayAlias: string }> = [];
    if (resolved.owner?.alias && resolved.owner?.displayAlias) {
      matches.push({ alias: resolved.owner.alias, displayAlias: resolved.owner.displayAlias });
    }

    if (matches.length !== 1) {
      continue;
    }

    const startPos = offsetToPosition(ref.location.start, lineStarts);
    const endPos = offsetToPosition(ref.location.end ?? ref.location.start, lineStarts);
    const key = `${startPos.line}:${startPos.character}:${colNorm}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);

    const match = matches[0];
    const replacement = `${match.displayAlias}.${name}`;
    diagnostics.push({
      code: SARAL_DIAGNOSTIC_CODES.ReadabilityQualifyColumn,
      severity: resolveDiagnosticSeverity(SARAL_DIAGNOSTIC_CODES.ReadabilityQualifyColumn, DiagnosticSeverity.Information, severityOverrides),
      range: {
        start: { line: startPos.line, character: startPos.character },
        end: { line: endPos.line, character: endPos.character }
      },
      message: `Consider qualifying '${name}' as '${replacement}' for readability`,
      source,
      data: {
        kind: "qualify-bare-column",
        alias: match.displayAlias,
        column: name,
        replacement
      }
    });
  }

  return diagnostics;
}

function collectOutputPseudoColumnStarts(ast: any): Set<number> {
  const starts = new Set<number>();

  const visit = (node: any): void => {
    if (!node || typeof node !== "object") {
      return;
    }

    if (node.type === "OutputColumn") {
      const sourceTable = normalizeName(String(node.sourceTable ?? ""));
      if (sourceTable === "inserted" || sourceTable === "deleted") {
        const start = Number(node.column?.expression?.start ?? node.column?.start);
        if (Number.isFinite(start)) {
          starts.add(start);
        }
      }
    }

    if (Array.isArray(node)) {
      for (const item of node) {
        visit(item);
      }
      return;
    }

    for (const value of Object.values(node)) {
      if (value && typeof value === "object") {
        visit(value);
      }
    }
  };

  visit(ast);
  return starts;
}

export function buildReadableBareColumnCodeAction(uri: string, diagnostic: Diagnostic): CodeAction | null {
  const data = diagnostic.data as any;
  const fallback = parseReadableBareColumnDiagnosticMessage(String(diagnostic.message ?? ""));
  const hasStructuredData = data && data.kind === "qualify-bare-column";
  if (!hasStructuredData && !fallback) {
    return null;
  }

  const replacement = String(hasStructuredData ? data.replacement ?? "" : fallback?.replacement ?? "");
  if (!replacement || !diagnostic.range) {
    return null;
  }

  const alias = String(hasStructuredData ? data.alias ?? "" : fallback?.alias ?? "");
  return {
    title: `Qualify with ${alias || replacement.split(".")[0] || ""}`,
    kind: CodeActionKind.QuickFix,
    diagnostics: [diagnostic],
    edit: {
      changes: {
        [uri]: [TextEdit.replace(diagnostic.range, replacement)]
      }
    }
  };
}

function parseReadableBareColumnDiagnosticMessage(message: string): { alias: string; replacement: string } | null {
  const match = /^Consider qualifying '([^']+)' as '([^']+)' for readability$/i.exec(message.trim());
  if (!match) {
    return null;
  }

  const replacement = String(match[2] ?? "");
  if (!replacement.includes(".")) {
    return null;
  }

  return {
    alias: replacement.split(".")[0] ?? "",
    replacement
  };
}

export function buildUpdateNoLockCodeAction(
  uri: string,
  diagnostic: Diagnostic,
  text: string,
  lineStarts: number[]
): CodeAction | null {
  if (String(diagnostic.code ?? "").toUpperCase() !== "DML004" || !diagnostic.range) {
    return null;
  }

  const diagStart = positionToOffset(diagnostic.range.start.line, diagnostic.range.start.character, lineStarts);
  const diagEnd = positionToOffset(diagnostic.range.end.line, diagnostic.range.end.character, lineStarts);
  const noLockMatch = findNearestWordCaseInsensitive(text, "nolock", diagStart, diagEnd);
  if (!noLockMatch) {
    return null;
  }

  const withRange = findWithHintRangeNoRegex(text, noLockMatch.start);
  if (!withRange) {
    return null;
  }

  const hintBody = text.slice(withRange.openParen + 1, withRange.closeParen);
  const hints = splitTopLevelByComma(hintBody);
  const kept = hints.filter((h: string) => normalizeName(h.replace(/[\[\]]/g, "")) !== "nolock");
  if (kept.length === hints.length) {
    return null;
  }

  const editStart = withRange.withStart;
  const editEnd = withRange.closeParen + 1;
  const replacement = kept.length === 0 ? "" : `WITH (${kept.join(", ")})`;

  const startPos = offsetToPosition(editStart, lineStarts);
  const endPos = offsetToPosition(editEnd, lineStarts);
  const title = kept.length === 0
    ? "Remove WITH (NOLOCK)"
    : "Remove NOLOCK table hint";

  return {
    title,
    kind: CodeActionKind.QuickFix,
    diagnostics: [diagnostic],
    edit: {
      changes: {
        [uri]: [TextEdit.replace({
          start: { line: startPos.line, character: startPos.character },
          end: { line: endPos.line, character: endPos.character }
        }, replacement)]
      }
    }
  };
}

function positionToOffset(line: number, character: number, lineStarts: number[]): number {
  return (lineStarts[line] ?? 0) + character;
}

function isWordBoundary(ch: string | undefined): boolean {
  if (!ch) {
    return true;
  }
  const code = ch.charCodeAt(0);
  const isAlphaNum = (code >= 48 && code <= 57) || (code >= 65 && code <= 90) || (code >= 97 && code <= 122);
  return !isAlphaNum && ch !== "_" && ch !== "$" && ch !== "#";
}

function isWhitespace(ch: string | undefined): boolean {
  return ch === " " || ch === "\t" || ch === "\r" || ch === "\n";
}

function findNearestWordCaseInsensitive(
  text: string,
  word: string,
  start: number,
  end: number
): { start: number; end: number } | null {
  const lower = text.toLowerCase();
  const target = word.toLowerCase();
  let idx = -1;
  let best: { start: number; end: number; score: number } | null = null;

  while (true) {
    idx = lower.indexOf(target, idx + 1);
    if (idx < 0) {
      break;
    }
    const before = idx > 0 ? text[idx - 1] : undefined;
    const after = idx + target.length < text.length ? text[idx + target.length] : undefined;
    if (!isWordBoundary(before) || !isWordBoundary(after)) {
      continue;
    }
    const tokenEnd = idx + target.length;
    const overlaps = !(tokenEnd < start || idx > end);
    const score = overlaps ? 0 : Math.min(Math.abs(idx - start), Math.abs(idx - end));
    if (!best || score < best.score) {
      best = { start: idx, end: tokenEnd, score };
    }
    if (overlaps) {
      break;
    }
  }

  return best ? { start: best.start, end: best.end } : null;
}

function findWithHintRangeNoRegex(
  text: string,
  noLockStart: number
): { withStart: number; openParen: number; closeParen: number } | null {
  const lower = text.toLowerCase();
  let cursor = 0;
  let candidate: { withStart: number; openParen: number } | null = null;

  while (cursor < lower.length) {
    const withStart = lower.indexOf("with", cursor);
    if (withStart < 0 || withStart > noLockStart) {
      break;
    }

    const before = withStart > 0 ? text[withStart - 1] : undefined;
    const after = withStart + 4 < text.length ? text[withStart + 4] : undefined;
    if (!isWordBoundary(before) || !isWordBoundary(after)) {
      cursor = withStart + 4;
      continue;
    }

    let i = withStart + 4;
    while (i < text.length && isWhitespace(text[i])) {
      i++;
    }
    if (i < text.length && text[i] === "(" && i <= noLockStart) {
      candidate = { withStart, openParen: i };
    }
    cursor = withStart + 4;
  }

  if (!candidate) {
    return null;
  }

  let depth = 0;
  for (let i = candidate.openParen; i < text.length; i++) {
    const ch = text[i];
    if (ch === "(") {
      depth++;
    } else if (ch === ")") {
      depth--;
      if (depth === 0) {
        if (i >= noLockStart) {
          return { withStart: candidate.withStart, openParen: candidate.openParen, closeParen: i };
        }
        return null;
      }
    }
  }

  return null;
}

function splitTopLevelByComma(input: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let start = 0;
  for (let i = 0; i < input.length; i++) {
    const ch = input[i];
    if (ch === "(") {
      depth++;
    } else if (ch === ")") {
      if (depth > 0) {
        depth--;
      }
    } else if (ch === "," && depth === 0) {
      const piece = input.slice(start, i).trim();
      if (piece) {
        parts.push(piece);
      }
      start = i + 1;
    }
  }

  const tail = input.slice(start).trim();
  if (tail) {
    parts.push(tail);
  }
  return parts;
}

export function buildSelectStarExpansionCodeActions(
  uri: string,
  parsed: any,
  lineStarts: number[],
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>,
  selectionStartOffset: number,
  selectionEndOffset: number
): CodeAction[] {
  if (!parsed?.ast) {
    return [];
  }

  const actions: CodeAction[] = [];

  const visit = (node: any): void => {
    if (!node || typeof node !== "object") {
      return;
    }

    if (node.type === "SelectStatement" && Array.isArray(node.columns) && Array.isArray(node.from)) {
      for (const col of node.columns) {
        if (col?.type !== "Column" || col?.wildcard !== true) {
          continue;
        }
        const expr = col.expression;
        if (!expr || expr.type !== "WildcardExpression") {
          continue;
        }

        const start = Number(expr.start);
        const end = Number(expr.end);
        if (!Number.isFinite(start) || !Number.isFinite(end)) {
          continue;
        }
        if (end < selectionStartOffset || start > selectionEndOffset) {
          continue;
        }

        const expansion = expandWildcardForSelect(parsed, node, expr, tablesByName, tableTypesByName, start);
        if (!expansion) {
          continue;
        }

        const startPos = offsetToPosition(start, lineStarts);
        const endPos = offsetToPosition(end, lineStarts);
        const label = expr.tablePrefix?.name ? `${expr.tablePrefix.name}.*` : "*";

        actions.push({
          title: `Expand ${label} to explicit columns`,
          kind: CodeActionKind.QuickFix,
          edit: {
            changes: {
              [uri]: [TextEdit.replace({
                start: { line: startPos.line, character: startPos.character },
                end: { line: endPos.line, character: endPos.character }
              }, expansion)]
            }
          }
        });
      }
    }

    if (Array.isArray(node)) {
      for (const item of node) {
        visit(item);
      }
      return;
    }

    for (const value of Object.values(node)) {
      if (value && typeof value === "object") {
        visit(value);
      }
    }
  };

  visit(parsed.ast);
  return actions;
}

function expandWildcardForSelect(
  parsed: any,
  selectNode: any,
  wildcardExpr: any,
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>,
  offset: number
): string | null {
  const sources = collectSelectSources(selectNode.from ?? []);
  if (sources.length === 0) {
    return null;
  }

  const prefix = wildcardExpr?.tablePrefix?.name
    ? normalizeName(String(wildcardExpr.tablePrefix.name))
    : "";

  const targetSources = prefix
    ? sources.filter(s => normalizeName(s.qualifier) === prefix)
    : sources;

  if (targetSources.length === 0) {
    return null;
  }

  const cols: string[] = [];
  for (const src of targetSources) {
    const sourceCols = getSourceColumns(src, parsed, offset, tablesByName, tableTypesByName);
    if (!sourceCols.length) {
      continue;
    }
    for (const c of sourceCols) {
      const rawCol = String(c?.rawName ?? c?.name ?? "").trim();
      if (!rawCol) {
        continue;
      }
      cols.push(`${src.qualifier}.${rawCol}`);
    }
  }

  if (cols.length === 0) {
    return null;
  }
  return cols.join(", ");
}

function collectSelectSources(fromNodes: any[]): Array<{ qualifier: string; tableName: string }> {
  const sources: Array<{ qualifier: string; tableName: string }> = [];

  for (const tableRef of fromNodes ?? []) {
    const own = collectSourceFromTableLike(tableRef);
    if (own) {
      sources.push(own);
    }

    for (const join of tableRef?.joins ?? []) {
      const joined = collectSourceFromTableLike(join);
      if (joined) {
        sources.push(joined);
      }
    }
  }

  return sources;
}

function collectSourceFromTableLike(node: any): { qualifier: string; tableName: string } | null {
  const tableName = normalizeName(normalizeAstTableName(node?.table) ?? "");
  const alias = String(node?.alias ?? "").trim();
  if (!tableName) {
    // Derived source (subquery/APPLY/etc.) can still support alias.* expansion via scope symbol columns.
    if (!alias) {
      return null;
    }
    const derivedKey = `__derived__.${normalizeName(alias)}`;
    return { qualifier: alias, tableName: derivedKey };
  }
  const qualifier = alias || getIdentifierTail(node?.table) || tableName;
  return { qualifier, tableName };
}

function getIdentifierTail(identifierNode: any): string {
  if (Array.isArray(identifierNode?.parts) && identifierNode.parts.length > 0) {
    return String(identifierNode.parts[identifierNode.parts.length - 1] ?? "");
  }
  if (typeof identifierNode?.name === "string") {
    const raw = String(identifierNode.name);
    const parts = raw.split(".");
    return parts[parts.length - 1] ?? raw;
  }
  return "";
}

function resolveTableDefinition(
  tableName: string,
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>
): any {
  const stripped = tableName.replace(/^dbo\./, "");
  return tablesByName.get(tableName)
    || tablesByName.get(stripped)
    || tableTypesByName.get(tableName)
    || tableTypesByName.get(stripped)
    || null;
}

function getSourceColumns(
  src: { qualifier: string; tableName: string },
  parsed: any,
  offset: number,
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>
): any[] {
  const def = resolveTableDefinition(src.tableName, tablesByName, tableTypesByName);
  if (def?.columns?.length) {
    return def.columns;
  }

  const scopeAtPos = parsed?.scope?.root?.findInnermost?.(offset) ?? parsed?.scope?.root;
  if (!scopeAtPos) {
    return [];
  }

  let scope = scopeAtPos;
  const targetAlias = normalizeName(src.qualifier);
  const targetTable = normalizeName(src.tableName);
  while (scope) {
    const symbols = typeof scope.getOwnSymbols === "function"
      ? scope.getOwnSymbols()
      : Object.values(scope.symbols ?? {});

    for (const sym of symbols) {
      const symName = normalizeName(String(sym?.name ?? ""));
      if (symName === targetAlias && Array.isArray(sym?.columns) && sym.columns.length > 0) {
        return sym.columns;
      }

      if (symName === targetAlias && sym?.kind === "Alias") {
        const aliasedTable = normalizeName(resolveAliasTableName(sym) ?? "");
        if (aliasedTable) {
          const aliasedSym = resolveSymbolCaseInsensitive(scope, aliasedTable);
          if (aliasedSym) {
            if (Array.isArray(aliasedSym.columns) && aliasedSym.columns.length > 0) {
              return aliasedSym.columns;
            }
            const typeName = normalizeName(String(aliasedSym.dataType ?? ""));
            if (typeName) {
              const typeDef = tableTypesByName.get(typeName) || tablesByName.get(typeName);
              if (typeDef?.columns?.length) {
                return typeDef.columns;
              }
            }
          }
        }
      }

      if ((sym?.kind === "Table" || sym?.kind === "TempTable" || sym?.kind === "CTE")
        && symName === targetTable
        && Array.isArray(sym?.columns)
        && sym.columns.length > 0) {
        return sym.columns;
      }

      if (symName === targetTable) {
        if (Array.isArray(sym?.columns) && sym.columns.length > 0) {
          return sym.columns;
        }
        const typeName = normalizeName(String(sym?.dataType ?? ""));
        if (typeName) {
          const typeDef = tableTypesByName.get(typeName) || tablesByName.get(typeName);
          if (typeDef?.columns?.length) {
            return typeDef.columns;
          }
        }
      }
    }

    scope = scope.parent ?? null;
  }

  return [];
}

type StringLikeSqlType = "varchar" | "nvarchar";

function getSymbolColumns(sym: any): any[] {
  if (!sym) {
    return [];
  }
  if (Array.isArray(sym.localColumns) && sym.localColumns.length > 0) {
    return sym.localColumns.map((c: any) => ({
      rawName: c?.rawName ?? c?.name ?? "",
      name: c?.normalizedName ?? normalizeName(String(c?.rawName ?? c?.name ?? "")),
      type: c?.dataType ?? c?.type ?? undefined
    }));
  }
  if (Array.isArray(sym.columns)) {
    return sym.columns;
  }
  return [];
}

export function collectStringComparisonDiagnostics(
  parsed: any,
  lineStarts: number[],
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>,
  source = "SaralSQL",
  severityOverrides = new Map<string, DiagnosticSeverity>()
): Diagnostic[] {
  if (!parsed?.ast || !parsed?.scope?.root) {
    return [];
  }

  const diagnostics: Diagnostic[] = [];
  const seen = new Set<string>();

  const visit = (node: any): void => {
    if (!node || typeof node !== "object") {
      return;
    }

    if (node.type === "BinaryExpression" && isComparisonOperator(String(node.operator ?? ""))) {
      const left = inferStringLikeType(node.left, parsed, tablesByName, tableTypesByName);
      const right = inferStringLikeType(node.right, parsed, tablesByName, tableTypesByName);

      if (left?.type && right?.type && left.type !== right.type) {
        const pair = [left.type, right.type].sort().join(":");
        const key = `${node.start ?? 0}:${node.end ?? 0}:${pair}`;
        if (!seen.has(key)) {
          seen.add(key);
          const startPos = offsetToPosition(typeof node.start === "number" ? node.start : node.left?.start ?? 0, lineStarts);
          const endPos = offsetToPosition(typeof node.end === "number" ? node.end : node.right?.end ?? node.left?.end ?? 0, lineStarts);
          diagnostics.push({
            code: SARAL_DIAGNOSTIC_CODES.StringComparison,
            severity: resolveDiagnosticSeverity(SARAL_DIAGNOSTIC_CODES.StringComparison, DiagnosticSeverity.Warning, severityOverrides),
            range: {
              start: { line: startPos.line, character: startPos.character },
              end: { line: endPos.line, character: endPos.character }
            },
            message: `Direct comparison between ${left.type} and ${right.type} may cause implicit conversion`,
            source
          });
        }
      }
    }

    if (Array.isArray(node)) {
      for (const item of node) {
        visit(item);
      }
      return;
    }

    for (const value of Object.values(node)) {
      if (value && typeof value === "object") {
        visit(value);
      }
    }
  };

  visit(parsed.ast);
  return diagnostics;
}

function isComparisonOperator(op: string): boolean {
  return ["=", "<>", "!=", "<", ">", "<=", ">="].includes(op);
}

function inferStringLikeType(
  node: any,
  parsed: any,
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>
): { type: StringLikeSqlType | null; rawName: string } | null {
  if (!node || typeof node !== "object") {
    return null;
  }

  if (node.type === "Literal" && String(node.variant ?? "").toLowerCase() === "string") {
    const raw = String(node.value ?? "");
    return {
      type: raw.trimStart().toUpperCase().startsWith("N'") ? "nvarchar" : "varchar",
      rawName: raw
    };
  }

  if (node.type === "CastExpression") {
    const castType = getStringLikeType(String(node.dataType ?? ""));
    if (castType) {
      return { type: castType, rawName: String(node.dataType ?? "") };
    }
    return null;
  }

  if (node.type !== "Identifier") {
    if (node.expression) {
      return inferStringLikeType(node.expression, parsed, tablesByName, tableTypesByName);
    }
    return null;
  }

  const parts = Array.isArray(node.parts) ? node.parts : [String(node.name ?? "")];
  const offset = typeof node.start === "number" ? node.start : undefined;
  const scopeAtPos = parsed?.scope?.root && typeof offset === "number"
    ? parsed.scope.root.findInnermost(offset)
    : parsed?.scope?.root;
  const statement = typeof offset === "number" ? getContainingStatementNode(parsed?.ast, offset) : null;

  if (parts.length === 2) {
    const tableOrAlias = normalizeName(parts[0]);
    const columnName = normalizeName(parts[1]);
    const owner = resolveTableForQualifier(scopeAtPos, statement, tableOrAlias, tablesByName, tableTypesByName);
    const col = owner?.columns?.find((c: any) => normalizeName(c.rawName ?? c.name) === columnName || normalizeName(c.name) === columnName);
    if (col?.type) {
      return { type: getStringLikeType(String(col.type)), rawName: String(node.name ?? parts.join(".")) };
    }
    return null;
  }

  const columnName = normalizeName(parts[0]);
  const owner = resolveSingleColumnOwner(scopeAtPos, statement, columnName, tablesByName, tableTypesByName);
  if (owner?.column?.type) {
    return { type: getStringLikeType(String(owner.column.type)), rawName: owner.column.rawName ?? owner.column.name };
  }

  return null;
}

function getStringLikeType(typeText: string): StringLikeSqlType | null {
  const norm = normalizeName(typeText);
  if (norm.startsWith("nvar") || norm.startsWith("nchar") || norm === "nvarchar" || norm === "ntext") {
    return "nvarchar";
  }
  if (norm.startsWith("var") || norm.startsWith("char") || norm === "varchar" || norm === "text") {
    return "varchar";
  }
  return null;
}


function resolveTableForQualifier(
  scopeAtPos: any,
  statement: any,
  qualifier: string,
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>
): any | null {
  const direct = tablesByName.get(qualifier) || tableTypesByName.get(qualifier);
  if (direct) {
    return direct;
  }

  if (scopeAtPos) {
    const visibleSymbols = typeof scopeAtPos.getVisibleSymbols === "function"
      ? scopeAtPos.getVisibleSymbols()
      : Object.values(scopeAtPos.symbols ?? {});

    for (const sym of visibleSymbols) {
      if (sym.kind === "Alias" && normalizeName(sym.name) === qualifier) {
        const tableName = normalizeName(resolveAliasTableName(sym) ?? "");
        if (tableName) {
          return tablesByName.get(tableName) || tableTypesByName.get(tableName);
        }
        const localCols = getSymbolColumns(sym);
        if (localCols.length > 0) {
          return { columns: localCols };
        }
      }

      if ((sym.kind === "Table" || sym.kind === "TempTable" || sym.kind === "CTE") && normalizeName(sym.name) === qualifier) {
        if (sym.kind === "CTE") {
          return { columns: getCteColumns(sym).map((c: any) => ({ rawName: c.rawName, name: c.name })) };
        }
        const localCols = getSymbolColumns(sym);
        if (localCols.length > 0) {
          return { columns: localCols };
        }
        const tableName = normalizeName(String(sym.name ?? ""));
        return tablesByName.get(tableName) || tableTypesByName.get(tableName);
      }
    }
  }

  return null;
}

function resolveSingleColumnOwner(
  scopeAtPos: any,
  statement: any,
  columnName: string,
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>
): { column: any } | null {
  const matches: Array<{ column: any }> = [];
  const visibleSymbols = scopeAtPos
    ? (typeof scopeAtPos.getVisibleSymbols === "function"
      ? scopeAtPos.getVisibleSymbols()
      : Object.values(scopeAtPos.symbols ?? {}))
    : [];

  for (const sym of visibleSymbols) {
    let def: any = null;

    if (sym.kind === "Alias") {
      const tableName = normalizeName(resolveAliasTableName(sym) ?? "");
      def = tableName ? (tablesByName.get(tableName) || tableTypesByName.get(tableName)) : null;
      if (!def) {
        const localCols = getSymbolColumns(sym);
        const col = localCols.find((c: any) => normalizeName(c.rawName ?? c.name) === columnName || normalizeName(c.name) === columnName);
        if (col) {
          matches.push({ column: col });
        }
        continue;
      }
    } else if (sym.kind === "Table" || sym.kind === "TempTable") {
      const localCols = getSymbolColumns(sym);
      const localCol = localCols.find((c: any) => normalizeName(c.rawName ?? c.name) === columnName || normalizeName(c.name) === columnName);
      if (localCol) {
        matches.push({ column: localCol });
        continue;
      }
      const tableName = normalizeName(String(sym.name ?? ""));
      def = tablesByName.get(tableName) || tableTypesByName.get(tableName);
    } else if (sym.kind === "CTE") {
      const cteCols = getCteColumns(sym);
      const col = cteCols.find(c => normalizeName(c.name) === columnName);
      if (col) {
        matches.push({ column: col });
      }
      continue;
    }

    if (def?.columns) {
      const col = def.columns.find((c: any) => normalizeName(c.rawName ?? c.name) === columnName || normalizeName(c.name) === columnName);
      if (col) {
        matches.push({ column: col });
      }
    }
  }

  if (matches.length === 0 && statement) {
    const statementTables = collectTablesFromAstNode(statement);
    for (const table of statementTables) {
      const norm = normalizeName(table);
      const stripped = norm.replace(/^dbo\./, "");
      const def = tablesByName.get(norm) || tableTypesByName.get(norm) || tablesByName.get(stripped) || tableTypesByName.get(stripped);
      const col = def?.columns?.find((c: any) => normalizeName(c.rawName ?? c.name) === columnName || normalizeName(c.name) === columnName);
      if (col) {
        matches.push({ column: col });
      }
    }
  }

  return matches.length === 1 ? matches[0] : null;
}

function getContainingStatementNode(ast: any, offset: number): any | null {
  if (!ast) {
    return null;
  }

  const statements = Array.isArray(ast?.body) ? ast.body : (Array.isArray(ast) ? ast : [ast]);
  let best: any | null = null;
  for (const stmt of statements) {
    if (typeof stmt?.start !== "number" || typeof stmt?.end !== "number") {
      continue;
    }
    if (offset < stmt.start || offset > stmt.end) {
      continue;
    }
    if (!best || (stmt.end - stmt.start) < (best.end - best.start)) {
      best = stmt;
    }
  }

  return best;
}

function collectTablesFromAstNode(node: any): string[] {
  const candidates = new Set<string>();
  const add = (name: string | undefined) => {
    const n = normalizeName(String(name ?? ""));
    if (n) {
      candidates.add(n);
    }
  };

  const visit = (current: any) => {
    if (!current || typeof current !== "object") {
      return;
    }

    if (current.type === "TableReference") {
      const table = current.table;
      if (typeof table?.name === "string") {
        add(table.name);
      } else if (typeof table === "string") {
        add(table);
      }
      if (Array.isArray(current.joins)) {
        for (const join of current.joins) {
          const joinTable = join?.table;
          if (typeof joinTable?.name === "string") {
            add(joinTable.name);
          } else if (typeof joinTable === "string") {
            add(joinTable);
          }
        }
      }
    }

    if (Array.isArray(current)) {
      for (const item of current) {
        visit(item);
      }
      return;
    }

    for (const value of Object.values(current)) {
      if (value && typeof value === "object") {
        visit(value);
      }
    }
  };

  visit(node);
  return Array.from(candidates);
}

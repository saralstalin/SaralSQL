import { CodeAction, CodeActionKind, Diagnostic, DiagnosticSeverity, TextEdit } from "vscode-languageserver/node";
import { isSqlKeyword, normalizeName, offsetToPosition } from "./text-utils";
import { getCteColumns, getDisplaySymbolName, resolveAliasTableName, resolveDerivedAliasColumn } from "./ast-utils";
import { extractReferences } from "@saralsql/tsql-parser";

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

    const matchedResolution = resolutions.find((r: any) => r.location?.start === ref.location.start);
    const resolvedSources = new Set<string>();
    if (matchedResolution?.inputs) {
      for (const input of matchedResolution.inputs) {
        if (input.kind === "column" && input.source) {
          resolvedSources.add(normalizeName(String(input.source)));
        }
      }
    }
    if (resolvedSources.size <= 1 && resolvedSources.size > 0) {
      continue;
    }

    const scopeAtPos = parsed.scope.root.findInnermost?.(ref.location.start) ?? parsed.scope.root;
    const visibleSymbols = typeof scopeAtPos.getVisibleSymbols === "function"
      ? scopeAtPos.getVisibleSymbols()
      : Object.values(scopeAtPos.symbols ?? {});

    const colNorm = normalizeName(name);
    const owners = new Set<string>();

    for (const sym of visibleSymbols) {
      if (sym.kind === "CTE") {
        const cteCols = getCteColumns(sym);
        if (cteCols.some(c => normalizeName(c.name) === colNorm)) {
          owners.add(normalizeName(String(sym.name ?? "")));
        }
        continue;
      }

      let tableName = "";
      if (sym.kind === "Alias") {
        tableName = normalizeName(resolveAliasTableName(sym) ?? "");
      } else if (sym.kind === "Table" || sym.kind === "TempTable") {
        tableName = normalizeName(String(sym.name ?? ""));
      }

      if (!tableName) {
        continue;
      }

      const stripped = tableName.replace(/^dbo\./, "");
      const def = tablesByName.get(tableName) || tablesByName.get(stripped) || tableTypesByName.get(tableName) || tableTypesByName.get(stripped);
      if (def?.columns?.some((c: any) => normalizeName(c.name) === colNorm)) {
        owners.add(normalizeName(def.rawName ?? def.name));
      }
    }

    if (owners.size <= 1) {
      continue;
    }

    const startPos = offsetToPosition(ref.location.start, lineStarts);
    const endPos = offsetToPosition(ref.location.end ?? ref.location.start, lineStarts);
    const key = `${startPos.line}:${startPos.character}:${colNorm}`;
    if (seen.has(key)) {
      continue;
    }
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

  return diagnostics;
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

    const scopeAtPos = parsed.scope.root.findInnermost?.(ref.location.start) ?? parsed.scope.root;
    const visibleSymbols = typeof scopeAtPos.getVisibleSymbols === "function"
      ? scopeAtPos.getVisibleSymbols()
      : Object.values(scopeAtPos.symbols ?? {});

    const colNorm = normalizeName(name);
    const matches: Array<{ alias: string; displayAlias: string }> = [];

    for (const sym of visibleSymbols) {
      if (sym.kind !== "Alias") {
        continue;
      }

      const resolved = resolveReadableAliasColumn(sym, colNorm, tablesByName, tableTypesByName);
      if (!resolved) {
        continue;
      }

      matches.push({
        alias: normalizeName(String(sym.name ?? "")),
        displayAlias: getDisplaySymbolName(sym) ?? normalizeName(String(sym.name ?? ""))
      });
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

export function buildReadableBareColumnCodeAction(uri: string, diagnostic: Diagnostic): CodeAction | null {
  const data = diagnostic.data as any;
  if (!data || data.kind !== "qualify-bare-column") {
    return null;
  }

  const replacement = String(data.replacement ?? "");
  if (!replacement || !diagnostic.range) {
    return null;
  }

  return {
    title: `Qualify with ${String(data.alias ?? replacement.split(".")[0] ?? "")}`,
    kind: CodeActionKind.QuickFix,
    diagnostics: [diagnostic],
    edit: {
      changes: {
        [uri]: [TextEdit.replace(diagnostic.range, replacement)]
      }
    }
  };
}

type StringLikeSqlType = "varchar" | "nvarchar";

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

function resolveReadableAliasColumn(
  sym: any,
  colNorm: string,
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>
): boolean {
  if (!sym || sym.kind !== "Alias") {
    return false;
  }

  if (Array.isArray(sym.columns)) {
    if (sym.columns.some((c: any) => normalizeName(c?.rawName ?? c?.name) === colNorm || normalizeName(c?.name) === colNorm)) {
      return true;
    }
  }

  const tableName = normalizeName(resolveAliasTableName(sym) ?? "");
  if (!tableName) {
    const derived = resolveDerivedAliasColumn(sym, colNorm);
    return Boolean(derived);
  }

  const stripped = tableName.replace(/^dbo\./, "");
  const def = tablesByName.get(tableName) || tablesByName.get(stripped) || tableTypesByName.get(tableName) || tableTypesByName.get(stripped);
  if (!def?.columns) {
    return false;
  }

  if (def.columns.some((c: any) => normalizeName(c.rawName ?? c.name) === colNorm || normalizeName(c.name) === colNorm)) {
    return true;
  }

  const cteCols = getCteColumns(sym);
  if (cteCols.some(c => normalizeName(c.name) === colNorm)) {
    return true;
  }

  return false;
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
        if (Array.isArray(sym.columns)) {
          return { columns: sym.columns };
        }
      }

      if ((sym.kind === "Table" || sym.kind === "TempTable" || sym.kind === "CTE") && normalizeName(sym.name) === qualifier) {
        if (sym.kind === "CTE") {
          return { columns: getCteColumns(sym).map((c: any) => ({ rawName: c.rawName, name: c.name })) };
        }
        const tableName = normalizeName(String(sym.name ?? ""));
        return tablesByName.get(tableName) || tableTypesByName.get(tableName);
      }
    }
  }

  if (statement) {
    const tableCandidates = collectTablesFromAstNode(statement);
    if (tableCandidates.length === 1 && normalizeName(tableCandidates[0]) === qualifier) {
      const tableName = normalizeName(tableCandidates[0]);
      return tablesByName.get(tableName) || tableTypesByName.get(tableName);
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

  const tableCandidates = new Set<string>();

  for (const sym of visibleSymbols) {
    let def: any = null;

    if (sym.kind === "Alias") {
      const tableName = normalizeName(resolveAliasTableName(sym) ?? "");
      def = tableName ? (tablesByName.get(tableName) || tableTypesByName.get(tableName)) : null;
      if (!def && Array.isArray(sym.columns)) {
        const col = sym.columns.find((c: any) => normalizeName(c.rawName ?? c.name) === columnName || normalizeName(c.name) === columnName);
        if (col) {
          matches.push({ column: col });
        }
        continue;
      }
    } else if (sym.kind === "Table" || sym.kind === "TempTable") {
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
    for (const t of collectTablesFromAstNode(statement)) {
      tableCandidates.add(normalizeName(t));
    }
    for (const tbl of tableCandidates) {
      const def = tablesByName.get(tbl) || tableTypesByName.get(tbl);
      if (!def?.columns) {
        continue;
      }
      const col = def.columns.find((c: any) => normalizeName(c.rawName ?? c.name) === columnName || normalizeName(c.name) === columnName);
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

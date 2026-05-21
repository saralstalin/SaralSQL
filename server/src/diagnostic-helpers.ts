import { Diagnostic, DiagnosticSeverity } from "vscode-languageserver/node";
import { isSqlKeyword, normalizeName, offsetToPosition } from "./text-utils";
import { getCteColumns, resolveAliasTableName } from "./ast-utils";

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
  source = "SaralSQL"
): Diagnostic[] {
  if (!parsed?.ast || !parsed?.scope?.root) {
    return [];
  }

  const extractRefs = (() => {
    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      return require("@saralsql/tsql-parser").extractReferences;
    } catch {
      return null;
    }
  })();
  if (!extractRefs) {
    return [];
  }

  const diagnostics: Diagnostic[] = [];
  const seen = new Set<string>();
  const refs = extractRefs(parsed.ast) ?? [];
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
      severity: DiagnosticSeverity.Warning,
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
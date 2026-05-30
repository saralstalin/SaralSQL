import { normalizeName } from "./text-utils";
import { normalizeAstTableName, resolveAliasTableName, resolveSymbolCaseInsensitive } from "./ast-utils";
import { collectNearestScopeColumnOwners, type ScopeColumnOwner } from "./scope-column-resolver";

const selectRangesCache = new WeakMap<object, Array<{ start: number; end: number; node: any }>>();

type ResolveParams = {
  parsed: any;
  offset: number;
  columnName: string;
  tablesByName: Map<string, any>;
  tableTypesByName: Map<string, any>;
  scopeAtPos?: any;
  localDefsByName?: Map<string, any>;
  resolverOptions?: ResolverOptions;
};

export type ResolverOptions = {
  // Allow schema map lookup for qualified owners when qualifier is not bound in statement scope.
  // Keep false for strict scope semantics.
  allowQualifiedSchemaLookup?: boolean;
};

export type ResolutionScopeType =
  | "qualified"
  | "mutation"
  | "read"
  | "projection"
  | "orderBy"
  | "lexical"
  | "unknown";

export type ResolutionDecisionTrace = {
  scopeType: ResolutionScopeType;
  reason: string;
  ownerCount: number;
};

export type ResolutionContext = {
  statementRange: { start: number; end: number } | null;
  tokenRange: { start: number; end: number };
  scopeAtPos: any;
  readOwners: ScopeColumnOwner[];
  mutationOwner: ScopeColumnOwner | null;
  parserHintOwner: ScopeColumnOwner | null;
  parserHintConfidence: "high" | "medium" | "low" | "none";
};

export type BareColumnResolution = {
  verdict: "resolved" | "unknown-owner" | "missing-column" | "ambiguous";
  status: "resolved" | "ambiguous" | "unresolved";
  owner?: ScopeColumnOwner;
  owners: ScopeColumnOwner[];
  matchedResolution: any;
  ambiguityCandidates: string[];
  decisionReason: string;
  trace?: ResolutionDecisionTrace;
  context?: ResolutionContext;
};

export function resolveBareColumnAtOffset(params: ResolveParams): BareColumnResolution {
  const colNorm = normalizeName(params.columnName);
  if (!colNorm) {
    return { verdict: "unknown-owner", status: "unresolved", owners: [], matchedResolution: null, ambiguityCandidates: [], decisionReason: "" };
  }

  const scopeAtPos = params.scopeAtPos ?? params.parsed?.scope?.root?.findInnermost?.(params.offset) ?? params.parsed?.scope?.root;
  const matchedResolution = params.parsed?.columns?.resolutions?.find((r: any) => {
    const s = Number(r?.location?.start);
    const e = Number(r?.location?.end);
    return Number.isFinite(s) && Number.isFinite(e) && params.offset >= s && params.offset <= e;
  }) ?? null;
  const ambiguityCandidates = Array.isArray(matchedResolution?.ambiguityCandidates)
    ? matchedResolution.ambiguityCandidates
    : Array.isArray(matchedResolution?.decision?.ambiguityCandidates)
      ? matchedResolution.decision.ambiguityCandidates
      : [];
  const decisionReason = normalizeName(String(matchedResolution?.decisionReason ?? matchedResolution?.decision?.decisionReason ?? ""));
  const context = buildResolutionContext(params, colNorm, matchedResolution, scopeAtPos);

  // Phase: unified precedence path
  // 1) Mutation target ownership has priority inside update/delete statement contexts.
  if (context.mutationOwner) {
    return {
      verdict: "resolved",
      status: "resolved",
      owner: context.mutationOwner,
      owners: [context.mutationOwner],
      matchedResolution,
      ambiguityCandidates,
      decisionReason,
      trace: { scopeType: "mutation", reason: "mutation-target-owner", ownerCount: 1 },
      context
    };
  }

  // 2) Read scope ownership (statement-local only)
  if (context.readOwners.length === 1) {
    return {
      verdict: "resolved",
      status: "resolved",
      owner: context.readOwners[0],
      owners: context.readOwners,
      matchedResolution,
      ambiguityCandidates,
      decisionReason,
      trace: { scopeType: "read", reason: "single-read-owner", ownerCount: 1 },
      context
    };
  }

  if (context.readOwners.length > 1) {
    return {
      verdict: "ambiguous",
      status: "ambiguous",
      owners: context.readOwners,
      matchedResolution,
      ambiguityCandidates,
      decisionReason,
      trace: { scopeType: "read", reason: "multiple-read-owners", ownerCount: context.readOwners.length },
      context
    };
  }

  // 3) Correlated lexical outer-walk: only for subquery-select contexts with no local FROM sources.
  const lexicalOwner = resolveCorrelatedLexicalOwner(params, colNorm, scopeAtPos);
  if (lexicalOwner) {
    return {
      verdict: "resolved",
      status: "resolved",
      owner: lexicalOwner,
      owners: [lexicalOwner],
      matchedResolution,
      ambiguityCandidates,
      decisionReason,
      trace: { scopeType: "lexical", reason: "correlated-outer-walk", ownerCount: 1 },
      context
    };
  }

  // 4) Parser-owned fallback only when hint owner is provably one of current statement read sources.
  // This keeps fallback statement-local and avoids global scope leakage.
  if (context.parserHintOwner && parserHintOwnerIsCurrentReadSource(params, context.parserHintOwner)) {
    return {
      verdict: "resolved",
      status: "resolved",
      owner: context.parserHintOwner,
      owners: [context.parserHintOwner],
      matchedResolution,
      ambiguityCandidates,
      decisionReason,
      trace: { scopeType: "read", reason: "parser-hint-current-read-source", ownerCount: 1 },
      context
    };
  }

  // 3) No owner in statement scope: unresolved.
  // Parser hints remain non-authoritative for diagnostics.
  return {
    verdict: "unknown-owner",
    status: "unresolved",
    owners: [],
    matchedResolution,
    ambiguityCandidates,
    decisionReason,
    trace: {
      scopeType: "unknown",
      reason: context.parserHintOwner ? `no-scope-owner-parser-hint-${context.parserHintConfidence}` : "no-scope-owner",
      ownerCount: 0
    },
    context
  };
}

function parserHintOwnerIsCurrentReadSource(params: ResolveParams, owner: ScopeColumnOwner): boolean {
  const ownerNorm = normalizeName(String(owner?.ownerName ?? ""));
  if (!ownerNorm) {
    return false;
  }
  const select = findContainingSelectAtOffset(params.parsed?.ast, params.offset);
  const from = Array.isArray(select?.from) ? select.from : [];
  if (from.length === 0) {
    return false;
  }

  const names = new Set<string>();
  const addName = (value: any): void => {
    const norm = normalizeName(String(value ?? ""));
    if (!norm) {
      return;
    }
    names.add(norm);
    names.add(normalizeName(norm.split(".").pop() ?? norm));
  };

  for (const t of from) {
    addName(t?.alias);
    addName(t?.table?.name ?? t?.table);
    const joins = Array.isArray(t?.joins) ? t.joins : [];
    for (const j of joins) {
      addName(j?.alias);
      addName(j?.table?.name ?? j?.table);
    }
  }

  if (names.size === 0) {
    return false;
  }
  const ownerTail = normalizeName(ownerNorm.split(".").pop() ?? ownerNorm);
  return names.has(ownerNorm) || names.has(ownerTail) || names.has(normalizeName(String(owner.alias ?? "")));
}

export function resolveColumnAtOffset(
  params: ResolveParams & { tokenText: string }
): BareColumnResolution {
  const token = String(params.tokenText ?? "").trim();
  if (!token) {
    return { verdict: "unknown-owner", status: "unresolved", owners: [], matchedResolution: null, ambiguityCandidates: [], decisionReason: "" };
  }

  const dot = token.lastIndexOf(".");
  if (dot <= 0) {
    return resolveBareColumnAtOffset({
      ...params,
      columnName: token
    });
  }

  const qualifierRaw = token.slice(0, dot).trim();
  const columnRaw = token.slice(dot + 1).trim();
  const qualifierNorm = normalizeName(qualifierRaw);
  const columnNorm = normalizeName(columnRaw);
  if (!qualifierNorm || !columnNorm) {
    return { verdict: "unknown-owner", status: "unresolved", owners: [], matchedResolution: null, ambiguityCandidates: [], decisionReason: "" };
  }

  const scopeAtPos = params.scopeAtPos ?? params.parsed?.scope?.root?.findInnermost?.(params.offset) ?? params.parsed?.scope?.root;
  const qualified = resolveQualifiedOwnerFromScope(params, scopeAtPos, qualifierNorm, columnNorm);
  if (qualified.owner) {
    return {
      verdict: "resolved",
      status: "resolved",
      owner: qualified.owner,
      owners: [qualified.owner],
      matchedResolution: null,
      ambiguityCandidates: [],
      decisionReason: "qualified-scope-owner",
      trace: { scopeType: "qualified", reason: "qualified-scope-owner", ownerCount: 1 }
    };
  }

  if (params.resolverOptions?.allowQualifiedSchemaLookup) {
    const fallback = resolveCandidateOwner(params, qualifierNorm, columnNorm);
    if (fallback) {
      return {
        verdict: "resolved",
        status: "resolved",
        owner: fallback,
        owners: [fallback],
        matchedResolution: null,
        ambiguityCandidates: [],
        decisionReason: "qualified-schema-fallback",
        trace: { scopeType: "qualified", reason: "qualified-schema-fallback", ownerCount: 1 }
      };
    }
  }

  const lineageDerived = resolveQualifiedFromLineageDerived(params, qualifierNorm, columnNorm);
  if (lineageDerived) {
    return {
      verdict: "resolved",
      status: "resolved",
      owner: lineageDerived,
      owners: [lineageDerived],
      matchedResolution: null,
      ambiguityCandidates: [],
      decisionReason: "qualified-lineage-derived-owner",
      trace: { scopeType: "qualified", reason: "qualified-lineage-derived-owner", ownerCount: 1 }
    };
  }

  const statementDerived = resolveQualifiedFromStatementDerivedAlias(params.parsed?.ast, params.offset, qualifierNorm, columnNorm);
  if (statementDerived?.columnExists === true) {
    return {
      verdict: "resolved",
      status: "resolved",
      owner: {
        kindLabel: "derived table",
        ownerName: qualifierNorm,
        column: { name: columnNorm, rawName: columnNorm },
        alias: qualifierNorm
      },
      owners: [],
      matchedResolution: null,
      ambiguityCandidates: [],
      decisionReason: "qualified-statement-derived-owner",
      trace: { scopeType: "qualified", reason: "qualified-statement-derived-owner", ownerCount: 1 }
    };
  }

  const lineageDerivedQualifierKnown = hasLineageDerivedQualifierKnowledge(params, qualifierNorm);
  const statementDerivedQualifierKnown = statementDerived?.knownQualifier === true;

  return {
    verdict: (qualified.qualifierMatched || lineageDerivedQualifierKnown || statementDerivedQualifierKnown) ? "missing-column" : "unknown-owner",
    status: "unresolved",
    owners: [],
    matchedResolution: null,
    ambiguityCandidates: [],
    decisionReason: "qualified-unresolved",
    trace: {
      scopeType: "qualified",
      reason: (qualified.qualifierMatched || lineageDerivedQualifierKnown || statementDerivedQualifierKnown) ? "qualified-owner-missing-column" : "qualified-owner-unresolved",
      ownerCount: 0
    }
  };
}

function resolveQualifiedOwnerFromScope(
  params: ResolveParams,
  scopeAtPos: any,
  qualifierNorm: string,
  columnNorm: string
): { owner: ScopeColumnOwner | null; qualifierMatched: boolean } {
  if (!scopeAtPos) {
    return { owner: null, qualifierMatched: false };
  }

  const aliasCandidates = collectAliasCandidatesFromScopeChain(scopeAtPos, qualifierNorm);
  if (aliasCandidates.length > 0) {
    let anyKnown = false;
    for (const candidate of aliasCandidates) {
      const resolved = resolveQualifiedFromSymbol(params, scopeAtPos, candidate, qualifierNorm, columnNorm);
      if (resolved) {
        return { owner: resolved, qualifierMatched: true };
      }
      anyKnown = anyKnown || hasColumnKnowledgeForSymbol(params, scopeAtPos, candidate);
    }
    return { owner: null, qualifierMatched: anyKnown };
  }

  const sym = resolveSymbolCaseInsensitive(scopeAtPos, qualifierNorm);
  if (sym) {
    const fromSym = resolveQualifiedFromSymbol(params, scopeAtPos, sym, qualifierNorm, columnNorm);
    if (fromSym) {
      return { owner: fromSym, qualifierMatched: true };
    }
    return { owner: null, qualifierMatched: hasColumnKnowledgeForSymbol(params, scopeAtPos, sym) };
  }

  // Also try tail lookup (dbo.Table -> Table) against scope symbols.
  const qualifierTail = normalizeName(qualifierNorm.split(".").pop() ?? qualifierNorm);
  if (qualifierTail && qualifierTail !== qualifierNorm) {
    const tailSym = resolveSymbolCaseInsensitive(scopeAtPos, qualifierTail);
    if (tailSym) {
      const fromTail = resolveQualifiedFromSymbol(params, scopeAtPos, tailSym, qualifierNorm, columnNorm);
      if (fromTail) {
        return { owner: fromTail, qualifierMatched: true };
      }
      return { owner: null, qualifierMatched: hasColumnKnowledgeForSymbol(params, scopeAtPos, tailSym) };
    }
  }

  return { owner: null, qualifierMatched: false };
}

function resolveQualifiedFromLineageDerived(
  params: ResolveParams,
  qualifierNorm: string,
  columnNorm: string
): ScopeColumnOwner | null {
  const sources = Array.isArray(params.parsed?.lineage?.sources) ? params.parsed.lineage.sources : [];
  for (const src of sources) {
    const aliasNorm = normalizeName(String(src?.alias ?? src?.name ?? ""));
    if (aliasNorm !== qualifierNorm) {
      continue;
    }
    const isDerived = normalizeName(String(src?.kind ?? "")) === "derived_subquery"
      || Boolean(src?.location?.table?.query);
    if (!isDerived) {
      continue;
    }
    const projection = Array.isArray(src?.projection) ? src.projection : [];
    for (const p of projection) {
      const projNorm = normalizeName(String(p?.name ?? ""));
      if (projNorm === columnNorm) {
        return {
          kindLabel: "derived table",
          ownerName: String(src?.alias ?? src?.name ?? qualifierNorm),
          column: { name: columnNorm, rawName: columnNorm },
          alias: qualifierNorm
        };
      }
    }
  }
  return null;
}

function hasLineageDerivedQualifierKnowledge(params: ResolveParams, qualifierNorm: string): boolean {
  const sources = Array.isArray(params.parsed?.lineage?.sources) ? params.parsed.lineage.sources : [];
  for (const src of sources) {
    const aliasNorm = normalizeName(String(src?.alias ?? src?.name ?? ""));
    if (aliasNorm !== qualifierNorm) {
      continue;
    }
    const isDerived = normalizeName(String(src?.kind ?? "")) === "derived_subquery"
      || Boolean(src?.location?.table?.query);
    if (!isDerived) {
      continue;
    }
    const projection = Array.isArray(src?.projection) ? src.projection : [];
    if (projection.length > 0) {
      return true;
    }
  }
  return false;
}

function resolveQualifiedFromStatementDerivedAlias(
  ast: any,
  offset: number,
  qualifierNorm: string,
  columnNorm: string
): { knownQualifier: boolean; columnExists: boolean } | null {
  const select = findContainingSelectAtOffset(ast, offset);
  if (!select || !Array.isArray(select.from)) {
    return null;
  }

  const allSources: any[] = [];
  for (const src of select.from) {
    if (src) {
      allSources.push(src);
    }
    if (Array.isArray(src?.joins)) {
      for (const join of src.joins) {
        if (join?.table) {
          allSources.push(join.table);
        }
      }
    }
  }

  for (const source of allSources) {
    const aliasNorm = normalizeName(String(source?.alias ?? source?.tableAlias ?? ""));
    if (!aliasNorm || aliasNorm !== qualifierNorm) {
      continue;
    }
    const queryColumns = source?.query?.columns;
    if (!Array.isArray(queryColumns)) {
      return { knownQualifier: true, columnExists: false };
    }
    const projected = new Set<string>();
    for (const c of queryColumns) {
      if (!c || c?.wildcard === true) {
        continue;
      }
      const alias = normalizeName(String(c?.alias ?? c?.name ?? ""));
      if (alias) {
        projected.add(alias);
      }
    }
    return {
      knownQualifier: true,
      columnExists: projected.has(columnNorm)
    };
  }

  return null;
}

function collectAliasCandidatesFromScopeChain(scopeAtPos: any, qualifierNorm: string): any[] {
  const out: any[] = [];
  const seen = new Set<any>();
  let current: any = scopeAtPos;
  while (current) {
    const ownSymbols = typeof current?.getOwnSymbols === "function"
      ? current.getOwnSymbols()
      : Object.values(current?.symbols ?? {});
    for (const sym of ownSymbols as any[]) {
      if (!sym || seen.has(sym)) {
        continue;
      }
      const kindNorm = normalizeName(String(sym?.kind ?? ""));
      const isAliasKind = kindNorm === "alias";
      const isDerivedSurface = Boolean(sym?.location?.table?.query);
      if (!isAliasKind && !isDerivedSurface) {
        continue;
      }
      if (normalizeName(String(sym?.name ?? "")) !== qualifierNorm) {
        continue;
      }
      seen.add(sym);
      out.push(sym);
    }
    current = current.parent ?? null;
  }
  return out;
}

function hasColumnKnowledgeForSymbol(params: ResolveParams, scopeAtPos: any, sym: any): boolean {
  if (Boolean(sym?.location?.table?.query)) {
    const projected = getAliasProjectedColumns(params, sym);
    if (projected.size > 0) {
      return true;
    }
  }

  if (Array.isArray(sym?.columns) && sym.columns.length > 0) {
    return true;
  }

  if (sym?.kind === "CTE") {
    return Array.isArray(sym?.columns) && sym.columns.length > 0;
  }

  if (sym?.kind === "Alias") {
    if (Boolean(sym?.location?.table?.query)) {
      // Only claim column knowledge when we can enumerate the projected columns.
      // Zero-column derived aliases (e.g. FOR XML/JSON subqueries) are opaque —
      // returning true here would cause false "missing-column" verdicts.
      return getAliasProjectedColumns(params, sym).size > 0;
    }
    const projected = getAliasProjectedColumns(params, sym);
    if (projected.size > 0) {
      return true;
    }
    const aliasTarget = normalizeName(resolveAliasTableName(sym) ?? "");
    if (!aliasTarget) {
      return false;
    }
    const targetSym = resolveSymbolCaseInsensitive(scopeAtPos, aliasTarget);
    if (targetSym) {
      return hasColumnKnowledgeForSymbol(params, scopeAtPos, targetSym);
    }
    const stripped = aliasTarget.replace(/^dbo\./, "");
    const dbo = stripped ? `dbo.${stripped}` : aliasTarget;
    const defs = [aliasTarget, stripped, dbo];
    for (const key of defs) {
      const tableDef = params.localDefsByName?.get(key) ?? params.tablesByName.get(key) ?? params.tableTypesByName.get(key);
      if (Array.isArray(tableDef?.columns) && tableDef.columns.length > 0) {
        return true;
      }
    }
    return false;
  }

  if ((sym?.kind === "Variable" || sym?.kind === "Parameter") && sym.dataType) {
    const typeKey = normalizeName(String(sym.dataType));
    const typeDef = params.tableTypesByName.get(typeKey) || params.tablesByName.get(typeKey);
    if (Array.isArray(typeDef?.columns) && typeDef.columns.length > 0) {
      return true;
    }
  }

  const symName = normalizeName(String(sym?.name ?? ""));
  if (!symName) {
    return false;
  }
  const stripped = symName.replace(/^dbo\./, "");
  const dbo = stripped ? `dbo.${stripped}` : symName;
  const defs = [symName, stripped, dbo];
  for (const key of defs) {
    const tableDef = params.localDefsByName?.get(key) ?? params.tablesByName.get(key) ?? params.tableTypesByName.get(key);
    if (Array.isArray(tableDef?.columns) && tableDef.columns.length > 0) {
      return true;
    }
  }
  return false;
}

function resolveQualifiedFromSymbol(
  params: ResolveParams,
  scopeAtPos: any,
  sym: any,
  qualifierNorm: string,
  columnNorm: string
): ScopeColumnOwner | null {
  if (Boolean(sym?.location?.table?.query)) {
    const projected = getAliasProjectedColumns(params, sym);
    if (projected.has(columnNorm)) {
      return {
        kindLabel: "derived table",
        ownerName: String(sym?.rawName ?? sym?.name ?? qualifierNorm),
        column: { name: columnNorm, rawName: columnNorm },
        alias: normalizeName(String(sym?.name ?? ""))
      };
    }
  }

  if (Array.isArray(sym?.columns) && sym.columns.length > 0) {
    const col = findColumn(sym.columns, columnNorm);
    if (col) {
      const kind = normalizeName(String(sym?.kind ?? ""));
      const kindLabel = kind === "cte"
        ? "CTE"
        : kind === "temptable"
          ? "temp table"
          : (kind === "variable" || kind === "parameter")
            ? "table variable"
            : "table";
      return {
        kindLabel,
        ownerName: String(sym?.rawName ?? sym?.name ?? qualifierNorm),
        column: col,
        alias: normalizeName(String(sym?.name ?? ""))
      };
    }
  }

  if (sym?.kind === "CTE") {
    const c = getCteColumnByName(sym, columnNorm);
    if (c) {
      return {
        kindLabel: "CTE",
        ownerName: String(sym?.rawName ?? sym?.name ?? qualifierNorm),
        column: c,
        alias: normalizeName(String(sym?.name ?? ""))
      };
    }
  }

  if (sym?.kind === "Alias") {
    const projected = getAliasProjectedColumns(params, sym);
    if (projected.has(columnNorm)) {
      return {
        kindLabel: "derived table",
        ownerName: String(sym?.rawName ?? sym?.name ?? qualifierNorm),
        column: { name: columnNorm, rawName: columnNorm },
        alias: normalizeName(String(sym?.name ?? ""))
      };
    }

    // TVF / .nodes() APPLY alias: location.table is a FunctionCall — passthrough so hover works.
    if (Boolean(sym?.location?.table?.query)) {
      return null;
    }

    if (String(sym?.location?.table?.type ?? "") === "FunctionCall") {
      return {
        kindLabel: "TVF alias",
        ownerName: String(sym?.rawName ?? sym?.name ?? qualifierNorm),
        column: { name: columnNorm, rawName: columnNorm },
        alias: normalizeName(String(sym?.name ?? ""))
      };
    }

    const aliasTarget = normalizeName(resolveAliasTableName(sym) ?? "");
    if (!aliasTarget) {
      return null;
    }
    const targetSym = resolveSymbolCaseInsensitive(scopeAtPos, aliasTarget);
    if (targetSym) {
      const fromTargetSym = resolveQualifiedFromSymbol(params, scopeAtPos, targetSym, aliasTarget, columnNorm);
      if (fromTargetSym) {
        return {
          ...fromTargetSym,
          alias: normalizeName(String(sym?.name ?? ""))
        };
      }
    }
    const owner = resolveCandidateOwner(params, aliasTarget, columnNorm);
    if (owner) {
      return {
        ...owner,
        alias: normalizeName(String(sym?.name ?? ""))
      };
    }
    return null;
  }

  if (sym?.kind === "Variable" || sym?.kind === "Parameter") {
    if (sym.dataType) {
      const typeKey = normalizeName(String(sym.dataType));
      const typeDef = params.tableTypesByName.get(typeKey) || params.tablesByName.get(typeKey);
      const col = findColumn(typeDef?.columns ?? [], columnNorm);
      if (col) {
        return {
          kindLabel: "table type",
          ownerName: String(sym?.rawName ?? sym?.name ?? qualifierNorm),
          column: col,
          alias: normalizeName(String(sym?.name ?? ""))
        };
      }
    }
  }

  const symName = normalizeName(String(sym?.name ?? ""));
  if (symName) {
    const owner = resolveCandidateOwner(params, symName, columnNorm);
    if (owner) {
      return owner;
    }
  }

  return null;
}

function getAliasProjectedColumns(params: ResolveParams, sym: any): Set<string> {
  const names = new Set<string>();
  const aliasName = normalizeName(sym?.name ?? "");
  const isDerivedAlias = Boolean(sym?.location?.table?.query);
  if (!aliasName) {
    return names;
  }

  if (!isDerivedAlias && Array.isArray(sym?.columns)) {
    for (const col of sym.columns) {
      const n = normalizeName(String(col?.rawName ?? col?.name ?? col ?? ""));
      if (n) {
        names.add(n);
      }
    }
  }

  const sources = Array.isArray(params.parsed?.lineage?.sources) ? params.parsed.lineage.sources : [];
  for (const source of sources) {
    const sourceName = normalizeName(source?.alias ?? source?.name ?? "");
    if (sourceName !== aliasName || !Array.isArray(source?.projection)) {
      continue;
    }
    for (const projection of source.projection) {
      const n = normalizeName(String(projection?.name ?? ""));
      if (n && n !== "*" && n !== "expression") {
        names.add(n);
      }
    }
  }

  const queryCols = sym?.location?.table?.query?.columns;
  if (Array.isArray(queryCols)) {
    for (const col of queryCols) {
      if (!col || col?.wildcard === true) {
        continue;
      }
      const candidates = [
        String(col?.alias ?? "").trim(),
        String(col?.name ?? "").trim(),
        String(col?.outputName ?? "").trim(),
        String(col?.sourceName ?? "").trim()
      ].filter(Boolean);
      for (const candidate of candidates) {
        const n = normalizeName(candidate);
        if (n && n !== "*" && n !== "expression") {
          names.add(n);
        }
      }
    }
  }

  return names;
}

function getCteColumnByName(sym: any, columnNorm: string): any | null {
  const cols = Array.isArray(sym?.columns) ? sym.columns : [];
  for (const c of cols) {
    const raw = normalizeName(String(c?.rawName ?? c?.name ?? c));
    const name = normalizeName(String(c?.name ?? c));
    if (raw === columnNorm || name === columnNorm) {
      return c;
    }
  }
  return null;
}

function resolveCorrelatedLexicalOwner(
  params: ResolveParams,
  colNorm: string,
  scopeAtPos: any
): ScopeColumnOwner | null {
  if (!isCorrelatedOuterWalkEligible(params.parsed?.ast, params.offset)) {
    return null;
  }

  const owners = dedupeOwners(
    collectNearestScopeColumnOwners(
      scopeAtPos,
      colNorm,
      params.tablesByName,
      params.tableTypesByName,
      params.localDefsByName,
      { stopAtSubqueryBoundary: false, stopAtPotentialLocalSource: false }
    )
  );

  return owners.length === 1 ? owners[0] : null;
}

function isCorrelatedOuterWalkEligible(ast: any, offset: number): boolean {
  const select = findContainingSelectAtOffset(ast, offset);
  if (!select) {
    return false;
  }
  const hasLocalFromSources = Array.isArray(select?.from) && select.from.length > 0;
  return !hasLocalFromSources;
}

function buildResolutionContext(
  params: ResolveParams,
  colNorm: string,
  matchedResolution: any,
  scopeAtPos: any
): ResolutionContext {
  const tokenRange = { start: params.offset, end: params.offset };
  const mutationOwner = resolveMutationTargetOwner(params, colNorm);
  const parserHintOwner = resolveParserDecisionOwner(params, matchedResolution, colNorm);
  const parserHintConfidence: "high" | "medium" | "low" | "none" =
    parserHintOwner
      ? (normalizeName(String(matchedResolution?.decisionReason ?? matchedResolution?.decision?.decisionReason ?? "")).includes("ambiguous")
          ? "medium"
          : "high")
      : "none";

  const collectedOwners = collectNearestScopeColumnOwners(
    scopeAtPos,
    colNorm,
    params.tablesByName,
    params.tableTypesByName,
    params.localDefsByName
  );
  const readOwners = dedupeOwners(
    narrowOwnersByReadScope(
      params,
      narrowOwnersForCurrentSelectSources(
        params,
        narrowOwnersForRecursiveCteSelfShadow(params, narrowOwnersForInsertReadContext(params, collectedOwners))
      )
    )
  );

  const stmt = findContainingStatementAtOffset(params.parsed?.ast, params.offset);
  const statementRange = stmt && Number.isFinite(Number(stmt?.start)) && Number.isFinite(Number(stmt?.end))
    ? { start: Number(stmt.start), end: Number(stmt.end) }
    : null;

  return {
    statementRange,
    tokenRange,
    scopeAtPos,
    readOwners,
    mutationOwner,
    parserHintOwner,
    parserHintConfidence
  };
}

function findContainingStatementAtOffset(ast: any, offset: number): any | null {
  if (!ast || typeof offset !== "number") {
    return null;
  }
  let best: any = null;
  const visit = (node: any): void => {
    if (!node || typeof node !== "object") {
      return;
    }
    const start = Number(node?.start);
    const end = Number(node?.end);
    if (Number.isFinite(start) && Number.isFinite(end)) {
      if (offset < start || offset > end) {
        return;
      }
      if (
        node?.type?.endsWith?.("Statement")
        || node?.type === "CreateProcedureStatement"
        || node?.type === "CreateFunctionStatement"
        || node?.type === "CreateViewStatement"
      ) {
        if (!best || (end - start) < (Number(best.end) - Number(best.start))) {
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

function narrowOwnersByReadScope(params: ResolveParams, owners: ScopeColumnOwner[]): ScopeColumnOwner[] {
  if (!Array.isArray(owners) || owners.length <= 1) {
    return owners;
  }

  const readScopes = Array.isArray(params.parsed?.lineage?.readScopes) ? params.parsed.lineage.readScopes : [];
  if (readScopes.length === 0) {
    return owners;
  }

  let bestScope: any = null;
  for (const rs of readScopes) {
    const start = Number(rs?.location?.start);
    const end = Number(rs?.location?.end);
    if (!Number.isFinite(start) || !Number.isFinite(end) || params.offset < start || params.offset > end) {
      continue;
    }
    if (!bestScope || (end - start) < (Number(bestScope.location.end) - Number(bestScope.location.start))) {
      bestScope = rs;
    }
  }

  if (!bestScope || !Array.isArray(bestScope.sources) || bestScope.sources.length === 0) {
    return owners;
  }

  const sourceNames = new Set<string>();
  const addName = (value: any): void => {
    const norm = normalizeName(String(value ?? ""));
    if (!norm) {
      return;
    }
    sourceNames.add(norm);
    sourceNames.add(normalizeName(norm.split(".").pop() ?? norm));
  };

  for (const src of bestScope.sources) {
    addName(src?.alias);
    addName(src?.name);
    addName(src?.baseName);
    addName(src?.tableName);
  }

  if (sourceNames.size === 0) {
    return owners;
  }

  const filtered = owners.filter((o) => {
    const ownerNorm = normalizeName(String(o?.ownerName ?? ""));
    const aliasNorm = normalizeName(String(o?.alias ?? ""));
    const ownerTail = normalizeName(ownerNorm.split(".").pop() ?? ownerNorm);
    return sourceNames.has(ownerNorm) || sourceNames.has(ownerTail) || sourceNames.has(aliasNorm);
  });

  return filtered.length > 0 ? filtered : owners;
}

function narrowOwnersForCurrentSelectSources(params: ResolveParams, owners: ScopeColumnOwner[]): ScopeColumnOwner[] {
  if (!Array.isArray(owners) || owners.length <= 1) {
    return owners;
  }

  const select = findContainingSelectAtOffset(params.parsed?.ast, params.offset);
  const from = Array.isArray(select?.from) ? select.from : null;
  if (!from || from.length === 0) {
    return owners;
  }

  const sourceNames = new Set<string>();
  const addName = (value: any): void => {
    const norm = normalizeName(String(value ?? ""));
    if (!norm) {
      return;
    }
    sourceNames.add(norm);
    sourceNames.add(normalizeName(norm.split(".").pop() ?? norm));
  };

  for (const t of from) {
    addName(t?.alias);
    addName(t?.table?.name ?? t?.table);
    const joins = Array.isArray(t?.joins) ? t.joins : [];
    for (const j of joins) {
      addName(j?.alias);
      addName(j?.table?.name ?? j?.table);
    }
  }

  if (sourceNames.size === 0) {
    return owners;
  }

  const filtered = owners.filter((o) => {
    const ownerNorm = normalizeName(String(o?.ownerName ?? ""));
    const ownerTail = normalizeName(ownerNorm.split(".").pop() ?? ownerNorm);
    const aliasNorm = normalizeName(String(o?.alias ?? ""));
    return sourceNames.has(ownerNorm) || sourceNames.has(ownerTail) || sourceNames.has(aliasNorm);
  });

  return filtered.length > 0 ? filtered : owners;
}

function findContainingSelectAtOffset(ast: any, offset: number): any | null {
  if (!ast || typeof offset !== "number") {
    return null;
  }
  const ranges = getCachedSelectRanges(ast);
  let best: any = null;
  for (const entry of ranges) {
    if (offset < entry.start || offset > entry.end) {
      continue;
    }
    if (!best || (entry.end - entry.start) < (best.end - best.start)) {
      best = entry.node;
    }
  }
  return best;
}

function getCachedSelectRanges(ast: any): Array<{ start: number; end: number; node: any }> {
  if (!ast || typeof ast !== "object") {
    return [];
  }

  const existing = selectRangesCache.get(ast as object);
  if (Array.isArray(existing)) {
    return existing;
  }

  const out: Array<{ start: number; end: number; node: any }> = [];
  const visit = (node: any): void => {
    if (!node || typeof node !== "object") {
      return;
    }
    if (node.type === "SelectStatement" && typeof node.start === "number" && typeof node.end === "number") {
      out.push({ start: node.start, end: node.end, node });
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
  selectRangesCache.set(ast as object, out);
  return out;
}

function narrowOwnersForRecursiveCteSelfShadow(params: ResolveParams, owners: ScopeColumnOwner[]): ScopeColumnOwner[] {
  if (!Array.isArray(owners) || owners.length <= 1) {
    return owners;
  }

  const selfCte = findContainingCteNameAtOffset(params.parsed?.scope?.root, params.offset);
  if (!selfCte) {
    return owners;
  }

  const selfNorm = normalizeName(selfCte);
  const withoutSelf = owners.filter((o) => {
    const isCteOwner = normalizeName(String(o?.kindLabel ?? "")) === "cte";
    const ownerNorm = normalizeName(String(o?.ownerName ?? ""));
    return !(isCteOwner && ownerNorm === selfNorm);
  });

  return withoutSelf.length > 0 ? withoutSelf : owners;
}

function findContainingCteNameAtOffset(scopeRoot: any, offset: number): string | null {
  if (!scopeRoot) {
    return null;
  }

  let bestName: string | null = null;
  let bestSpan: number | null = null;

  const visit = (scope: any): void => {
    if (!scope) {
      return;
    }

    const symbols = typeof scope.getOwnSymbols === "function"
      ? scope.getOwnSymbols()
      : Object.values(scope.symbols ?? {});

    for (const sym of symbols) {
      if (sym?.kind !== "CTE") {
        continue;
      }
      const start = Number(sym?.location?.query?.start);
      const end = Number(sym?.location?.query?.end);
      if (!Number.isFinite(start) || !Number.isFinite(end)) {
        continue;
      }
      if (offset < start || offset > end) {
        continue;
      }
      const span = end - start;
      if (bestSpan === null || span < bestSpan) {
        bestSpan = span;
        bestName = String(sym?.name ?? "");
      }
    }

    const children = typeof scope.getChildren === "function"
      ? scope.getChildren()
      : (scope.children ?? []);
    for (const child of children) {
      visit(child);
    }
  };

  visit(scopeRoot);
  return bestName;
}

function narrowOwnersForInsertReadContext(params: ResolveParams, owners: ScopeColumnOwner[]): ScopeColumnOwner[] {
  if (!Array.isArray(owners) || owners.length <= 1) {
    return owners;
  }

  const ast = params.parsed?.ast;
  if (!ast || !Array.isArray(ast.body)) {
    return owners;
  }

  for (const stmt of ast.body) {
    if (stmt?.type !== "InsertStatement") {
      continue;
    }

    const start = Number(stmt?.start);
    const end = Number(stmt?.end);
    if (!Number.isFinite(start) || !Number.isFinite(end) || params.offset < start || params.offset > end) {
      continue;
    }

    const selectQuery = stmt?.selectQuery;
    const selectStart = Number(selectQuery?.start);
    const selectEnd = Number(selectQuery?.end);
    const inReadSelect = Number.isFinite(selectStart) && Number.isFinite(selectEnd)
      && params.offset >= selectStart && params.offset <= selectEnd;

    if (!inReadSelect) {
      return owners;
    }

    const targetNorm = normalizeName(normalizeAstTableName(stmt?.table) ?? "");
    if (!targetNorm) {
      return owners;
    }

    const targetTail = normalizeName(targetNorm.split(".").pop() ?? targetNorm);
    const filtered = owners.filter((o) => {
      const ownerNorm = normalizeName(String(o?.ownerName ?? ""));
      if (!ownerNorm) {
        return true;
      }
      if (ownerNorm === targetNorm || ownerNorm === targetTail) {
        return false;
      }
      const ownerTail = normalizeName(ownerNorm.split(".").pop() ?? ownerNorm);
      if (ownerTail === targetTail) {
        return false;
      }
      return true;
    });

    return filtered.length > 0 ? filtered : owners;
  }

  return owners;
}

function resolveMutationTargetOwner(params: ResolveParams, colNorm: string): ScopeColumnOwner | null {
  const ast = params.parsed?.ast;
  if (!ast) {
    return null;
  }

  const scopeAtPos = params.scopeAtPos ?? params.parsed?.scope?.root?.findInnermost?.(params.offset) ?? params.parsed?.scope?.root;
  const candidates = collectMutationStatementsAtOffset(ast, params.offset);
  for (const stmt of candidates) {

    const targetName = normalizeName(normalizeAstTableName(stmt?.target) ?? "");
    if (!targetName) {
      continue;
    }

    const localOwner = resolveLocalScopeOwnerByName(scopeAtPos, targetName, colNorm);
    if (localOwner) {
      return localOwner;
    }

    const aliasOwner = resolveAliasMutationTargetOwner(scopeAtPos, targetName, params, colNorm);
    if (aliasOwner) {
      return aliasOwner;
    }

    // For MERGE, also try the target alias (e.g. "t" in MERGE Employee AS t …)
    if (stmt.type === "MergeStatement") {
      const mergeAlias = normalizeName(String(stmt?.targetAlias ?? ""));
      if (mergeAlias) {
        const aliasOwnerFromMerge = resolveAliasMutationTargetOwner(scopeAtPos, mergeAlias, params, colNorm);
        if (aliasOwnerFromMerge) {
          return aliasOwnerFromMerge;
        }
      }
    }

    const owner = resolveCandidateOwner(params, targetName, colNorm);
    if (owner) {
      return owner;
    }
  }

  return null;
}

function collectMutationStatementsAtOffset(ast: any, offset: number): any[] {
  const matches: any[] = [];
  const visit = (node: any): void => {
    if (!node || typeof node !== "object") {
      return;
    }
    const start = Number(node?.start);
    const end = Number(node?.end);
    if (Number.isFinite(start) && Number.isFinite(end) && (offset < start || offset > end)) {
      return;
    }
    if (node?.type === "UpdateStatement" || node?.type === "DeleteStatement" || node?.type === "MergeStatement") {
      matches.push(node);
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
  matches.sort((a, b) => (Number(a?.end) - Number(a?.start)) - (Number(b?.end) - Number(b?.start)));
  return matches;
}

function resolveAliasMutationTargetOwner(
  scopeAtPos: any,
  targetName: string,
  params: ResolveParams,
  colNorm: string
): ScopeColumnOwner | null {
  if (!scopeAtPos || !targetName) {
    return null;
  }

  const aliasSym = resolveSymbolCaseInsensitive(scopeAtPos, targetName);
  if (!aliasSym || aliasSym.kind !== "Alias") {
    return null;
  }

  const explicitCols = Array.isArray(aliasSym.columns) ? aliasSym.columns : [];
  const explicit = explicitCols.find((c: any) => normalizeName(String(c?.rawName ?? c?.name ?? c)) === colNorm);
  if (explicit) {
    return {
      kindLabel: "table",
      ownerName: String(aliasSym?.rawName ?? aliasSym?.name ?? targetName),
      column: explicit
    };
  }

  const aliasTarget = normalizeName(resolveAliasTableName(aliasSym) ?? "");
  if (!aliasTarget) {
    return null;
  }
  return resolveCandidateOwner(params, aliasTarget, colNorm);
}

function dedupeOwners(owners: ScopeColumnOwner[]): ScopeColumnOwner[] {
  if (!Array.isArray(owners) || owners.length <= 1) {
    return owners;
  }
  const seen = new Set<string>();
  const out: ScopeColumnOwner[] = [];
  for (const o of owners) {
    const key = `${normalizeName(String(o?.ownerName ?? ""))}:${normalizeName(String(o?.column?.rawName ?? o?.column?.name ?? ""))}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(o);
  }
  return out;
}


function resolveLocalScopeOwnerByName(scopeAtPos: any, targetName: string, colNorm: string): ScopeColumnOwner | null {
  if (!scopeAtPos || !targetName) {
    return null;
  }

  const targetNorm = normalizeName(targetName);
  let scope = scopeAtPos;
  while (scope) {
    const symbols = typeof scope.getOwnSymbols === "function"
      ? scope.getOwnSymbols()
      : Object.values(scope.symbols ?? {});

    for (const sym of symbols) {
      const symName = normalizeName(String(sym?.name ?? ""));
      if (symName !== targetNorm) {
        continue;
      }
      if (!Array.isArray(sym?.columns) || sym.columns.length === 0) {
        continue;
      }

      const col = sym.columns.find((c: any) => {
        const norm = normalizeName(String(c?.rawName ?? c?.name ?? c));
        return norm === colNorm;
      });
      if (!col) {
        continue;
      }

      const isTemp = String(symName).startsWith("#");
      const kindLabel = isTemp ? "temp table" : "table";
      return {
        kindLabel,
        ownerName: String(sym?.rawName ?? sym?.name ?? targetName),
        column: col
      };
    }
    scope = scope.parent ?? null;
  }

  return null;
}

function resolveParserDecisionOwner(params: ResolveParams, matchedResolution: any, colNorm: string): ScopeColumnOwner | null {
  const decisionOwner = normalizeName(String(matchedResolution?.owner ?? matchedResolution?.decision?.owner ?? ""));
  if (!decisionOwner) {
    return null;
  }
  const owner = resolveCandidateOwner(params, decisionOwner, colNorm);
  if (!owner) {
    return null;
  }
  return owner;
}

function resolveCandidateOwner(params: ResolveParams, normalizedCandidate: string, colNorm: string): ScopeColumnOwner | null {
  const candidateRaw = normalizedCandidate;
  const keys = [
    normalizeName(candidateRaw),
    normalizeName(normalizeAstTableName(candidateRaw) ?? ""),
    normalizeName(candidateRaw.split(".").pop() ?? candidateRaw)
  ].filter(Boolean);

  for (const key of keys) {
    const def = resolveDefByKey(params, key);
    if (!def) {
      continue;
    }
    const col = findColumn(def.columns, colNorm);
    if (!col) {
      continue;
    }
    return {
      kindLabel: def.isType ? "table type" : "table",
      ownerName: String(def.sym?.rawName ?? def.sym?.name ?? key),
      column: col
    };
  }

  return null;
}

function resolveDefByKey(
  params: ResolveParams,
  key: string
): { sym: any; columns: any[]; isType: boolean } | null {
  const stripped = key.replace(/^dbo\./, "");
  const local = params.localDefsByName?.get(key) || params.localDefsByName?.get(stripped);
  if (local && Array.isArray(local.columns)) {
    return { sym: local, columns: local.columns, isType: false };
  }

  const tbl = params.tablesByName.get(key) || params.tablesByName.get(stripped);
  if (tbl && Array.isArray(tbl.columns)) {
    return { sym: tbl, columns: tbl.columns, isType: false };
  }

  const typ = params.tableTypesByName.get(key) || params.tableTypesByName.get(stripped);
  if (typ && Array.isArray(typ.columns)) {
    return { sym: typ, columns: typ.columns, isType: true };
  }

  return null;
}

function findColumn(columns: any[], colNorm: string): any | null {
  for (const c of columns) {
    const norm = normalizeName(String(c?.rawName ?? c?.name ?? c));
    if (norm === colNorm) {
      return c;
    }
  }
  return null;
}

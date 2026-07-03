import { normalizeName } from "./text-utils";
import { normalizeAstTableName, resolveAliasTableName, resolveSymbolCaseInsensitive } from "./ast-utils";
import { collectNearestScopeColumnOwners, type ScopeColumnOwner } from "./scope-column-resolver";

type ResolveParams = {
  parsed: any;
  offset: number;
  columnName: string;
  tablesByName: Map<string, any>;
  tableTypesByName: Map<string, any>;
  scopeAtPos?: any;
  localDefsByName?: Map<string, any>;
};

export type BareColumnResolution = {
  status: "resolved" | "ambiguous" | "unresolved";
  owner?: ScopeColumnOwner;
  owners: ScopeColumnOwner[];
  matchedResolution: any;
  ambiguityCandidates: string[];
  decisionReason: string;
};

export function resolveBareColumnAtOffset(params: ResolveParams): BareColumnResolution {
  const colNorm = normalizeName(params.columnName);
  if (!colNorm) {
    return { status: "unresolved", owners: [], matchedResolution: null, ambiguityCandidates: [], decisionReason: "" };
  }

  const scopeAtPos = params.scopeAtPos ?? params.parsed?.scope?.root?.findInnermost?.(params.offset) ?? params.parsed?.scope?.root;
  const collectedOwners = collectNearestScopeColumnOwners(scopeAtPos, colNorm, params.tablesByName, params.tableTypesByName, params.localDefsByName);
  const owners = dedupeOwners(narrowOwnersByReadScope(
    params,
    narrowOwnersForCurrentSelectSources(
      params,
      narrowOwnersForRecursiveCteSelfShadow(params, narrowOwnersForInsertReadContext(params, collectedOwners))
    )
  ));
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

  const parserNativeOwner = resolveParserNativeDecisionOwner(params, matchedResolution, colNorm, owners);
  if (parserNativeOwner) {
    return { status: "resolved", owner: parserNativeOwner, owners: [parserNativeOwner], matchedResolution, ambiguityCandidates, decisionReason };
  }

  const parserOwner = resolveParserDecisionOwner(params, matchedResolution, colNorm);
  if (parserOwner && ownerInList(parserOwner, owners)) {
    return { status: "resolved", owner: parserOwner, owners: [parserOwner], matchedResolution, ambiguityCandidates, decisionReason };
  }

  const singleInputOwner = resolveSingleInputOwner(params, matchedResolution, colNorm);
  if (singleInputOwner && ownerInList(singleInputOwner, owners)) {
    return { status: "resolved", owner: singleInputOwner, owners: [singleInputOwner], matchedResolution, ambiguityCandidates, decisionReason };
  }

  const mutationTargetOwner = resolveMutationTargetOwner(params, colNorm);
  if (mutationTargetOwner) {
    return { status: "resolved", owner: mutationTargetOwner, owners: [mutationTargetOwner], matchedResolution, ambiguityCandidates, decisionReason };
  }

  // When the scope walk finds exactly one owner that is a CTE, the parser's more specific
  // attribution (the underlying table inside the CTE body) is more useful for navigation
  // and hover. If the parser provides a concrete table owner via matchedResolution and the
  // sole scope owner is a CTE, prefer the parser's attribution.
  if (owners.length === 1) {
    const soleScopeOwner = owners[0];
    if (normalizeName(String(soleScopeOwner?.kindLabel ?? "")) === "cte" && parserOwner) {
      return { status: "resolved", owner: parserOwner, owners: [parserOwner], matchedResolution, ambiguityCandidates, decisionReason };
    }
    return { status: "resolved", owner: soleScopeOwner, owners, matchedResolution, ambiguityCandidates, decisionReason };
  }

  const narrowedOwner = resolveNarrowedAmbiguityOwner(params, ambiguityCandidates, colNorm);
  if (narrowedOwner && ownerInList(narrowedOwner, owners)) {
    return { status: "resolved", owner: narrowedOwner, owners: [narrowedOwner], matchedResolution, ambiguityCandidates, decisionReason };
  }

  // Ambiguity should be rooted in scope-visible ownership, not parser-only broad candidates.
  // If scope walk does not find competing owners, treat as unresolved and let unknown-column flow handle it.
  if (owners.length > 1) {
    return { status: "ambiguous", owners, matchedResolution, ambiguityCandidates, decisionReason };
  }

  return { status: "unresolved", owners, matchedResolution, ambiguityCandidates, decisionReason };
}

function narrowOwnersByReadScope(params: ResolveParams, owners: ScopeColumnOwner[]): ScopeColumnOwner[] {
  if (!Array.isArray(owners) || owners.length <= 1) {
    return owners;
  }

  // Parser v0.4.4+ populates lineage.readScopes for all statement types, so
  // readScopes.length === 0 is now only possible for edge-case queries with no
  // trackable scope (e.g. pure expression SELECTs). Still guarded defensively.
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
  const cacheKey = "__saralsqlSelectRanges";
  const existing = ast?.[cacheKey];
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
  Object.defineProperty(ast, cacheKey, {
    value: out,
    enumerable: false,
    configurable: true
  });
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

function resolveSingleInputOwner(params: ResolveParams, matchedResolution: any, colNorm: string): ScopeColumnOwner | null {
  const sources = new Set<string>();
  for (const input of matchedResolution?.inputs ?? []) {
    if (input?.kind !== "column" || !input?.source) {
      continue;
    }
    const source = normalizeName(String(input.source));
    if (source) {
      sources.add(source);
    }
  }
  if (sources.size !== 1) {
    return null;
  }
  const only = Array.from(sources)[0];
  return resolveCandidateOwner(params, only, colNorm);
}

function resolveMutationTargetOwner(params: ResolveParams, colNorm: string): ScopeColumnOwner | null {
  const predicateInputOwner = resolveMutationPredicateInputOwner(params, colNorm);
  if (predicateInputOwner) {
    return predicateInputOwner;
  }

  const ast = params.parsed?.ast;
  if (!ast) {
    return null;
  }

  const scopeAtPos = params.scopeAtPos ?? params.parsed?.scope?.root?.findInnermost?.(params.offset) ?? params.parsed?.scope?.root;
  const candidates = collectMutationStatementsAtOffset(ast, params.offset);
  for (const stmt of candidates) {
    if (isInsideVariableAssignmentTarget(stmt, params.offset)) {
      continue;
    }

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

    const owner = resolveCandidateOwner(params, targetName, colNorm);
    if (owner) {
      return owner;
    }
  }

  return null;
}

function resolveMutationPredicateInputOwner(params: ResolveParams, colNorm: string): ScopeColumnOwner | null {
  const mutations = Array.isArray(params.parsed?.lineage?.mutations) ? params.parsed.lineage.mutations : [];
  for (const mutation of mutations) {
    const inputs = Array.isArray(mutation?.predicateInputs) ? mutation.predicateInputs : [];
    for (const input of inputs) {
      if (input?.kind !== "column" || !input?.source || !input?.location) {
        continue;
      }
      if (!locationContainsOffset(input.location, params.offset)) {
        continue;
      }
      const inputCol = normalizeName(String(input.name ?? "").split(".").pop() ?? "");
      const locationName = normalizeName(String(input.location?.name ?? "").split(".").pop() ?? "");
      if (inputCol !== colNorm && locationName !== colNorm) {
        continue;
      }
      const owner = resolveCandidateOwner(params, normalizeName(String(input.source)), colNorm);
      if (owner) {
        return owner;
      }
    }
  }
  return null;
}

function isInsideVariableAssignmentTarget(stmt: any, offset: number): boolean {
  const assignments = Array.isArray(stmt?.assignments) ? stmt.assignments : [];
  for (const assignment of assignments) {
    if (normalizeName(String(assignment?.targetKind ?? "")) !== "variable") {
      continue;
    }
    const target = assignment?.columnNode ?? assignment?.target ?? null;
    if (target && locationContainsOffset(target, offset)) {
      return true;
    }
  }
  return false;
}

function locationContainsOffset(location: any, offset: number): boolean {
  const start = Number(location?.start);
  const end = Number(location?.end);
  return Number.isFinite(start) && Number.isFinite(end) && offset >= start && offset <= end;
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
    if (node?.type === "UpdateStatement" || node?.type === "DeleteStatement") {
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
  // Caller guarantees !targetName is always false (it does `if (!targetName) continue`
  // before the call), so only the scopeAtPos guard is meaningful.
  if (!scopeAtPos) {
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

function ownerInList(owner: ScopeColumnOwner, owners: ScopeColumnOwner[]): boolean {
  return Boolean(findEquivalentScopeOwner(owner, owners));
}

function findEquivalentScopeOwner(owner: ScopeColumnOwner, owners: ScopeColumnOwner[]): ScopeColumnOwner | null {
  if (!owner || !Array.isArray(owners) || owners.length === 0) {
    return null;
  }
  const ownerName = normalizeName(String(owner.ownerName ?? ""));
  const ownerCol = normalizeName(String(owner.column?.rawName ?? owner.column?.name ?? ""));
  return owners.find((o) => {
    const n = normalizeName(String(o.ownerName ?? ""));
    const c = normalizeName(String(o.column?.rawName ?? o.column?.name ?? ""));
    return n === ownerName && c === ownerCol;
  }) ?? null;
}


function resolveLocalScopeOwnerByName(scopeAtPos: any, targetName: string, colNorm: string): ScopeColumnOwner | null {
  // Caller guarantees !targetName is always false (it does `if (!targetName) continue`
  // before the call), so only the scopeAtPos guard is meaningful.
  if (!scopeAtPos) {
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

function resolveParserNativeDecisionOwner(
  params: ResolveParams,
  matchedResolution: any,
  colNorm: string,
  owners: ScopeColumnOwner[]
): ScopeColumnOwner | null {
  if (!matchedResolution) {
    return null;
  }

  const decisionReason = normalizeName(String(matchedResolution?.decisionReason ?? matchedResolution?.decision?.decisionReason ?? ""));
  if (!decisionReason || decisionReason === "ambiguous_candidates" || decisionReason === "unresolved_external" || decisionReason === "non_column") {
    return null;
  }

  const scopeDepth = matchedResolution?.scopeDepth ?? matchedResolution?.decision?.scopeDepth;
  const hasParserScopeSignal = Number.isFinite(Number(scopeDepth))
    || decisionReason === "qualified_reference"
    || decisionReason === "single_scope_owner"
    || decisionReason === "single_candidate_promotion";
  if (!hasParserScopeSignal) {
    return null;
  }

  const fromInputs = resolveSingleInputOwner(params, matchedResolution, colNorm);
  if (fromInputs) {
    const scopeOwner = findEquivalentScopeOwner(fromInputs, owners);
    if (owners.length === 0 || scopeOwner) {
      return scopeOwner ?? fromInputs;
    }
  }

  const fromOwner = resolveParserDecisionOwner(params, matchedResolution, colNorm);
  if (fromOwner) {
    const scopeOwner = findEquivalentScopeOwner(fromOwner, owners);
    if (owners.length === 0 || scopeOwner) {
      return scopeOwner ?? fromOwner;
    }
  }

  return null;
}

function resolveNarrowedAmbiguityOwner(params: ResolveParams, parserCandidates: any[], colNorm: string): ScopeColumnOwner | null {
  if (!Array.isArray(parserCandidates) || parserCandidates.length === 0) {
    return null;
  }

  const matches: ScopeColumnOwner[] = [];
  const seen = new Set<string>();
  for (const candidate of parserCandidates) {
    const raw = String(candidate ?? "").trim();
    if (!raw) {
      continue;
    }
    const norm = normalizeName(raw);
    if (!norm || seen.has(norm)) {
      continue;
    }
    seen.add(norm);

    const owner = resolveCandidateOwner(params, norm, colNorm);
    if (owner) {
      const dedupe = normalizeName(owner.ownerName);
      if (!matches.some(m => normalizeName(m.ownerName) === dedupe)) {
        matches.push(owner);
      }
    }
  }

  return matches.length === 1 ? matches[0] : null;
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

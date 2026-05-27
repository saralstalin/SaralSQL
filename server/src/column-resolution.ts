import { normalizeName } from "./text-utils";
import { normalizeAstTableName } from "./ast-utils";
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
  const owners = narrowOwnersByReadScope(
    params,
    narrowOwnersForRecursiveCteSelfShadow(params, narrowOwnersForInsertReadContext(params, collectedOwners))
  );
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

  const parserOwner = resolveParserDecisionOwner(params, matchedResolution, colNorm);
  if (parserOwner) {
    return { status: "resolved", owner: parserOwner, owners: [parserOwner], matchedResolution, ambiguityCandidates, decisionReason };
  }

  const singleInputOwner = resolveSingleInputOwner(params, matchedResolution, colNorm);
  if (singleInputOwner) {
    return { status: "resolved", owner: singleInputOwner, owners: [singleInputOwner], matchedResolution, ambiguityCandidates, decisionReason };
  }

  const mutationTargetOwner = resolveMutationTargetOwner(params, colNorm);
  if (mutationTargetOwner) {
    return { status: "resolved", owner: mutationTargetOwner, owners: [mutationTargetOwner], matchedResolution, ambiguityCandidates, decisionReason };
  }

  // Local scope single-owner truth wins over broad parser ambiguity candidates.
  if (owners.length === 1) {
    return { status: "resolved", owner: owners[0], owners, matchedResolution, ambiguityCandidates, decisionReason };
  }

  const narrowedOwner = resolveNarrowedAmbiguityOwner(params, ambiguityCandidates, colNorm);
  if (narrowedOwner) {
    return { status: "resolved", owner: narrowedOwner, owners: [narrowedOwner], matchedResolution, ambiguityCandidates, decisionReason };
  }

  const parserSaysAmbiguous = ambiguityCandidates.length > 1 || decisionReason.includes("ambig");
  if (owners.length > 1 || parserSaysAmbiguous) {
    return { status: "ambiguous", owners, matchedResolution, ambiguityCandidates, decisionReason };
  }

  return { status: "unresolved", owners, matchedResolution, ambiguityCandidates, decisionReason };
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
  const ast = params.parsed?.ast;
  if (!ast || !Array.isArray(ast.body)) {
    return null;
  }

  const scopeAtPos = params.scopeAtPos ?? params.parsed?.scope?.root?.findInnermost?.(params.offset) ?? params.parsed?.scope?.root;

  for (const stmt of ast.body) {
    const start = Number(stmt?.start);
    const end = Number(stmt?.end);
    if (!Number.isFinite(start) || !Number.isFinite(end) || params.offset < start || params.offset > end) {
      continue;
    }

    if (stmt?.type !== "UpdateStatement" && stmt?.type !== "DeleteStatement") {
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

    const owner = resolveCandidateOwner(params, targetName, colNorm);
    if (owner) {
      return owner;
    }
  }

  return null;
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

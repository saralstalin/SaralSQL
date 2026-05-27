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
  const owners = collectNearestScopeColumnOwners(scopeAtPos, colNorm, params.tablesByName, params.tableTypesByName, params.localDefsByName);
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

    const owner = resolveCandidateOwner(params, targetName, colNorm);
    if (owner) {
      return owner;
    }
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

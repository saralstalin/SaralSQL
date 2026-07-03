import { normalizeName } from "./text-utils";
import { getCteColumns, getDisplaySymbolName, resolveAliasTableName, resolveSymbolCaseInsensitive } from "./ast-utils";

export type ScopeColumnOwner = {
  kindLabel: string;
  ownerName: string;
  column?: any;
  alias?: string;
  displayAlias?: string;
};

// hasPotentialLocalSourceSymbol (the guard that runs before this function) already
// returns true for any symbol with kind Alias/Table/TempTable/CTE/Variable/Parameter,
// so this function is only reached when ALL symbols have OTHER kinds. The only remaining
// meaningful check is whether any such unrecognised symbol carries an inline .columns
// array — that alone is enough to treat the scope level as column-bearing.
function hasColumnBearingLocalSource(symbols: any[]): boolean {
  return symbols.some(sym => sym && Array.isArray(sym.columns) && sym.columns.length > 0);
}

function hasPotentialLocalSourceSymbol(symbols: any[]): boolean {
  for (const sym of symbols) {
    const kind = String(sym?.kind ?? "");
    if (
      kind === "Alias"
      || kind === "Table"
      || kind === "TempTable"
      || kind === "CTE"
      || kind === "Variable"
      || kind === "Parameter"
    ) {
      return true;
    }
  }
  return false;
}

function findColumn(columns: any[] | undefined, colNorm: string): any | null {
  if (!Array.isArray(columns)) {
    return null;
  }
  return columns.find((c: any) => normalizeName(c?.rawName ?? c?.name ?? c) === colNorm || normalizeName(c?.name ?? c) === colNorm) ?? null;
}

function resolveDefColumns(
  tableName: string,
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>,
  localDefsByName?: Map<string, any>
): { def: any; columns: any[] } | null {
  const stripped = tableName.replace(/^dbo\./, "");
  const def = localDefsByName?.get(tableName)
    || localDefsByName?.get(stripped)
    || tablesByName.get(tableName)
    || tableTypesByName.get(tableName)
    || tablesByName.get(stripped)
    || tableTypesByName.get(stripped);
  if (!def?.columns || !Array.isArray(def.columns)) {
    return null;
  }
  return { def, columns: def.columns };
}

function resolveSymbolColumns(
  sym: any,
  scope: any,
  colNorm: string,
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>,
  localDefsByName?: Map<string, any>
): ScopeColumnOwner | null {
  if (!sym) {
    return null;
  }

  if (sym.kind === "CTE") {
    const c = getCteColumns(sym).find((col: any) => normalizeName(col?.name) === colNorm);
    if (c) {
      return { kindLabel: "CTE", ownerName: String(sym.name ?? ""), column: c };
    }
    return null;
  }

  if (Array.isArray(sym.columns)) {
    const c = findColumn(sym.columns, colNorm);
    if (c) {
      const kindLabel = (sym.kind === "Variable" || sym.kind === "Parameter") ? "table variable" : "derived table";
      return { kindLabel, ownerName: String(sym.rawName ?? sym.name ?? ""), column: c };
    }
  }

  if ((sym.kind === "Variable" || sym.kind === "Parameter") && sym.dataType) {
    const typeKey = normalizeName(String(sym.dataType));
    const typeDef = tableTypesByName.get(typeKey) || tablesByName.get(typeKey);
    const c = findColumn(typeDef?.columns, colNorm);
    if (c) {
      // Use symbol identity as owner so statement read-scope filtering can tell
      // whether this variable/parameter is actually present in FROM/JOIN.
      return {
        kindLabel: "table type",
        ownerName: String(sym.rawName ?? sym.name ?? typeKey),
        column: c,
        alias: normalizeName(String(sym.name ?? ""))
      };
    }
  }

  if (sym.kind === "Alias") {
    const aliasDisplay = getDisplaySymbolName(sym) ?? normalizeName(String(sym.name ?? ""));
    const isDerivedAlias = Boolean(sym?.location?.table?.query);

    // Derived/subquery alias boundaries are sealed: only projected alias columns are visible outside.
    if (isDerivedAlias) {
      const projected = findColumn(Array.isArray(sym.columns) ? sym.columns : undefined, colNorm);
      if (projected) {
        return {
          kindLabel: "derived table",
          ownerName: String(sym.rawName ?? sym.name ?? ""),
          column: projected,
          alias: normalizeName(String(sym.name ?? "")),
          displayAlias: aliasDisplay
        };
      }
      return null;
    }

    const targetName = normalizeName(resolveAliasTableName(sym) ?? "");
    if (!targetName) {
      return null;
    }

    const targetSym = resolveSymbolCaseInsensitive(scope, targetName);
    if (targetSym && targetSym !== sym) {
      const fromTarget = resolveSymbolColumns(targetSym, scope, colNorm, tablesByName, tableTypesByName, localDefsByName);
      if (fromTarget) {
        return { ...fromTarget, alias: normalizeName(String(sym.name ?? "")), displayAlias: aliasDisplay };
      }
    }

    const tableDef = resolveDefColumns(targetName, tablesByName, tableTypesByName, localDefsByName);
    const c = findColumn(tableDef?.columns, colNorm);
    if (c) {
      return {
        kindLabel: tableTypesByName.has(targetName) ? "table type" : "table",
        ownerName: String(tableDef?.def?.rawName ?? tableDef?.def?.name ?? targetName),
        column: c,
        alias: normalizeName(String(sym.name ?? "")),
        displayAlias: aliasDisplay
      };
    }
    return null;
  }

  if (sym.kind === "Table" || sym.kind === "TempTable") {
    const tableName = normalizeName(String(sym.name ?? ""));
    if (!tableName) {
      return null;
    }
    const tableDef = resolveDefColumns(tableName, tablesByName, tableTypesByName, localDefsByName);
    const c = findColumn(tableDef?.columns, colNorm);
    if (c) {
      return {
        kindLabel: tableTypesByName.has(tableName) ? "table type" : "table",
        ownerName: String(tableDef?.def?.rawName ?? tableDef?.def?.name ?? tableName),
        column: c
      };
    }
  }

  return null;
}

export function collectNearestScopeColumnOwners(
  scopeAtPos: any,
  colNorm: string,
  tablesByName: Map<string, any>,
  tableTypesByName: Map<string, any>,
  localDefsByName?: Map<string, any>
): ScopeColumnOwner[] {
  let scope = scopeAtPos;
  while (scope) {
    const symbols = typeof scope.getOwnSymbols === "function"
      ? scope.getOwnSymbols()
      : Object.values(scope.symbols ?? {});

    const owners: ScopeColumnOwner[] = [];
    for (const sym of symbols) {
      const resolved = resolveSymbolColumns(sym, scope, colNorm, tablesByName, tableTypesByName, localDefsByName);
      if (resolved) {
        owners.push(resolved);
      }
    }

    if (owners.length > 0) {
      return owners;
    }
    if (hasPotentialLocalSourceSymbol(symbols)) {
      return [];
    }
    if (hasColumnBearingLocalSource(symbols)) {
      return [];
    }
    if (String(scope.name ?? "").toLowerCase() === "subquery") {
      return [];
    }
    scope = scope.parent ?? null;
  }
  return [];
}

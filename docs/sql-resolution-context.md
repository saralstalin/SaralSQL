# SQL ResolutionContext Contract

## Purpose

Define one resolver input contract for SaralSQL so diagnostics, hover, definition, references, and readability hints all make ownership decisions from the same statement-local scope model.

This document is implementation guidance and acceptance contract.

Related:
- [sql-scope-model.md](C:\Users\Nimmy\source\repos\SaralSQL\docs\sql-scope-model.md)

## Core Principle

Visibility is not ownership.

A symbol can be visible in routine scope (for example TVP parameters, local table variables, temp tables, CTE names), but that does not make it eligible to own a token in every statement.

Resolver decisions must be statement-local and scope-typed.

## ResolutionContext Shape

```ts
type ScopeType =
  | "qualified"
  | "mutation"
  | "read"
  | "projection"
  | "orderBy"
  | "lexical"
  | "unknown";

type ResolutionContext = {
  uri: string;
  offset: number;
  tokenText: string;
  normalizedTokenText: string;

  // Token role and boundaries
  tokenRole:
    | "columnBare"
    | "columnQualified"
    | "tableName"
    | "tableAlias"
    | "projectionAlias"
    | "propertyAccess";
  statementRange: { start: number; end: number };
  tokenRange: { start: number; end: number };
  boundaryId: string; // subquery/cte/set-op/batch boundary id

  // Statement-local scope channels
  mutationTarget?: ResolvedSource; // UPDATE/DELETE target when applicable
  readSources: ResolvedSource[]; // FROM/JOIN readable sources in this statement
  projectionOutputs: ProjectedColumn[]; // statement output contract
  orderByBindingMode?: "projectionFirst" | "readAfterProjection";
  setOperatorProjection?: {
    isSetOperatorOutput: boolean;
    canonicalOutputFromLeftBranch: boolean;
  };

  // Parser hints (non-authoritative)
  parserDecision?: {
    owner?: string;
    scopeDepth?: number;
    decisionReason?: string;
    ambiguityCandidates?: string[];
    confidence?: "high" | "medium" | "low";
  };
};

type ResolvedSource = {
  sourceId: string; // stable per statement source
  sourceKind:
    | "table"
    | "view"
    | "cte"
    | "derived"
    | "tempTable"
    | "tableVariable"
    | "tvp"
    | "functionResult";
  logicalName: string; // alias or symbol identity used by resolver
  objectName?: string; // physical object for schema-backed sources
  columns: SourceColumn[]; // normalized + display names
};

type SourceColumn = {
  name: string;
  normalizedName: string;
  dataType?: string;
  location?: { start: number; end: number };
};

type ProjectedColumn = {
  outputName: string;
  normalizedOutputName: string;
  origin:
    | "selectAlias"
    | "baseColumn"
    | "expression"
    | "wildcardExpansion";
  sourceRef?: { sourceId: string; columnName: string };
  location?: { start: number; end: number };
};
```

## Precedence Rules

Resolver order for a token:

1. Qualified path (`alias.column`, `table.column`) using strict statement alias/table lookup.
2. Mutation scope if token is in UPDATE/DELETE target-bound context.
3. Read scope (`readSources`) for the containing statement.
4. Projection/OrderBy rules (only when token role and clause allow).
5. Lexical parent walk only for legal symbol classes.
6. Unknown.

No workspace-global fallback.

## OrderBy Rules (Explicit)

`ORDER BY` bare-name binding is special:

1. First bind to current SELECT projection outputs (alias/output names).
2. If no projection match, then attempt statement read-scope columns.
3. If duplicate projection output names exist, report ambiguity in ORDER BY path.
4. ORDER BY projection visibility is not reused by `WHERE`, `ON`, `HAVING`, or `SET`.

## Set Operator Projection Contract

For `UNION`/`UNION ALL`/`INTERSECT`/`EXCEPT` output:

1. Canonical output column names come from the left branch projection.
2. Right branch projection names are ignored for outer-name binding.
3. Post-set `ORDER BY` binds against the set output contract (left-branch names), then ordinal rules if applicable.

## Zero-Owner vs Parser-Confident Decision

When scope walk yields zero owners:

1. If parser decision is high-confidence and scope-compatible for token role:
   - Hover/Definition/References may use it as soft resolution metadata.
   - Diagnostics do not suppress unknown/ambiguity purely from this soft hint.
2. Otherwise resolve as unknown.

This preserves deterministic diagnostics while still offering navigation help in incomplete-schema contexts.

## Parser vs LSP Authority

Parser provides:
- statement ranges
- mutation target metadata
- read scope sources
- projection outputs
- parser decisions/candidates as hints

LSP enforces:
- precedence model
- scope-compatibility checks
- ambiguity policy
- feature-consistent output from one resolver path

## Canonical Regression Shapes

1. UPDATE target vs visible TVP collision:
```sql
CREATE TYPE dbo.InputType AS TABLE (LocationCode VARCHAR(20));
GO
CREATE TABLE dbo.InventoryByLocation (
  ItemKey VARCHAR(60), LocationCode VARCHAR(20), CategoryId INT, ReservedQty INT, IsActive BIT
);
GO
CREATE TABLE dbo.InventoryEvents (
  ItemKey VARCHAR(60), LocationCode VARCHAR(20), CategoryId INT, Qty INT, IsFinalized BIT, IsActive BIT
);
GO
CREATE PROCEDURE dbo.Repro
  @InputRows dbo.InputType READONLY,
  @ItemKey VARCHAR(60), @CategoryId INT, @TargetLocationCode VARCHAR(20)
AS
BEGIN
  UPDATE dbo.InventoryByLocation
  SET ReservedQty = (
      SELECT ISNULL(SUM(Qty), 0)
      FROM dbo.InventoryEvents
      WHERE ItemKey = @ItemKey
        AND LocationCode = @TargetLocationCode
        AND CategoryId = @CategoryId
        AND IsFinalized = 0
        AND IsActive = 1
  )
  WHERE ItemKey = @ItemKey
    AND LocationCode = @TargetLocationCode
    AND CategoryId = @CategoryId
    AND IsActive = 1;
END;
```
Expected: no ambiguity on `LocationCode` in UPDATE `WHERE`.

2. ORDER BY alias precedence:
```sql
SELECT e.EmployeeId AS X
FROM dbo.Employee e
ORDER BY X;
```
Expected: `X` binds projection alias, no read-scope ambiguity.

3. ORDER BY duplicate alias ambiguity:
```sql
SELECT a.Col1 AS X, b.Col2 AS X
FROM dbo.A a JOIN dbo.B b ON a.Id = b.Id
ORDER BY X;
```
Expected: ambiguity in ORDER BY path.

4. Set operator left-branch output names:
```sql
SELECT ColA AS OutName FROM dbo.T1
UNION ALL
SELECT ColB AS OtherName FROM dbo.T2
ORDER BY OutName;
```
Expected: `OutName` valid; `OtherName` not canonical output name.

5. SELECT alias vs JOIN alias clash:
```sql
SELECT CONCAT(bu.UnitId, '-', bu.UnitCode) AS [BU]
FROM dbo.RuleSource inr
LEFT JOIN dbo.UnitLookup bu ON inr.UnitId = bu.UnitId;
```
Expected: `bu` remains valid table alias; no unknown table.

## Acceptance Criteria

1. Same token, same owner across diagnostics/hover/definition/references.
2. No ambiguity from routine-visible-but-not-read symbols.
3. ORDER BY alias rules isolated to ORDER BY path.
4. Set operator output naming follows left branch contract.
5. No global fallback in bare-column ownership.
6. Zero-owner fallback behavior matches this contract in all features.

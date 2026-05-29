# SQL Scope Model for SaralSQL (Design Prep)

## Goal

Define a clean, explicit scope contract for SaralSQL so column/table resolution is deterministic and shared across diagnostics, hover, definition, and references.

This document is preparation only (no implementation steps executed from it yet).

## Why This Matters

Recent regressions showed that "visible in routine scope" is not enough for semantic resolution.  
We need explicit scope types and precedence, so symbols that are visible but not readable in a statement (for example TVP parameters not used in `FROM`) cannot compete.

## Authoritative T-SQL Semantics (References)

1. CTE scope is limited to a single statement execution scope (`SELECT`/`INSERT`/`UPDATE`/`MERGE`/`DELETE`):
   - https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql

2. `UPDATE` target semantics and alias usage are statement-specific:
   - https://learn.microsoft.com/en-us/sql/t-sql/queries/update-transact-sql

3. `ORDER BY` alias/column binding rules:
   - https://learn.microsoft.com/en-us/sql/t-sql/queries/select-order-by-clause-transact-sql

4. SELECT logical processing order (binding intent):
   - https://learn.microsoft.com/en-us/sql/t-sql/queries/select-transact-sql
   - (see binding/logical processing notes in T-SQL querying references)

## Scope Types (Proposed Contract)

1. Visibility Scope
   - Lexically visible symbols in current routine/block/batch.
   - Includes params, variables, CTE names, aliases, local temp/table vars.
   - Never sufficient alone for ownership decisions.

2. Read Scope
   - Statement sources that are actually readable for bare-column resolution.
   - Primarily `FROM`/`JOIN` sources for the containing `SELECT` or DML read side.
   - Derived/subquery source exports only projected columns.

3. Mutation Scope
   - DML target ownership for `UPDATE`/`DELETE` target table (or target alias).
   - Applies to bare columns in target-bound contexts (`SET`, `WHERE` for target resolution).

4. Projection Scope
   - `SELECT` list outputs and aliases.
   - Feeds outer derived-table/CTE consumers and `ORDER BY` alias behavior.

5. OrderBy Scope
   - Binding for `ORDER BY` with special alias visibility rules.
   - Must not leak into `WHERE`/`ON`.

6. Boundary Scope
   - Batch boundary (`GO`), subquery boundary, CTE definition boundary, set-operator boundary.
   - Inner scope must not leak outward except via projection contract.

## Precedence Model (Required)

For each token resolution:

1. Qualified path (`alias.column`, `table.column`) with strict alias/table lookup in statement context.
2. Mutation scope (if token inside `UPDATE`/`DELETE` target statement and relevant clause).
3. Read scope for containing statement (`FROM`/`JOIN` sources only).
4. Projection/order-specific rules (where applicable).
5. Parent lexical walk only when semantically legal for that token class.
6. Unknown (no global fallback guessing).

## Non-Goals

1. No workspace-global fallback for bare columns.
2. No parser-hint override that violates narrowed statement scope.
3. No regex-based semantic ownership hacks.

## Current Known Failure Family (Captured)

`UPDATE ... WHERE <bare column>` inside routine bodies can falsely mark ambiguity when:
- mutation target is not correctly identified in nested AST contexts, and
- TVP/table-variable symbols with same column are visible in routine scope.

Expected behavior:
- bind to mutation target when valid,
- only use read scope competitors when mutation target does not apply.

## Parser vs LSP Responsibility Split

Parser should provide:
- reliable statement locations/ranges
- mutation target metadata
- read scope sources per statement
- projected output columns for derived/CTE/set operators
- ambiguity candidates as hints only

LSP should enforce:
- scope precedence contract
- ownership arbitration and diagnostics policy
- consistent resolver usage across all features

## Implementation Phasing (Later)

1. Normalize one shared resolver contract object (`ResolutionContext`) consumed by all LSP features.
2. Move ambiguity emission to resolver truth only (no side fallback paths).
3. Make mutation/read/projection boundaries explicit per token role.
4. Add regression packs per scope type with enterprise repro minimizations.

## Acceptance Criteria for Scope Rewrite

1. No false ambiguity for routine-visible-but-not-read symbols.
2. Identical ownership result across diagnostics/hover/definition/reference for same token.
3. No global fallback in bare-column ownership.
4. Stable performance via per-parse cached statement/scope ranges.


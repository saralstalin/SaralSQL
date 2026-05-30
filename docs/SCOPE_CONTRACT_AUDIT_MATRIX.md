# Scope Contract Audit Matrix

Status: Draft baseline for refactor hardening  
Owner: LSP  
Date: 2026-05-30

## 1) Purpose

This document defines one contract for SQL name resolution across:
- Diagnostics
- Hover
- Definition
- References
- Rename
- Completion

Goal: prevent feature drift and scope leaks by enforcing one scope model and explicit fallback rules.

## 2) Core Principles

1. No global ownership fallback for semantic resolution.
2. Statement-local scope is authoritative for ownership.
3. Visibility alone is not ownership; ownership is scope-type specific.
4. Subquery/CTE boundaries are strict:
- Only projected columns cross subquery boundary.
- CTE visibility is statement-local.
5. Unknown is preferred over ambiguous when ownership cannot be proven.
6. Parser ambiguity candidates are hints, not authority.

## 3) Scope Types

1. Visibility Scope
- Symbols visible by lexical chain.
- Not enough alone to resolve ownership.

2. Read Scope
- Sources allowed to provide columns for expressions in SELECT/WHERE/ON/HAVING/GROUP BY.

3. Mutation Scope
- Target object for UPDATE/DELETE/INSERT column targets.
- Only applies in mutation-target contexts.

4. Projection Scope
- Output columns exposed by subquery/derived table/CTE.
- Outer consumers bind only to projection outputs.

5. OrderBy Scope
- Can bind to output aliases from same SELECT.
- Must not leak into WHERE/ON/GROUP BY.

6. Boundary Scope
- Batch/GO boundary.
- Subquery boundary.
- CTE statement boundary.
- Set operator output naming boundary.

## 4) Resolution Precedence (Contract)

For bare column token resolution:
1. Determine token context (mutation/read/order-by/projection).
2. Resolve inside corresponding scope type.
3. If exactly one owner => resolved.
4. If more than one owner => ambiguous.
5. If no owner:
- allow correlated lexical outer walk only in eligible subquery-read contexts.
- otherwise unresolved/unknown.

No workspace-global table scan is allowed at step 5.

## 5) Feature Matrix

| Feature | Must use shared resolver | Context-sensitive scope type | Allowed fallback | Forbidden fallback |
|---|---|---|---|---|
| Diagnostics | Yes | Read/Mutation/OrderBy | Parser hint for messaging only | Global owner pick |
| Hover | Yes (bare + qualified owner path) | All | Display-only metadata fallback | Semantic global owner fallback |
| Definition | Yes | Read/Mutation/Projection | Local derived projection location | Global table guess |
| References | Yes | Read/Mutation | None beyond resolved owner key | Global owner inference |
| Rename | Via References | Same as references | None | Any extra inference |
| Completion | Shared scope contract, completion-specific ranking | Visibility + Read + Projection | Parser completion parse for tokenization | Cross-statement/global semantic ownership |

## 6) Current Code Baseline (2026-05-30)

Shared resolver exists for bare columns:
- `server/src/column-resolution.ts`
- used by diagnostics/references/rename gate/definition(bare)/hover(bare).

Remaining divergence:
1. Qualified (`a.Col`) logic is still partially feature-local in hover/definition.
2. Completion uses separate heuristics and is not fully contract-bound.
3. Narrowing includes additional AST/lineage filters after scope walk.

## 7) Audit Cases (Must Stay Green)

1. UPDATE mutation target + TVP same column names:
- WHERE clause must not inherit mutation ownership incorrectly.

2. SELECT output alias vs table alias clash:
- no unknown-table false positive.

3. Subquery projection boundary:
- outer query sees only projected columns.
- `s.UNKNOWN` flagged; `s.ProjectedCol` resolves.

4. Recursive CTE:
- anchor bare columns do not self-bind ambiguously.

5. ORDER BY duplicate output alias:
- ambiguity in ORDER BY only.

6. View/table source in nested subquery:
- no scope leak, no global fallback.

7. GO batch isolation:
- symbols do not leak across batches.

## 8) Migration Checklist

1. Unify qualified owner resolution into shared resolver contract.
2. Introduce explicit `ResolutionContext` contract object for all providers.
3. Make hover/definition/references consume identical ownership output shape.
4. Restrict mutation ownership to mutation contexts only (target/set/where rules explicit).
5. Replace feature-local fallback branches with contract-driven fallback policy.
6. Bind completion candidate generation to same scope boundaries.
7. Add contract tests (one test per matrix row + known regressions).
8. Remove dead legacy paths after parity.

## 9) Parser vs LSP Responsibility

Parser:
- syntax, scope tree, lineage/read-scope metadata, ambiguity candidates, property access metadata.

LSP:
- semantic ownership decision under contract.
- diagnostics policy (unknown vs ambiguous vs readability).
- UX rendering (hover/definition/references/rename/completion).

When parser metadata is missing:
1. Statement-local synthesis only.
2. Unknown over ambiguous if not provable.
3. No global semantic fallback.

## 10) Exit Criteria for “Scope-Safe”

1. All feature paths use same resolver contract.
2. No known enterprise repros for scope leaks.
3. Regression suite covers all audit cases.
4. Manual smoke pack passes:
- hover
- definition
- references
- rename
- diagnostics
- completion
for each scope type.

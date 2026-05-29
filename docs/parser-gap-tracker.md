# Parser Gap Tracker (LSP-First Phase)

## Goal

Allow immediate LSP-only implementation of the scope architecture while explicitly tracking parser capabilities still missing for full fidelity.

This is a living tracker for parser follow-up work, not a blocker for LSP refactor.

## LSP-Only Implementation Scope (Now)

Proceed now in LSP with:

1. Single `ResolutionContext` authority used by diagnostics, hover, definition, references, and readability hints.
2. Strict precedence: qualified -> mutation -> read -> projection/order -> lexical (legal cases) -> unknown.
3. No workspace-global bare-column fallback.
4. Parser `columns.resolutions` used as hint-only metadata (soft navigation aid), never as diagnostic authority.
5. Statement-local scope boundaries enforced for subqueries, CTE consumers, and set-operator outputs.

## Known Parser Gaps (Documented, Non-Blocking)

1. Statement-local read scope completeness
   - `lineage.readScopes` may be absent/incomplete for some DML and nested statements.
   - LSP fallback needed: AST source extraction.

2. Projection contract completeness in deep nesting
   - Derived/subquery output columns and wildcard expansion metadata can be incomplete in some shapes.
   - LSP fallback needed: local projection synthesis.

3. Set-operator output naming metadata
   - Need explicit parser output contract for left-branch canonical names.
   - LSP currently enforces contract from AST shape.

4. ORDER BY binding metadata
   - Parser does not always emit clause-specific alias-binding decisions.
   - LSP must enforce projection-first ORDER BY behavior.

5. Mutation-target clause context
   - Target-bound context details for nested UPDATE/DELETE statements can be incomplete by offset in some shapes.
   - LSP currently maps token-to-nearest mutation statement and target.

6. Property-access semantics coverage
   - `columns.propertyAccesses` and `typeMembers` are available but not complete for all member signatures/edge cases.
   - LSP should treat unknown member diagnostics conservatively.

7. SQLCMD include handling ownership
   - Parser can signal unresolved includes, but path resolution and suppression policy are LSP concerns.
   - LSP keeps include resolution relative to current file and controls validation gating behavior.

8. Unknown-column inference boundaries
   - Parser must not emit schema-truth diagnostics where schema is unavailable.
   - LSP remains authority for schema-backed unknown/ambiguous diagnostics.

9. Mixed assignment forms in DML (`SET col = ..., @var = ...`)
   - Parser/LSP statement role boundaries can blur for variable + table updates in same SET list.
   - LSP rule: target columns resolved only via mutation/read scope; variable assignment isolated.

## Parser Follow-Up Backlog (After LSP Refactor)

1. Guarantee complete `readScopes` for INSERT...SELECT, UPDATE...FROM, DELETE...FROM, nested subqueries.
2. Emit explicit projection outputs for derived tables, CTE outputs, wildcard expansions.
3. Emit explicit set-operator output contract (left-branch canonical names).
4. Emit clause-aware ORDER BY binding decisions for aliases vs source columns.
5. Emit stable mutation-target binding metadata for nested DML by token range.
6. Expand property/member catalogs with signature-level metadata and return typing.
7. Keep parser diagnostics schema-agnostic; reserve schema-truth diagnostics for LSP.

## Release Rule for LSP-First Phase

If parser metadata is missing/incomplete, LSP must:

1. Prefer statement-local AST/context synthesis over global fallback.
2. Return unknown rather than ambiguous when ownership cannot be proven.
3. Keep hover/def/ref soft hints separate from diagnostic authority.

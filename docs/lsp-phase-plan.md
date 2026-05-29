# LSP Scope Rewrite - Phase Plan

## Objective

Complete an LSP-first scope rewrite using `ResolutionContext` as single authority, with test-first gates for every phase.

References:
- [sql-scope-model.md](C:\Users\Nimmy\source\repos\SaralSQL\docs\sql-scope-model.md)
- [sql-resolution-context.md](C:\Users\Nimmy\source\repos\SaralSQL\docs\sql-resolution-context.md)
- [parser-gap-tracker.md](C:\Users\Nimmy\source\repos\SaralSQL\docs\parser-gap-tracker.md)

## Phase 0 - Baseline and Guardrails

Status: Completed

Deliverables:
1. Regression baseline for enterprise repro shapes.
2. Diagnostic and regression test suites runnable locally.
3. Scope docs and parser-gap tracker in repo.

Gate:
1. `npm.cmd run test:validation` green.
2. `npm.cmd run test:regression` green.

## Phase 1 - ResolutionContext Foundation

Status: Completed

Deliverables:
1. `ResolutionContext` types added in LSP resolution module.
2. Context builder emits statement range, token range, read owners, mutation owner, parser hint metadata.
3. Decision trace emitted by resolver.

Gate:
1. Typecheck and compile green.
2. Existing validation/regression suites green.

## Phase 2 - Unified Precedence Resolver

Status: Completed (core path)

Deliverables:
1. Single precedence path in `resolveBareColumnAtOffset`:
   - mutation -> read(single) -> read(ambiguous) -> unresolved.
2. Parser snapshot no longer acts as co-equal authority for ownership.

Gate:
1. No fallback waterfall authority inversion in bare-column resolver.
2. Existing tests green.

## Phase 3 - Diagnostics Alignment

Status: Completed (initial)

Deliverables:
1. Ambiguity diagnostics grounded on resolver scope truth.
2. Parser fallback ambiguity path removed.
3. Enterprise-inspired update/TVP collision tests retained and passing.

Gate:
1. Zero regressions in ambiguity suite.
2. One diagnostic per token behavior preserved.

## Phase 4 - Feature Unification (Hover/Definition/References)

Status: Completed

Deliverables:
1. Hover, definition, references consume same resolver output contract.
2. Cross-feature parity tests for same token ownership.

Gate:
1. Same token resolves to same owner in diag/hover/def/ref.

## Phase 5 - ORDER BY and Set-Operator Contract

Status: Completed

Deliverables:
1. ORDER BY projection-first binding enforced via shared resolver context.
2. Duplicate select aliases ambiguous only in ORDER BY.
3. Set-operator output naming uses left-branch canonical contract.

Gate:
1. Canonical ORDER BY and set-op tests green.

## Phase 6 - Boundary Hardening

Status: Completed

Deliverables:
1. Subquery export-only projection boundaries.
2. CTE statement-local boundaries.
3. Batch boundary (`GO`) isolation checks in all resolution paths.

Gate:
1. No scope leaks in nested/subquery/CTE regression cases.

## Phase 7 - Parser Gap Compatible Behavior

Status: Completed

Deliverables:
1. Missing parser metadata fallback uses statement-local synthesis only.
2. Unknown preferred over ambiguity when ownership cannot be proven.
3. Soft parser hints only for navigation where allowed.

Gate:
1. Behavior matches `parser-gap-tracker.md` release rule.

## Phase 8 - Legacy Path Cleanup

Status: Completed

Deliverables:
1. Remove obsolete fallback branches and duplicate narrowing logic.
2. Keep one linear resolution flow and trace points.

Gate:
1. Lint, compile, validation, regression all green.
2. No duplicated ownership logic in feature handlers.

## Phase 9 - Enterprise Release Gate

Status: Pending

Deliverables:
1. Validate against 10 enterprise DB codebases.
2. Capture false-positive/false-negative and latency metrics.

Gate:
1. 0 blocker false positives.
2. 0 crashes.
3. No major typing latency regression.
4. Only then enable schema validation by default.

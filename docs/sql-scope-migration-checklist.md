# SQL Scope Migration Checklist

Use this checklist to migrate SaralSQL to the formal scope model described in:
- [sql-scope-model.md](C:\Users\Nimmy\source\repos\SaralSQL\docs\sql-scope-model.md)
- [sql-resolution-context.md](C:\Users\Nimmy\source\repos\SaralSQL\docs\sql-resolution-context.md)
- [parser-gap-tracker.md](C:\Users\Nimmy\source\repos\SaralSQL\docs\parser-gap-tracker.md)
- [lsp-phase-plan.md](C:\Users\Nimmy\source\repos\SaralSQL\docs\lsp-phase-plan.md)

## 0) Delivery Mode (Current)

- [ ] Execute LSP-first implementation now (non-blocking parser gaps tracked separately).
- [ ] Keep parser gaps and ownership boundaries updated in `parser-gap-tracker.md`.

## 1) Baseline and Guardrails

- [ ] Freeze current behavior with regression snapshots for:
  - [ ] UPDATE bare `WHERE` with visible TVP/table variables
  - [ ] DELETE bare `WHERE`
  - [ ] INSERT ... SELECT read-side resolution
  - [ ] ORDER BY alias (single alias, duplicate alias)
  - [ ] Derived/subquery boundary export-only behavior
  - [ ] CTE in CREATE VIEW / nested CTE usage
- [ ] Ensure no global fallback remains in bare-column resolution paths.
- [ ] Add per-token diagnostic trace toggle (debug-only) for owner/source decisions.

## 2) Shared Resolver Contract

- [ ] Adopt `ResolutionContext` exactly as specified in `sql-resolution-context.md`.
- [ ] Introduce one `ResolutionContext` contract used by:
  - [ ] Diagnostics
  - [ ] Hover
  - [ ] Definition
  - [ ] References
  - [ ] Readability hints
- [ ] Remove duplicate per-feature ownership logic.
- [ ] Ensure all features consume the same resolver result object.

## 3) Scope Layer Enforcement

- [ ] Implement explicit scope phases in resolver:
  - [ ] Qualified scope
  - [ ] Mutation scope
  - [ ] Read scope
  - [ ] Projection/OrderBy scope
  - [ ] Parent lexical walk (where legal)
  - [ ] Unknown
- [ ] Prevent parser metadata from overriding narrowed scope.
- [ ] Block visible-but-not-readable symbols from competing.

## 4) Mutation Scope Hardening

- [ ] Resolve nested UPDATE/DELETE targets (inside proc/function/view bodies).
- [ ] Handle `UPDATE alias FROM ...` target ownership explicitly.
- [ ] Verify `SET` and `WHERE` target binding consistency.
- [ ] Add negative tests for TVP collision in mutation context.

## 5) Read Scope Hardening

- [ ] Use statement read-sources from parser (`readScopes`) when present.
- [ ] Fallback to AST `FROM`/`JOIN` source extraction when parser readScopes are empty.
- [ ] Validate source alias/name normalization consistency.
- [ ] Ensure derived source contributes only projected columns.

## 6) Projection and Order Scope

- [ ] Enforce SELECT alias visibility only where valid (`ORDER BY`).
- [ ] Ensure duplicate select aliases trigger ambiguity only in ORDER BY path.
- [ ] Disallow projection alias visibility in `WHERE`/`ON`/`GROUP BY` unless parser semantics permit.
- [ ] Enforce ORDER BY precedence:
  - [ ] projection output first
  - [ ] read-scope second
  - [ ] no leakage to non-ORDER clauses

## 7) Boundary Controls

- [ ] Subquery boundaries: no inner-source leak to outer query.
- [ ] CTE boundaries: definition scope limited to owning statement.
- [ ] Batch boundaries (`GO`) remain isolated for variable declarations.
- [ ] Set operators: output-column contract from projected shape only.
- [ ] Set operators use left-branch output names as canonical projection contract.

## 8) Diagnostics Policy Alignment

- [ ] Emit ambiguity (`LSP003`) only from scope-proven competing owners.
- [ ] Keep unknown-column (`LSP002`) separate from ambiguity fallback.
- [ ] Ensure one diagnostic per token (no duplicate-owner fanout).
- [ ] Keep readability hints independent of error-grade ownership decisions.
- [ ] Define zero-owner behavior consistently:
  - [ ] parser high-confidence soft hint may aid hover/def/ref
  - [ ] diagnostics remain strict to scope-proven ownership

## 9) Performance Controls

- [ ] Cache select ranges per parse (done).
- [ ] Cache statement context lookup per offset where hot.
- [ ] Avoid repeated AST full walks in per-token loops.
- [ ] Measure validation latency before/after on large enterprise SQL files.

## 10) Release Readiness

- [ ] Run regression suite green.
- [ ] Run enterprise validation across 10 DBs.
- [ ] Pass criteria:
  - [ ] 0 blocker false positives
  - [ ] 0 crashes
  - [ ] no major latency regression
- [ ] Only then consider enabling schema validation by default.
- [ ] Confirm canonical regressions from `sql-resolution-context.md` are covered by tests.

## 11) Rollout Safety

- [ ] Keep a temporary feature flag for strict scope mode.
- [ ] Document migration in release notes with examples.
- [ ] Keep rollback patch plan for one release cycle.

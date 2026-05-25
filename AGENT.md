# SaralSQL LSP Resolution Contract

## Bare Column Resolution (Authoritative Rule)

For bare columns (for example `EmployeeId` without qualifier), all LSP features must follow the same scope-first contract:

1. Resolve in innermost/local scope first.
2. If unresolved, walk outward scope-by-scope.
3. Stop at the first scope that yields match(es).
4. If no match is found through scope walk, return unresolved.
5. Do **not** apply global metadata fallback for bare-column ownership.

## Ambiguity Rule

- Ambiguity is reported only when multiple visible sources in the same effective scope level own the same bare column.
- If a local scope has a single owner, no ambiguity should be emitted.

## Scope Sources Covered

Local scope ownership checks include:
- table aliases
- base tables
- temp tables (`#...`)
- table variables / TVPs (`@...`)
- CTEs

## LSP Features That Must Stay Aligned

The above behavior must stay consistent across:
- hover
- go to definition
- references
- diagnostics (ambiguity/readability/unknown-column ownership paths)

Any change that affects one of these paths must update tests for all affected behaviors.

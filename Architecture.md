# Architecture of SQL LSP Extension

## Overview
This extension provides SQL IntelliSense features in VS Code by implementing the Language Server Protocol (LSP).

## High-Level Design
- **Client (extension.ts)**: Starts/stops the LSP server, forwards document changes.
- **Server (server.ts)**: Indexes SQL definitions, provides definitions, references, and completions.
- **Definitions Index**: Central in-memory store of tables, types, and columns.

## Data Flow
1. User opens/edits `.sql` file.
2. Client notifies server via LSP.
3. Server re-indexes changed file.
4. User triggers "Go to Definition" / "Find References".
5. Server resolves query using definitions + alias map.
6. Result is sent back to client → VS Code shows navigation.

## Key Features
- Go to Definition (tables, columns, alias.column).
- Find References.
- Completions for tables & columns.
- Live updates as files change.

## Known Limitations
- Regex-based parsing (not full SQL grammar).
- Ambiguity if multiple schemas define same table/column.
- Possible false positives with generic column names (`Id`, `Name`).

## Future Improvements
- Schema-aware indexing (dbo.Employee vs hr.Employee).
- Richer completions (functions, keywords).
- Better cross-file analysis for stored procedures.

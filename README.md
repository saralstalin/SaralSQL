# SaralSQL — SQL IntelliSense & LSP for VS Code

> **Full SQL editor intelligence — no database connection, no server, no config.**  
> SaralSQL indexes your `.sql` files and gives you the navigation, completions, and diagnostics your team deserves.

---

## The problem it solves

T-SQL developers working in VS Code have had two bad options:

- Connect to a live database and hope SQL Server Extension catches your mistakes
- Write SQL blind, find errors at execution time

SaralSQL is the third option: **parse and index your `.sql` source files at editor speed**, with all schema context derived from the files already in your workspace.

---

## Who is it for?

- Teams using **SSDT / Database Projects** (`.sqlproj`) and versioning schema in `.sql` files  
- Developers who write **T-SQL stored procedures, views, and migrations**  
- Anyone tired of switching to SSMS or a DB connection just to check column names

---

## Features at a glance

| Feature | What you get |
|---|---|
| **Go to Definition** | Jump to any table, view, type, or column definition |
| **Find All References** | Locate every usage across all `.sql` files in your workspace |
| **Auto-completion** | Table names, column names, alias-prefixed columns (`e.`) | auto expand `*`
| **Hover** | See column type and definition context on hover |
| **Real-time diagnostics** | Unknown tables, unknown columns, unsafe DML, variable issues — as you type |
| **SSDT / .sqlproj support** | Schema contribution controlled by project membership |
| **Zero config** | Open a folder of `.sql` files. Done. |

---

## Navigation

**Go to Definition**   

Jump to the definition of a table, type, or column in your workspace.  
  Works with both plain names (`Employee`) and aliases (`e.EmployeeId`).

**Find All References** scans across files, skipping comments and unrelated identifiers:

![References](Images/References.png)

---

## Completions

Start typing a table name or type `alias.` and column suggestions appear instantly. Schema prefixes (`dbo.`) are handled automatically.

![Completions](Images/Completion.png)

---

## Hover

Hover over any table, alias, or column to see its definition and context — no database query needed.

![Column Hover](ColumnHover.png)

![Table Hover](Images/TableHover.png)

---

## Diagnostics

Real-time, parser-backed diagnostics across 16 codes. Every diagnostic has an individual enable/disable toggle and a severity dropdown (`error`, `warning`, `information`, `hint`) in VS Code Settings.

**Schema validation** — flags unknown tables and columns against workspace schema (auto-suppressed when no schema files are present):

| Code | Diagnostic |
|---|---|
| `LSP001` | Unknown table |
| `LSP002` | Unknown column |
| `LSP003` | Ambiguous bare column (resolves to multiple tables) |
| `LSP004` | Readability hint — qualify a bare column when an alias is in scope |
| `LSP005` | Direct `varchar` / `nvarchar` comparison |

**Safe coding checks:**

| Code | Diagnostic |
|---|---|
| `DML001` | `UPDATE` without `WHERE` |
| `DML002` | `DELETE` without `WHERE` |
| `DML003` | `INSERT` without column list |
| `DML004` | `UPDATE` target uses `WITH (NOLOCK)` |
| `LOG001` | Self-comparison (`column = column`) |
| `DDL002` | Unnamed `PRIMARY KEY` or `UNIQUE` constraint |
| `DDL003` | Unnamed `DEFAULT` constraint |

**Variable tracking:**

| Code | Diagnostic |
|---|---|
| `VAR001` | Undeclared variable |
| `VAR002` | Unused variable |
| `VAR003` | Unused parameter |
| `VAR004` | Variable used before being set |

![Self-comparison diagnostic](Images/DiagnosticsSelfComparison.png)

![UPDATE without WHERE diagnostic](Images/DiagnosticsUnsafeUpdate.png)

![Unused variable diagnostic](Images/DiagnosticsUnusedVariable.png)

### Advanced diagnostic settings

- `saralsql.showDiagnostics` — master on/off switch (default: `true`)  
- `saralsql.enableSchemaValidation` — schema diagnostics LSP001–LSP004 (default: `true`, auto-suppressed when no schema is indexed)  
- `saralsql.showParseIssues` — show raw parser errors (default: `false`; when off, SaralSQL only shows diagnostics on clean-parsing documents)  
- `saralsql.disabledDiagnostics` — suppress specific codes by list, e.g. `["LSP004", "DML001"]`

---

## SSDT / .sqlproj support

For SQL Database Projects, SaralSQL respects project membership:

- `saralsql.sqlproj.strictBuildMembership` (default: `true`) — only `Build` items contribute to workspace schema; Pre/Post Deploy files are validated locally only  
- `saralsql.sqlproj.warnMissingProjectFile` (default: `true`) — flags SQL files missing from all `.sqlproj` items (`SSDT001`)  
- `saralsql.sqlproj.missingProjectFileSeverity` (default: `warning`) — severity for `SSDT001`

---

## Performance

- Indexes **2000+ SQL files in under 60 seconds** on first open  
- Live update as you type — no save required  
- Runs in a dedicated language server process; never blocks the editor UI  

---

## Getting started

1. Install **SaralSQL** from the VS Code Marketplace  
2. Open a folder or workspace containing `.sql` files  
3. Features activate automatically — no extension configuration needed

> Works best when your schema objects (tables, views, types, stored procedures) are defined as `.sql` files inside your workspace.

---

## Known limitations

- **Schema prefixes** — `dbo.TableName` and `TableName` resolve to the same object; multiple schemas with the same table name are not yet distinguished  
- **Dialect** — optimized for **T-SQL / SQL Server**; Postgres, MySQL and others may partially work but are not tested  
- **Cross-file accuracy** — schema resolution depends on having all schema files present in the open workspace  

---

## ⚠️ Preview

SaralSQL is in **Early Access**. The core feature set is production-ready for T-SQL projects; the preview label reflects areas still being expanded:

- Richer schema-prefix and multi-schema support  
- Outline view for procedures, tables, and columns  
- Workspace symbol search (`Ctrl+T`) across SQL objects  
- Enhanced dialect support (Postgres, MySQL)  
- Incremental indexing for very large workspaces  

**Feedback shapes the roadmap.** If something doesn't behave as expected, [open an issue](https://github.com/saralstalin/SaralSQL/issues) with the SQL snippet — reports from real projects have directly driven every release.

---

## Privacy

No telemetry. No network requests. All code and analysis stays in your workspace.

---

## License

MIT

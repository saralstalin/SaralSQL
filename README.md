# SaralSQL : SQL Language Features (Preview)

This VS Code extension adds **Language Server Protocol (LSP)** features for working with SQL code.  
It is fast and lightweight, works on plain `.sql` files, and does **not** require a database connection.  

It combines a **regex-based indexer** (for speed and responsiveness) with a **lightweight SQL parser (node-sql-parser)** for more accurate context.  
The goal is practical productivity — not perfect SQL understanding.

---

## ✨ Features

- **Go to Definition**  
  Jump to the definition of a table, type, or column in your workspace.  
  Works with both plain names (`Employee`) and aliases (`e.EmployeeId`).  

- **Find All References**  
  Locate table and column references across SQL files.  
  Skips matches in comments and unrelated identifiers to reduce false positives.  

- **IntelliSense / Auto-completion**  
  - Suggests table names and types.  
  - After typing `alias.` or `TableName.`, column suggestions appear.  
  - Schema prefixes are handled (`dbo.TableName` is treated the same as `TableName`).  

- **Hover Information**  
  Hover over a table, alias, or column to see its definition and context.  

- **Workspace Indexing**  
  - Automatically indexes all `.sql` files in the workspace.  
  - Updates instantly as you type or save.  

- **Hybrid Resolution**  
  - Regex-based indexing ensures fast responses.  
  - Parser-based fallback improves accuracy in `FROM`/`JOIN` contexts and complex queries.  

---

## 🚀 Getting Started

1. Install the extension.  
2. Open a folder or workspace containing `.sql` files.  
3. Start editing — features like definitions, references, completions, and hovers activate automatically.  

> 💡 Works best when your schema objects (tables, types, procedures) are defined in `.sql` files within your workspace.  

---

## ⚠️ Preview Notice

This is a **Preview release**.  
- Optimized for **T-SQL / SQL Server** style DDL & DML.  
- Dialects like Postgres or MySQL may partially work but are not fully supported yet.  
- Column and reference detection is heuristic — complex scripts may produce misses or false positives.  
- Certain constructs (e.g. `TOP(@Variable)`) may require sanitisation before parsing and are still being improved.  

We’re releasing early to gather real-world feedback before expanding the feature set.  

---

## 🛠 Planned Improvements

- **Outline view** for procedures, tables, and columns.  
- **Workspace symbol search** (`Ctrl+T`) across SQL objects.  
- **Diagnostics** for undefined tables/columns and duplicate definitions.  
- **Smarter reference resolution** using parser context (`FROM` / `JOIN` scope).  
- **Better handling of parameterised constructs** like `TOP(@var)` and `OUTPUT INTO`.  
- **Schema-aware resolution** when multiple schemas contain the same table name.  
- **Incremental indexing** for faster performance on very large workspaces.  

---

## 🧑‍💻 Contributing

We welcome feedback and bug reports!  
- Open issues with examples of SQL code that doesn’t behave as expected.  
- Pull requests are encouraged — especially for dialect support or smarter parsing.  

---

## ⚠️ Known Limitations

This extension is intentionally lightweight and does **not** do full SQL parsing or semantic analysis.  
Be aware of these trade-offs:

- **Column References**  
  Column references are matched globally by name in regex fallback mode.  
  If multiple tables have the same column (e.g. `Id`), all may appear in references.  
  Teams using explicit naming standards like `EmployeeId`, `DepartmentId` will be less affected.  

- **Bare Columns**  
  Bare columns (`SELECT EmployeeId`) are usually resolved to their table if the statement parses successfully.  
  In fallback (regex-only) mode, they are treated as global and may be ambiguous.  
  Aliased usage (`e.EmployeeId`) is always more reliable.  

- **Schemas**  
  `dbo.TableName` and `TableName` are treated as the same.  
  Multiple schemas with the same table name are not yet distinguished.  

- **Cross-File Consistency**  
  Accuracy depends on having all schema files in your workspace.  
  Missing files = missing definitions.  

- **Parser Gaps**  
  Some T-SQL constructs (e.g. `TOP(@var)`, `MERGE`, `OUTPUT INTO`) are not fully supported by the parser.  
  Regex fallback ensures partial functionality, but AST-powered features may be incomplete.  

- **Mid-Edit States**  
  While typing (e.g. after `e.` without a column), some features may only partially work until the statement is complete.  

---

## 📜 License

MIT License

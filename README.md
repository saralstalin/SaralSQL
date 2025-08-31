# SaralSQL : SQL Language Features (Preview)

This VS Code extension adds basic **Language Server Protocol (LSP)** features for working with SQL code.  
It is fast and lightweight, works on plain `.sql` files, and does **not** require a database connection.  

It uses a regex-based indexer instead of a full SQL parser — so results won’t be perfect in every scenario, but it provides a solid, practical foundation for SQL development in VS Code.

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

- **Workspace Indexing**  
  - Automatically indexes all `.sql` files in the workspace.  
  - Updates instantly as you type or save.  

---

## 🚀 Getting Started

1. Install the extension.  
2. Open a folder or workspace containing `.sql` files.  
3. Start editing — features like definitions, references, and completions activate automatically.  

> 💡 Works best when your schema objects (tables, types, procedures) are defined in `.sql` files within your workspace.  

---

## ⚠️ Preview Notice

This is a **Preview release**.  
- Optimized for **T-SQL / SQL Server** style DDL & DML.  
- Dialects like Postgres or MySQL may partially work but are not fully supported yet.  
- Column and reference detection is heuristic — complex scripts may produce misses or false positives.  

We’re releasing early to gather real-world feedback before expanding the feature set.  

---

## 🛠 Planned Improvements

- Outline view for procedures, tables, and columns.  
- Workspace symbol search (`Ctrl+T`).  
- Diagnostics for undefined tables/columns.  
- Hover information and quick documentation.  
- Smarter reference resolution (scoped by `FROM` / `JOIN` context).  

---

## 🧑‍💻 Contributing

We welcome feedback and bug reports!  
- Open issues with examples of SQL code that doesn’t behave as expected.  
- Pull requests are encouraged — especially for dialect support or smarter parsing.  

---

## Known Limitations

This extension is intentionally lightweight and does **not** do full SQL parsing or semantic analysis.  
Be aware of these trade-offs:

- **Column References**  
  Column references are matched globally by name.  
  If multiple tables have the same column (e.g. `Id`), all may appear in references.  
  (Teams using explicit naming standards like `EmployeeId`, `DepartmentId` will be less affected.)  

- **Bare Columns**  
  Bare columns (`SELECT EmployeeId`) are treated as global — not resolved to a specific table.  
  Aliased usage (`e.EmployeeId`) works better.  

- **Schemas**  
  By default, `dbo.TableName` and `TableName` are treated as the same.  
  Multiple schemas with the same table name are not yet distinguished.  

- **Cross-File Consistency**  
  Accuracy depends on having all schema files in your workspace.  
  Missing files = missing definitions.  
  (For SSDT projects, this typically isn’t an issue since all objects are included.)  

---

## 📜 License

MIT License

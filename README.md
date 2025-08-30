# SaralSQL : SQL Language Features (Preview)

This VS Code extension adds basic **Language Server Protocol (LSP)** features for working with SQL code.  
It is lightweight, works on plain `.sql` files, and does **not** require a database connection.  

---

## ✨ Features

- **Go to Definition**  
  Jump to the definition of a table, view, function, or stored procedure in your workspace.  

- **Find All References**  
  Locate table and column references across SQL files.  
  *(with smarter filtering to skip false positives in comments or unrelated identifiers)*  

- **IntelliSense / Auto-completion**  
  - Complete table names and stored procedures.  
  - After typing `alias.` you’ll get column suggestions for that table.  
  - Supports schema prefixes (`dbo.TableName` == `TableName`).  

- **Workspace Indexing**  
  - Automatically indexes all `.sql` files in the workspace.  
  - Updates instantly as you type  

---

## 🚀 Getting Started

1. Install the extension.  
2. Open a folder or workspace containing `.sql` files.  
3. Start editing — the language features will activate automatically.  

> 💡 Works best when your database objects are defined in `.sql` files (tables, procs, views).  

---

## ⚠️ Preview Notice

This is a **Preview release**.  
- Tested mainly with **T-SQL / SQL Server style** DDL & DML.  
- Some features may not yet handle all dialects (Postgres, MySQL, etc).  
- Column and reference detection may miss edge cases in complex scripts.  

We’re releasing early to gather feedback and stabilize before expanding feature set.  

---

## 🛠 Planned Improvements

- Outline view for procedures, tables, and columns.  
- Workspace symbol search (`Ctrl+T`).  
- Diagnostics for undefined tables/columns.  
- Hover info and quick documentation.  

---

## 🧑‍💻 Contributing

We welcome feedback and bug reports!  
- Please open issues with examples of SQL code that doesn’t behave as expected.  
- Pull requests are encouraged — especially for new dialect support or improved parsing.  

---

## 📜 License

MIT License

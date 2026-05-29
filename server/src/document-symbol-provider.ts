import type { DocumentSymbol, Range, SymbolKind } from "vscode-languageserver/node";

type DocumentSymbolProviderDeps = {
  toNormUri: (uri: string) => string;
  definitions: Map<string, any[]>;
  Range: typeof Range;
  symbolKindClass: SymbolKind;
  symbolKindField: SymbolKind;
};

export function onDocumentSymbolProvider(textDocumentUri: string, deps: DocumentSymbolProviderDeps): DocumentSymbol[] {
  const uri = deps.toNormUri(textDocumentUri);
  const defs = deps.definitions.get(uri) || [];
  const symbols: DocumentSymbol[] = [];

  for (const def of defs) {
    const tableSymbol: DocumentSymbol = {
      name: def.rawName,
      kind: deps.symbolKindClass,
      range: deps.Range.create(def.line, 0, def.line, 200),
      selectionRange: deps.Range.create(def.line, 0, def.line, 200),
      children: []
    };

    if (def.columns) {
      tableSymbol.children = def.columns.map((col: any) => {
        const startChar = typeof col.start === "number" ? col.start : 0;
        const endChar = typeof col.end === "number" ? col.end : 200;
        return {
          name: col.rawName,
          kind: deps.symbolKindField,
          range: deps.Range.create(col.line, startChar, col.line, endChar),
          selectionRange: deps.Range.create(col.line, startChar, col.line, endChar),
          detail: col.type || undefined
        };
      });
    }

    symbols.push(tableSymbol);
  }

  return symbols;
}

type IndexColumnDef = {
  name: string;
  rawName: string;
  type?: string;
  line: number;
  start?: number;
  end?: number;
};

type IndexSymbolDef = {
  name: string;
  rawName: string;
  uri: string;
  line: number;
  columns?: IndexColumnDef[];
  kind?: string;
};

type IndexReferenceDef = {
  name: string;
  uri: string;
  line: number;
  start: number;
  end: number;
  kind: "table" | "column" | "parameter";
  context?: string;
  validateSchema?: boolean;
};

export class IndexStore {
  readonly columnsByTable = new Map<string, Set<string>>();
  readonly aliasesByUri = new Map<string, Map<string, string>>();
  readonly definitions = new Map<string, IndexSymbolDef[]>();
  readonly referencesIndex = new Map<string, Map<string, IndexReferenceDef[]>>();
  readonly tablesByName = new Map<string, IndexSymbolDef>();
  readonly tableTypesByName = new Map<string, IndexSymbolDef>();
  readonly tempTablesByUri = new Map<string, Map<string, { columns: Set<string>; declaredAt: number }>>();

  clearAll(): void {
    this.columnsByTable.clear();
    this.aliasesByUri.clear();
    this.definitions.clear();
    this.referencesIndex.clear();
    this.tablesByName.clear();
    this.tableTypesByName.clear();
    this.tempTablesByUri.clear();
  }
}

export const indexStore = new IndexStore();

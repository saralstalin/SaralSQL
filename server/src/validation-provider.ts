import type { TextDocuments } from "vscode-languageserver";
import type { Diagnostic, DiagnosticSeverity, Range } from "vscode-languageserver";
import { DiagnosticSeverity as DiagnosticSeverityValue } from "vscode-languageserver";
import type { TextDocument } from "vscode-languageserver-textdocument";

type DiagnosticsSender = (uri: string, diagnostics: any[]) => void;
type Logger = (message: string) => void;
type Validator = (doc: TextDocument) => Promise<void>;

type ParserDiagnosticsDeps = {
  parseSql: (text: string) => any;
  hasBlockingParseIssues: (parsed: any, parserIssues: any[]) => boolean;
  showParseIssues: boolean;
  schemaValidationReady: boolean;
  schemaDiagnosticCodes: Set<string>;
  resolveExistingSqlCmdInclude: (docUri: string, message: string) => boolean;
  shouldSuppressDiagnosticCode: (code: string, disabledDiagnosticCodes: Set<string>) => boolean;
  disabledDiagnosticCodes: Set<string>;
  SARAL_DIAGNOSTIC_CODES: Record<string, string>;
};

function toDiagnostic(
  diag: any,
  source: string,
  lineStarts: number[],
  mapSeverity: (severity: any, code: string) => DiagnosticSeverity,
  offsetToPosition: (offset: number, lineStarts: number[]) => { line: number; character: number }
): Diagnostic | null {
  let range: Range;

  if (typeof diag.start === "number") {
    const startPos = offsetToPosition(diag.start, lineStarts);
    const endPos = offsetToPosition(diag.end ?? diag.start, lineStarts);
    range = {
      start: { line: startPos.line, character: startPos.character },
      end: { line: endPos.line, character: endPos.character }
    };
  } else if (diag.range && typeof diag.range.start === "number") {
    const startPos = offsetToPosition(diag.range.start, lineStarts);
    const endPos = offsetToPosition(diag.range.end, lineStarts);
    range = {
      start: { line: startPos.line, character: startPos.character },
      end: { line: endPos.line, character: endPos.character }
    };
  } else {
    return null;
  }

  return {
    code: String(diag.code ?? ""),
    severity: mapSeverity(diag.severity, String(diag.code ?? "")),
    range,
    message: String(diag.message ?? "SQL diagnostic"),
    source
  };
}

export function collectParserDiagnosticsProvider(
  doc: TextDocument,
  text: string,
  lineStarts: number[],
  diagnostics: Diagnostic[],
  mapSeverity: (severity: any, code: string) => DiagnosticSeverity,
  offsetToPosition: (offset: number, lineStarts: number[]) => { line: number; character: number },
  deps: ParserDiagnosticsDeps
): { parsed: any; hasParseIssues: boolean } {
  const parsed = deps.parseSql(text);
  const combinedDiagnostics = parsed?.diagnostics ?? [];
  const parserIssues = (parsed?.issues?.length
    ? parsed.issues
    : combinedDiagnostics.filter((diag: any) => {
      const source = String(diag.source ?? "").toLowerCase();
      const code = String(diag.code ?? "").toUpperCase();
      return source === "parser" || code.startsWith("PARSE_");
    })) ?? [];

  const hasParseIssues = deps.hasBlockingParseIssues(parsed, parserIssues);

  if (hasParseIssues) {
    if (deps.showParseIssues) {
      for (const issue of parserIssues) {
        const diagnostic = toDiagnostic(issue, "SaralSQL Parser", lineStarts, mapSeverity, offsetToPosition);
        if (diagnostic && !deps.shouldSuppressDiagnosticCode(String((issue as any).code ?? diagnostic.code ?? ""), deps.disabledDiagnosticCodes)) {
          diagnostics.push(diagnostic);
        }
      }
    }
    return { parsed, hasParseIssues };
  }

  const semanticDiags = parsed?.semanticDiagnostics?.length
    ? parsed.semanticDiagnostics
    : combinedDiagnostics.filter((diag: any) => {
      const source = String(diag.source ?? "").toLowerCase();
      const code = String(diag.code ?? "").toUpperCase();
      return source !== "parser" && !code.startsWith("PARSE_");
    });

  for (const diag of semanticDiags) {
    const diagCode = String((diag as any)?.code ?? "").toUpperCase();
    if (!deps.schemaValidationReady && deps.schemaDiagnosticCodes.has(diagCode)) {
      continue;
    }
    if (diagCode === "SQLCMD_UNRESOLVED_INCLUDE") {
      if (deps.resolveExistingSqlCmdInclude(doc.uri, String((diag as any)?.message ?? ""))) {
        continue;
      }
    }
    const diagnostic = toDiagnostic(diag, "SaralSQL Parser", lineStarts, mapSeverity, offsetToPosition);
    if (diagnostic && !deps.shouldSuppressDiagnosticCode(String((diag as any).code ?? diagnostic.code ?? ""), deps.disabledDiagnosticCodes)) {
      if ((diagnostic.code === deps.SARAL_DIAGNOSTIC_CODES.UnknownColumn || diagnostic.code === deps.SARAL_DIAGNOSTIC_CODES.AmbiguousColumn) && typeof diag.start === "number") {
        const resolution = parsed.columns?.resolutions?.find((r: any) => r.location?.start === diag.start);
        if (resolution?.isUnverifiable) {
          continue;
        }
      }

      diagnostics.push(diagnostic);
    }
  }

  return { parsed, hasParseIssues };
}

type SchemaValidationContextDeps = {
  normDocUri: string;
  definitions: Map<string, any[]>;
  aliasesByUri: Map<string, any>;
  getReferencesForUri: (uri: string) => any[];
  parsed: any;
  normalizeName: (name: string) => string;
};

export function buildSchemaValidationContextProvider(deps: SchemaValidationContextDeps): {
  fileAliases: any;
  refsForDoc: any[];
  localDefs: any[];
  localDefsByName: Map<string, any>;
  cteNames: Set<string>;
  seenTables: Set<string>;
  reportedMissingTables: Set<string>;
  seenColumns: Set<string>;
  diagnosticTextCache: Map<string, string>;
  tableRefsByName: Map<string, any[]>;
  propertyAccessStarts: Set<number>;
} {
  const fileAliases = deps.aliasesByUri.get(deps.normDocUri);
  const refsForDoc = deps.getReferencesForUri(deps.normDocUri);
  const localDefs = deps.definitions.get(deps.normDocUri) ?? [];
  const localDefsByName = new Map<string, any>();
  for (const def of localDefs) {
    localDefsByName.set(deps.normalizeName(def.name), def);
  }

  return {
    fileAliases,
    refsForDoc,
    localDefs,
    localDefsByName,
    cteNames: new Set<string>(),
    seenTables: new Set<string>(),
    reportedMissingTables: new Set<string>(),
    seenColumns: new Set<string>(),
    diagnosticTextCache: new Map<string, string>(),
    tableRefsByName: new Map<string, any[]>(),
    propertyAccessStarts: new Set<number>(
      Array.isArray(deps.parsed?.columns?.propertyAccesses)
        ? deps.parsed.columns.propertyAccesses
          .map((p: any) => Number(p?.location?.start))
          .filter((n: number) => Number.isFinite(n))
        : []
    )
  };
}

export function buildTableRefsByNameProvider(
  refsForDoc: any[],
  normalizeName: (name: string) => string
): Map<string, any[]> {
  const tableRefsByName = new Map<string, any[]>();
  for (const ref of refsForDoc) {
    if (ref.kind !== "table") {
      continue;
    }
    const key = normalizeName(ref.name);
    const bucket = tableRefsByName.get(key) ?? [];
    bucket.push(ref);
    tableRefsByName.set(key, bucket);
  }
  return tableRefsByName;
}

export function createDiagnosticTextReaderProvider(
  doc: TextDocument,
  diagnosticTextCache: Map<string, string>
): (startLine: number, startChar: number, endLine: number, endChar: number) => string {
  return (startLine: number, startChar: number, endLine: number, endChar: number): string => {
    const key = `${startLine}:${startChar}:${endLine}:${endChar}`;
    const cached = diagnosticTextCache.get(key);
    if (cached !== undefined) {
      return cached;
    }

    const value = doc.getText({
      start: { line: startLine, character: startChar },
      end: { line: endLine, character: endChar }
    }).trim();
    diagnosticTextCache.set(key, value);
    return value;
  };
}

export function tableExistsInContextProvider(
  tableName: string,
  deps: {
    normalizeName: (name: string) => string;
    isOutputPseudoTableReference: (name: string) => boolean;
    isSystemTableReference: (name: string) => boolean;
    localDefsByName: Map<string, any>;
    tableExists: (tableName: string) => boolean;
  }
): boolean {
  const norm = deps.normalizeName(tableName);
  if (deps.isOutputPseudoTableReference(norm)) { return true; }
  if (deps.isSystemTableReference(norm)) { return true; }
  if (deps.localDefsByName.has(norm)) { return true; }
  const stripped = norm.replace(/^dbo\./, "");
  if (deps.localDefsByName.has(stripped)) { return true; }
  return deps.tableExists(tableName);
}

export function columnExistsInContextProvider(
  tableName: string,
  columnName: string,
  deps: {
    normalizeName: (name: string) => string;
    isOutputPseudoTableReference: (name: string) => boolean;
    isSystemTableReference: (name: string) => boolean;
    localDefsByName: Map<string, any>;
    columnExists: (tableName: string, columnName: string) => boolean;
  }
): boolean {
  const norm = deps.normalizeName(tableName);
  if (deps.isOutputPseudoTableReference(norm)) { return true; }
  if (deps.isSystemTableReference(norm)) { return true; }
  const stripped = norm.replace(/^dbo\./, "");
  const localDef = deps.localDefsByName.get(norm) || deps.localDefsByName.get(stripped);
  const targetCol = deps.normalizeName(columnName);
  if (localDef?.columns?.some((c: any) => deps.normalizeName(c.name ?? c.rawName) === targetCol)) {
    return true;
  }
  return deps.columnExists(tableName, columnName);
}

type AddUnknownTableAtDeps = {
  shouldSuppressDiagnosticCode: (code: string, disabledDiagnosticCodes: Set<string>) => boolean;
  SARAL_DIAGNOSTIC_CODES: Record<string, string>;
  disabledDiagnosticCodes: Set<string>;
  normalizeName: (name: string) => string;
  reportedMissingTables: Set<string>;
  seenTables: Set<string>;
  lineStarts: number[];
  resolveCteSymbolAtOffset: (tableName: string, offset: number) => any | null;
  cteNames: Set<string>;
  isSystemTableReference: (name: string) => boolean;
  tableExistsInContext: (name: string) => boolean;
  getTextAtRange: (startLine: number, startChar: number, endLine: number, endChar: number) => string;
  resolveDiagnosticSeverity: (code: string, fallback: DiagnosticSeverity, overrides: Map<string, DiagnosticSeverity>) => DiagnosticSeverity;
  diagnosticSeverityOverrides: Map<string, DiagnosticSeverity>;
  diagnostics: Diagnostic[];
};

export function addUnknownTableAtProvider(
  name: string,
  startLine: number,
  startChar: number,
  endLine: number,
  endChar: number,
  isFallback: boolean,
  deps: AddUnknownTableAtDeps
): void {
  if (!name) { return; }
  if (deps.shouldSuppressDiagnosticCode(deps.SARAL_DIAGNOSTIC_CODES.UnknownTable, deps.disabledDiagnosticCodes)) { return; }

  const clean = deps.normalizeName(name);
  if (isFallback && deps.reportedMissingTables.has(clean)) {
    return;
  }

  const key = isFallback ? `fallback:${clean}` : `${clean}:${startLine}:${startChar}`;
  if (deps.seenTables.has(key)) { return; }
  deps.seenTables.add(key);
  deps.reportedMissingTables.add(clean);

  const refOffset = (deps.lineStarts[startLine] ?? 0) + startChar;
  if (deps.resolveCteSymbolAtOffset(clean, refOffset)) { return; }
  if (deps.cteNames.has(clean)) { return; }
  if (clean.startsWith("#") || clean.startsWith("@")) { return; }
  if (deps.isSystemTableReference(clean)) { return; }
  if (deps.tableExistsInContext(clean)) { return; }

  let displayTableName = name;
  const rangeText = deps.getTextAtRange(startLine, startChar, endLine, endChar);
  if (rangeText) {
    const rangeClean = deps.normalizeName(rangeText);
    if (rangeClean === clean || rangeClean.endsWith("." + clean)) {
      displayTableName = rangeText;
    }
  }

  deps.diagnostics.push({
    code: deps.SARAL_DIAGNOSTIC_CODES.UnknownTable,
    severity: deps.resolveDiagnosticSeverity(deps.SARAL_DIAGNOSTIC_CODES.UnknownTable, DiagnosticSeverityValue.Error, deps.diagnosticSeverityOverrides),
    range: {
      start: { line: startLine, character: startChar },
      end: { line: endLine, character: endChar }
    },
    message: `Unknown table '${displayTableName}'`,
    source: "SaralSQL"
  });
}

type AddWrongColumnDeps = {
  shouldSuppressDiagnosticCode: (code: string, disabledDiagnosticCodes: Set<string>) => boolean;
  SARAL_DIAGNOSTIC_CODES: Record<string, string>;
  disabledDiagnosticCodes: Set<string>;
  seenColumns: Set<string>;
  resolveDiagnosticSeverity: (code: string, fallback: DiagnosticSeverity, overrides: Map<string, DiagnosticSeverity>) => DiagnosticSeverity;
  diagnosticSeverityOverrides: Map<string, DiagnosticSeverity>;
  getTextAtRange: (startLine: number, startChar: number, endLine: number, endChar: number) => string;
  diagnostics: Diagnostic[];
};

export function addWrongColumnProvider(
  table: string,
  column: string,
  line: number,
  start: number,
  end: number,
  tableDisplay: string | undefined,
  deps: AddWrongColumnDeps
): void {
  if (deps.shouldSuppressDiagnosticCode(deps.SARAL_DIAGNOSTIC_CODES.UnknownColumn, deps.disabledDiagnosticCodes)) { return; }

  const key = `${column}:${line}:${start}:${end}`;
  if (deps.seenColumns.has(key)) { return; }
  deps.seenColumns.add(key);

  deps.diagnostics.push({
    code: deps.SARAL_DIAGNOSTIC_CODES.UnknownColumn,
    severity: deps.resolveDiagnosticSeverity(deps.SARAL_DIAGNOSTIC_CODES.UnknownColumn, DiagnosticSeverityValue.Error, deps.diagnosticSeverityOverrides),
    range: {
      start: { line, character: start },
      end: { line, character: end }
    },
    message: `Column '${deps.getTextAtRange(line, start, line, end) || column}' not found in table '${tableDisplay ?? table}'`,
    source: "SaralSQL"
  });
}

export function createOffsetRangeProvider(
  lineStarts: number[],
  offsetToPosition: (offset: number, lineStarts: number[]) => { line: number; character: number }
): (start: number, end: number) => Range {
  return (start: number, end: number): Range => {
    const startPos = offsetToPosition(start, lineStarts);
    const endPos = offsetToPosition(end, lineStarts);
    return {
      start: { line: startPos.line, character: startPos.character },
      end: { line: endPos.line, character: endPos.character }
    };
  };
}

export function getDisplayTableNameProvider(
  table: string,
  deps: {
    normalizeName: (name: string) => string;
    localDefsByName: Map<string, any>;
    tablesByName: Map<string, any>;
    tableTypesByName: Map<string, any>;
  }
): string {
  const norm = deps.normalizeName(table);
  const stripped = norm.replace(/^dbo\./, "");
  const def = deps.localDefsByName.get(norm)
    || deps.localDefsByName.get(stripped)
    || deps.tablesByName.get(norm)
    || deps.tableTypesByName.get(norm)
    || deps.tablesByName.get(stripped)
    || deps.tableTypesByName.get(stripped);
  return def?.rawName ?? table;
}


export async function validateWorkspaceDocumentsProvider(
  documents: TextDocuments<TextDocument>,
  workspaceDocuments: Map<string, TextDocument>,
  validateTextDocument: Validator,
  log: Logger
): Promise<void> {
  const openDocs = new Map(documents.all().map(doc => [doc.uri, doc]));
  const validatedUris = new Set<string>();

  for (const doc of openDocs.values()) {
    try {
      await validateTextDocument(doc);
      validatedUris.add(doc.uri);
    } catch (e) {
      log(`[validateWorkspace] open document validate threw: ${String(e)}`);
    }
  }

  let validatedCount = validatedUris.size;

  for (const doc of workspaceDocuments.values()) {
    if (validatedUris.has(doc.uri)) {
      continue;
    }

    try {
      await validateTextDocument(doc);
      validatedUris.add(doc.uri);
      validatedCount++;

      if (validatedCount % 25 === 0) {
        await new Promise(resolve => setTimeout(resolve, 0));
      }
    } catch (e) {
      log(`[validateWorkspace] workspace document validate threw for ${doc.uri}: ${String(e)}`);
    }
  }

  log(`[validateWorkspace] validated ${validatedUris.size} workspace SQL documents`);
}

export function clearWorkspaceDiagnosticsProvider(
  documents: TextDocuments<TextDocument>,
  workspaceDocuments: Map<string, TextDocument>,
  sendDiagnostics: DiagnosticsSender
): void {
  const uris = new Set<string>();

  for (const doc of workspaceDocuments.values()) {
    uris.add(doc.uri);
  }

  for (const doc of documents.all()) {
    uris.add(doc.uri);
  }

  for (const uri of uris) {
    sendDiagnostics(uri, []);
  }
}

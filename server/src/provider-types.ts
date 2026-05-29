import type { CompletionParams, Hover, Location, Position, Range } from "vscode-languageserver/node";
import type { TextDocument } from "vscode-languageserver-textdocument";
import type { ParseResult } from "./sql-parser";
import type { ReferenceDef, SymbolDef } from "./definitions";

export type CommonStatementOwnerResolver = (
  statementText: string,
  columnName: string,
  scopeAtPos?: any,
  parsed?: ParseResult | null,
  offset?: number,
  localDefsByName?: Map<string, any>
) => any;

export type HoverProviderDeps = {
  toNormUri: (uri: string) => string;
  definitions: Map<string, any[]>;
  tablesByName: Map<string, any>;
  tableTypesByName: Map<string, any>;
  findReferenceAtPosition: (uri: string, line: number, character: number) => any;
  getParsedDocument: (doc: TextDocument) => any;
  getPropertyAccessAtOffset: (parsed: any, offset: number) => any | null;
  getWordRangeAtPosition: (doc: TextDocument, pos: Position) => Range | null;
  getUpdateSetTargetTable: (parsed: any, offset: number) => string | null;
  findStatementLocalColumnOwner: CommonStatementOwnerResolver;
  getCurrentStatement: (doc: TextDocument, position: Position) => string;
  getHoverColumnLabel: (column: any, tokenText?: string) => string;
  getResolvedObjectKindLabel: (normalizedName: string, def?: any, options?: { titleCase?: boolean }) => string;
  resolveSymbolCaseInsensitive: (scope: any, symbolName: string) => any;
  getSymbolLocalColumns: (sym: any) => any[] | undefined;
  getCteColumns: (sym: any) => any[];
  getDisplaySymbolName: (sym: any) => string;
  resolveAliasTableName: (sym: any) => string;
  isFunctionCallInAst: (ast: any, tableNameNorm: string) => { isFunc: boolean; rawName: string };
  getParserAliasColumnNames: (parsed: any, aliasSym: any, localDefsByName?: Map<string, any>) => string[];
  safeLog: (message: string) => void;
};

export type DefinitionProviderDeps = {
  toNormUri: (uri: string) => string;
  definitions: Map<string, any[]>;
  tablesByName: Map<string, any>;
  tableTypesByName: Map<string, any>;
  referencesIndex: Map<string, Map<string, any[]>>;
  getParsedDocument: (doc: TextDocument) => any;
  findReferenceAtPosition: (uri: string, line: number, character: number) => any;
  isAmbiguousBareColumnAtPosition: (doc: TextDocument, position: Position, parsed: any) => boolean;
  getWordRangeAtPosition: (doc: TextDocument, pos: Position) => Range | null;
  findStatementLocalColumnOwner: CommonStatementOwnerResolver;
  getCurrentStatement: (doc: TextDocument, position: Position) => string;
  findColumnInTable: (tableName: string, colName: string) => Location[];
  findDerivedAliasProjectedColumnRange: (doc: TextDocument, parsed: any, offset: number, tableName: string, colName: string) => Location[] | null;
  getResolutionSourceColumns: (resolution: any) => Array<{ table: string; column: string }>;
  resolveSymbolCaseInsensitive: (scope: any, symbolName: string) => any;
  getCteColumns: (sym: any) => any[];
};

export type CompletionProviderDeps = {
  toNormUri: (uri: string) => string;
  getDocument: (rawUri: string, normUri: string) => TextDocument | undefined;
  safeLog: (message: string) => void;
  getParsedDocument: (doc: TextDocument) => any;
  getUpdateSetTargetTable: (parsed: any, offset: number) => string | null | undefined;
  getInsertColumnTargetTable: (parsed: any, offset: number) => string | null | undefined;
  endsWithDotToken: (linePrefix: string) => boolean;
  getAliasBeforeDot: (linePrefix: string) => string | null;
  resolveSymbolCaseInsensitive: (scope: any, symbolName: string) => any;
  getParserAliasColumnNames: (parsed: any, sym: any, localDefsByName?: Map<string, any>) => string[];
  resolveAliasTableName: (sym: any) => string | undefined;
  getCteColumns: (sym: any) => any[];
  getCompletionParsedDocument: (doc: TextDocument, offset: number) => { parsed: any; offset: number } | null;
  resolveAliasTableFromStatementAst: (parsed: any, offset: number, aliasNorm: string) => string | null;
  isFromJoinTableContext: (linePrefix: string) => boolean;
  isInSelectProjectionContext: (parsed: any, offset: number, linePrefix: string) => boolean;
  getStatementTableCandidatesFromAst: (parsed: any, offset: number) => string[];
  tablesByName: Map<string, any>;
  tableTypesByName: Map<string, any>;
};

export type RefsProviderDeps = {
  toNormUri: (uri: string) => string;
  tablesByName: Map<string, any>;
  tableTypesByName: Map<string, any>;
  definitions: Map<string, SymbolDef[]>;
  referencesIndex: Map<string, Map<string, ReferenceDef[]>>;
  findReferenceAtPosition: (uri: string, line: number, character: number) => ReferenceDef | undefined;
  getParsedDocument: (doc: TextDocument) => ParseResult | null;
  findStatementLocalColumnOwner: CommonStatementOwnerResolver;
  getCurrentStatement: (doc: TextDocument, position: Position) => string;
};

export type RenameProviderDeps = {
  getWordRangeAtPosition: (doc: TextDocument, pos: Position) => Range | null;
  findReferencesForWord: (rawWord: string, doc: TextDocument, position?: Position) => Location[];
};

export type OnCompletionProvider = (params: CompletionParams, deps: CompletionProviderDeps) => any[];
export type OnHoverProvider = (doc: TextDocument, pos: Position, deps: HoverProviderDeps) => Promise<Hover | null>;

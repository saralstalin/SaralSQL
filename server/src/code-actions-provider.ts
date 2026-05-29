import type { CodeAction, CodeActionParams } from "vscode-languageserver/node";
import type { TextDocument } from "vscode-languageserver-textdocument";

type CodeActionsDeps = {
  buildReadableBareColumnCodeAction: (uri: string, diagnostic: any) => CodeAction | null;
  buildUpdateNoLockCodeAction: (uri: string, diagnostic: any, text: string, lineStarts: number[]) => CodeAction | null;
  buildSelectStarExpansionCodeActions: (
    uri: string,
    parsed: any,
    lineStarts: number[],
    tablesByName: Map<string, any>,
    tableTypesByName: Map<string, any>,
    selectionStartOffset: number,
    selectionEndOffset: number
  ) => CodeAction[];
  getLineStarts: (text: string) => number[];
  getParsedDocument: (doc: TextDocument) => any;
  tablesByName: Map<string, any>;
  tableTypesByName: Map<string, any>;
};

export function buildCodeActionsForDocument(
  params: CodeActionParams,
  doc: TextDocument | undefined,
  deps: CodeActionsDeps
): CodeAction[] {
  const actions: CodeAction[] = [];
  const docText = doc?.getText() ?? "";
  const lineStarts = doc ? deps.getLineStarts(docText) : [];

  for (const diagnostic of params.context.diagnostics ?? []) {
    const action = deps.buildReadableBareColumnCodeAction(params.textDocument.uri, diagnostic);
    if (action) {
      actions.push(action);
    }
    if (doc) {
      const noLockAction = deps.buildUpdateNoLockCodeAction(
        params.textDocument.uri,
        diagnostic,
        docText,
        lineStarts
      );
      if (noLockAction) {
        actions.push(noLockAction);
      }
    }
  }

  if (doc) {
    const parsed = deps.getParsedDocument(doc);
    const startOffset = (lineStarts[params.range.start.line] ?? 0) + params.range.start.character;
    const endOffset = (lineStarts[params.range.end.line] ?? 0) + params.range.end.character;
    actions.push(
      ...deps.buildSelectStarExpansionCodeActions(
        params.textDocument.uri,
        parsed,
        lineStarts,
        deps.tablesByName,
        deps.tableTypesByName,
        startOffset,
        endOffset
      )
    );
  }

  return actions;
}


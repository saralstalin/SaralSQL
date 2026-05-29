import type { Position, RenameParams, TextEdit, WorkspaceEdit } from "vscode-languageserver/node";
import type { TextDocument } from "vscode-languageserver-textdocument";
import * as url from "url";
import type { RenameProviderDeps } from "./provider-types";

export function onRenameProvider(
  params: RenameParams,
  doc: TextDocument | undefined,
  deps: RenameProviderDeps
): WorkspaceEdit | null {
  if (!doc) { return null; }

  const range = deps.getWordRangeAtPosition(doc, params.position);
  if (!range) { return null; }

  const rawWord = doc.getText(range);
  const newName = params.newName;

  const locations = deps.findReferencesForWord(rawWord, doc, params.position);
  if (!locations || locations.length === 0) { return null; }

  const editsByUri: { [uri: string]: TextEdit[] } = {};
  const seen = new Set<string>();

  for (const loc of locations) {
    const key = `${loc.uri}:${loc.range.start.line}:${loc.range.start.character}:${loc.range.end.line}:${loc.range.end.character}`;
    if (seen.has(key)) { continue; }
    seen.add(key);

    const uri = loc.uri.startsWith("file://") ? loc.uri : url.pathToFileURL(loc.uri).toString();

    if (!editsByUri[uri]) { editsByUri[uri] = []; }
    editsByUri[uri].push({ range: loc.range, newText: newName });
  }

  return { changes: editsByUri };
}

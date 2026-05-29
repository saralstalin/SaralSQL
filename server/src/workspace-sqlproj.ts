import * as fs from "fs";
import * as fg from "fast-glob";
import * as path from "path";
import * as url from "url";
import { normalizeFileLikeUri } from "./uri-utils";

export type SqlProjItemKind = "build" | "none" | "preDeploy" | "postDeploy" | "other";

export function toSqlProjMembershipKey(rawUri: string): string {
  return normalizeFileLikeUri(rawUri).toLowerCase();
}

export function shouldContributeToWorkspaceSchemaFor(
  membership: Map<string, SqlProjItemKind>,
  hasSqlProj: boolean,
  strictBuildMembership: boolean,
  rawUri: string
): boolean {
  if (!strictBuildMembership) {
    return true;
  }
  if (!hasSqlProj) {
    return true;
  }
  const kind = membership.get(toSqlProjMembershipKey(rawUri));
  return kind === "build";
}

function decodeXmlAttribute(value: string): string {
  return value
    .replace(/&quot;/g, "\"")
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function registerSqlProjItem(
  projectDir: string,
  includeValue: string,
  kind: SqlProjItemKind,
  membership: Map<string, SqlProjItemKind>
): void {
  const trimmed = decodeXmlAttribute(String(includeValue ?? "").trim());
  if (!trimmed || !trimmed.toLowerCase().endsWith(".sql")) {
    return;
  }

  const absPath = path.isAbsolute(trimmed)
    ? trimmed
    : path.resolve(projectDir, trimmed);
  const sqlUri = url.pathToFileURL(absPath).toString();
  membership.set(toSqlProjMembershipKey(sqlUri), kind);
}

function ingestSqlProj(
  absSqlProjPath: string,
  membership: Map<string, SqlProjItemKind>
): void {
  const projectXml = fs.readFileSync(absSqlProjPath, "utf8");
  const projectDir = path.dirname(absSqlProjPath);

  const collect = (tagName: string, kind: SqlProjItemKind): void => {
    const rx = new RegExp(`<${tagName}\\b[^>]*\\bInclude\\s*=\\s*\"([^\"]+)\"[^>]*>`, "gi");
    for (const m of projectXml.matchAll(rx)) {
      registerSqlProjItem(projectDir, String(m[1] ?? ""), kind, membership);
    }
  };

  collect("Build", "build");
  collect("None", "none");
  collect("PreDeploy", "preDeploy");
  collect("PostDeploy", "postDeploy");
  collect("Content", "other");
}

export async function rebuildSqlProjMembershipFromWorkspaceFolders(
  folders: Array<{ uri: string }> | null | undefined,
  membership: Map<string, SqlProjItemKind>,
  onError: (message: string, err?: unknown) => void
): Promise<boolean> {
  membership.clear();
  let hasSqlProjInWorkspace = false;
  if (!folders) {
    return hasSqlProjInWorkspace;
  }

  for (const folder of folders) {
    const folderPath = url.fileURLToPath(folder.uri);
    const sqlProjFiles = await fg.glob("**/*.sqlproj", { cwd: folderPath, absolute: true });
    if (sqlProjFiles.length > 0) {
      hasSqlProjInWorkspace = true;
    }
    for (const sqlProjFile of sqlProjFiles) {
      try {
        ingestSqlProj(sqlProjFile, membership);
      } catch (err) {
        onError(`Failed to parse ${sqlProjFile}`, err);
      }
    }
  }

  return hasSqlProjInWorkspace;
}


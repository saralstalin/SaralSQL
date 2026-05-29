import * as url from "url";

export function normalizeFileLikeUri(rawUri: string): string {
  try {
    let uri = rawUri;
    if (!uri.startsWith("file://")) {
      uri = url.pathToFileURL(uri).toString();
    }

    const prefix = "file:///";
    if (!uri.toLowerCase().startsWith(prefix)) {
      return uri;
    }

    const pathPart = decodeURIComponent(uri.substring(prefix.length));
    const normalizedPath = pathPart
      .replace(/\\/g, "/")
      .replace(/^([A-Za-z]):\//, (_m, drive) => `${String(drive).toLowerCase()}:/`);

    return prefix + encodeURI(normalizedPath);
  } catch {
    return rawUri;
  }
}

export function isSqlProjectUri(rawUri: string): boolean {
  return normalizeFileLikeUri(rawUri).toLowerCase().endsWith(".sqlproj");
}


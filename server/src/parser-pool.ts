import { LruCache } from "./lru-cache";
import * as crypto from 'crypto';
import { parseSql } from "./sql-parser";

// Simple synchronous parser with caching
// Since @saralsql/tsql-parser is fast and synchronous, we don't need worker threads

class AstCache {
    private cache = new LruCache();

    get(key: string): any {
        return this.cache.get(key);
    }

    set(key: string, value: any): void {
        this.cache.set(key, value);
    }

    parse(sql: string, opts: any = {}): any {
        try {
            const hash = contentHash(sql);
            const cached = this.cache.get(hash);
            
            if (cached !== undefined) {
                return cached;
            }

            const result = parseSql(sql, opts);
            this.cache.set(hash, result?.ast ?? null);
            return result?.ast ?? null;
        } catch (e) {
            console.error('[AstCache] Parse error:', e);
            return null;
        }
    }
}

export const astPool = new AstCache();

const astCache = new LruCache();

// Helper: parser is always ready (synchronous)
export function isAstPoolReady(): boolean {
    return true;
}

function contentHash(s: string) {
    return crypto.createHash('sha1').update(s).digest('hex');
}

export async function parseSqlWithWorker(
    sql: string,
    opts: any = {},
    timeoutMs = 2000
): Promise<any> {
    try {
        // cheap guard - avoid parsing huge statements
        if (!sql || sql.length > 4 * 1024) {
            return null;
        }

        const key = contentHash(sql);
        const cached = astCache.get(key);
        if (cached !== null && cached !== undefined) {
            return cached;
        }

        if (!isAstPoolReady()) {
            // cache null so we don't repeatedly try on hot paths
            astCache.set(key, null);
            return null;
        }

        let cleaned = sql
            .replace(/CREATE\s+(OR\s+ALTER\s+)?PROCEDURE[\s\S]*?AS\s+BEGIN/i, "")
            .replace(/END\s*;?$/i, "");

        // Use the synchronous parser (transactsql dialect by default)
        const ast = astPool.parse(cleaned, { database: "transactsql", ...opts });

        // cache (including null)
        astCache.set(key, ast ?? null);
        return ast ?? null;
    } catch (e) {
        // On parse errors, store null so we don’t retry every time
        astCache.set(contentHash(sql), null);
        return null;
    }
}


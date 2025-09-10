import { Worker } from 'worker_threads';
import { LruCache } from "./lru-cache";
import * as crypto from 'crypto';

const WORKER_PATH = require.resolve("./sqlAstWorker.js");
const MAX_WORKERS = 2;


type Pending = { id: string, sql: string, opts: any, resolve: (v: any) => void, reject: (e: any) => void, deadline: number };

class AstWorkerPool {
    workers: Worker[] = [];
    idle: Worker[] = [];
    pending: Pending[] = [];

    // --- minimal additions for backoff/retry ---
    private spawnFailures = 0; // counts recent consecutive spawn failures
    private readonly MAX_SPAWN_ATTEMPTS = 6;      // stop respawning after this many attempts
    private readonly BASE_BACKOFF_MS = 200;       // base backoff (will expon. backoff)
    // ------------------------------------------------

    constructor() { for (let i = 0; i < MAX_WORKERS; i++) { this.spawnWorker(); } }

    spawnWorker() {
        // don't spawn indefinitely if workers keep crashing
        if (this.spawnFailures >= this.MAX_SPAWN_ATTEMPTS) {
            console.error('[AstWorkerPool] max spawn attempts reached, not spawning more workers');
            return;
        }

        const w = new Worker(WORKER_PATH);

        // when a real message comes back from worker, consider it a healthy sign:
        // reduce the failure counter a bit (but keep it >= 0) and then forward to your handler.
        w.on('message', (m: any) => {
            // small stabilization: a successful message likely means the worker is healthy
            this.spawnFailures = Math.max(0, this.spawnFailures - 1);
            this.onMessage(w, m);
        });

        // on exit, remove worker and schedule a respawn using exponential backoff
        w.on('exit', (code: number) => {
            this.workers = this.workers.filter(x => x !== w);
            this.idle = this.idle.filter(x => x !== w);

            // increment failure count and schedule respawn with backoff
            this.spawnFailures++;
            const attempt = this.spawnFailures;
            const delay = Math.min(30_000, this.BASE_BACKOFF_MS * Math.pow(2, Math.max(0, attempt - 1)));
            console.warn(`[AstWorkerPool] worker exited (code=${code}); scheduling respawn in ${delay}ms (attempt ${attempt}/${this.MAX_SPAWN_ATTEMPTS})`);
            setTimeout(() => {
                // double-check the limit again before spawning
                if (this.spawnFailures < this.MAX_SPAWN_ATTEMPTS) {
                    this.spawnWorker();
                } else {
                    console.error('[AstWorkerPool] reached max spawn attempts; not respawning further.');
                }
            }, delay);
        });

        this.workers.push(w);
        this.idle.push(w);
    }

    onMessage(worker: Worker, msg: any) {
        const pIndex = this.pending.findIndex(p => p.id === msg.id);
        if (pIndex === -1) { return; }
        const p = this.pending.splice(pIndex, 1)[0];
        if (msg.error) { p.resolve(null); }
        else { p.resolve(msg.ast); }
        this.idle.push(worker);
        this.runQueue();
    }

    runQueue() {
        while (this.idle.length && this.pending.length) {
            const w = this.idle.shift()!;
            const p = this.pending.shift()!;
            try {
                w.postMessage({ id: p.id, sql: p.sql, opts: p.opts });
            } catch (e) {
                // if postMessage fails, requeue pending and respawn worker
                this.pending.unshift(p);
                // use spawnWorker() — it now checks max attempts and won't spin forever
                this.spawnWorker();
            }
        }
    }

    parse(sql: string, opts: any, timeout = 800) {
        return new Promise((resolve, reject) => {
            const id = Math.random().toString(36).slice(2);
            const p: Pending = { id, sql, opts, resolve, reject, deadline: Date.now() + timeout };
            this.pending.push(p);
            const w = this.idle.shift();
            if (w) {
                try { w.postMessage({ id: p.id, sql: p.sql, opts: p.opts }); }
                catch (err) { this.pending.unshift(p); this.spawnWorker(); }
            }
            const to = setTimeout(() => {
                const idx = this.pending.findIndex(x => x.id === id);
                if (idx !== -1) { this.pending.splice(idx, 1); resolve(null); }
                clearTimeout(to);
            }, timeout + 50);
        });
    }

}


export const astPool = new AstWorkerPool();

const astCache = new LruCache();


// Helper: check if the pool has at least one worker available (simple readiness)
export function isAstPoolReady(): boolean {
    try {
        // astPool is constructed at module load; ensure at least one worker exists
        return Array.isArray((astPool as any).workers) && (astPool as any).workers.length > 0;
    } catch {
        return false;
    }
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


        // ✅ Use transactsql dialect by default, allow opts override
        const ast = await astPool.parse(cleaned, { database: "transactsql", ...opts }, timeoutMs);

        // cache (including null)
        astCache.set(key, ast ?? null);
        return ast ?? null;
    } catch (e) {
        // On parse errors, store null so we don’t retry every time
        astCache.set(contentHash(sql), null);
        return null;
    }
}


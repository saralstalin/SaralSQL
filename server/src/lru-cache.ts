

export class LruCache {
    private map = new Map<string, { value: any; ts: number }>();
    readonly capacity: number;
    readonly ttlMs: number;

    constructor(capacity = 2000, ttlMs = 30 * 60 * 1000) {
        this.capacity = capacity;
        this.ttlMs = Math.max(1, ttlMs);
    }

    has(k: string): boolean {
        const e = this.map.get(k);
        if (!e) { return false; }
        if (Date.now() - e.ts > this.ttlMs) {
            this.map.delete(k);
            return false;
        }
        return true;
    }

    get(k: string): any | null {
        const e = this.map.get(k);
        if (!e) { return null; }
        if (Date.now() - e.ts > this.ttlMs) {
            this.map.delete(k);
            return null;
        }
        // refresh LRU position
        this.map.delete(k);
        this.map.set(k, e);
        return e.value;
    }

    set(k: string, v: any) {
        if (this.map.size >= this.capacity) {
            const first = this.map.keys().next().value;
            if (first !== undefined) {
                this.map.delete(first);
            }
        }
        this.map.set(k, { value: v, ts: Date.now() });
    }
}

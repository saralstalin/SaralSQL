/**
 * Direct SQL Parser wrapper using @saralsql/tsql-parser
 * 
 * This module wraps the new synchronous parser and provides:
 * - Direct parsing without worker threads
 * - Consistent interface with the previous parser-pool
 * - Full semantic analysis (scope, diagnostics, lineage, column info)
 */

import { analyze } from '@saralsql/tsql-parser';

export interface ParseResult {
    ast: any;
    issues?: any[];
    diagnostics?: any[];
    semanticDiagnostics?: any[];
    scope?: any;
    lineage?: any;
    columns?: any;
}

/**
 * Parse SQL using the new @saralsql/tsql-parser
 * Returns the full analysis result or null on failure
 */
export function parseSql(sql: string, opts: any = {}): ParseResult | null {
    try {
        if (!sql || sql.length > 4 * 1024) {
            return null;
        }

        // Analyze SQL - this returns all semantic and syntactic info
        // @saralsql/tsql-parser handles full T-SQL including procedures natively
        const result = analyze(sql);

        return result;
    } catch (e: any) {
        const message = e?.message || "SQL parse error";
        return {
            ast: null,
            issues: [
                {
                    message,
                    severity: "error",
                    start: 0,
                    end: Math.min(sql.length, 1)
                }
            ],
            diagnostics: [
                {
                    source: "parser",
                    message,
                    severity: "error",
                    start: 0,
                    end: Math.min(sql.length, 1)
                }
            ]
        };
    }
}

/**
 * Get just the AST from SQL (lightweight parsing)
 */
export function parseAst(sql: string): any | null {
    const result = parseSql(sql);
    return result?.ast ?? null;
}

/**
 * Get diagnostics from SQL
 */
export function parseDiagnostics(sql: string): any[] {
    try {
        const result = analyze(sql);
        return result?.diagnostics ?? [];
    } catch (e: any) {
        // Convert hard parser failures into LSP diagnostics
        return [
            {
                message: e?.message || "SQL parse error",
                severity: "error",
                start: 0,
                end: Math.min(sql.length, 1)
            }
        ];
    }
}

/**
 * Get scope information from SQL
 */
export function parseScope(sql: string): any | null {
    const result = parseSql(sql);
    return result?.scope ?? null;
}

/**
 * Get lineage information from SQL
 */
export function parseLineage(sql: string): any | null {
    const result = parseSql(sql);
    return result?.lineage ?? null;
}

/**
 * Get column resolution information from SQL
 */
export function parseColumns(sql: string): any | null {
    const result = parseSql(sql);
    return result?.columns ?? null;
}

/**
 * Walk through AST nodes (helper for ast traversal)
 */
export function walkAstNodes(node: any, callback: (n: any) => void): void {
    if (!node || typeof node !== 'object') {
        return;
    }
    callback(node);

    if (Array.isArray(node)) {
        node.forEach(item => typeof item === 'object' && walkAstNodes(item, callback));
    } else {
        for (const key in node) {
            if (node.hasOwnProperty(key)) {
                const child = node[key];
                if (Array.isArray(child)) {
                    child.forEach(item => typeof item === 'object' && walkAstNodes(item, callback));
                } else if (child && typeof child === 'object') {
                    walkAstNodes(child, callback);
                }
            }
        }
    }
}

/**
 * Direct SQL Parser wrapper using @saralsql/tsql-parser.
 * Returns the full semantic analysis result from a single parse.
 */

import { analyze, type AnalysisResult } from "@saralsql/tsql-parser";

type ParseIssueLike = {
    message: string;
    severity?: string;
    start: number;
    end: number;
    code?: string;
};

type AnalysisDiagnosticLike = {
    source: string;
    message: string;
    severity?: string;
    start: number;
    end: number;
    code?: string;
};

export interface ParseResult extends Omit<Partial<AnalysisResult>, "ast" | "issues" | "diagnostics"> {
    ast: AnalysisResult["ast"] | null;
    issues?: ParseIssueLike[];
    diagnostics?: AnalysisDiagnosticLike[];
}

/**
 * Parse SQL using the new @saralsql/tsql-parser.
 * Returns the full analysis result or null on failure.
 */
export function parseSql(sql: string): ParseResult | null {
    try {
        if (!sql) {
            return null;
        }

        const result = analyze(sql);
        return result as ParseResult;
    } catch (e: unknown) {
        const message = e instanceof Error ? e.message : "SQL parse error";
        return {
            ast: null,
            issues: [
                {
                    message,
                    severity: "error",
                    start: 0,
                    end: Math.min(sql.length, 1),
                    code: "PARSE_ERROR"
                }
            ],
            diagnostics: [
                {
                    source: "parser",
                    message,
                    severity: "error",
                    start: 0,
                    end: Math.min(sql.length, 1),
                    code: "PARSE_ERROR"
                }
            ]
        };
    }
}

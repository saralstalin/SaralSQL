/**
 * Quick test to understand the new AST structure from @saralsql/tsql-parser
 */

import { analyze } from '@saralsql/tsql-parser';

const testSql = `
SELECT 
  u.Id,
  u.Name
FROM dbo.Users u
WHERE u.Id = 1
`;

const result = analyze(testSql);

console.log('=== AST Structure ===');
console.log(JSON.stringify(result.ast, null, 2));

console.log('\n=== Scope ===');
console.log(JSON.stringify(result.scope, null, 2));

console.log('\n=== Diagnostics ===');
console.log(JSON.stringify(result.diagnostics, null, 2));

console.log('\n=== Columns ===');
console.log(JSON.stringify(result.columns, null, 2));

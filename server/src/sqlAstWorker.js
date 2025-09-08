const { parentPort } = require('worker_threads');

let Parser;
try {
  Parser = require('node-sql-parser').Parser;
} catch (e) {
  parentPort.postMessage({ error: 'node-sql-parser-not-installed' });
  process.exit(0);
}
const parser = new Parser();

parentPort.on('message', (msg) => {
  const { id, sql, opts } = msg;
  try {
    const ast = parser.astify(sql, opts || {});
    parentPort.postMessage({ id, ast });
  } catch (err) {
    parentPort.postMessage({ id, error: String(err) });
  }
});

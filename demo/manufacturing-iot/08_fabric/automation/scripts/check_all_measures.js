// モデルの全メジャーを 1 つずつ DAX で評価し、エラーになるメジャーを列挙する。1 件でもエラーがあれば終了コード 1
// Usage: node check_all_measures.js <workspaceId> <datasetId> <measure_tables.json>   （環境変数 PBI_TOKEN に AAD トークン）
"use strict";
const fs = require("fs");

const measureQuery = (name) => `EVALUATE ROW ( "v", [${name.replace(/]/g, "]]")}] )`;

// executeQueries の応答からエラーの要約を取り出す（エラーがなければ null）
function extractQueryError(json) {
  const err = (json && json.error) || (json && Array.isArray(json.results) && json.results[0] && json.results[0].error);
  if (!err) return null;
  const s = JSON.stringify(err);
  const detail = s.match(/"value":"([^"]+)"/);
  return detail ? detail[1] : s.slice(0, 200);
}

async function checkAllMeasures({ names, executeQuery }) {
  const results = [];
  for (const name of names) {
    const message = extractQueryError(await executeQuery(measureQuery(name)));
    results.push(message ? { name, message } : null);
  }
  return { checked: names.length, errors: results.filter(Boolean) };
}

const formatMeasureCheck = ({ checked, errors }) => [`checked ${checked}, errors ${errors.length}`, ...errors.map((e) => `  ${e.name}: ${e.message}`)];

// CLI 用: PBI_TOKEN を使う executeQuery（lib の HTTP クライアント: タイムアウト・リトライ付き）
async function defaultExecuteQueryFactory(workspaceId, datasetId, token) {
  const { createHttpClient } = require("../lib/http");
  const { createApiClient } = require("../lib/api");
  const { createPowerBiClient } = require("../lib/powerbi");
  const api = createApiClient({ http: createHttpClient(), tokens: { getToken: async () => token } });
  const pbi = createPowerBiClient({ api, logger: { info() {}, warn() {}, error() {} } });
  return (query) => pbi.executeQuery(workspaceId, datasetId, query);
}

async function main(argv, env, { io = console, makeExecuteQuery = defaultExecuteQueryFactory } = {}) {
  const [workspaceId, datasetId, map] = argv;
  if (!map) { io.error("usage: check_all_measures.js <workspaceId> <datasetId> <measure_tables.json>"); return 2; }
  if (!env.PBI_TOKEN) { io.error("PBI_TOKEN is not set"); return 2; }
  const names = Object.keys(JSON.parse(fs.readFileSync(map, "utf8")));
  const result = await checkAllMeasures({ names, executeQuery: await makeExecuteQuery(workspaceId, datasetId, env.PBI_TOKEN) });
  formatMeasureCheck(result).forEach((l) => io.log(l));
  return result.errors.length === 0 ? 0 : 1;
}

if (require.main === module) {
  main(process.argv.slice(2), process.env).then((code) => process.exit(code), (e) => { console.error(`check_all_measures failed: ${e.message}`); process.exit(1); });
}

module.exports = { checkAllMeasures, measureQuery, extractQueryError, formatMeasureCheck, main };

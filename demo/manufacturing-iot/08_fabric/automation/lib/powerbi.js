// Power BI REST API: セマンティックモデルの更新（要求 ID で自分の更新を特定して待つ）と DAX クエリの実行
"use strict";
const { PBI_API } = require("./endpoints");
const { DeployError, TimeoutError } = require("./errors");

const DONE_OK = "Completed";
const DONE_NG = Object.freeze(["Failed", "Cancelled", "Disabled"]);
const defaultSleep = (ms) => new Promise((r) => setTimeout(r, ms));
const enc = encodeURIComponent;

// 更新 API（202）の応答から要求 ID を取り出す。更新履歴の requestId と照合する
function extractRefreshRequestId(headers) {
  // Location の /refreshes/{id} が更新履歴の requestId そのもの。x-ms-request-id は要求全般の ID なので最後の手段
  const m = (headers.location || "").match(/\/refreshes\/([^/?#]+)/);
  if (m) return decodeURIComponent(m[1]);
  return headers.requestid || headers["x-ms-request-id"] || null;
}

function createPowerBiClient(options) {
  const o = { sleep: defaultSleep, now: Date.now, pollMs: 5_000, budgetMs: 15 * 60_000, ...options };
  const { api, logger } = o;
  const datasetUrl = (ws, ds) => `${PBI_API}/groups/${enc(ws)}/datasets/${enc(ds)}`;

  async function refreshDataset(workspaceId, datasetId) {
    const base = `${datasetUrl(workspaceId, datasetId)}/refreshes`;
    const res = await api.request("POST", base, { body: { type: "full" }, expectStatus: [202] });
    const requestId = extractRefreshRequestId(res.headers);
    if (!requestId) throw new DeployError("refresh accepted but no request id was returned (x-ms-request-id / RequestId / Location)", { code: "NO_REQUEST_ID" });
    const started = o.now();
    for (;;) {
      if (o.now() - started + o.pollMs > o.budgetMs) throw new TimeoutError(`refresh ${requestId} did not finish within ${o.budgetMs} ms`);
      await o.sleep(o.pollMs);
      const { body } = await api.request("GET", `${base}?$top=20`, { expectStatus: [200] });
      const mine = ((body && body.value) || []).find((r) => r.requestId === requestId);
      const status = mine ? mine.status : "NotListedYet";
      logger.info("model_refresh_status", { request_id: requestId, status, elapsed_ms: o.now() - started });
      if (status === DONE_OK) return { requestId, status };
      if (DONE_NG.includes(status)) throw new DeployError(`refresh ${status}: ${String(mine.serviceExceptionJson || "").slice(0, 500)}`, { code: "REFRESH_FAILED" });
    }
  }

  // DAX のエラーは 400 + error 本文で返るため、本文を呼び出し側に渡して判定させる（読み取り専用なので再送してよい）
  async function executeQuery(workspaceId, datasetId, query) {
    const res = await api.request("POST", `${datasetUrl(workspaceId, datasetId)}/executeQueries`, {
      body: { queries: [{ query }], serializerSettings: { includeNulls: true } },
      expectStatus: [200, 400],
      idempotent: true,
    });
    return res.body || { error: { message: res.text.slice(0, 300) } };
  }

  return Object.freeze({ refreshDataset, executeQuery });
}

module.exports = { createPowerBiClient, extractRefreshRequestId };

// Fabric REST API: 一覧（ページング）、表示名での作成または定義更新、長時間処理（202 + Location）の待機、ジョブ実行
"use strict";
const { FABRIC_API, OPERATION_HOSTS, assertTrustedUrl } = require("./endpoints");
const { parseRetryAfter } = require("./http");
const { DeployError, TimeoutError } = require("./errors");

// 項目の種類 → 作成・定義更新 API のコレクション名
const COLLECTIONS = Object.freeze({ Notebook: "notebooks", SemanticModel: "semanticModels", Report: "reports", DataPipeline: "dataPipelines", Lakehouse: "lakehouses" });
const JOB_TYPES = Object.freeze(["RunNotebook", "Pipeline"]);
const JOB_FAILED = Object.freeze(["Failed", "Cancelled", "Deduped"]);
const defaultSleep = (ms) => new Promise((r) => setTimeout(r, ms));

const toUrl = (pathOrUrl) => (/^https:\/\//.test(pathOrUrl) ? pathOrUrl : `${FABRIC_API}${pathOrUrl}`);
const enc = encodeURIComponent;

// 長時間処理の結果 URL: パスの末尾に /result を付ける（クエリ文字列はそのまま残す）
function resultUrl(url) {
  const u = new URL(url);
  u.pathname = `${u.pathname.replace(/\/+$/, "")}/result`;
  return u.toString();
}

function createFabricClient(options) {
  const o = { sleep: defaultSleep, now: Date.now, lroPollMs: 4_000, lroBudgetMs: 15 * 60_000, jobPollMs: 30_000, ...options };
  const { api, logger } = o;
  const call = (method, pathOrUrl, opts) => api.request(method, toUrl(pathOrUrl), opts);
  const retryAfter = (res) => parseRetryAfter(res.headers["retry-after"], o.now());

  async function listAll(path) {
    const seen = new Set();
    const collect = async (url, acc) => {
      const { body } = await call("GET", url, { expectStatus: [200] });
      const next = [...acc, ...((body && body.value) || [])];
      const token = body && body.continuationToken;
      if (!token) return next;
      if (seen.has(token)) throw new DeployError(`pagination loop detected on ${path}`, { code: "PAGINATION" });
      seen.add(token);
      const u = new URL(toUrl(path));
      u.searchParams.set("continuationToken", token);
      return collect(u.toString(), next);
    };
    return collect(toUrl(path), []);
  }

  const findByName = (items, name) => (items.find((i) => i.displayName === name) || {}).id || null;
  const findWorkspaceId = async (name) => findByName(await listAll("/workspaces"), name);
  const findItemId = async (workspaceId, type, name) => findByName(await listAll(`/workspaces/${enc(workspaceId)}/items?type=${enc(type)}`), name);

  async function waitOperation(location, { budgetMs = o.lroBudgetMs, retryAfterMs = null, withResult = false } = {}) {
    if (!location) throw new DeployError("202 Accepted without a Location header", { code: "NO_LOCATION" });
    const url = assertTrustedUrl(location, OPERATION_HOSTS).toString();
    const started = o.now();
    let delay = retryAfterMs ?? o.lroPollMs;
    for (;;) {
      if (o.now() - started + delay > budgetMs) throw new TimeoutError(`operation did not finish within ${budgetMs} ms: ${url}`);
      await o.sleep(delay);
      const res = await call("GET", url, { expectStatus: [200] });
      const status = res.body && res.body.status;
      if (status === "Succeeded") return withResult ? (await call("GET", resultUrl(url), { expectStatus: [200] })).body : null;
      if (status === "Failed" || status === "Undefined") {
        throw new DeployError(`operation ${status}: ${JSON.stringify(res.body.error || res.body).slice(0, 500)}`, { code: "OPERATION_FAILED" });
      }
      delay = retryAfter(res) ?? o.lroPollMs;
    }
  }

  async function createItem(path, body) {
    const res = await call("POST", path, { body, expectStatus: [201, 202] });
    const item = res.status === 201 ? res.body : await waitOperation(res.headers.location, { retryAfterMs: retryAfter(res), withResult: true });
    if (!item || !item.id) throw new DeployError(`create ${path} returned no id`, { code: "NO_ID" });
    return item;
  }

  async function updateDefinition(workspaceId, collection, id, definition) {
    const res = await call("POST", `/workspaces/${enc(workspaceId)}/${collection}/${enc(id)}/updateDefinition`, { body: { definition }, expectStatus: [200, 202], idempotent: true });
    if (res.status === 202) await waitOperation(res.headers.location, { retryAfterMs: retryAfter(res) });
  }

  async function createOrUpdateItem({ workspaceId, type, body }) {
    const collection = COLLECTIONS[type];
    if (!collection) throw new DeployError(`unsupported item type: ${type}`, { code: "ITEM_TYPE" });
    const existing = await findItemId(workspaceId, type, body.displayName);
    if (existing) {
      await updateDefinition(workspaceId, collection, existing, body.definition);
      return { id: existing, created: false };
    }
    return { id: (await createItem(`/workspaces/${enc(workspaceId)}/${collection}`, body)).id, created: true };
  }

  async function ensureWorkspace({ name, capacityId, description }) {
    const existing = await findWorkspaceId(name);
    if (existing) return { id: existing, created: false };
    return { id: (await createItem("/workspaces", { displayName: name, description, capacityId })).id, created: true };
  }

  async function ensureLakehouse({ workspaceId, name }) {
    const existing = await findItemId(workspaceId, "Lakehouse", name);
    if (existing) return { id: existing, created: false };
    const item = await createItem(`/workspaces/${enc(workspaceId)}/lakehouses`, { displayName: name, creationPayload: { enableSchemas: true } });
    return { id: item.id, created: true };
  }

  const getLakehouse = async (workspaceId, id) => (await call("GET", `/workspaces/${enc(workspaceId)}/lakehouses/${enc(id)}`, { expectStatus: [200] })).body;

  // Notebook 直後は SQL 分析エンドポイントのテーブル同期が遅れるため明示的に同期する。プレビュー API のため失敗しても警告で続行する
  async function refreshSqlEndpointMetadata(workspaceId, sqlEndpointId) {
    try {
      const res = await call("POST", `/workspaces/${enc(workspaceId)}/sqlEndpoints/${enc(sqlEndpointId)}/refreshMetadata`, { body: {}, expectStatus: [200, 202], idempotent: true });
      if (res.status === 202) await waitOperation(res.headers.location, { retryAfterMs: retryAfter(res) });
      return true;
    } catch (err) {
      logger.warn("sql_endpoint_refresh_failed", { error_type: err.name, message: err.message, status: err.status });
      return false;
    }
  }

  async function runJob({ workspaceId, itemId, jobType, body, timeoutMs = 3_600_000 }) {
    if (!JOB_TYPES.includes(jobType)) throw new DeployError(`unsupported job type: ${jobType}`, { code: "JOB_TYPE" });
    const res = await call("POST", `/workspaces/${enc(workspaceId)}/items/${enc(itemId)}/jobs/instances?jobType=${jobType}`, { body: body || {}, expectStatus: [202] });
    if (!res.headers.location) throw new DeployError(`job ${jobType} started without a Location header`, { code: "NO_LOCATION" });
    const url = assertTrustedUrl(res.headers.location, OPERATION_HOSTS).toString();
    return pollJob(url, jobType, timeoutMs);
  }

  async function pollJob(url, jobType, timeoutMs) {
    const started = o.now();
    for (;;) {
      const job = (await call("GET", url, { expectStatus: [200] })).body || {};
      const elapsed = o.now() - started;
      logger.info("job_status", { job_type: jobType, status: job.status, elapsed_ms: elapsed });
      if (job.status === "Completed") return job;
      if (JOB_FAILED.includes(job.status)) {
        throw new DeployError(`job ${jobType} ${job.status}: ${JSON.stringify(job.failureReason || {}).slice(0, 500)}`, { code: "JOB_FAILED", details: { job_id: job.id } });
      }
      if (elapsed + o.jobPollMs > timeoutMs) throw new TimeoutError(`job ${jobType} did not finish within ${timeoutMs} ms`);
      await o.sleep(o.jobPollMs);
    }
  }

  return Object.freeze({ listAll, findWorkspaceId, findItemId, waitOperation, createItem, updateDefinition, createOrUpdateItem, ensureWorkspace, ensureLakehouse, getLakehouse, refreshSqlEndpointMetadata, runJob });
}

module.exports = { createFabricClient, COLLECTIONS, JOB_TYPES };

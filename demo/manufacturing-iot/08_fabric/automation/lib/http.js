// fetch のラッパー: リクエストごとのタイムアウト、指数バックオフでのリトライ（ネットワークエラー / 5xx / 429 の Retry-After）、
// 想定外のステータスは HttpError で失敗させる。ヘッダー（トークン）とクエリ文字列はログに出さない。
"use strict";
const { HttpError, TimeoutError, NetworkError } = require("./errors");

// 再送しても結果が変わらないメソッド。POST / PATCH は idempotent: true を指定した場合だけ 5xx・通信エラーで再送する（429 は常に再送）
const IDEMPOTENT_METHODS = new Set(["GET", "HEAD", "PUT", "DELETE", "OPTIONS"]);
const defaultSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function parseRetryAfter(value, nowMs) {
  if (value == null || value === "") return null;
  if (/^\d+(\.\d+)?$/.test(String(value).trim())) return Math.round(Number(value) * 1000);
  const at = Date.parse(value);
  return Number.isNaN(at) ? null : Math.max(0, at - nowMs);
}

function backoffDelay(attempt, { baseDelayMs, maxDelayMs, random }) {
  const ceiling = Math.min(maxDelayMs, baseDelayMs * 2 ** attempt);
  return Math.round(ceiling * (0.5 + random() / 2));
}

// ログ用: ホストとパスだけ（クエリ文字列は出さない）
const safeUrl = (url) => {
  try { const u = new URL(url); return `${u.origin}${u.pathname}`; } catch { return "<invalid url>"; }
};

function encodeBody(body, headers) {
  if (body === undefined || body === null) return { body: undefined, headers };
  if (typeof body === "string" || body instanceof Uint8Array) return { body, headers };
  const hasType = Object.keys(headers).some((k) => k.toLowerCase() === "content-type");
  return { body: JSON.stringify(body), headers: hasType ? headers : { ...headers, "Content-Type": "application/json; charset=utf-8" } };
}

function parseBody(text, contentType) {
  if (!text) return null;
  const looksJson = /json/i.test(contentType || "") || /^\s*[[{]/.test(text);
  if (!looksJson) return null;
  try { return JSON.parse(text); } catch { return null; }
}

const isExpected = (status, expectStatus) => (expectStatus ? expectStatus.includes(status) : status >= 200 && status < 300);

function createHttpClient(options = {}) {
  const o = {
    fetchImpl: globalThis.fetch, sleep: defaultSleep, now: Date.now, random: Math.random,
    timeoutMs: 60_000, maxRetries: 4, baseDelayMs: 1_000, maxDelayMs: 30_000, maxRetryAfterMs: 120_000, logger: null,
    ...options,
  };

  async function attemptOnce({ method, url, headers, body, timeoutMs }) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new TimeoutError(`${method} ${safeUrl(url)} timed out after ${timeoutMs} ms`)), timeoutMs);
    try {
      // リダイレクトは追わない（Authorization ヘッダーを別のホストに送らないため）。3xx は想定外のステータスとして失敗する
      const res = await o.fetchImpl(url, { method, headers, body, signal: controller.signal, redirect: "manual" });
      const text = await res.text();
      const resHeaders = Object.fromEntries([...res.headers].map(([k, v]) => [k.toLowerCase(), v]));
      return { status: res.status, headers: resHeaders, text, body: parseBody(text, resHeaders["content-type"]) };
    } catch (err) {
      if (controller.signal.aborted) throw controller.signal.reason instanceof TimeoutError ? controller.signal.reason : new TimeoutError(`${method} ${safeUrl(url)} aborted`, { cause: err });
      throw new NetworkError(`${method} ${safeUrl(url)} failed: ${err.message}${err.cause ? ` (${err.cause.code || err.cause.message})` : ""}`, { cause: err });
    } finally {
      clearTimeout(timer);
    }
  }

  function retryDelay(result, attempt, idempotent) {
    if (attempt >= o.maxRetries) return null;
    if (result.error) return idempotent ? backoffDelay(attempt, o) : null;
    if (result.res.status === 429) {
      const ra = parseRetryAfter(result.res.headers["retry-after"], o.now());
      return Math.min(ra == null ? backoffDelay(attempt, o) : ra, o.maxRetryAfterMs);
    }
    if (result.res.status >= 500 && idempotent) return backoffDelay(attempt, o);
    return null;
  }

  async function request({ method = "GET", url, headers = {}, body, timeoutMs = o.timeoutMs, expectStatus, idempotent } = {}) {
    const m = method.toUpperCase();
    const canRepeat = idempotent ?? IDEMPOTENT_METHODS.has(m);
    const encoded = encodeBody(body, headers);
    for (let attempt = 0; ; attempt += 1) {
      const result = await attemptOnce({ method: m, url, headers: encoded.headers, body: encoded.body, timeoutMs }).then((res) => ({ res }), (error) => ({ error }));
      if (result.res && isExpected(result.res.status, expectStatus)) return result.res;
      const delay = retryDelay(result, attempt, canRepeat);
      if (delay == null) {
        if (result.error) throw result.error;
        throw new HttpError({ method: m, url: safeUrl(url), status: result.res.status, text: result.res.text });
      }
      if (o.logger) o.logger.warn("http_retry", { method: m, url: safeUrl(url), status: result.res ? result.res.status : null, error_type: result.error ? result.error.name : null, attempt: attempt + 1, delay_ms: delay });
      await o.sleep(delay);
    }
  }

  return Object.freeze({ request });
}

module.exports = { createHttpClient, parseRetryAfter, backoffDelay, safeUrl, IDEMPOTENT_METHODS };

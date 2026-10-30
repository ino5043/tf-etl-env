// テスト共通: 偽の時計、偽の fetch（実際のエンドポイントには一切接続しない）、ログの収集
"use strict";

function fakeClock(start = 1_800_000_000_000) {
  let t = start;
  return {
    now: () => t,
    sleep: async (ms) => { t += ms; },
    sleeps: [],
  };
}

// sleep の呼び出しを記録する時計（待ち時間の検証用）
function recordingClock(start = 1_800_000_000_000) {
  let t = start;
  const sleeps = [];
  return { now: () => t, sleep: async (ms) => { sleeps.push(ms); t += ms; }, sleeps };
}

function jsonResponse(status, body, headers = {}) {
  const text = body === undefined ? null : JSON.stringify(body);
  return new Response(status === 204 ? null : text, { status, headers: { "content-type": "application/json", ...headers } });
}

function textResponse(status, text, headers = {}) {
  return new Response(text, { status, headers });
}

// routes: [[method, urlRegExp, responder]]。responder は (req) => Response、または Response を返す関数の配列（呼ばれるたびに順に使う。最後の要素は繰り返す）
function mockFetch(routes) {
  const calls = [];
  const counters = new Map();
  const fetchImpl = async (url, init = {}) => {
    const method = (init.method || "GET").toUpperCase();
    const headers = Object.fromEntries(Object.entries(init.headers || {}).map(([k, v]) => [k.toLowerCase(), v]));
    let body = init.body;
    if (body instanceof Uint8Array) body = Buffer.from(body).toString("utf8");
    const req = { method, url: String(url), headers, body, json: () => JSON.parse(body), signal: init.signal, redirect: init.redirect };
    calls.push(req);
    const idx = routes.findIndex(([m, re]) => m === method && re.test(req.url));
    if (idx < 0) throw new Error(`mockFetch: no route for ${method} ${req.url}`);
    const responder = routes[idx][2];
    if (Array.isArray(responder)) {
      const n = counters.get(idx) || 0;
      counters.set(idx, n + 1);
      return responder[Math.min(n, responder.length - 1)](req);
    }
    return responder(req);
  };
  return { fetchImpl, calls };
}

function collectingLogger() {
  const lines = [];
  const mk = (level) => (event, fields = {}) => lines.push({ level, event, ...fields });
  const logger = { info: mk("info"), warn: mk("warn"), error: mk("error"), child: () => logger, lines };
  return logger;
}

const fakeTokens = (calls = []) => ({
  getToken: async (aud) => { calls.push(aud); return `tok-for-${aud}`; },
});

module.exports = { fakeClock, recordingClock, jsonResponse, textResponse, mockFetch, collectingLogger, fakeTokens };

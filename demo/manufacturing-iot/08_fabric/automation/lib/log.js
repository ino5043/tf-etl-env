// 構造化ログ（JSON Lines を標準エラーへ）。トークン類は入れ子のオブジェクト・配列の中も含めて出力しない
"use strict";

const SECRET_KEY = /token|authorization|secret|password/i;
// 値そのものがトークンに見えるもの（Bearer ヘッダー、JWT）
const SECRET_VALUE = /\bBearer\s+\S+|\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*/;
const REDACTED = "[REDACTED]";
const MAX_DEPTH = 8;

function redact(value, depth = 0) {
  if (typeof value === "string") return SECRET_VALUE.test(value) ? REDACTED : value;
  if (value === null || typeof value !== "object") return value;
  if (depth >= MAX_DEPTH) return "[TRUNCATED]";
  if (Array.isArray(value)) return value.map((v) => redact(v, depth + 1));
  return Object.fromEntries(Object.entries(value).filter(([k]) => !SECRET_KEY.test(k)).map(([k, v]) => [k, redact(v, depth + 1)]));
}

function createLogger({ write = (s) => process.stderr.write(s), now = () => new Date(), base = {} } = {}) {
  const emit = (level) => (event, fields) => write(`${JSON.stringify({ ts: now().toISOString(), level, event, ...base, ...redact(fields || {}) })}\n`);
  return Object.freeze({
    info: emit("info"),
    warn: emit("warn"),
    error: emit("error"),
    child: (extra) => createLogger({ write, now, base: { ...base, ...redact(extra || {}) } }),
  });
}

module.exports = { createLogger, redact };

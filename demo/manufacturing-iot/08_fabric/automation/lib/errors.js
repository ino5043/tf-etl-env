// 型付きエラー。ログには code / status / bodyExcerpt を構造化して出す（トークンは含めない）
"use strict";

const EXCERPT_LIMIT = 500;
const excerpt = (text, limit = EXCERPT_LIMIT) => {
  const s = text == null ? "" : String(text);
  return s.length > limit ? `${s.slice(0, limit)}…(${s.length} chars)` : s;
};

class DeployError extends Error {
  constructor(message, { code = "DEPLOY_ERROR", cause, details } = {}) {
    super(message, cause ? { cause } : undefined);
    this.name = this.constructor.name;
    this.code = code;
    if (details) this.details = details;
  }
}

class HttpError extends DeployError {
  constructor({ method, url, status, text }) {
    const bodyExcerpt = excerpt(text);
    super(`${method} ${url} -> HTTP ${status}: ${excerpt(text, 200)}`, { code: "HTTP_STATUS" });
    this.method = method;
    this.url = url;
    this.status = status;
    this.bodyExcerpt = bodyExcerpt;
  }
}

class TimeoutError extends DeployError {
  constructor(message, opts = {}) { super(message, { code: "TIMEOUT", ...opts }); }
}

class NetworkError extends DeployError {
  constructor(message, opts = {}) { super(message, { code: "NETWORK", ...opts }); }
}

class AuthError extends DeployError {
  constructor(message, opts = {}) { super(message, { code: "AUTH", ...opts }); }
}

class ConfigError extends DeployError {
  constructor(message, opts = {}) { super(message, { code: "CONFIG", ...opts }); }
}

module.exports = { DeployError, HttpError, TimeoutError, NetworkError, AuthError, ConfigError, excerpt };

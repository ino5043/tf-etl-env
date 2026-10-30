// 認証付きの API 呼び出し。URL のホストから対象（audience）を決めてトークンを付ける。既知のホスト以外には送らない
"use strict";
const { audienceFor } = require("./endpoints");
const { AuthError } = require("./errors");

function createApiClient({ http, tokens }) {
  async function request(method, url, opts = {}) {
    const audience = audienceFor(url); // 既知のホストでなければここで失敗する（トークンは取得もしない）
    const token = await tokens.getToken(audience);
    if (!token) throw new AuthError(`empty access token for ${audience}`);
    return http.request({ ...opts, method, url, headers: { ...(opts.headers || {}), Authorization: `Bearer ${token}` } });
  }
  return Object.freeze({ request });
}

module.exports = { createApiClient };

// 接続先の定数と、トークンを付けて送ってよい URL の検証（https かつ既知のホストのみ）
"use strict";
const { DeployError } = require("./errors");

const FABRIC_API = "https://api.fabric.microsoft.com/v1";
const PBI_API = "https://api.powerbi.com/v1.0/myorg";
const ONELAKE = "https://onelake.dfs.fabric.microsoft.com";

const AUDIENCES = Object.freeze({
  fabric: "https://api.fabric.microsoft.com",
  storage: "https://storage.azure.com",
  powerbi: "https://analysis.windows.net/powerbi/api",
});

const AUDIENCE_BY_HOST = Object.freeze({
  "api.fabric.microsoft.com": AUDIENCES.fabric,
  "onelake.dfs.fabric.microsoft.com": AUDIENCES.storage,
  "api.powerbi.com": AUDIENCES.powerbi,
});

// Fabric の長時間処理は、地域のフロントエンド（例: wabi-japan-west-a-primary-redirect.analysis.windows.net）を
// Location に返す。このホストには Fabric 用のトークンを付ける（2026-10-30 に実環境で確認）
const FABRIC_REGIONAL_HOST = /^wabi-[a-z0-9]+(?:-[a-z0-9]+)*-redirect\.analysis\.windows\.net$/;

// 長時間処理（202 の Location）やジョブの状態 URL として追ってよいホスト（文字列は完全一致、正規表現はパターン）
const OPERATION_HOSTS = Object.freeze(["api.fabric.microsoft.com", "api.powerbi.com", FABRIC_REGIONAL_HOST]);

const DEFAULT_HOSTS = Object.freeze([...Object.keys(AUDIENCE_BY_HOST), FABRIC_REGIONAL_HOST]);

const hostAllowed = (hostname, allowed) => allowed.some((h) => (h instanceof RegExp ? h.test(hostname) : h === hostname));

function assertTrustedUrl(url, allowedHosts = DEFAULT_HOSTS) {
  let u;
  try { u = new URL(url); } catch {
    throw new DeployError(`invalid URL: ${String(url).slice(0, 100)}`, { code: "UNTRUSTED_URL" });
  }
  if (u.protocol !== "https:" || u.username || u.password || !hostAllowed(u.hostname, allowedHosts)) {
    throw new DeployError(`URL not allowed (https on ${allowedHosts.map(String).join(", ")} only): ${u.protocol}//${u.host}`, { code: "UNTRUSTED_URL" });
  }
  return u;
}

function audienceFor(url) {
  const { hostname } = assertTrustedUrl(url);
  return AUDIENCE_BY_HOST[hostname] || AUDIENCES.fabric; // 既知の固定ホスト以外で許可されるのは Fabric の地域ホストだけ
}

module.exports = { FABRIC_API, PBI_API, ONELAKE, AUDIENCES, AUDIENCE_BY_HOST, OPERATION_HOSTS, assertTrustedUrl, audienceFor };

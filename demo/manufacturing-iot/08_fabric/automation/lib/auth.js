// Azure CLI（az account get-access-token）でアクセストークンを取得し、audience ごとに有効期限までキャッシュする。
// トークンは標準出力で受け取り、コマンドライン引数には決して載せない。az が失敗したら AuthError で止める。
// az は PATH から絶対パスで解決する（カレントフォルダーに置かれた az.cmd 等を実行しないため）。
"use strict";
const fs = require("node:fs");
const path = require("node:path");
const { execFile } = require("node:child_process");
const { promisify } = require("node:util");
const { AUDIENCES } = require("./endpoints");
const { AuthError, excerpt } = require("./errors");

const ALLOWED = new Set(Object.values(AUDIENCES));
const FALLBACK_TTL_MS = 30 * 60_000;
const WIN_EXTS = Object.freeze([".cmd", ".bat", ".exe"]);

function resolveAzPath({ env = process.env, platform = process.platform, cwd = process.cwd(), exists = fs.existsSync } = {}) {
  const p = platform === "win32" ? path.win32 : path.posix;
  const norm = (d) => (platform === "win32" ? p.resolve(d).toLowerCase() : p.resolve(d));
  const here = norm(cwd);
  const pathVar = env.PATH ?? env.Path ?? "";
  const dirs = pathVar.split(platform === "win32" ? ";" : ":").map((d) => d.trim().replace(/^"(.*)"$/, "$1"))
    .filter((d) => d !== "" && d !== "." && p.isAbsolute(d) && norm(d) !== here);
  const names = platform === "win32" ? WIN_EXTS.map((e) => `az${e}`) : ["az"];
  for (const dir of dirs) {
    for (const n of names) {
      const full = p.join(dir, n);
      if (!exists(full)) continue;
      // cmd.exe の引用で安全に扱えない文字（" と %）を含むパスは使わない
      if (platform === "win32" && /["%]/.test(full)) throw new AuthError(`az path contains characters that cannot be quoted safely: ${full}`);
      return full;
    }
  }
  throw new AuthError("Azure CLI (az) が PATH に見つからない（カレントフォルダーは探さない）。インストールして az login する");
}

function azCommand(audience, { platform, env, azPath }) {
  if (!ALLOWED.has(audience)) throw new AuthError(`unknown token audience: ${audience}`);
  const args = ["account", "get-access-token", "--resource", audience, "-o", "json"];
  if (platform !== "win32") return { file: azPath, args, options: {} };
  // Windows の az は az.cmd（バッチ）なので cmd.exe 経由で起動する。/s /c "…" は外側の " を 1 組外す。
  // 引数は固定値と許可済みの audience、解決済みの絶対パスだけなので、そのままの文字列で渡す
  return { file: env.ComSpec || "cmd.exe", args: ["/d", "/s", "/c", `""${azPath}" ${args.join(" ")}"`], options: { windowsVerbatimArguments: true } };
}

function parseAzToken(stdout, nowMs) {
  let j;
  try { j = JSON.parse(stdout); } catch {
    throw new AuthError("az account get-access-token returned non-JSON output");
  }
  if (!j || typeof j.accessToken !== "string" || j.accessToken === "") throw new AuthError("az account get-access-token returned no accessToken");
  const epoch = j.expires_on != null && j.expires_on !== "" && Number.isFinite(Number(j.expires_on)) ? Number(j.expires_on) * 1000 : null;
  const local = typeof j.expiresOn === "string" ? Date.parse(j.expiresOn.replace(" ", "T")) : NaN;
  const expiresAtMs = epoch ?? (Number.isNaN(local) ? nowMs + FALLBACK_TTL_MS : local);
  return { token: j.accessToken, expiresAtMs };
}

function createTokenProvider(options = {}) {
  const o = { execFileImpl: promisify(execFile), platform: process.platform, env: process.env, cwd: process.cwd(), exists: fs.existsSync, now: Date.now, refreshMarginMs: 5 * 60_000, caBundle: null, azPath: null, ...options };
  const cache = new Map(); // audience -> Promise<{ token, expiresAtMs }>
  let azPath = o.azPath;

  async function fetchToken(audience) {
    azPath = azPath || resolveAzPath(o);
    const { file, args, options: extra } = azCommand(audience, { platform: o.platform, env: o.env, azPath });
    // NoDefaultCurrentDirectoryInExePath: az.cmd の中からさらに起動されるコマンドも、カレントフォルダーから探さない
    const env = { ...o.env, NoDefaultCurrentDirectoryInExePath: "1", ...(o.caBundle ? { REQUESTS_CA_BUNDLE: o.caBundle } : {}) };
    try {
      const { stdout } = await o.execFileImpl(file, args, { env, timeout: 120_000, maxBuffer: 1024 * 1024, windowsHide: true, ...extra });
      return parseAzToken(stdout, o.now());
    } catch (err) {
      if (err instanceof AuthError) throw err;
      const hint = err.code === "ENOENT" ? "Azure CLI (az) が見つからない。インストールして az login する" : excerpt(err.stderr || err.message, 400);
      throw new AuthError(`token for ${audience} failed: ${hint}`, { cause: err });
    }
  }

  async function getToken(audience) {
    const cached = cache.get(audience);
    if (cached) {
      const v = await cached.catch(() => null);
      if (v && v.expiresAtMs - o.refreshMarginMs > o.now()) return v.token;
    }
    const pending = fetchToken(audience);
    cache.set(audience, pending);
    try {
      return (await pending).token;
    } catch (err) {
      cache.delete(audience);
      throw err;
    }
  }

  return Object.freeze({ getToken });
}

module.exports = { createTokenProvider, azCommand, parseAzToken, resolveAzPath };

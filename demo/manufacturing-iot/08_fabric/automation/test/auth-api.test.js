"use strict";
const { test, describe } = require("node:test");
const assert = require("node:assert/strict");
const { createTokenProvider, azCommand, parseAzToken } = require("../lib/auth");
const { createApiClient } = require("../lib/api");
const { assertTrustedUrl, audienceFor, AUDIENCES, OPERATION_HOSTS } = require("../lib/endpoints");
const { createHttpClient } = require("../lib/http");
const { AuthError, DeployError } = require("../lib/errors");
const { mockFetch, jsonResponse, recordingClock, fakeTokens } = require("./helpers");

describe("endpoints", () => {
  test("audience per host", () => {
    assert.equal(audienceFor("https://api.fabric.microsoft.com/v1/x"), AUDIENCES.fabric);
    assert.equal(audienceFor("https://onelake.dfs.fabric.microsoft.com/w/l/Files/a"), AUDIENCES.storage);
    assert.equal(audienceFor("https://api.powerbi.com/v1.0/myorg/groups"), AUDIENCES.powerbi);
  });
  test("rejects non-https, unknown hosts, look-alike hosts and invalid URLs", () => {
    for (const bad of ["http://api.fabric.microsoft.com/v1", "https://evil.example.com/v1", "https://api.fabric.microsoft.com.evil.com/x", "https://user@evil.com/", "not a url", ""]) {
      assert.throws(() => assertTrustedUrl(bad), (e) => e instanceof DeployError && e.code === "UNTRUSTED_URL", bad);
    }
  });
  // Fabric の長時間処理は地域のフロントエンド（例: wabi-japan-west-a-primary-redirect.analysis.windows.net）を Location に返す
  const REGIONAL = "https://wabi-japan-west-a-primary-redirect.analysis.windows.net/v1/operations/44025670";
  test("accepts the regional Fabric front-end host for operations, with the Fabric audience", () => {
    assert.equal(assertTrustedUrl(REGIONAL, OPERATION_HOSTS).hostname, "wabi-japan-west-a-primary-redirect.analysis.windows.net");
    assert.equal(audienceFor(REGIONAL), AUDIENCES.fabric);
  });
  test("rejects look-alikes of the regional host", () => {
    for (const bad of [
      "http://wabi-japan-west-a-primary-redirect.analysis.windows.net/v1/operations/1",
      "https://wabi-japan-west-a-primary-redirect.analysis.windows.net.evil.com/x",
      "https://evil-redirect.analysis.windows.net/x",
      "https://wabi-x-redirect.evil.windows.net/x",
      "https://wabi--redirect.analysis.windows.net/x",
      "https://analysis.windows.net/x",
    ]) {
      assert.throws(() => assertTrustedUrl(bad, OPERATION_HOSTS), (e) => e instanceof DeployError && e.code === "UNTRUSTED_URL", bad);
    }
  });
  test("respects a narrower allow list", () => {
    assert.throws(() => assertTrustedUrl("https://onelake.dfs.fabric.microsoft.com/x", ["api.fabric.microsoft.com"]), /not allowed/);
  });
});

describe("parseAzToken / azCommand", () => {
  test("uses expires_on epoch seconds", () => {
    assert.deepEqual(parseAzToken(JSON.stringify({ accessToken: "t", expires_on: 2000 }), 0), { token: "t", expiresAtMs: 2_000_000 });
  });
  test("falls back to expiresOn, then to 30 minutes", () => {
    const r = parseAzToken(JSON.stringify({ accessToken: "t", expiresOn: "2026-10-30 12:00:00.000000" }), 0);
    assert.equal(r.expiresAtMs, new Date(2026, 9, 30, 12, 0, 0).getTime());
    assert.equal(parseAzToken(JSON.stringify({ accessToken: "t" }), 1000).expiresAtMs, 1000 + 30 * 60_000);
  });
  test("rejects empty / malformed output without echoing it", () => {
    assert.throws(() => parseAzToken("{}", 0), AuthError);
    assert.throws(() => parseAzToken("not json SECRET", 0), (e) => e instanceof AuthError && !/SECRET/.test(e.message));
  });
  test("az is called via cmd.exe on Windows and directly elsewhere, token never on argv", () => {
    const w = azCommand(AUDIENCES.fabric, { platform: "win32", env: { ComSpec: "C:\\Windows\\cmd.exe" }, azPath: "C:\\Program Files\\Azure CLI\\wbin\\az.cmd" });
    assert.equal(w.file, "C:\\Windows\\cmd.exe");
    assert.deepEqual(w.args, ["/d", "/s", "/c", '""C:\\Program Files\\Azure CLI\\wbin\\az.cmd" account get-access-token --resource https://api.fabric.microsoft.com -o json"']);
    assert.equal(w.options.windowsVerbatimArguments, true);
    assert.deepEqual(azCommand(AUDIENCES.storage, { platform: "linux", env: {}, azPath: "/usr/bin/az" }), { file: "/usr/bin/az", args: ["account", "get-access-token", "--resource", AUDIENCES.storage, "-o", "json"], options: {} });
  });
  test("unknown audience is refused (no shell injection)", () => {
    assert.throws(() => azCommand("https://x & calc", { platform: "win32", env: {}, azPath: "C:\\az.cmd" }), AuthError);
  });
});

describe("createTokenProvider", () => {
  const okExec = (calls, expiresOn = 10_000) => async (file, args, opts) => {
    calls.push({ file, args, opts });
    return { stdout: JSON.stringify({ accessToken: `tok${calls.length}`, expires_on: expiresOn }), stderr: "" };
  };

  test("caches per audience until shortly before expiry", async () => {
    const calls = [];
    let now = 0;
    const p = createTokenProvider({ execFileImpl: okExec(calls, 3600), platform: "linux", azPath: "/usr/bin/az", env: {}, now: () => now });
    assert.equal(await p.getToken(AUDIENCES.fabric), "tok1");
    assert.equal(await p.getToken(AUDIENCES.fabric), "tok1");
    assert.equal(await p.getToken(AUDIENCES.storage), "tok2");
    now = 3600_000 - 60_000; // 残り 1 分 → 更新
    assert.equal(await p.getToken(AUDIENCES.fabric), "tok3");
    assert.equal(calls.length, 3);
  });

  test("passes REQUESTS_CA_BUNDLE to az through the environment", async () => {
    const calls = [];
    const p = createTokenProvider({ execFileImpl: okExec(calls), platform: "linux", azPath: "/usr/bin/az", env: { PATH: "/bin" }, caBundle: "/ca.pem", now: () => 0 });
    await p.getToken(AUDIENCES.powerbi);
    assert.equal(calls[0].opts.env.REQUESTS_CA_BUNDLE, "/ca.pem");
    assert.equal(calls[0].opts.env.PATH, "/bin");
  });

  test("fails loudly when az fails", async () => {
    const exec = async () => { const e = new Error("Command failed"); e.code = 1; e.stderr = "ERROR: Please run 'az login'"; throw e; };
    const p = createTokenProvider({ execFileImpl: exec, platform: "linux", azPath: "/usr/bin/az", env: {}, now: () => 0 });
    await assert.rejects(p.getToken(AUDIENCES.fabric), (e) => e instanceof AuthError && /az login/.test(e.message));
  });

  test("az missing gives a helpful message", async () => {
    const exec = async () => { const e = new Error("spawn az ENOENT"); e.code = "ENOENT"; throw e; };
    const p = createTokenProvider({ execFileImpl: exec, platform: "linux", azPath: "/usr/bin/az", env: {}, now: () => 0 });
    await assert.rejects(p.getToken(AUDIENCES.fabric), /Azure CLI/);
  });
});

describe("createApiClient", () => {
  test("attaches the bearer token for the URL's host audience", async () => {
    const { fetchImpl, calls } = mockFetch([["GET", /onelake/, () => jsonResponse(200, {})], ["GET", /powerbi/, () => jsonResponse(200, {})]]);
    const clock = recordingClock();
    const audiences = [];
    const api = createApiClient({ http: createHttpClient({ fetchImpl, sleep: clock.sleep, now: clock.now }), tokens: fakeTokens(audiences) });
    await api.request("GET", "https://onelake.dfs.fabric.microsoft.com/w/l/Files/x");
    await api.request("GET", "https://api.powerbi.com/v1.0/myorg/groups");
    assert.deepEqual(audiences, [AUDIENCES.storage, AUDIENCES.powerbi]);
    assert.equal(calls[0].headers.authorization, `Bearer tok-for-${AUDIENCES.storage}`);
  });

  test("never sends a token to an untrusted host", async () => {
    const { fetchImpl, calls } = mockFetch([["GET", /./, () => jsonResponse(200, {})]]);
    const audiences = [];
    const api = createApiClient({ http: createHttpClient({ fetchImpl }), tokens: fakeTokens(audiences) });
    await assert.rejects(api.request("GET", "https://attacker.example/v1/operations/1"), /not allowed/);
    assert.equal(calls.length, 0);
    assert.equal(audiences.length, 0);
  });

  test("refuses an empty token", async () => {
    const api = createApiClient({ http: createHttpClient({ fetchImpl: async () => { throw new Error("no"); } }), tokens: { getToken: async () => "" } });
    await assert.rejects(api.request("GET", "https://api.fabric.microsoft.com/v1/workspaces"), AuthError);
  });
});

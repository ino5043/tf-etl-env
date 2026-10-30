"use strict";
// 再レビュー（フェーズ 1）の指摘に対する回帰テスト
const { test, describe } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { EventEmitter } = require("node:events");
const { parseConfigText } = require("../lib/config");
const { resolveAzPath, createTokenProvider, parseAzToken } = require("../lib/auth");
const { AUDIENCES } = require("../lib/endpoints");
const { createHttpClient } = require("../lib/http");
const { createApiClient } = require("../lib/api");
const { createFabricClient } = require("../lib/fabric");
const { createLogger } = require("../lib/log");
const { createStateStore, checkWorkspaceState } = require("../lib/state");
const { STEPS } = require("../lib/steps");
const { validateConfig } = require("../lib/config");
const { defaultRunShots, main } = require("../deploy");
const { AuthError } = require("../lib/errors");
const { mockFetch, jsonResponse, textResponse, recordingClock, collectingLogger, fakeTokens } = require("./helpers");

const GUID = "0f8fad5b-d9cb-469f-a165-70867728950e";
const F = "https://api.fabric.microsoft.com/v1";

describe("M1: backslashes in double-quoted config values follow bash semantics", () => {
  test("Windows paths keep their backslashes", () => {
    const c = parseConfigText('WORK_DIR="C:\\work\\x"\nexport REQUESTS_CA_BUNDLE="C:\\certs\\ca bundle.pem"\n');
    assert.equal(c.WORK_DIR, "C:\\work\\x");
    assert.equal(c.REQUESTS_CA_BUNDLE, "C:\\certs\\ca bundle.pem");
  });
  test("only \\$ \\` \\\" \\\\ are escapes", () => {
    assert.equal(parseConfigText('A="a\\"b\\\\c\\nd\\$e"').A, 'a"b\\c\\nd$e');
    assert.equal(parseConfigText('A="\\\\server\\share"').A, "\\server\\share");
  });
  test("single-quoted values are literal; unescaped $ / ` still rejected", () => {
    assert.equal(parseConfigText("A='C:\\work\\x'").A, "C:\\work\\x");
    assert.throws(() => parseConfigText('A="$HOME"'), /展開/);
    assert.throws(() => parseConfigText('A="\\\\$HOME"'), /展開/); // \\ の後の $ はエスケープされていない
  });
});

describe("M3: az is resolved from PATH, never from the current directory", () => {
  const win = (files) => ({ platform: "win32", cwd: "C:\\proj", exists: (p) => files.includes(p) });

  test("skips the current directory, '.' and empty PATH entries on Windows", () => {
    const env = { PATH: ";.;C:\\proj;C:\\Program Files\\Azure CLI\\wbin" };
    const p = resolveAzPath({ env, ...win(["C:\\proj\\az.cmd", "C:\\Program Files\\Azure CLI\\wbin\\az.cmd"]) });
    assert.equal(p, "C:\\Program Files\\Azure CLI\\wbin\\az.cmd");
  });
  test("uses Path when PATH is not set, and prefers .cmd, .bat, .exe in that order", () => {
    assert.equal(resolveAzPath({ env: { Path: "C:\\a" }, ...win(["C:\\a\\az.exe", "C:\\a\\az.bat"]) }), "C:\\a\\az.bat");
  });
  test("POSIX: first az in PATH outside the cwd", () => {
    const p = resolveAzPath({ env: { PATH: ":/home/me/proj:/usr/bin" }, platform: "linux", cwd: "/home/me/proj", exists: (f) => f === "/home/me/proj/az" || f === "/usr/bin/az" });
    assert.equal(p, "/usr/bin/az");
  });
  test("not found, or a path cmd.exe cannot quote safely, is an AuthError", () => {
    assert.throws(() => resolveAzPath({ env: { PATH: "C:\\x" }, ...win([]) }), (e) => e instanceof AuthError && /PATH/.test(e.message));
    assert.throws(() => resolveAzPath({ env: { PATH: "C:\\100%" }, ...win(["C:\\100%\\az.cmd"]) }), AuthError);
  });
  test("the provider runs the resolved az with NoDefaultCurrentDirectoryInExePath set", async () => {
    const calls = [];
    const exec = async (file, args, opts) => { calls.push({ file, args, opts }); return { stdout: JSON.stringify({ accessToken: "t", expires_on: 9e9 }) }; };
    const p = createTokenProvider({ execFileImpl: exec, platform: "win32", env: { ComSpec: "C:\\Windows\\cmd.exe", PATH: "C:\\az" }, cwd: "C:\\proj", exists: (f) => f === "C:\\az\\az.cmd", now: () => 0 });
    await p.getToken(AUDIENCES.fabric);
    assert.equal(calls[0].file, "C:\\Windows\\cmd.exe");
    assert.match(calls[0].args[3], /^""C:\\az\\az\.cmd" account get-access-token/);
    assert.equal(calls[0].opts.env.NoDefaultCurrentDirectoryInExePath, "1");
    assert.equal(calls[0].opts.windowsVerbatimArguments, true);
  });
});

describe("M4: the screenshot child process has an overall timeout", () => {
  function fakeSpawn(behaviour) {
    const spawned = [];
    const spawnImpl = (file, args, opts) => {
      const child = new EventEmitter();
      child.killed = false;
      child.kill = (sig) => { child.killed = true; child.signal = sig; setImmediate(() => child.emit("exit", null, sig || "SIGTERM")); return true; };
      spawned.push({ file, args, opts, child });
      if (behaviour === "exit0") setImmediate(() => child.emit("exit", 0, null));
      return child;
    };
    return { spawnImpl, spawned };
  }

  test("a hung child is killed after the timeout and the result is non-zero", async () => {
    const { spawnImpl, spawned } = fakeSpawn("hang");
    const logger = collectingLogger();
    const code = await defaultRunShots(fakeTokens(), {}, { spawnImpl, timeoutMs: 20, logger })({ workspaceId: "w", reportId: "r", outDir: "/o", pages: ["p1"] });
    assert.notEqual(code, 0);
    assert.ok(spawned[0].child.killed);
    assert.ok(logger.lines.some((l) => l.event === "shots_timeout"));
  });
  test("a normal exit is passed through and the token is only in the child env", async () => {
    const { spawnImpl, spawned } = fakeSpawn("exit0");
    const code = await defaultRunShots(fakeTokens(), { A: "1" }, { spawnImpl, timeoutMs: 10_000 })({ workspaceId: "w", reportId: "r", outDir: "/o", pages: ["p1"] });
    assert.equal(code, 0);
    assert.equal(spawned[0].opts.env.PBI_TOKEN, `tok-for-${AUDIENCES.powerbi}`);
    assert.ok(!spawned[0].args.some((a) => /tok-for/.test(a)));
  });
});

describe("LOW: auth expiry parsing", () => {
  test("expires_on null or empty falls back to expiresOn / 30 minutes instead of epoch 0", () => {
    assert.equal(parseAzToken(JSON.stringify({ accessToken: "t", expires_on: null }), 1000).expiresAtMs, 1000 + 30 * 60_000);
    assert.equal(parseAzToken(JSON.stringify({ accessToken: "t", expires_on: "" }), 1000).expiresAtMs, 1000 + 30 * 60_000);
    assert.equal(parseAzToken(JSON.stringify({ accessToken: "t", expires_on: null, expiresOn: "2026-10-30 12:00:00.000000" }), 0).expiresAtMs, new Date(2026, 9, 30, 12).getTime());
  });
});

function fabricSetup(routes) {
  const { fetchImpl, calls } = mockFetch(routes);
  const clock = recordingClock();
  const http = createHttpClient({ fetchImpl, sleep: clock.sleep, now: clock.now, random: () => 1, baseDelayMs: 1, maxDelayMs: 1 });
  const fabric = createFabricClient({ api: createApiClient({ http, tokens: fakeTokens() }), logger: collectingLogger(), sleep: clock.sleep, now: clock.now, lroPollMs: 10 });
  return { fabric, calls };
}

describe("LOW: fabric", () => {
  test("/result is appended to the path even when the Location has a query string", async () => {
    const { fabric, calls } = fabricSetup([
      ["GET", /\/operations\/op1\?api-version=2$/, () => jsonResponse(200, { status: "Succeeded" })],
      ["GET", /\/operations\/op1\/result\?api-version=2$/, () => jsonResponse(200, { id: "x" })],
    ]);
    assert.deepEqual(await fabric.waitOperation(`${F}/operations/op1?api-version=2`, { withResult: true }), { id: "x" });
    assert.equal(calls.length, 2);
  });
  test("updateDefinition is retried on 5xx (same definition, idempotent)", async () => {
    const { fabric, calls } = fabricSetup([["POST", /updateDefinition$/, [() => textResponse(503, ""), () => jsonResponse(200, {})]]]);
    await fabric.updateDefinition("w", "notebooks", "n1", { parts: [] });
    assert.equal(calls.length, 2);
  });
  test("SQL endpoint refreshMetadata is retried on 5xx", async () => {
    const { fabric, calls } = fabricSetup([["POST", /refreshMetadata$/, [() => textResponse(500, ""), () => jsonResponse(200, {})]]]);
    assert.equal(await fabric.refreshSqlEndpointMetadata("w", "s1"), true);
    assert.equal(calls.length, 2);
  });
});

describe("LOW: http does not follow redirects", () => {
  test("fetch is called with redirect: manual and a 3xx fails without being followed", async () => {
    const { fetchImpl, calls } = mockFetch([["GET", /x/, () => new Response(null, { status: 302, headers: { location: "https://evil.example.com/" } })]]);
    await assert.rejects(createHttpClient({ fetchImpl }).request({ url: "https://api.fabric.microsoft.com/x", headers: { Authorization: "Bearer t" } }), (e) => e.status === 302);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].redirect, "manual");
  });
});

describe("LOW: log redaction is recursive", () => {
  test("nested secret keys and bearer / JWT-looking values are redacted", () => {
    const out = [];
    const log = createLogger({ write: (s) => out.push(s), now: () => new Date(0) });
    log.error("x", { details: { headers: { Authorization: "Bearer SECRET1" }, list: [{ access_token: "SECRET2" }, "Bearer SECRET3"] }, note: "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.SECRET4sig", ok: { n: 1 } });
    const s = out.join("");
    assert.doesNotMatch(s, /SECRET/);
    const line = JSON.parse(s);
    assert.equal(line.ok.n, 1);
    assert.equal(line.details.list[1], "[REDACTED]");
  });
});

describe("LOW: state remembers the workspace name", () => {
  const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), "state-ws-"));

  test("checkWorkspaceState: match, mismatch, unknown", () => {
    assert.equal(checkWorkspaceState({}, "a"), "empty");
    assert.equal(checkWorkspaceState({ WORKSPACE_NAME: "a", WORKSPACE_ID: "1" }, "a"), "match");
    assert.equal(checkWorkspaceState({ WORKSPACE_ID: "1" }, "a"), "unverified");
    assert.throws(() => checkWorkspaceState({ WORKSPACE_NAME: "a", WORKSPACE_ID: "1" }, "b"), (e) => e.code === "STATE_MISMATCH" && /WORK_DIR/.test(e.message));
  });

  test("the workspace step stores WORKSPACE_NAME", async () => {
    const workDir = tmp();
    const ctx = {
      config: validateConfig({ CAPACITY_ID: GUID, WORKSPACE_NAME: "ws-x" }, { defaultWorkDir: workDir }),
      store: createStateStore(workDir),
      fabric: { ensureWorkspace: async () => ({ id: "w1", created: true }) },
    };
    await STEPS.workspace(ctx);
    assert.deepEqual(ctx.store.get(), { WORKSPACE_ID: "w1", WORKSPACE_NAME: "ws-x" });
  });

  test("deploy refuses to run against state written for another workspace", async () => {
    const dir = tmp();
    const workDir = path.join(dir, "work");
    fs.mkdirSync(workDir);
    fs.writeFileSync(path.join(workDir, "state.json"), JSON.stringify({ WORKSPACE_ID: "w1", WORKSPACE_NAME: "old-ws" }));
    fs.writeFileSync(path.join(dir, "config.sh"), `CAPACITY_ID="${GUID}"\nWORKSPACE_NAME="new-ws"\n`);
    const out = [];
    let ran = false;
    const code = await main(["lakehouse"], { env: { CONFIG: path.join(dir, "config.sh"), WORK_DIR: workDir }, write: (s) => out.push(s), deps: { tokens: fakeTokens(), steps: { lakehouse: async () => { ran = true; } } } });
    assert.equal(code, 1);
    assert.equal(ran, false);
    assert.match(out.join(""), /STATE_MISMATCH/);
  });

  test("deploy warns when the state has no workspace name (bash-era or pre-fix state)", async () => {
    const dir = tmp();
    const workDir = path.join(dir, "work");
    fs.mkdirSync(workDir);
    fs.writeFileSync(path.join(workDir, "state.json"), JSON.stringify({ WORKSPACE_ID: "w1" }));
    fs.writeFileSync(path.join(dir, "config.sh"), `CAPACITY_ID="${GUID}"\n`);
    const out = [];
    const code = await main(["lakehouse"], { env: { CONFIG: path.join(dir, "config.sh"), WORK_DIR: workDir }, write: (s) => out.push(s), deps: { tokens: fakeTokens(), steps: { lakehouse: async () => {} } } });
    assert.equal(code, 0);
    assert.match(out.join(""), /state_workspace_unverified/);
  });
});

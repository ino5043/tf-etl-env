"use strict";
const { test, describe } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { main, createContext, defaultRunShots } = require("../deploy");
const { validateConfig } = require("../lib/config");
const { DeployError } = require("../lib/errors");
const { mockFetch, jsonResponse, fakeTokens } = require("./helpers");

const GUID = "0f8fad5b-d9cb-469f-a165-70867728950e";

function setup(configText = `CAPACITY_ID="${GUID}"\nWORKSPACE_NAME="ws-test"\n`) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "deploy-main-"));
  const cfg = path.join(dir, "config.sh");
  fs.writeFileSync(cfg, configText);
  const out = [];
  const env = { CONFIG: cfg, WORK_DIR: path.join(dir, "work") };
  const lines = () => out.join("").split("\n").filter((l) => l.startsWith("{")).map((l) => JSON.parse(l));
  return { dir, env, out, write: (s) => out.push(s), lines };
}

describe("deploy.js main", () => {
  test("no arguments prints usage and exits 1; --help exits 0", async () => {
    const s = setup();
    assert.equal(await main([], { env: s.env, write: s.write }), 1);
    assert.match(s.out.join(""), /usage: node deploy\.js/);
    assert.equal(await main(["--help"], { env: s.env, write: s.write }), 0);
  });

  test("unknown step exits 2 before reading config or calling anything", async () => {
    const s = setup();
    let created = false;
    const code = await main(["model", "nope"], { env: { CONFIG: "/does/not/exist" }, write: s.write, deps: { createContext: () => { created = true; } } });
    assert.equal(code, 2);
    assert.equal(created, false);
    assert.equal(s.lines()[0].code, "USAGE");
  });

  test("invalid config fails with a structured error", async () => {
    const s = setup('CAPACITY_ID="not-a-guid"\n');
    assert.equal(await main(["workspace"], { env: s.env, write: s.write }), 1);
    const err = s.lines().find((l) => l.event === "deploy_failed");
    assert.equal(err.code, "CONFIG");
    assert.match(err.message, /CAPACITY_ID/);
  });

  test("runs the steps in order with JSON-line logs and saves state", async () => {
    const s = setup(`CAPACITY_ID="${GUID}"\nTYPO=1\n`);
    const steps = {
      workspace: async (ctx) => { ctx.store.commit({ WORKSPACE_ID: "w1" }); return { workspace_id: "w1" }; },
      lakehouse: async (ctx) => { ctx.store.commit({ LAKEHOUSE_ID: "l1" }); return {}; },
    };
    const code = await main(["workspace", "lakehouse"], { env: s.env, write: s.write, deps: { steps, tokens: fakeTokens(), runShots: async () => 0 } });
    assert.equal(code, 0);
    const events = s.lines().map((l) => l.event);
    assert.deepEqual(events, ["config_unknown_keys", "deploy_started", "step_started", "step_completed", "step_started", "step_completed", "deploy_completed"]);
    const done = s.lines().at(-1);
    assert.deepEqual(done.ids, { WORKSPACE_ID: "w1", LAKEHOUSE_ID: "l1" });
    assert.deepEqual(JSON.parse(fs.readFileSync(path.join(s.env.WORK_DIR, "state.json"), "utf8")), { WORKSPACE_ID: "w1", LAKEHOUSE_ID: "l1" });
  });

  test("a failing step stops the run with exit 1 and names the step", async () => {
    const s = setup();
    let later = false;
    const steps = { workspace: async () => { throw new DeployError("boom", { code: "X" }); }, lakehouse: async () => { later = true; } };
    assert.equal(await main(["workspace", "lakehouse"], { env: s.env, write: s.write, deps: { steps, tokens: fakeTokens() } }), 1);
    assert.equal(later, false);
    const fail = s.lines().find((l) => l.event === "deploy_failed");
    assert.equal(fail.step, "workspace");
    assert.equal(fail.code, "X");
  });

  test("tokens never appear in the logs", async () => {
    const s = setup();
    const steps = { workspace: async (ctx) => { ctx.logger.info("x", { token: "SECRET", Authorization: "Bearer SECRET" }); } };
    await main(["workspace"], { env: s.env, write: s.write, deps: { steps } });
    assert.doesNotMatch(s.out.join(""), /SECRET/);
  });
});

describe("createContext", () => {
  test("wires the real clients over the injected fetch and tokens (no real network)", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "deploy-ctx-"));
    const { fetchImpl, calls } = mockFetch([["GET", /\/v1\/workspaces$/, () => jsonResponse(200, { value: [{ id: "w9", displayName: "factory-etl-demo" }] })]]);
    const ctx = createContext({ config: validateConfig({ CAPACITY_ID: GUID }, { defaultWorkDir: dir }), env: {}, write: () => {}, deps: { fetchImpl, tokens: fakeTokens() } });
    assert.deepEqual(await ctx.fabric.ensureWorkspace({ name: "factory-etl-demo", capacityId: GUID }), { id: "w9", created: false });
    assert.equal(calls[0].headers.authorization, "Bearer tok-for-https://api.fabric.microsoft.com");
    assert.equal(ctx.paths.workDir, dir);
    assert.equal(typeof ctx.runShots, "function");
  });

  test("defaultRunShots passes the token via env and returns the child's exit code", async () => {
    const audiences = [];
    // 子プロセスは偽物（実際の shoot_report.js もブラウザーも起動しない）
    const { EventEmitter } = require("node:events");
    const spawned = [];
    const spawnImpl = (file, args, opts) => {
      const child = Object.assign(new EventEmitter(), { kill: () => true });
      spawned.push({ file, args, opts });
      setImmediate(() => child.emit("exit", 2, null));
      return child;
    };
    const code = await defaultRunShots(fakeTokens(audiences), { A: "1" }, { spawnImpl, timeoutMs: 10_000 })({ workspaceId: "w", reportId: "r", outDir: os.tmpdir(), pages: [] });
    assert.equal(code, 2);
    assert.match(spawned[0].args[0], /shoot_report\.js$/);
    assert.equal(spawned[0].opts.env.A, "1");
    assert.deepEqual(audiences, ["https://analysis.windows.net/powerbi/api"]);
  });
});

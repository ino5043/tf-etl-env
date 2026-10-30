"use strict";
const { test, describe } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { parseConfigText, validateConfig, loadConfig, isIsoDate } = require("../lib/config");
const { loadState, updateState, saveState, createStateStore, parseLegacyEnv } = require("../lib/state");
const { createLogger } = require("../lib/log");
const { ConfigError } = require("../lib/errors");

const GUID = "0f8fad5b-d9cb-469f-a165-70867728950e";
const tmpDir = () => fs.mkdtempSync(path.join(os.tmpdir(), "fabric-auto-"));

describe("parseConfigText", () => {
  test("parses quoted, unquoted, export, comments and the shipped example", () => {
    const text = fs.readFileSync(path.join(__dirname, "..", "config.example.sh"), "utf8");
    const c = parseConfigText(text);
    assert.equal(c.CAPACITY_ID, "");
    assert.equal(c.MODEL_NAME, "工場生産ダッシュボード");
    assert.equal(c.REPORT_NAME, "工場長 朝会レポート");
    assert.equal(c.TARGET_DATE, "2026-10-30");
    assert.equal(c.REQUESTS_CA_BUNDLE, undefined); // コメントアウトされている
  });

  test("value forms", () => {
    const c = parseConfigText([
      "A=plain  # comment",
      'B="with space" # c',
      "C='single $NOT_EXPANDED'",
      'export D="/path/ca.pem"',
      'E="esc \\"q\\" \\\\"',
      "F=",
      "\r",
    ].join("\n"));
    assert.deepEqual({ ...c }, { A: "plain", B: "with space", C: "single $NOT_EXPANDED", D: "/path/ca.pem", E: 'esc "q" \\', F: "" });
    assert.ok(Object.isFrozen(c));
  });

  test("rejects shell syntax because config.sh is never executed", () => {
    for (const bad of ['A="$(whoami)"', "A=$HOME", "A=`id`", 'A="x" ; rm -rf /', "if true; then", 'A="unterminated', "A=two words"]) {
      assert.throws(() => parseConfigText(bad), ConfigError, bad);
    }
  });

  test("error message carries the line number", () => {
    assert.throws(() => parseConfigText("# ok\nA=1\nB=$(x)"), /line 3/);
  });
});

describe("validateConfig / loadConfig", () => {
  const base = { CAPACITY_ID: GUID };

  test("applies defaults and returns a frozen camelCase config", () => {
    const c = validateConfig(base, { defaultWorkDir: "/w" });
    assert.equal(c.capacityId, GUID);
    assert.equal(c.workspaceName, "factory-etl-demo");
    assert.equal(c.lakehouseName, "factory_lakehouse");
    assert.equal(c.demoFrom, "2026-10-01");
    assert.equal(c.workDir, "/w");
    assert.ok(Object.isFrozen(c));
  });

  test("collects every problem in one error", () => {
    assert.throws(
      () => validateConfig({ CAPACITY_ID: "nope", TARGET_DATE: "2026-13-01", DEMO_FROM: "2026-10-31", DEMO_TO: "2026-10-01", LAKEHOUSE_NAME: "bad-name", REPORT_NAME: "a\u0001b" }, { defaultWorkDir: "/w" }),
      (e) => e instanceof ConfigError && ["CAPACITY_ID", "TARGET_DATE", "DEMO_FROM", "LAKEHOUSE_NAME", "REPORT_NAME"].every((k) => e.message.includes(k)),
    );
  });

  test("CAPACITY_ID is required", () => assert.throws(() => validateConfig({}, { defaultWorkDir: "/w" }), /CAPACITY_ID/));

  test("isIsoDate checks the format and the calendar", () => {
    assert.ok(isIsoDate("2026-02-28"));
    for (const d of ["2026-02-30", "2026-1-01", "20261001", "2026-10-01T00:00", "", undefined]) assert.equal(isIsoDate(d), false, String(d));
  });

  test("env overrides file values; WORK_DIR resolved; unknown keys reported", () => {
    const dir = tmpDir();
    const file = path.join(dir, "config.sh");
    fs.writeFileSync(file, `CAPACITY_ID="${GUID}"\nWORKSPACE_NAME="from-file"\nTYPO_KEY=1\n`);
    const c = loadConfig({ configPath: file, env: { WORKSPACE_NAME: "from-env", WORK_DIR: "rel/work" }, defaultWorkDir: "/w", cwd: dir });
    assert.equal(c.workspaceName, "from-env");
    assert.equal(c.workDir, path.resolve(dir, "rel/work"));
    assert.deepEqual(c.unknownKeys, ["TYPO_KEY"]);
  });

  test("missing config file is a ConfigError pointing at config.example.sh", () => {
    assert.throws(() => loadConfig({ configPath: path.join(tmpDir(), "none.sh"), env: {}, defaultWorkDir: "/w" }), /config\.example\.sh/);
  });
});

describe("state", () => {
  test("updateState returns a new frozen object and never mutates", () => {
    const s0 = Object.freeze({ WORKSPACE_ID: "w" });
    const s1 = updateState(s0, { LAKEHOUSE_ID: "l" });
    assert.notEqual(s0, s1);
    assert.deepEqual(s0, { WORKSPACE_ID: "w" });
    assert.deepEqual(s1, { WORKSPACE_ID: "w", LAKEHOUSE_ID: "l" });
    assert.ok(Object.isFrozen(s1));
    assert.throws(() => { s1.X = 1; }, TypeError);
  });

  test("updateState rejects non-string ids and bad keys", () => {
    assert.throws(() => updateState({}, { A: 1 }), /string/);
    assert.throws(() => updateState({}, { "bad key": "x" }), /key/);
  });

  test("save / load round trip (atomic, JSON) and empty when missing", () => {
    const dir = tmpDir();
    assert.deepEqual(loadState(dir), {});
    saveState(dir, { WORKSPACE_ID: "w" });
    assert.deepEqual(loadState(dir), { WORKSPACE_ID: "w" });
    assert.deepEqual(fs.readdirSync(dir), ["state.json"]);
  });

  test("migrates the legacy state.env of the bash version", () => {
    const dir = tmpDir();
    fs.writeFileSync(path.join(dir, "state.env"), "WORKSPACE_ID=w1\nLAKEHOUSE_ID=l1\n\n");
    assert.deepEqual(loadState(dir), { WORKSPACE_ID: "w1", LAKEHOUSE_ID: "l1" });
    assert.deepEqual(parseLegacyEnv("A=1\r\njunk\n"), { A: "1" });
  });

  test("corrupt state.json is reported, not silently reset", () => {
    const dir = tmpDir();
    fs.writeFileSync(path.join(dir, "state.json"), "{oops");
    assert.throws(() => loadState(dir), /state\.json/);
  });

  test("store.commit persists immediately", () => {
    const dir = tmpDir();
    const store = createStateStore(dir);
    const before = store.get();
    store.commit({ MODEL_ID: "m" });
    assert.deepEqual(before, {});
    assert.deepEqual(store.get(), { MODEL_ID: "m" });
    assert.deepEqual(loadState(dir), { MODEL_ID: "m" });
  });
});

describe("createLogger", () => {
  test("writes one JSON object per line with ts, level, event and fields", () => {
    const out = [];
    const log = createLogger({ write: (s) => out.push(s), now: () => new Date(0) });
    log.info("step_completed", { step: "model", status: "ok", duration_ms: 12 });
    assert.equal(out.length, 1);
    assert.ok(out[0].endsWith("\n"));
    assert.deepEqual(JSON.parse(out[0]), { ts: "1970-01-01T00:00:00.000Z", level: "info", event: "step_completed", step: "model", status: "ok", duration_ms: 12 });
  });

  test("child adds base fields; secrets are dropped", () => {
    const out = [];
    const log = createLogger({ write: (s) => out.push(s), now: () => new Date(0) }).child({ step: "shots" });
    log.error("x", { accessToken: "SECRET", Authorization: "Bearer SECRET", token: "SECRET", n: 1 });
    log.warn("y");
    assert.doesNotMatch(out.join(""), /SECRET/);
    assert.equal(JSON.parse(out[0]).step, "shots");
    assert.equal(JSON.parse(out[1]).level, "warn");
  });
});

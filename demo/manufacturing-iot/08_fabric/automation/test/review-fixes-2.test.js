"use strict";
// 再レビュー（フェーズ 2: scripts/）の指摘に対する回帰テスト。ブラウザーもネットワークも使わない（puppeteer は偽物）
const { test, describe } = require("node:test");
const assert = require("node:assert/strict");
const { parseDax, looksLikeFormat } = require("../scripts/build_model");
const { main: shootMain, launchOptions } = require("../scripts/shoot_report");

describe("build_model: empty measure expressions are an error", () => {
  test("body at the same indent as the MEASURE line", () => {
    assert.throws(() => parseDax("DEFINE\n    MEASURE t[空] =\n    SUM ( t[x] )\n"), /empty expression.*空/);
  });
  test("`MEASURE t[x] =` followed by nothing (end of file, blank lines, next measure, EVALUATE)", () => {
    assert.throws(() => parseDax("DEFINE\n    MEASURE t[x] ="), /empty expression.*x/);
    assert.throws(() => parseDax("DEFINE\n    MEASURE t[x] =\n\n\n"), /empty expression.*x/);
    assert.throws(() => parseDax("DEFINE\n    MEASURE t[x] =\n    MEASURE t[y] = 1\n"), /empty expression.*x/);
    assert.throws(() => parseDax("DEFINE\n    MEASURE t[x] =\nEVALUATE ROW(\"v\", 1)\n"), /empty expression.*x/);
  });
  test("a comment-only body is also empty", () => {
    assert.throws(() => parseDax("DEFINE\n    MEASURE t[x] =\n        // TODO\n"), /empty expression.*x/);
  });
});

describe("build_model: looksLikeFormat rejects unit / description words", () => {
  test("unit words without a digit placeholder, % or full date pattern", () => {
    for (const f of ["mm/s", "mm", "/s", "pt", "dd", "hh:nn", "mm s", "nn"]) assert.equal(looksLikeFormat(f), false, f);
  });
  test("every format used in powerbi/dax-measures.md is still accepted", () => {
    for (const f of ["#,0", "#,0.00", "0.0%", "0.00%", "yyyy/mm/dd", "yyyy/mm/dd hh:nn", '+0.00" pt";-0.00" pt";0.00" pt"', '0.0" ℃"', '0.00" mm/s"', "+#,0;-#,0;0", "+0.0%;-0.0%;0.0%"]) {
      assert.equal(looksLikeFormat(f), true, f);
    }
  });
  test("a full date pattern with mm and dd but no yyyy is accepted", () => {
    assert.equal(looksLikeFormat("mm/dd"), true);
  });
});

describe("shoot_report: the browser does not inherit PBI_TOKEN", () => {
  function fakePuppeteer() {
    const rec = { launch: null };
    const page = {
      on: () => {}, setContent: async () => {}, addScriptTag: async () => {},
      evaluate: async () => "rendered",
      $: async () => ({ screenshot: async () => {} }),
    };
    return { rec, puppeteer: { launch: async (o) => { rec.launch = o; return { newPage: async () => page, close: async () => {} }; } } };
  }
  const deps = (p) => ({ loadPuppeteer: () => p.puppeteer, resolveBrowser: () => "/edge", resolveScript: () => "/x.js", io: { log() {}, error() {} }, settleMs: 0, mkdir: () => {} });

  test("launch gets an explicit env without PBI_TOKEN, and the token still reaches the page", async () => {
    const p = fakePuppeteer();
    const env = { PBI_TOKEN: "SECRET", PATH: "/bin", LOCALAPPDATA: "C:\\x" };
    assert.equal(await shootMain(["g", "r", "/out", "p1"], env, deps(p)), 0);
    assert.equal(p.rec.launch.env.PBI_TOKEN, undefined);
    assert.equal(p.rec.launch.env.PATH, "/bin");
    assert.equal(env.PBI_TOKEN, undefined); // 呼び出し元の env からも消す
  });

  test("when called with process.env, PBI_TOKEN is deleted from process.env before launch", async () => {
    const p = fakePuppeteer();
    const saved = process.env.PBI_TOKEN;
    process.env.PBI_TOKEN = "SECRET";
    let seenAtLaunch;
    const orig = p.puppeteer.launch;
    p.puppeteer.launch = async (o) => { seenAtLaunch = process.env.PBI_TOKEN; return orig(o); };
    try {
      assert.equal(await shootMain(["g", "r", "/out", "p1"], process.env, deps(p)), 0);
      assert.equal(seenAtLaunch, undefined);
      assert.equal(process.env.PBI_TOKEN, undefined);
    } finally {
      if (saved === undefined) delete process.env.PBI_TOKEN; else process.env.PBI_TOKEN = saved;
    }
  });

  test("launchOptions never forwards PBI_TOKEN even if given", () => {
    assert.equal(launchOptions({ browserPath: "/b", platform: "win32", env: { PBI_TOKEN: "x", A: "1" } }).env.PBI_TOKEN, undefined);
    assert.equal(launchOptions({ browserPath: "/b", platform: "win32", env: { A: "1" } }).env.A, "1");
  });
});

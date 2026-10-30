"use strict";
const { test, describe } = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { resolveBrowserPath, launchOptions, resolvePowerBiClient, PAGE_HTML, shoot, main } = require("../scripts/shoot_report");

function stubPuppeteer(statuses, { failLaunch = false, failScreenshot = false } = {}) {
  const rec = { launched: null, scripts: [], shots: [], closed: false, evaluated: [] };
  const queue = [...statuses];
  const page = {
    on: () => {},
    setContent: async (html) => { rec.html = html; },
    addScriptTag: async (opts) => { rec.scripts.push(opts); },
    evaluate: async (fn, ...args) => { rec.evaluated.push(args); return queue.shift(); },
    $: async () => ({ screenshot: async ({ path: p }) => { if (failScreenshot) throw new Error("detached"); rec.shots.push(p); } }),
  };
  const browser = { newPage: async () => page, close: async () => { rec.closed = true; } };
  return { rec, puppeteer: { launch: async (o) => { if (failLaunch) throw new Error("no browser"); rec.launched = o; return browser; } } };
}

const base = { token: "T", groupId: "g-1", reportId: "r-1", outDir: path.join(require("node:os").tmpdir(), "shots-test"), pages: ["p1", "p2"], scriptPath: "/x/powerbi.min.js", settleMs: 0, browserPath: "/edge", platform: "win32", isRoot: false, mkdir: () => {} };

describe("shoot_report helpers", () => {
  test("page HTML loads nothing from a CDN", () => {
    assert.doesNotMatch(PAGE_HTML, /<script|https?:\/\//);
  });

  test("browser path: BROWSER_PATH wins, else first existing platform default", () => {
    assert.equal(resolveBrowserPath({ env: { BROWSER_PATH: "/my/chrome" }, platform: "linux", exists: () => false }), "/my/chrome");
    assert.match(resolveBrowserPath({ env: {}, platform: "win32", exists: () => true }), /msedge\.exe$/);
    assert.equal(resolveBrowserPath({ env: {}, platform: "linux", exists: (p) => p === "/usr/bin/chromium" }), "/usr/bin/chromium");
    assert.throws(() => resolveBrowserPath({ env: {}, platform: "linux", exists: () => false }), /BROWSER_PATH/);
  });

  test("--no-sandbox only as root on Linux", () => {
    assert.deepEqual(launchOptions({ browserPath: "/b", platform: "win32", isRoot: false }).args, []);
    assert.deepEqual(launchOptions({ browserPath: "/b", platform: "linux", isRoot: false }).args, []);
    assert.deepEqual(launchOptions({ browserPath: "/b", platform: "linux", isRoot: true }).args, ["--no-sandbox"]);
    assert.equal(launchOptions({ browserPath: "/b", platform: "linux", isRoot: false }).executablePath, "/b");
  });

  test("powerbi-client is resolved from node_modules (min build preferred)", () => {
    assert.equal(resolvePowerBiClient((id) => `/nm/${id}`), "/nm/powerbi-client/dist/powerbi.min.js");
    const fallback = (id) => { if (id.endsWith(".min.js")) throw new Error("x"); return `/nm/${id}/dist/powerbi.js`; };
    assert.equal(resolvePowerBiClient(fallback), "/nm/powerbi-client/dist/powerbi.js");
    assert.throws(() => resolvePowerBiClient(() => { throw new Error("x"); }), /npm install/);
  });
});

describe("shoot", () => {
  test("injects the local script, renders each page and closes the browser", async () => {
    const { rec, puppeteer } = stubPuppeteer(["rendered", "rendered"]);
    const results = await shoot({ ...base, puppeteer });
    assert.deepEqual(results.map((r) => r.status), ["rendered", "rendered"]);
    assert.deepEqual(rec.scripts, [{ path: "/x/powerbi.min.js" }]);
    assert.equal(rec.shots.length, 2);
    assert.ok(rec.closed);
    assert.deepEqual(rec.launched.args, []);
    assert.deepEqual(rec.evaluated[0].slice(0, 4), ["T", "g-1", "r-1", "p1"]);
  });

  test("closes the browser even when a screenshot fails", async () => {
    const { rec, puppeteer } = stubPuppeteer(["rendered"], { failScreenshot: true });
    await assert.rejects(shoot({ ...base, pages: ["p1"], puppeteer }), /detached/);
    assert.ok(rec.closed);
  });
});

describe("main exit codes", () => {
  const deps = (stub) => ({ loadPuppeteer: () => stub.puppeteer, resolveBrowser: () => "/edge", resolveScript: () => "/x/powerbi.min.js", io: { log() {}, error() {} }, settleMs: 0, mkdir: () => {} });

  test("0 when every page rendered", async () => {
    assert.equal(await main(["g", "r", "/out", "p1"], { PBI_TOKEN: "t" }, deps(stubPuppeteer(["rendered"]))), 0);
  });
  test("1 on timeout or embed error", async () => {
    assert.equal(await main(["g", "r", "/out", "p1", "p2"], { PBI_TOKEN: "t" }, deps(stubPuppeteer(["rendered", "timeout"]))), 1);
    assert.equal(await main(["g", "r", "/out", "p1"], { PBI_TOKEN: "t" }, deps(stubPuppeteer(["error: {\"message\":\"TokenExpired\"}"]))), 1);
  });
  test("1 when the browser cannot start; 2 on usage errors", async () => {
    assert.equal(await main(["g", "r", "/out", "p1"], { PBI_TOKEN: "t" }, deps(stubPuppeteer([], { failLaunch: true }))), 1);
    assert.equal(await main(["g", "r", "/out"], { PBI_TOKEN: "t" }, deps(stubPuppeteer([]))), 2);
    assert.equal(await main(["g", "r", "/out", "p1"], {}, deps(stubPuppeteer([]))), 2);
  });
});

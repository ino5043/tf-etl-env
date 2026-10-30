"use strict";
const { test, describe } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { STEPS, notebookRunBody } = require("../lib/steps");
const { resolveSteps, runSteps, errorFields, STEP_ORDER } = require("../lib/runner");
const { createStateStore } = require("../lib/state");
const { validateConfig } = require("../lib/config");
const { DeployError, HttpError } = require("../lib/errors");
const { collectingLogger } = require("./helpers");

const HERE = path.join(__dirname, "..");
const FABRIC_DIR = path.join(HERE, "..");
const FIX = path.join(__dirname, "fixtures");
const GUID = "0f8fad5b-d9cb-469f-a165-70867728950e";
const read = (f) => fs.readFileSync(path.join(FIX, f), "utf8");

// 呼び出しを記録する偽のクライアント（HTTP には一切出ない）
function fakes(overrides = {}) {
  const calls = [];
  const rec = (name, impl) => async (...args) => { calls.push([name, ...args]); return impl(...args); };
  const fabric = {
    ensureWorkspace: rec("ensureWorkspace", () => ({ id: "ws1", created: true })),
    ensureLakehouse: rec("ensureLakehouse", () => ({ id: "lh1", created: false })),
    createOrUpdateItem: rec("createOrUpdateItem", ({ type }) => ({ id: `${type}-id`, created: true })),
    runJob: rec("runJob", () => ({ status: "Completed" })),
    getLakehouse: rec("getLakehouse", () => ({ properties: { sqlEndpointProperties: { id: "sql1", connectionString: "x.datawarehouse.fabric.microsoft.com" } } })),
    refreshSqlEndpointMetadata: rec("refreshSqlEndpointMetadata", () => true),
    ...overrides.fabric,
  };
  const onelake = {
    uploadFile: rec("uploadFile", () => undefined),
    downloadText: rec("downloadText", ({ remotePath }) => (remotePath.includes("schema_dump") ? read("schema.json") : read("verify_ok.json"))),
    ...overrides.onelake,
  };
  const powerbi = {
    refreshDataset: rec("refreshDataset", () => ({ status: "Completed", requestId: "r" })),
    executeQuery: rec("executeQuery", () => ({ results: [{ tables: [] }] })),
    ...overrides.powerbi,
  };
  return { fabric, onelake, powerbi, calls };
}

function ctxFor(f, { state = {}, runShots } = {}) {
  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), "deploy-steps-"));
  const store = createStateStore(workDir);
  if (Object.keys(state).length) store.commit(state);
  const config = validateConfig({ CAPACITY_ID: GUID }, { defaultWorkDir: workDir });
  return { config, paths: { here: HERE, fabricDir: FABRIC_DIR, workDir }, store, logger: collectingLogger(), runShots: runShots || (async () => 0), ...f };
}

const ALL_IDS = { WORKSPACE_ID: "ws1", LAKEHOUSE_ID: "lh1", NOTEBOOK_ID: "nb1", MODEL_ID: "m1", REPORT_ID: "r1" };

describe("steps", () => {
  test("workspace / lakehouse store their ids", async () => {
    const f = fakes();
    const ctx = ctxFor(f);
    await STEPS.workspace(ctx);
    await STEPS.lakehouse(ctx);
    assert.deepEqual(ctx.store.get(), { WORKSPACE_ID: "ws1", WORKSPACE_NAME: "factory-etl-demo", LAKEHOUSE_ID: "lh1" });
    assert.equal(f.calls[0][1].capacityId, GUID);
  });

  test("steps fail clearly when a prerequisite id is missing", async () => {
    const ctx = ctxFor(fakes());
    await assert.rejects(STEPS.lakehouse(ctx), (e) => e.code === "MISSING_STATE" && /workspace/.test(e.message));
    await assert.rejects(STEPS.report(ctx), /MODEL_ID/);
  });

  test("upload sends every sample CSV", async () => {
    const f = fakes();
    const r = await STEPS.upload(ctxFor(f, { state: ALL_IDS }));
    const csvs = fs.readdirSync(path.join(FABRIC_DIR, "sample_data")).filter((x) => x.endsWith(".csv"));
    assert.equal(r.files, csvs.length);
    assert.ok(f.calls.every(([n, a]) => n === "uploadFile" && a.remotePath.startsWith("sample_data/") && Buffer.isBuffer(a.content)));
  });

  test("notebook creates/updates fabric_pipeline with the default lakehouse; run executes it", async () => {
    const f = fakes();
    const ctx = ctxFor(f, { state: { WORKSPACE_ID: "ws1", LAKEHOUSE_ID: "lh1" } });
    await STEPS.notebook(ctx);
    assert.equal(ctx.store.get().NOTEBOOK_ID, "Notebook-id");
    assert.equal(f.calls[0][1].body.displayName, "fabric_pipeline");
    await STEPS.run(ctx);
    const [, job] = f.calls[1];
    assert.equal(job.jobType, "RunNotebook");
    assert.deepEqual(job.body, notebookRunBody(ctx.config, ctx.store.get()));
    assert.equal(job.body.executionData.configuration.defaultLakehouse.id, "lh1");
  });

  test("verify passes on expected values and fails the step on mismatch", async () => {
    const ok = fakes();
    const ctx = ctxFor(ok, { state: ALL_IDS });
    const r = await STEPS.verify(ctx);
    assert.equal(r.ng, 0);
    assert.ok(fs.existsSync(path.join(ctx.paths.workDir, "verify_pipeline.json")));
    const bad = fakes({ onelake: { downloadText: async () => JSON.stringify({ ...JSON.parse(read("verify_ok.json")), dead_letter: [] }) } });
    await assert.rejects(STEPS.verify(ctxFor(bad, { state: ALL_IDS })), (e) => e.code === "VERIFY_FAILED" && /Dead Letter/.test(e.message));
  });

  test("verify: invalid JSON from OneLake is an error", async () => {
    const f = fakes({ onelake: { downloadText: async () => "<html>" } });
    await assert.rejects(STEPS.verify(ctxFor(f, { state: ALL_IDS })), /not valid JSON/);
  });

  test("model: builds TMDL, refreshes, checks every measure and stores MODEL_ID", async () => {
    const f = fakes();
    const ctx = ctxFor(f, { state: { WORKSPACE_ID: "ws1", LAKEHOUSE_ID: "lh1" } });
    const r = await STEPS.model(ctx);
    assert.equal(r.measures, 80);
    assert.equal(ctx.store.get().MODEL_ID, "SemanticModel-id");
    assert.ok(fs.existsSync(path.join(ctx.paths.workDir, "measure_tables.json")));
    assert.ok(f.calls.some(([n]) => n === "refreshSqlEndpointMetadata"));
    assert.equal(f.calls.filter(([n]) => n === "executeQuery").length, 80);
  });

  test("model: a failing measure fails the step (deploy must stop)", async () => {
    const f = fakes({ powerbi: { executeQuery: async (ws, ds, q) => (/\[不良率\]/.test(q) ? { error: { message: "bad" } } : { results: [{}] }) } });
    const ctx = ctxFor(f, { state: { WORKSPACE_ID: "ws1", LAKEHOUSE_ID: "lh1" } });
    await assert.rejects(STEPS.model(ctx), (e) => e.code === "MEASURE_ERRORS" && /不良率/.test(e.message));
    assert.equal(ctx.store.get().MODEL_ID, "SemanticModel-id"); // 作成済みの ID は保存されている（再実行で更新になる）
  });

  test("model: SQL endpoint not ready", async () => {
    const f = fakes({ fabric: { getLakehouse: async () => ({ properties: {} }) } });
    await assert.rejects(STEPS.model(ctxFor(f, { state: { WORKSPACE_ID: "ws1", LAKEHOUSE_ID: "lh1" } })), /SQL/);
  });

  test("report uses measure_tables.json from the model step", async () => {
    const f = fakes();
    const ctx = ctxFor(f, { state: ALL_IDS });
    await assert.rejects(STEPS.report(ctx), /measure_tables\.json/);
    fs.copyFileSync(path.join(FIX, "measure_tables.json"), path.join(ctx.paths.workDir, "measure_tables.json"));
    await STEPS.report(ctx);
    assert.equal(ctx.store.get().REPORT_ID, "Report-id");
    assert.equal(f.calls[0][1].type, "Report");
  });

  test("pipeline creates or updates the definition, then runs with target_date", async () => {
    const f = fakes();
    const ctx = ctxFor(f, { state: ALL_IDS });
    await STEPS.pipeline(ctx);
    assert.equal(f.calls[0][1].type, "DataPipeline");
    assert.deepEqual(f.calls[1][1].body, { executionData: { parameters: { target_date: "2026-10-30" } } });
    assert.equal(f.calls[1][1].jobType, "Pipeline");
  });

  test("shots fails when the screenshot script fails", async () => {
    const seen = [];
    const ok = ctxFor(fakes(), { state: ALL_IDS, runShots: async (a) => { seen.push(a); return 0; } });
    await STEPS.shots(ok);
    assert.deepEqual(seen[0].pages, ["p1", "p2", "p3"]);
    const ng = ctxFor(fakes(), { state: ALL_IDS, runShots: async () => 1 });
    await assert.rejects(STEPS.shots(ng), (e) => e.code === "SHOTS_FAILED");
  });
});

describe("runner", () => {
  test("resolveSteps", () => {
    assert.deepEqual(resolveSteps(["all"]), STEP_ORDER);
    assert.deepEqual(resolveSteps(["model", "report"]), ["model", "report"]);
    assert.throws(() => resolveSteps([]), /usage/i);
    assert.throws(() => resolveSteps(["model", "bogus", "x"]), (e) => /bogus, x/.test(e.message));
    assert.throws(() => resolveSteps(["all", "model"]), /all/);
  });

  test("runSteps logs start/completion with duration and stops at the first failure", async () => {
    const logger = collectingLogger();
    let t = 0;
    const ran = [];
    const steps = { a: async () => { ran.push("a"); t += 5; return { x: 1 }; }, b: async () => { ran.push("b"); throw new HttpError({ method: "POST", url: "u", status: 400, text: "bad" }); }, c: async () => { ran.push("c"); } };
    await assert.rejects(runSteps(["a", "b", "c"], steps, { logger }, { now: () => t }), (e) => e.step === "b");
    assert.deepEqual(ran, ["a", "b"]);
    assert.deepEqual(logger.lines.map((l) => `${l.event}:${l.step}`), ["step_started:a", "step_completed:a", "step_started:b", "step_failed:b"]);
    assert.equal(logger.lines[1].duration_ms, 5);
    assert.equal(logger.lines[1].x, 1);
    assert.equal(logger.lines[3].http_status, 400);
  });

  test("errorFields includes code, status and body excerpt", () => {
    const f = errorFields(new DeployError("m", { code: "C", details: { a: 1 } }));
    assert.deepEqual(f, { error_type: "DeployError", code: "C", message: "m", details: { a: 1 } });
  });
});

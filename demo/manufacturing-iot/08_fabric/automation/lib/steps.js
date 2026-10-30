// 配置の各手順（deploy-guide.md 手順 2〜9）。どれも表示名で既存の項目を探し、あれば更新するので再実行してよい
"use strict";
const fs = require("node:fs");
const path = require("node:path");
const { DeployError } = require("./errors");
const { buildNotebookBodyFromFile } = require("../scripts/make_notebook_body");
const { evaluateVerify, formatVerify } = require("../scripts/check_verify");
const modelBuilder = require("../scripts/build_model");
const reportBuilder = require("../scripts/build_report");
const { buildPipeline } = require("../scripts/build_pipeline");
const { checkAllMeasures } = require("../scripts/check_all_measures");

const MIN = 60_000;
const SHOT_PAGES = Object.freeze(["p1", "p2", "p3"]);

function need(ctx, key, step) {
  const v = ctx.store.get()[key];
  if (!v) throw new DeployError(`${key} がない。先に '${step}' を実行する`, { code: "MISSING_STATE" });
  return v;
}

const notebookRunBody = (config, state) => ({
  executionData: { configuration: { defaultLakehouse: { name: config.lakehouseName, id: state.LAKEHOUSE_ID, workspaceId: state.WORKSPACE_ID } } },
});

async function upsertNotebook(ctx, file, name) {
  const workspaceId = need(ctx, "WORKSPACE_ID", "workspace");
  const lakehouseId = need(ctx, "LAKEHOUSE_ID", "lakehouse");
  const body = buildNotebookBodyFromFile(file, { name, workspaceId, lakehouseId, lakehouseName: ctx.config.lakehouseName });
  return (await ctx.fabric.createOrUpdateItem({ workspaceId, type: "Notebook", body })).id;
}

// Python コードを一時 Notebook として実行し、Files/_verify/<name>.json を取得する（WORK_DIR にも保存する）
async function runCode(ctx, name, file) {
  const itemId = await upsertNotebook(ctx, file, name);
  const state = ctx.store.get();
  await ctx.fabric.runJob({ workspaceId: state.WORKSPACE_ID, itemId, jobType: "RunNotebook", body: notebookRunBody(ctx.config, state), timeoutMs: 30 * MIN });
  const text = await ctx.onelake.downloadText({ workspaceId: state.WORKSPACE_ID, lakehouseId: state.LAKEHOUSE_ID, remotePath: `_verify/${name}.json` });
  fs.writeFileSync(path.join(ctx.paths.workDir, `${name}.json`), text);
  try { return JSON.parse(text); } catch {
    throw new DeployError(`_verify/${name}.json is not valid JSON`, { code: "BAD_RESULT" });
  }
}

async function workspace(ctx) {
  const { workspaceName: name, capacityId } = ctx.config;
  const { id, created } = await ctx.fabric.ensureWorkspace({ name, capacityId, description: "Manufacturing IoT ETL demo (tf-etl-env)" });
  ctx.store.commit({ WORKSPACE_ID: id, WORKSPACE_NAME: name });
  return { workspace_id: id, created };
}

async function lakehouse(ctx) {
  const workspaceId = need(ctx, "WORKSPACE_ID", "workspace");
  const { id, created } = await ctx.fabric.ensureLakehouse({ workspaceId, name: ctx.config.lakehouseName });
  ctx.store.commit({ LAKEHOUSE_ID: id });
  return { lakehouse_id: id, created };
}

async function upload(ctx) {
  const workspaceId = need(ctx, "WORKSPACE_ID", "workspace");
  const lakehouseId = need(ctx, "LAKEHOUSE_ID", "lakehouse");
  const dir = path.join(ctx.paths.fabricDir, "sample_data");
  const files = fs.readdirSync(dir).filter((f) => f.endsWith(".csv")).sort();
  for (const f of files) {
    await ctx.onelake.uploadFile({ workspaceId, lakehouseId, remotePath: `sample_data/${f}`, content: fs.readFileSync(path.join(dir, f)) });
  }
  return { files: files.length, target: "Files/sample_data" };
}

async function notebook(ctx) {
  const id = await upsertNotebook(ctx, path.join(ctx.paths.fabricDir, "notebook", "fabric_pipeline.ipynb"), "fabric_pipeline");
  ctx.store.commit({ NOTEBOOK_ID: id });
  return { notebook_id: id };
}

async function run(ctx) {
  const itemId = need(ctx, "NOTEBOOK_ID", "notebook");
  const state = ctx.store.get();
  const job = await ctx.fabric.runJob({ workspaceId: state.WORKSPACE_ID, itemId, jobType: "RunNotebook", body: notebookRunBody(ctx.config, state), timeoutMs: 60 * MIN });
  return { job_status: job.status };
}

async function verify(ctx) {
  const result = evaluateVerify(await runCode(ctx, "verify_pipeline", path.join(ctx.paths.here, "notebooks", "verify_pipeline.py")));
  const lines = formatVerify(result);
  if (result.ng > 0) throw new DeployError(`verify: ${lines.filter((l) => l.startsWith("NG")).join(" / ")}`, { code: "VERIFY_FAILED" });
  return { checks: result.checks.length, ng: 0 };
}

async function buildModelBody(ctx) {
  const workspaceId = need(ctx, "WORKSPACE_ID", "workspace");
  const lakehouseId = need(ctx, "LAKEHOUSE_ID", "lakehouse");
  const schema = await runCode(ctx, "schema_dump", path.join(ctx.paths.here, "notebooks", "schema_dump.py"));
  const sql = ((await ctx.fabric.getLakehouse(workspaceId, lakehouseId)).properties || {}).sqlEndpointProperties || {};
  if (!sql.id || !sql.connectionString) throw new DeployError("SQL 分析エンドポイントの準備ができていない。しばらく待って 'model' を再実行する", { code: "SQL_ENDPOINT_NOT_READY" });
  await ctx.fabric.refreshSqlEndpointMetadata(workspaceId, sql.id);
  const pbiDir = path.join(ctx.paths.fabricDir, "powerbi");
  const model = modelBuilder.buildModel({
    daxText: fs.readFileSync(path.join(pbiDir, "dax-measures.dax"), "utf8"),
    mdText: fs.readFileSync(path.join(pbiDir, "dax-measures.md"), "utf8"),
    schema, sqlServer: sql.connectionString, sqlEndpointId: sql.id, modelName: ctx.config.modelName,
  });
  modelBuilder.writeOutputs(path.join(ctx.paths.workDir, "model.json"), model);
  if (model.missingMeta.length) ctx.logger.warn("measure_meta_missing", { measures: model.missingMeta });
  return model;
}

async function model(ctx) {
  const m = await buildModelBody(ctx);
  const workspaceId = ctx.store.get().WORKSPACE_ID;
  const { id, created } = await ctx.fabric.createOrUpdateItem({ workspaceId, type: "SemanticModel", body: m.body });
  ctx.store.commit({ MODEL_ID: id });
  // Direct Lake のフレーミング（更新）。自分の更新要求の結果だけを見る
  await ctx.powerbi.refreshDataset(workspaceId, id);
  const check = await checkAllMeasures({ names: Object.keys(m.measureTables), executeQuery: (q) => ctx.powerbi.executeQuery(workspaceId, id, q) });
  if (check.errors.length) {
    throw new DeployError(`${check.errors.length}/${check.checked} measures failed: ${check.errors.map((e) => `${e.name}: ${e.message}`).join(" / ").slice(0, 1500)}`, { code: "MEASURE_ERRORS" });
  }
  return { model_id: id, created, measures: check.checked };
}

async function report(ctx) {
  const modelId = need(ctx, "MODEL_ID", "model");
  const mapFile = path.join(ctx.paths.workDir, "measure_tables.json");
  if (!fs.existsSync(mapFile)) throw new DeployError(`${mapFile} がない。先に 'model' を実行する`, { code: "MISSING_STATE" });
  const { reportName, demoFrom, demoTo } = ctx.config;
  const r = reportBuilder.buildReport({ modelId, measureTables: JSON.parse(fs.readFileSync(mapFile, "utf8")), reportName, demoFrom, demoTo });
  reportBuilder.writeOutputs(path.join(ctx.paths.workDir, "report.json"), r);
  const { id, created } = await ctx.fabric.createOrUpdateItem({ workspaceId: ctx.store.get().WORKSPACE_ID, type: "Report", body: r.body });
  ctx.store.commit({ REPORT_ID: id });
  return { report_id: id, created, pages: r.summary };
}

async function pipeline(ctx) {
  const notebookId = need(ctx, "NOTEBOOK_ID", "notebook");
  const workspaceId = ctx.store.get().WORKSPACE_ID;
  const { pipelineName: name, targetDate } = ctx.config;
  const { id, created } = await ctx.fabric.createOrUpdateItem({ workspaceId, type: "DataPipeline", body: buildPipeline({ name, workspaceId, notebookId, targetDate }) });
  ctx.store.commit({ PIPELINE_ID: id });
  const job = await ctx.fabric.runJob({ workspaceId, itemId: id, jobType: "Pipeline", body: { executionData: { parameters: { target_date: targetDate } } }, timeoutMs: 60 * MIN });
  return { pipeline_id: id, created, job_status: job.status };
}

async function shots(ctx) {
  const reportId = need(ctx, "REPORT_ID", "report");
  const outDir = path.join(ctx.paths.workDir, "shots");
  const code = await ctx.runShots({ workspaceId: ctx.store.get().WORKSPACE_ID, reportId, outDir, pages: [...SHOT_PAGES] });
  if (code !== 0) throw new DeployError(`shoot_report.js exited with ${code}`, { code: "SHOTS_FAILED" });
  return { out_dir: outDir, pages: SHOT_PAGES.length };
}

const STEPS = Object.freeze({ workspace, lakehouse, upload, notebook, run, verify, model, report, pipeline, shots });

module.exports = { STEPS, notebookRunBody, need };

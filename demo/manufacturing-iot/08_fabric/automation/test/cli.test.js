"use strict";
// scripts/*.js のコマンドラインとしての動作（従来どおりの引数・出力ファイル・終了コード）
const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const ROOT = path.join(__dirname, "..");
const FIX = path.join(__dirname, "fixtures");
const run = (script, args) => spawnSync(process.execPath, [path.join(ROOT, "scripts", script), ...args], { encoding: "utf8" });
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), "cli-"));

test("build_model.js writes the body, tmdl/ and measure_tables.json", () => {
  const out = path.join(tmp(), "model.json");
  const r = run("build_model.js", [path.join(ROOT, "..", "powerbi"), path.join(FIX, "schema.json"), "srv", "ep", "M", out]);
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /measures: 80/);
  assert.ok(fs.existsSync(path.join(path.dirname(out), "tmdl", "definition", "tables", "dim_date.tmdl")));
  assert.equal(Object.keys(JSON.parse(fs.readFileSync(path.join(path.dirname(out), "measure_tables.json"), "utf8"))).length, 80);
});

test("build_report.js / build_pipeline.js / make_notebook_body.js write their bodies", () => {
  const dir = tmp();
  assert.equal(run("build_report.js", ["m", path.join(FIX, "measure_tables.json"), "R", "2026-10-01", "2026-10-31", path.join(dir, "r.json")]).status, 0);
  assert.ok(fs.existsSync(path.join(dir, "report", "definition", "report.json")));
  assert.equal(run("build_pipeline.js", ["pl", "w", "n", "2026-10-30", path.join(dir, "p.json")]).status, 0);
  assert.equal(JSON.parse(fs.readFileSync(path.join(dir, "p.json"), "utf8")).displayName, "pl");
  assert.equal(run("make_notebook_body.js", [path.join(ROOT, "notebooks", "schema_dump.py"), "sd", "w", "l", "lh", path.join(dir, "n.json")]).status, 0);
  assert.equal(JSON.parse(fs.readFileSync(path.join(dir, "n.json"), "utf8")).displayName, "sd");
});

test("missing arguments or invalid input exit non-zero with a message", () => {
  for (const s of ["build_model.js", "build_report.js", "build_pipeline.js", "make_notebook_body.js", "check_verify.js"]) {
    const r = run(s, []);
    assert.notEqual(r.status, 0, s);
    assert.match(r.stderr, /failed|usage/, s);
  }
  assert.equal(run("build_pipeline.js", ["pl", "w", "n", "30/10/2026", path.join(tmp(), "p.json")]).status, 1);
  assert.equal(run("check_all_measures.js", []).status, 2);
});

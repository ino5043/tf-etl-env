"use strict";
const { test, describe } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const { evaluateVerify, formatVerify } = require("../scripts/check_verify");
const { checkAllMeasures, measureQuery, extractQueryError, main: measuresMain } = require("../scripts/check_all_measures");

const FIX = path.join(__dirname, "fixtures");
const verifyOk = () => JSON.parse(fs.readFileSync(path.join(FIX, "verify_ok.json"), "utf8"));

describe("check_verify", () => {
  test("all expectations met for the shipped sample data", () => {
    const r = evaluateVerify(verifyOk());
    assert.equal(r.ng, 0);
    assert.equal(r.checks.length, 13);
    assert.match(formatVerify(r).join("\n"), /すべて期待値どおり/);
  });

  test("mismatches are reported with the expected value", () => {
    const v = verifyOk();
    v.counts = v.counts.map((c) => (c.table_name === "gold.dim_date" ? { ...c, row_count: 364 } : c));
    v.equipment_alerts = [];
    const r = evaluateVerify(v);
    assert.equal(r.ng, 2);
    const text = formatVerify(r).join("\n");
    assert.match(text, /NG {2}gold\.dim_date: 364（期待 365）/);
    assert.match(text, /2 件が期待値と不一致/);
  });

  test("malformed verify JSON fails clearly", () => {
    assert.throws(() => evaluateVerify({}), /verify result/);
  });

  test("CLI exits 0 on pass and 1 on mismatch", () => {
    const script = path.join(__dirname, "..", "scripts", "check_verify.js");
    assert.equal(spawnSync(process.execPath, [script, path.join(FIX, "verify_ok.json")]).status, 0);
    const bad = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "cv-")), "v.json");
    fs.writeFileSync(bad, JSON.stringify({ ...verifyOk(), totals: [{ actual: 1, defect: 9471 }] }));
    const r = spawnSync(process.execPath, [script, bad], { encoding: "utf8" });
    assert.equal(r.status, 1);
    assert.match(r.stdout, /実績数量の合計/);
  });
});

describe("check_all_measures", () => {
  test("measureQuery escapes ] in measure names", () => {
    assert.equal(measureQuery("a]b"), 'EVALUATE ROW ( "v", [a]]b] )');
  });

  test("extractQueryError reads top-level and per-result errors", () => {
    assert.equal(extractQueryError({ results: [{ tables: [] }] }), null);
    assert.equal(extractQueryError({ error: { code: "X", pbi_error: { details: [{ detail: { type: 1, value: "Column not found" } }] } } }), "Column not found");
    assert.match(extractQueryError({ results: [{ error: { code: "Y" } }] }), /"code":"Y"/);
  });

  test("collects every failing measure", async () => {
    const queries = [];
    const exec = async (q) => {
      queries.push(q);
      return /\[bad\]/.test(q) ? { error: { pbi_error: { details: [{ detail: { value: "boom" } }] } } } : { results: [{ tables: [{ rows: [{ "[v]": 1 }] }] }] };
    };
    const r = await checkAllMeasures({ names: ["ok", "bad", "ok2"], executeQuery: exec });
    assert.equal(r.checked, 3);
    assert.deepEqual(r.errors, [{ name: "bad", message: "boom" }]);
    assert.equal(queries.length, 3);
  });

  test("CLI main returns 1 when any measure errors, 0 when all pass, 2 on usage errors", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "cm-"));
    const map = path.join(dir, "m.json");
    fs.writeFileSync(map, JSON.stringify({ ok: "t", bad: "t" }));
    const out = [];
    const io = { log: (s) => out.push(s), error: (s) => out.push(s) };
    const exec = (fail) => async () => async (q) => (fail && /bad/.test(q) ? { error: { message: "x" } } : { results: [{}] });
    assert.equal(await measuresMain(["w", "d", map], { PBI_TOKEN: "t" }, { io, makeExecuteQuery: exec(true) }), 1);
    assert.equal(await measuresMain(["w", "d", map], { PBI_TOKEN: "t" }, { io, makeExecuteQuery: exec(false) }), 0);
    assert.equal(await measuresMain(["w", "d", map], {}, { io, makeExecuteQuery: exec(false) }), 2);
    assert.equal(await measuresMain(["w"], { PBI_TOKEN: "t" }, { io, makeExecuteQuery: exec(false) }), 2);
    assert.ok(out.some((s) => /errors 1/.test(s)));
  });
});

"use strict";
const { test, describe } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { buildReport, lit, dateRangeFilter, recentFrom } = require("../scripts/build_report");
const { buildPipeline } = require("../scripts/build_pipeline");
const { buildNotebookBody } = require("../scripts/make_notebook_body");

const decode = (body, p) => Buffer.from(body.definition.parts.find((x) => x.path === p).payload, "base64").toString("utf8");
const ROOT = path.join(__dirname, "..");

describe("build_report helpers", () => {
  test("lit escapes single quotes and formats numbers as doubles", () => {
    assert.deepEqual(lit("O'Brien"), { expr: { Literal: { Value: "'O''Brien'" } } });
    assert.deepEqual(lit(1.2), { expr: { Literal: { Value: "1.2D" } } });
  });

  test("recentFrom: 14 days before the exclusive end, across month and year boundaries", () => {
    assert.equal(recentFrom("2026-10-31", 14), "2026-10-17");
    assert.equal(recentFrom("2026-03-01", 14), "2026-02-15");
    assert.equal(recentFrom("2027-01-05", 14), "2026-12-22");
  });

  test("dateRangeFilter: from (>=) and to (<) on dim_date[calendar_date]", () => {
    const f = dateRangeFilter("2026-10-01", "2026-10-31");
    const { Left, Right } = f.filter.Where[0].Condition.And;
    assert.equal(Left.Comparison.ComparisonKind, 2);
    assert.equal(Left.Comparison.Right.Literal.Value, "datetime'2026-10-01T00:00:00'");
    assert.equal(Right.Comparison.ComparisonKind, 3);
    assert.equal(Right.Comparison.Right.Literal.Value, "datetime'2026-10-31T00:00:00'");
    assert.equal(f.field.Column.Property, "calendar_date");
  });
});

describe("buildReport", () => {
  const measureTables = JSON.parse(fs.readFileSync(path.join(__dirname, "fixtures", "measure_tables.json"), "utf8"));
  const args = { modelId: "m-1", measureTables, reportName: "R", demoFrom: "2026-10-01", demoTo: "2026-10-31" };

  test("3 pages, report-level filter and 14-day filters on heatmaps", () => {
    const r = buildReport(args);
    assert.equal(r.body.displayName, "R");
    assert.deepEqual(JSON.parse(decode(r.body, "definition/pages/pages.json")).pageOrder, ["p1", "p2", "p3"]);
    const report = JSON.parse(decode(r.body, "definition/report.json"));
    assert.equal(report.filterConfig.filters[0].name, "demo_period");
    const heat = JSON.parse(decode(r.body, "definition/pages/p2/visuals/p2_q6/visual.json"));
    assert.equal(heat.filterConfig.filters[0].name, "recent_14_days");
    assert.equal(heat.filterConfig.filters[0].filter.Where[0].Condition.And.Left.Comparison.Right.Literal.Value, "datetime'2026-10-17T00:00:00'");
    assert.match(decode(r.body, "definition.pbir"), /semanticmodelid=m-1/);
  });

  test("visual z-order is deterministic and starts again for each build (no module-level state)", () => {
    const z = (r) => JSON.parse(decode(r.body, "definition/pages/p1/visuals/p1_h1/visual.json")).position.z;
    assert.equal(z(buildReport(args)), 100);
    assert.equal(z(buildReport(args)), 100);
  });

  test("unknown measure and invalid dates fail", () => {
    assert.throws(() => buildReport({ ...args, measureTables: {} }), /unknown measure/);
    assert.throws(() => buildReport({ ...args, demoTo: "2026-10-32" }), /date/);
    assert.throws(() => buildReport({ ...args, demoFrom: "2026-11-01" }), /before/);
  });
});

describe("buildPipeline", () => {
  test("notebook activity with target_date parameter", () => {
    const body = buildPipeline({ name: "pl", workspaceId: "w", notebookId: "n", targetDate: "2026-10-30" });
    assert.equal(body.displayName, "pl");
    const c = JSON.parse(decode(body, "pipeline-content.json"));
    assert.equal(c.properties.parameters.target_date.defaultValue, "2026-10-30");
    const a = c.properties.activities[0];
    assert.equal(a.type, "TridentNotebook");
    assert.equal(a.typeProperties.notebookId, "n");
    assert.equal(a.typeProperties.workspaceId, "w");
    assert.equal(a.typeProperties.parameters.TARGET_DATE.value.value, "@pipeline().parameters.target_date");
  });
  test("invalid target date is rejected", () => {
    assert.throws(() => buildPipeline({ name: "pl", workspaceId: "w", notebookId: "n", targetDate: "2026/10/30" }), /date/);
  });
});

describe("buildNotebookBody", () => {
  const lh = { workspaceId: "w", lakehouseId: "l", lakehouseName: "lh" };

  test(".py becomes a one-cell notebook with the default lakehouse", () => {
    const body = buildNotebookBody({ source: "print(1)\nprint(2)\n", ext: ".py", name: "nb", ...lh });
    assert.equal(body.displayName, "nb");
    assert.equal(body.definition.format, "ipynb");
    const nb = JSON.parse(decode(body, "notebook-content.ipynb"));
    assert.equal(nb.cells.length, 1);
    assert.deepEqual(nb.cells[0].source, ["print(1)\n", "print(2)\n"]);
    assert.deepEqual(nb.metadata.dependencies.lakehouse, { default_lakehouse: "l", default_lakehouse_name: "lh", default_lakehouse_workspace_id: "w" });
  });

  test(".ipynb is passed through, keeping cells and other metadata", () => {
    const src = fs.readFileSync(path.join(ROOT, "..", "notebook", "fabric_pipeline.ipynb"), "utf8");
    const nb = JSON.parse(decode(buildNotebookBody({ source: src, ext: ".ipynb", name: "fp", ...lh }), "notebook-content.ipynb"));
    const orig = JSON.parse(src);
    assert.deepEqual(nb.cells, orig.cells);
    assert.equal(nb.metadata.dependencies.lakehouse.default_lakehouse, "l");
    assert.deepEqual(nb.metadata.kernelspec, orig.metadata.kernelspec);
  });

  test(".ipynb without metadata and unsupported extensions", () => {
    const nb = JSON.parse(decode(buildNotebookBody({ source: '{"cells":[],"nbformat":4}', ext: ".ipynb", name: "x", ...lh }), "notebook-content.ipynb"));
    assert.equal(nb.metadata.dependencies.lakehouse.default_lakehouse_name, "lh");
    assert.throws(() => buildNotebookBody({ source: "", ext: ".r", name: "x", ...lh }), /unsupported/);
  });
});

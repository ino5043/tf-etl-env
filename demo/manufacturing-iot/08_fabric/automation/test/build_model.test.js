"use strict";
const { test, describe } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { parseDax, parseMdMeta, looksLikeFormat, expandTabs, mString, buildModel } = require("../scripts/build_model");

const PBI = path.join(__dirname, "..", "..", "powerbi");
const schema = JSON.parse(fs.readFileSync(path.join(__dirname, "fixtures", "schema.json"), "utf8"));
const decode = (body, p) => Buffer.from(body.definition.parts.find((x) => x.path === p).payload, "base64").toString("utf8");
let n = 0;
const seqUuid = () => `00000000-0000-0000-0000-${String(++n).padStart(12, "0")}`;

describe("expandTabs", () => {
  test("expands to 4-column tab stops", () => {
    assert.equal(expandTabs("\tx"), "    x");
    assert.equal(expandTabs("  \tx"), "    x");
    assert.equal(expandTabs("\t\tx"), "        x");
  });
});

describe("parseDax", () => {
  test("single-line and multi-line measures with space indentation", () => {
    const m = parseDax([
      "DEFINE",
      "    MEASURE t[A] = SUM ( t[x] )",
      "",
      "    MEASURE t[B] =",
      "        VAR _a = [A]",
      "        RETURN",
      "            _a * 2",
      "",
      "    VAR __not_a_measure = 1",
      "EVALUATE ROW ( \"v\", [B] )",
    ].join("\n"));
    assert.deepEqual(m.map((x) => [x.table, x.name, x.expr]), [
      ["t", "A", "SUM ( t[x] )"],
      ["t", "B", "VAR _a = [A]\nRETURN\n    _a * 2"],
    ]);
  });

  test("tab indentation is expanded (a tab no longer counts as 1 column)", () => {
    const m = parseDax("DEFINE\n\tMEASURE t[A] =\n\t\tCALCULATE (\n\t\t\t[x],\n\t\t\tALL ( t )\n\t\t)\n\tMEASURE t[B] = 1\n");
    assert.equal(m.length, 2);
    assert.equal(m[0].expr, "CALCULATE (\n    [x],\n    ALL ( t )\n)");
  });

  test("boundary is relative to the MEASURE line: MEASURE at column 0 with nested parens at column 4", () => {
    const m = parseDax([
      "DEFINE",
      "MEASURE t[A] =",
      "    CALCULATE (",
      "    [x],",
      "    ALL ( t )",
      "    )",
      "MEASURE t[B] = 2",
    ].join("\n"));
    assert.equal(m.length, 2);
    assert.equal(m[0].expr, "CALCULATE (\n[x],\nALL ( t )\n)");
  });

  test("quoted table names (spaces, quotes) and digits; ]] inside names", () => {
    const m = parseDax([
      "DEFINE",
      "    MEASURE 'Sales 2026'[Total] = 1",
      "    MEASURE 'It''s'[x]] y] = 2",
      "    MEASURE fact_2026[n] = 3",
    ].join("\n"));
    assert.deepEqual(m.map((x) => [x.table, x.name]), [["Sales 2026", "Total"], ["It's", "x] y"], ["fact_2026", "n"]]);
  });

  test("comments inside a measure are kept, comments at DEFINE level end it", () => {
    const m = parseDax("DEFINE\n    MEASURE t[A] =\n        // inner\n        1\n// 検証\n    MEASURE t[B] = 2\n");
    assert.equal(m[0].expr, "// inner\n1");
    assert.equal(m.length, 2);
  });

  test("throws when a MEASURE line cannot be parsed (count mismatch)", () => {
    assert.throws(() => parseDax("DEFINE\n    MEASURE t[A] = 1\n    MEASURE broken table[B] = 2\n"), /MEASURE lines: 2.*parsed: 1/s);
  });

  test("throws on an unexpected dedent inside an expression", () => {
    assert.throws(() => parseDax("DEFINE\n    MEASURE t[A] =\n        CALCULATE (\n    )\n"), /dedent/);
  });

  test("throws on duplicate measure names", () => {
    assert.throws(() => parseDax("DEFINE\n    MEASURE t[A] = 1\n    MEASURE u[A] = 2\n"), /duplicate/);
  });

  test("the real dax-measures.dax parses to 80 measures", () => {
    const m = parseDax(fs.readFileSync(path.join(PBI, "dax-measures.dax"), "utf8"));
    assert.equal(m.length, 80);
    assert.ok(m.every((x) => x.expr.length > 0));
    assert.ok(!m.some((x) => /__last_equipment_date|EVALUATE/.test(x.expr)));
  });
});

describe("looksLikeFormat", () => {
  test("accepts real format strings", () => {
    for (const f of ["#,0", "0.0%", "0.00%", "yyyy/mm/dd", "yyyy/mm/dd hh:nn", "+#,0;-#,0;0", '0.00" mm/s"', '+0.0" pt";-0.0" pt";0.0" pt"', "0"]) assert.ok(looksLikeFormat(f), f);
  });
  test("rejects lone symbols, ymdhns-only words, numbers and prose", () => {
    for (const f of ["", "-", "/", ":", "()", "ymd", "Ms", "Nh", "65", "0.05", "1.00", "—", "上記を最終日で評価", '"text only"']) assert.equal(looksLikeFormat(f), false, f);
  });
});

describe("parseMdMeta", () => {
  test("folder from headings, format from the 2nd column, first definition wins", () => {
    const md = [
      "### 3.1 `00_共通`",
      "| メジャー | 書式 | 説明 |",
      "|---------|------|------|",
      "| `最終日` | `yyyy/mm/dd` | x |",
      "| `判定` | - | 説明 |",
      "#### 3.2 `01_生産\\時系列`",
      "| `実績数 前日比`, `別名` | `+0.0%;-0.0%;0.0%` | y |",
      "| `目標` | 0.05 | 値 |",
      "### 9 `検証`",
      "| `最終日` | `#,0` | 再登場 |",
    ].join("\n");
    const meta = parseMdMeta(md);
    assert.deepEqual(meta["最終日"], { folder: "00_共通", format: "yyyy/mm/dd" });
    assert.deepEqual(meta["判定"], { folder: "00_共通", format: null });
    assert.deepEqual(meta["別名"], { folder: "01_生産\\時系列", format: "+0.0%;-0.0%;0.0%" });
    assert.equal(meta["目標"].format, null);
  });
});

describe("mString", () => {
  test("escapes double quotes for an M string literal and rejects line breaks", () => {
    assert.equal(mString('a"b'), '"a""b"');
    assert.throws(() => mString("a\nb"), /control/);
  });
});

describe("buildModel", () => {
  const dax = fs.readFileSync(path.join(PBI, "dax-measures.dax"), "utf8");
  const md = fs.readFileSync(path.join(PBI, "dax-measures.md"), "utf8");
  const args = { daxText: dax, mdText: md, schema, sqlServer: 'srv"x.datawarehouse.fabric.microsoft.com', sqlEndpointId: "ep-1", modelName: "M", uuid: seqUuid };

  test("produces the model body, TMDL parts and the measure → table map", () => {
    const r = buildModel(args);
    assert.equal(r.body.displayName, "M");
    assert.equal(Object.keys(r.measureTables).length, 80);
    assert.deepEqual(r.missingMeta, []);
    assert.deepEqual(Object.keys(r.parts).sort(), [
      "definition.pbism", "definition/database.tmdl", "definition/expressions.tmdl", "definition/model.tmdl", "definition/relationships.tmdl",
      ...["daily_production_kpi", "dim_date", "dim_equipment", "dim_line", "dim_status", "equipment_health"].map((t) => `definition/tables/${t}.tmdl`),
    ].sort());
    assert.equal(r.body.definition.parts.length, 11);
  });

  test("SQL server string is escaped inside the M expression", () => {
    const expr = decode(buildModel(args).body, "definition/expressions.tmdl");
    assert.match(expr, /Sql\.Database\("srv""x\.datawarehouse\.fabric\.microsoft\.com", "ep-1"\)/);
  });

  test("measures carry format and folder; dimension ints use plain 0; sort-by and hidden columns", () => {
    const t = buildModel(args).parts["definition/tables/daily_production_kpi.tmdl"];
    assert.match(t, /\tmeasure '達成率' = DIVIDE \( \[実績数\], \[計画数\] \)\n\t\tformatString: 0\.0%\n\t\tdisplayFolder: 01_生産/);
    const d = buildModel(args).parts["definition/tables/dim_date.tmdl"];
    assert.match(d, /column year\n\t\tdataType: int64\n\t\tformatString: 0\n/);
    assert.match(d, /column year_month\n[^]*?sortByColumn: year_month_sort/);
    assert.match(d, /column calendar_date\n\t\tdataType: dateTime\n\t\tisKey/);
  });

  test("missing schema table and unknown column type fail", () => {
    const { "gold.dim_status": _drop, ...partial } = schema;
    assert.throws(() => buildModel({ ...args, schema: partial }), /schema not found: gold\.dim_status/);
    assert.throws(() => buildModel({ ...args, schema: { ...schema, "gold.dim_status": [{ name: "x", type: "map<string,int>" }] } }), /unknown type/);
  });
});

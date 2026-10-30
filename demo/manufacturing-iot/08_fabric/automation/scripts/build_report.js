// dashboard-spec.md に沿った Power BI レポート（PBIR 形式）を生成する
// Usage: node build_report.js <semantic_model_id> <measure_tables.json> <report_name> <demo_from YYYY-MM-DD> <demo_to YYYY-MM-DD（含まない）> <out_body.json>
"use strict";
const fs = require("fs");
const path = require("path");
const { isIsoDate } = require("../lib/config");

const S = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition";
const DAY_MS = 86_400_000;

// ---------- 値・フィルター（純粋関数） ----------
// PBIR のリテラル: 文字列は '...'（' は '' にエスケープ）、数値は 1.2D
const lit = (v) => ({ expr: { Literal: { Value: typeof v === "string" ? `'${v.replace(/'/g, "''")}'` : `${v}D` } } });
const bool = (b) => ({ expr: { Literal: { Value: b ? "true" : "false" } } });
const color = (hex) => ({ solid: { color: { expr: { Literal: { Value: `'${hex}'` } } } } });
const title = (text) => ({ title: [{ properties: { show: bool(true), text: lit(text), fontSize: lit(11) } }] });

// 期間の終わり（含まない）から days 日前の日付
const recentFrom = (to, days) => new Date(Date.parse(`${to}T00:00:00Z`) - days * DAY_MS).toISOString().slice(0, 10);

// デモデータの期間（from 以上 to 未満）で絞り込む。本番運用では相対日付フィルターに置き換える
function dateRangeFilter(from, to) {
  const col = { Column: { Expression: { SourceRef: { Source: "d" } }, Property: "calendar_date" } };
  const cmp = (kind, date) => ({ Comparison: { ComparisonKind: kind, Left: col, Right: { Literal: { Value: `datetime'${date}T00:00:00'` } } } });
  return {
    name: "demo_period", type: "Advanced", howCreated: "User",
    field: { Column: { Expression: { SourceRef: { Entity: "dim_date" } }, Property: "calendar_date" } },
    filter: { Version: 2, From: [{ Name: "d", Entity: "dim_date", Type: 0 }], Where: [{ Condition: { And: { Left: cmp(2, from), Right: cmp(3, to) } } }] },
  };
}

// ---------- ビジュアル部品（z 順の連番はレポート 1 つ分の生成ごとに閉じる） ----------
function createKit(MT, recent14) {
  let z = 0;
  const nextZ = () => (z += 100);
  const M = (name, displayName) => {
    const table = MT[name];
    if (!table) throw new Error(`unknown measure: ${name}`);
    return { field: { Measure: { Expression: { SourceRef: { Entity: table } }, Property: name } }, queryRef: `${table}.${name}`, nativeQueryRef: name, ...(displayName ? { displayName } : {}) };
  };
  const Col = (table, column, displayName) => ({ field: { Column: { Expression: { SourceRef: { Entity: table } }, Property: column } }, queryRef: `${table}.${column}`, nativeQueryRef: column, ...(displayName ? { displayName } : {}) });
  const colorByMeasure = (name) => {
    if (!MT[name]) throw new Error(`unknown measure: ${name}`);
    return { solid: { color: { expr: { Measure: { Expression: { SourceRef: { Entity: MT[name] } }, Property: name } } } } };
  };
  // 条件付き書式（フィールド値）: 値列 queryRef に対して色メジャーを当てる
  const cfByField = (prop, colorMeasure, targetQueryRef) => ({ properties: { [prop]: colorByMeasure(colorMeasure) }, selector: { data: [{ dataViewWildcard: { matchingOption: 1 } }], metadata: targetQueryRef } });

  function visual(name, x, y, w, h, visualType, queryState, extra = {}) {
    const v = { visualType, ...(queryState ? { query: { queryState } } : {}), ...(extra.objects ? { objects: extra.objects } : {}), drillFilterOtherVisuals: true, ...(extra.containerObjects ? { visualContainerObjects: extra.containerObjects } : {}) };
    const json = { $schema: `${S}/visualContainer/2.0.0/schema.json`, name, position: { x, y, z: nextZ(), width: w, height: h }, visual: v, ...(extra.filters ? { filterConfig: { filters: extra.filters } } : {}) };
    return { name, json };
  }
  // labelDisplayUnits 1 = 単位の省略なし（21.31K ではなく 21,311）
  const card = (name, x, y, w, h, measure, label, size = 20) => visual(name, x, y, w, h, "card", { Values: { projections: [M(measure, label)] } }, {
    objects: { labels: [{ properties: { fontSize: lit(size), labelDisplayUnits: lit(1) } }], categoryLabels: [{ properties: { show: bool(true), fontSize: lit(10) } }] },
  });
  const textbox = (name, x, y, w, h, text, size = 20, bold = true) => ({
    name,
    json: {
      $schema: `${S}/visualContainer/2.0.0/schema.json`, name, position: { x, y, z: nextZ(), width: w, height: h },
      visual: { visualType: "textbox", objects: { general: [{ properties: { paragraphs: [{ textRuns: [{ value: text, textStyle: { fontSize: `${size}pt`, fontWeight: bold ? "bold" : "normal", color: "#223A5E" } }] }] } }] } },
    },
  });
  // ヒートマップ（ライン・設備 × 日）は直近 14 日に絞る（dashboard-spec.md Q6 / E5 / E6）
  const recent14Filter = () => [{ ...recent14, name: "recent_14_days" }];
  const noSubTotals = { subTotals: [{ properties: { rowSubtotals: bool(false), columnSubtotals: bool(false) } }] };
  return { M, Col, colorByMeasure, cfByField, visual, card, textbox, recent14Filter, noSubTotals };
}

// ---------- 共通ヘッダー ----------
function header(k, pageTitle, prefix) {
  return [
    k.textbox(`${prefix}_h1`, 20, 12, 250, 60, pageTitle, 22, true),
    k.card(`${prefix}_h2`, 280, 8, 170, 70, "最終日", "対象日", 16),
    k.card(`${prefix}_h3`, 460, 8, 210, 70, "最終集計日時 (JST)", "データ更新（JST）", 14),
    k.visual(`${prefix}_h4`, 680, 8, 340, 70, "slicer", { Values: { projections: [k.Col("dim_date", "calendar_date", "期間")] } }, {
      objects: { data: [{ properties: { mode: lit("Between") } }], header: [{ properties: { show: bool(true) } }] },
    }),
    k.visual(`${prefix}_h5`, 1030, 8, 230, 70, "slicer", { Values: { projections: [k.Col("dim_line", "line_name", "ライン")] } }, {
      objects: { data: [{ properties: { mode: lit("Dropdown") } }], selection: [{ properties: { selectAllCheckboxEnabled: bool(true) } }] },
    }),
  ];
}

// ---------- ページ 1: 生産実績 ----------
function page1(k) {
  const { M, Col } = k;
  return [
    ...header(k, "生産実績", "p1"),
    k.card("p1_p1", 20, 90, 230, 110, "実績数 (最終日)", "実績数（対象日）"),
    k.card("p1_p2", 260, 90, 230, 110, "達成率 (最終日)", "達成率（対象日）"),
    k.card("p1_p3", 500, 90, 230, 110, "計画差異 (最終日)", "計画差異（対象日）"),
    k.card("p1_p4", 740, 90, 230, 110, "実績数 7日移動平均 (最終日)", "7 日平均（稼働日）"),
    k.visual("p1_p5", 980, 90, 280, 110, "gauge", { Y: { projections: [M("達成率 (最終日)", "達成率")] }, TargetValue: { projections: [M("達成率 目標", "目標")] } },
      { objects: { axis: [{ properties: { min: lit(0), max: lit(1.2) } }] }, containerObjects: title("達成率と目標") }),
    k.visual("p1_p6", 20, 210, 1240, 250, "lineClusteredColumnComboChart", {
      Category: { projections: [Col("dim_date", "calendar_date", "日付")] }, Y: { projections: [M("計画数"), M("実績数")] }, Y2: { projections: [M("達成率"), M("達成率 7日移動")] },
    }, { objects: { dataPoint: [{ properties: { fill: color("#C9D1DC") }, selector: { metadata: "daily_production_kpi.計画数" } }, { properties: { fill: color("#2B4F76") }, selector: { metadata: "daily_production_kpi.実績数" } }, { properties: { fill: color("#E45E32") }, selector: { metadata: "daily_production_kpi.達成率" } }] }, containerObjects: title("計画数・実績数と達成率の日次推移") }),
    k.visual("p1_p7", 20, 470, 760, 240, "pivotTable", {
      Rows: { projections: [Col("dim_line", "line_name", "ライン")] },
      Values: { projections: [M("計画数 (最終日)", "計画"), M("実績数 (最終日)", "実績"), M("計画差異 (最終日)", "差異"), M("達成率 (最終日)", "達成率"), M("実績数 前日比 (最終日)", "前日比")] },
    }, { objects: { values: [k.cfByField("backColor", "達成率 色 (最終日)", "daily_production_kpi.達成率 (最終日)")] }, containerObjects: title("ライン別（対象日）") }),
    k.visual("p1_p8", 790, 470, 470, 240, "barChart", { Category: { projections: [Col("dim_line", "line_name", "ライン")] }, Y: { projections: [M("日勤実績数", "日勤"), M("夜勤実績数", "夜勤")] } },
      { containerObjects: title("シフト別実績（期間）") }),
  ];
}

// ---------- ページ 2: 品質管理 ----------
function page2(k) {
  const { M, Col } = k;
  return [
    ...header(k, "品質管理", "p2"),
    k.card("p2_q1", 20, 90, 300, 110, "不良率 (最終日)", "不良率（対象日）"),
    k.card("p2_q2", 330, 90, 300, 110, "歩留まり率 (最終日)", "歩留まり率（対象日）"),
    k.card("p2_q3", 640, 90, 300, 110, "不良数 (最終日)", "不良数（対象日）"),
    k.card("p2_q4", 950, 90, 310, 110, "不良率 目標超過ライン数 (最終日)", "目標超過ライン"),
    k.visual("p2_q5", 20, 210, 1240, 230, "lineChart", { Category: { projections: [Col("dim_date", "calendar_date", "日付")] }, Y: { projections: [M("不良率"), M("不良率 7日移動"), M("不良率 目標", "目標 5%")] } },
      { objects: { dataPoint: [{ properties: { fill: color("#2B4F76") }, selector: { metadata: "daily_production_kpi.不良率" } }, { properties: { fill: color("#6B7B8D") }, selector: { metadata: "daily_production_kpi.不良率 7日移動" } }, { properties: { fill: color("#D93025") }, selector: { metadata: "daily_production_kpi.不良率 目標" } }] }, containerObjects: title("不良率の日次推移（目標 5%）") }),
    k.visual("p2_q6", 20, 450, 800, 260, "pivotTable", { Rows: { projections: [Col("dim_line", "line_name", "ライン")] }, Columns: { projections: [Col("dim_date", "calendar_date", "日付")] }, Values: { projections: [M("不良率")] } },
      { objects: { values: [k.cfByField("backColor", "不良率 色", "daily_production_kpi.不良率")], ...k.noSubTotals }, containerObjects: title("ライン × 日の不良率（直近 14 日）"), filters: k.recent14Filter() }),
    k.visual("p2_q7", 830, 450, 430, 260, "clusteredBarChart", { Category: { projections: [Col("dim_line", "line_name", "ライン")] }, Y: { projections: [M("不良率")] } },
      { objects: { dataPoint: [{ properties: { fill: k.colorByMeasure("不良率 色") }, selector: { data: [{ dataViewWildcard: { matchingOption: 1 } }] } }] }, containerObjects: title("ライン別不良率（期間）") }),
  ];
}

// ---------- ページ 3: 設備稼働 ----------
function page3(k) {
  const { M, Col } = k;
  const heat = (name, x, measure, label, colorMeasure, queryRef, heading) => k.visual(name, x, 210, 615, 230, "pivotTable", {
    Rows: { projections: [Col("dim_equipment", "equipment_name", "設備")] }, Columns: { projections: [Col("dim_date", "calendar_date", "日付")] }, Values: { projections: [M(measure, label)] },
  }, { objects: { values: [k.cfByField("backColor", colorMeasure, queryRef)], ...k.noSubTotals }, containerObjects: title(heading), filters: k.recent14Filter() });
  return [
    ...header(k, "設備稼働", "p3"),
    k.card("p3_e1", 20, 90, 250, 110, "アラート設備数 (最終日)", "アラート設備（対象日）"),
    k.card("p3_e2", 280, 90, 250, 110, "異常発生回数 (最終日)", "異常・停止回数"),
    k.card("p3_e3", 540, 90, 250, 110, "正常稼働率 (最終日)", "正常稼働率（簡易）"),
    k.textbox("p3_e4", 800, 95, 240, 100, "正常稼働率（簡易）= 計測時点のうち異常・停止でなかった割合。時間ベースの稼働率・OEE には稼働ログが必要", 9, false),
    k.visual("p3_e0", 1050, 90, 210, 110, "slicer", { Values: { projections: [Col("dim_equipment", "equipment_name", "設備")] } }, { objects: { data: [{ properties: { mode: lit("Dropdown") } }] } }),
    heat("p3_e5", 20, "平均温度 (7日)", "温度", "温度 色", "equipment_health.平均温度 (7日)", "温度 7 日平均（設備 × 日、直近 14 日）"),
    heat("p3_e6", 645, "平均振動 (7日)", "振動", "振動 色", "equipment_health.平均振動 (7日)", "振動 7 日平均（設備 × 日、直近 14 日）"),
    k.visual("p3_e7", 20, 450, 760, 260, "lineChart", {
      Category: { projections: [Col("dim_date", "calendar_date", "日付")] }, Y: { projections: [M("平均温度 (7日)", "温度 7 日平均")] }, Series: { projections: [Col("dim_equipment", "equipment_name", "設備")] },
    }, { containerObjects: title("温度 7 日平均の推移（設備別）") }),
    k.visual("p3_e8", 790, 450, 470, 260, "tableEx", {
      Values: { projections: [Col("dim_equipment", "equipment_name", "設備"), M("状態 (最終日)", "状態"), M("平均温度 (7日) (最終日)", "温度"), M("平均振動 (7日) (最終日)", "振動"), M("異常回数 (最終日・行)", "異常")] },
    }, { objects: { values: [k.cfByField("backColor", "設備状態 色 (最終日)", "dim_equipment.equipment_name"), k.cfByField("backColor", "温度 色 (最終日)", "equipment_health.平均温度 (7日) (最終日)"), k.cfByField("backColor", "振動 色 (最終日)", "equipment_health.平均振動 (7日) (最終日)")] }, containerObjects: title("設備一覧（対象日）") }),
  ];
}

// ---------- テーマ（Tech Fusion デザインシステム） ----------
const THEME = Object.freeze({
  name: "TechFusion",
  dataColors: ["#2B4F76", "#E45E32", "#6B7B8D", "#22C55E", "#F59E0B", "#223A5E", "#8A94A6", "#3B82F6", "#EF4444", "#A3B8D0"],
  background: "#FFFFFF", foreground: "#1B294B", tableAccent: "#2B4F76",
  good: "#1E8E3E", neutral: "#F9A825", bad: "#D93025",
  textClasses: { title: { fontFace: "Segoe UI Semibold", color: "#223A5E" }, label: { color: "#1B294B" } },
});

function validateArgs({ modelId, demoFrom, demoTo }) {
  if (!modelId) throw new Error("modelId is required");
  if (!isIsoDate(demoFrom) || !isIsoDate(demoTo)) throw new Error(`demo dates must be YYYY-MM-DD: ${demoFrom} / ${demoTo}`);
  if (demoFrom >= demoTo) throw new Error(`demoFrom must be before demoTo: ${demoFrom} / ${demoTo}`);
}

function reportParts({ modelId, demoFrom, pages, demoTo }) {
  const parts = {
    "definition.pbir": JSON.stringify({ version: "4.0", datasetReference: { byConnection: { connectionString: `semanticmodelid=${modelId}`, pbiServiceModelId: null, pbiModelVirtualServerName: "sobe_wowvirtualserver", pbiModelDatabaseName: modelId, name: "EntityDataSource", connectionType: "pbiServiceXmlaStyleLive" } } }),
    "definition/version.json": JSON.stringify({ $schema: `${S}/versionMetadata/1.0.0/schema.json`, version: "2.0.0" }),
    "definition/report.json": JSON.stringify({
      $schema: `${S}/report/3.0.0/schema.json`,
      themeCollection: { customTheme: { name: "TechFusion.json", reportVersionAtImport: { visual: "1.8.50", report: "2.0.50", page: "1.3.50" }, type: "RegisteredResources" } },
      resourcePackages: [{ name: "RegisteredResources", type: "RegisteredResources", items: [{ name: "TechFusion.json", path: "TechFusion.json", type: "CustomTheme" }] }],
      settings: { useStylableVisualContainerHeader: true, defaultDrillFilterOtherVisuals: true },
      filterConfig: { filters: [dateRangeFilter(demoFrom, demoTo)] },
    }),
    "StaticResources/RegisteredResources/TechFusion.json": JSON.stringify(THEME),
    "definition/pages/pages.json": JSON.stringify({ $schema: `${S}/pagesMetadata/1.0.0/schema.json`, pageOrder: pages.map((p) => p[0]), activePageName: "p1" }),
  };
  for (const [id, name, visuals] of pages) {
    parts[`definition/pages/${id}/page.json`] = JSON.stringify({ $schema: `${S}/page/2.0.0/schema.json`, name: id, displayName: name, displayOption: "FitToPage", height: 720, width: 1280 });
    for (const v of visuals) parts[`definition/pages/${id}/visuals/${v.name}/visual.json`] = JSON.stringify(v.json, null, 1);
  }
  return parts;
}

function buildReport({ modelId, measureTables, reportName, demoFrom, demoTo }) {
  validateArgs({ modelId, demoFrom, demoTo });
  const k = createKit(measureTables, dateRangeFilter(recentFrom(demoTo, 14), demoTo));
  const pages = [["p1", "生産実績", page1(k)], ["p2", "品質管理", page2(k)], ["p3", "設備稼働", page3(k)]];
  const parts = reportParts({ modelId, demoFrom, demoTo, pages });
  const body = {
    displayName: reportName,
    description: "dashboard-spec.md: 生産実績 / 品質管理 / 設備稼働",
    definition: { parts: Object.entries(parts).map(([p, c]) => ({ path: p, payload: Buffer.from(c, "utf8").toString("base64"), payloadType: "InlineBase64" })) },
  };
  return { body, parts, summary: pages.map((p) => `${p[1]}(${p[2].length})`).join(", ") };
}

function writeOutputs(outFile, result) {
  const outDir = path.join(path.dirname(outFile), "report");
  fs.rmSync(outDir, { recursive: true, force: true });
  for (const [p, c] of Object.entries(result.parts)) {
    const f = path.join(outDir, p);
    fs.mkdirSync(path.dirname(f), { recursive: true });
    fs.writeFileSync(f, c);
  }
  fs.writeFileSync(outFile, JSON.stringify(result.body));
}

function main(argv) {
  const [modelId, measureMap, reportName, demoFrom, demoTo, out] = argv;
  if (!out) throw new Error("usage: build_report.js <model_id> <measure_tables.json> <report_name> <from> <to> <out>");
  const result = buildReport({ modelId, measureTables: JSON.parse(fs.readFileSync(measureMap, "utf8")), reportName, demoFrom, demoTo });
  writeOutputs(out, result);
  console.log("pages:", result.summary, "parts:", Object.keys(result.parts).length);
}

if (require.main === module) {
  try { main(process.argv.slice(2)); } catch (e) { console.error(`build_report failed: ${e.message}`); process.exit(1); }
}

module.exports = { buildReport, writeOutputs, lit, dateRangeFilter, recentFrom, main };

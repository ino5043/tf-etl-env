// dashboard-spec.md に沿った Power BI レポート（PBIR 形式）を生成する
// Usage: node build_report.js <semantic_model_id> <measure_tables.json> <report_name> <demo_from YYYY-MM-DD> <demo_to YYYY-MM-DD（含まない）> <out_body.json>
const fs = require("fs");
const path = require("path");

const [MODEL_ID, MEASURE_MAP, REPORT_NAME, DEMO_FROM, DEMO_TO, OUT] = process.argv.slice(2);
if (!OUT) throw new Error("usage: build_report.js <model_id> <measure_tables.json> <report_name> <from> <to> <out>");
// 直近 14 日（ヒートマップ用）: 期間の終わり（含まない）から 14 日前
const RECENT_FROM = new Date(Date.parse(`${DEMO_TO}T00:00:00Z`) - 14 * 86400000).toISOString().slice(0, 10);
const MT = JSON.parse(fs.readFileSync(MEASURE_MAP, "utf8"));
const S = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition";

// ---------- フィールド参照 ----------
const M = (name, displayName) => {
  const table = MT[name];
  if (!table) throw new Error(`unknown measure: ${name}`);
  return { field: { Measure: { Expression: { SourceRef: { Entity: table } }, Property: name } }, queryRef: `${table}.${name}`, nativeQueryRef: name, ...(displayName ? { displayName } : {}) };
};
const Col = (table, column, displayName) => ({ field: { Column: { Expression: { SourceRef: { Entity: table } }, Property: column } }, queryRef: `${table}.${column}`, nativeQueryRef: column, ...(displayName ? { displayName } : {}) });
const measureExpr = (name) => ({ Measure: { Expression: { SourceRef: { Entity: MT[name] } }, Property: name } });
const lit = (v) => ({ expr: { Literal: { Value: typeof v === "string" ? `'${v}'` : `${v}D` } } });
const bool = (b) => ({ expr: { Literal: { Value: b ? "true" : "false" } } });
const color = (hex) => ({ solid: { color: { expr: { Literal: { Value: `'${hex}'` } } } } });
const colorByMeasure = (name) => ({ solid: { color: { expr: measureExpr(name) } } });

// 条件付き書式（フィールド値）: 値列 queryRef に対して色メジャーを当てる
const cfByField = (prop, colorMeasure, targetQueryRef) => ({
  properties: { [prop]: colorByMeasure(colorMeasure) },
  selector: { data: [{ dataViewWildcard: { matchingOption: 1 } }], metadata: targetQueryRef },
});

const title = (text) => ({ title: [{ properties: { show: bool(true), text: lit(text), fontSize: lit(11) } }] });

let z = 0;
function visual(name, x, y, w, h, visualType, queryState, extra = {}) {
  z += 100;
  const v = { visualType, ...(queryState ? { query: { queryState } } : {}), ...(extra.objects ? { objects: extra.objects } : {}), drillFilterOtherVisuals: true };
  if (extra.containerObjects) v.visualContainerObjects = extra.containerObjects;
  const json = { $schema: `${S}/visualContainer/2.0.0/schema.json`, name, position: { x, y, z, width: w, height: h }, visual: v };
  if (extra.filters) json.filterConfig = { filters: extra.filters };
  return { name, json };
}

// ヒートマップ（ライン・設備 × 日）は直近 14 日に絞る（dashboard-spec.md Q6 / E5 / E6）
const RECENT_14_DAYS = () => [{ ...dateRangeFilter(RECENT_FROM, DEMO_TO), name: "recent_14_days" }];

const card = (name, x, y, w, h, measure, label, size = 20) =>
  visual(name, x, y, w, h, "card", { Values: { projections: [M(measure, label)] } }, {
    // labelDisplayUnits 1 = 単位の省略なし（21.31K ではなく 21,311）
    objects: { labels: [{ properties: { fontSize: lit(size), labelDisplayUnits: lit(1) } }], categoryLabels: [{ properties: { show: bool(true), fontSize: lit(10) } }] },
  });

const textbox = (name, x, y, w, h, text, size = 20, bold = true) => ({
  name,
  json: {
    $schema: `${S}/visualContainer/2.0.0/schema.json`, name, position: { x, y, z: (z += 100), width: w, height: h },
    visual: { visualType: "textbox", objects: { general: [{ properties: { paragraphs: [{ textRuns: [{ value: text, textStyle: { fontSize: `${size}pt`, fontWeight: bold ? "bold" : "normal", color: "#223A5E" } }] }] } }] } },
  },
});

// ---------- 共通ヘッダー ----------
function header(pageTitle, prefix) {
  return [
    textbox(`${prefix}_h1`, 20, 12, 250, 60, pageTitle, 22, true),
    card(`${prefix}_h2`, 280, 8, 170, 70, "最終日", "対象日", 16),
    card(`${prefix}_h3`, 460, 8, 210, 70, "最終集計日時 (JST)", "データ更新（JST）", 14),
    visual(`${prefix}_h4`, 680, 8, 340, 70, "slicer", { Values: { projections: [Col("dim_date", "calendar_date", "期間")] } }, {
      objects: { data: [{ properties: { mode: lit("Between") } }], header: [{ properties: { show: bool(true) } }] },
    }),
    visual(`${prefix}_h5`, 1030, 8, 230, 70, "slicer", { Values: { projections: [Col("dim_line", "line_name", "ライン")] } }, {
      objects: { data: [{ properties: { mode: lit("Dropdown") } }], selection: [{ properties: { selectAllCheckboxEnabled: bool(true) } }] },
    }),
  ];
}

// ---------- ページ 1: 生産実績 ----------
const page1 = [
  ...header("生産実績", "p1"),
  card("p1_p1", 20, 90, 230, 110, "実績数 (最終日)", "実績数（対象日）"),
  card("p1_p2", 260, 90, 230, 110, "達成率 (最終日)", "達成率（対象日）"),
  card("p1_p3", 500, 90, 230, 110, "計画差異 (最終日)", "計画差異（対象日）"),
  card("p1_p4", 740, 90, 230, 110, "実績数 7日移動平均 (最終日)", "7 日平均（稼働日）"),
  visual("p1_p5", 980, 90, 280, 110, "gauge", {
    Y: { projections: [M("達成率 (最終日)", "達成率")] }, TargetValue: { projections: [M("達成率 目標", "目標")] },
  }, { objects: { axis: [{ properties: { min: lit(0), max: lit(1.2) } }] }, containerObjects: title("達成率と目標") }),
  visual("p1_p6", 20, 210, 1240, 250, "lineClusteredColumnComboChart", {
    Category: { projections: [Col("dim_date", "calendar_date", "日付")] },
    Y: { projections: [M("計画数"), M("実績数")] },
    Y2: { projections: [M("達成率"), M("達成率 7日移動")] },
  }, { objects: { dataPoint: [{ properties: { fill: color("#C9D1DC") }, selector: { metadata: "daily_production_kpi.計画数" } }, { properties: { fill: color("#2B4F76") }, selector: { metadata: "daily_production_kpi.実績数" } }, { properties: { fill: color("#E45E32") }, selector: { metadata: "daily_production_kpi.達成率" } }] }, containerObjects: title("計画数・実績数と達成率の日次推移") }),
  visual("p1_p7", 20, 470, 760, 240, "pivotTable", {
    Rows: { projections: [Col("dim_line", "line_name", "ライン")] },
    Values: { projections: [M("計画数 (最終日)", "計画"), M("実績数 (最終日)", "実績"), M("計画差異 (最終日)", "差異"), M("達成率 (最終日)", "達成率"), M("実績数 前日比 (最終日)", "前日比")] },
  }, { objects: { values: [cfByField("backColor", "達成率 色 (最終日)", "daily_production_kpi.達成率 (最終日)")] }, containerObjects: title("ライン別（対象日）") }),
  visual("p1_p8", 790, 470, 470, 240, "barChart", {
    Category: { projections: [Col("dim_line", "line_name", "ライン")] },
    Y: { projections: [M("日勤実績数", "日勤"), M("夜勤実績数", "夜勤")] },
  }, { containerObjects: title("シフト別実績（期間）") }),
];

// ---------- ページ 2: 品質管理 ----------
const page2 = [
  ...header("品質管理", "p2"),
  card("p2_q1", 20, 90, 300, 110, "不良率 (最終日)", "不良率（対象日）"),
  card("p2_q2", 330, 90, 300, 110, "歩留まり率 (最終日)", "歩留まり率（対象日）"),
  card("p2_q3", 640, 90, 300, 110, "不良数 (最終日)", "不良数（対象日）"),
  card("p2_q4", 950, 90, 310, 110, "不良率 目標超過ライン数 (最終日)", "目標超過ライン"),
  visual("p2_q5", 20, 210, 1240, 230, "lineChart", {
    Category: { projections: [Col("dim_date", "calendar_date", "日付")] },
    Y: { projections: [M("不良率"), M("不良率 7日移動"), M("不良率 目標", "目標 5%")] },
  }, { objects: { dataPoint: [{ properties: { fill: color("#2B4F76") }, selector: { metadata: "daily_production_kpi.不良率" } }, { properties: { fill: color("#6B7B8D") }, selector: { metadata: "daily_production_kpi.不良率 7日移動" } }, { properties: { fill: color("#D93025") }, selector: { metadata: "daily_production_kpi.不良率 目標" } }] }, containerObjects: title("不良率の日次推移（目標 5%）") }),
  visual("p2_q6", 20, 450, 800, 260, "pivotTable", {
    Rows: { projections: [Col("dim_line", "line_name", "ライン")] },
    Columns: { projections: [Col("dim_date", "calendar_date", "日付")] },
    Values: { projections: [M("不良率")] },
  }, { objects: { values: [cfByField("backColor", "不良率 色", "daily_production_kpi.不良率")], subTotals: [{ properties: { rowSubtotals: bool(false), columnSubtotals: bool(false) } }] }, containerObjects: title("ライン × 日の不良率（直近 14 日）"), filters: RECENT_14_DAYS() }),
  visual("p2_q7", 830, 450, 430, 260, "clusteredBarChart", {
    Category: { projections: [Col("dim_line", "line_name", "ライン")] },
    Y: { projections: [M("不良率")] },
  }, { objects: { dataPoint: [{ properties: { fill: colorByMeasure("不良率 色") }, selector: { data: [{ dataViewWildcard: { matchingOption: 1 } }] } }] }, containerObjects: title("ライン別不良率（期間）") }),
];

// ---------- ページ 3: 設備稼働 ----------
const page3 = [
  ...header("設備稼働", "p3"),
  card("p3_e1", 20, 90, 250, 110, "アラート設備数 (最終日)", "アラート設備（対象日）"),
  card("p3_e2", 280, 90, 250, 110, "異常発生回数 (最終日)", "異常・停止回数"),
  card("p3_e3", 540, 90, 250, 110, "正常稼働率 (最終日)", "正常稼働率（簡易）"),
  textbox("p3_e4", 800, 95, 240, 100, "正常稼働率（簡易）= 計測時点のうち異常・停止でなかった割合。時間ベースの稼働率・OEE には稼働ログが必要", 9, false),
  visual("p3_e0", 1050, 90, 210, 110, "slicer", { Values: { projections: [Col("dim_equipment", "equipment_name", "設備")] } }, {
    objects: { data: [{ properties: { mode: lit("Dropdown") } }] },
  }),
  visual("p3_e5", 20, 210, 615, 230, "pivotTable", {
    Rows: { projections: [Col("dim_equipment", "equipment_name", "設備")] },
    Columns: { projections: [Col("dim_date", "calendar_date", "日付")] },
    Values: { projections: [M("平均温度 (7日)", "温度")] },
  }, { objects: { values: [cfByField("backColor", "温度 色", "equipment_health.平均温度 (7日)")], subTotals: [{ properties: { rowSubtotals: bool(false), columnSubtotals: bool(false) } }] }, containerObjects: title("温度 7 日平均（設備 × 日、直近 14 日）"), filters: RECENT_14_DAYS() }),
  visual("p3_e6", 645, 210, 615, 230, "pivotTable", {
    Rows: { projections: [Col("dim_equipment", "equipment_name", "設備")] },
    Columns: { projections: [Col("dim_date", "calendar_date", "日付")] },
    Values: { projections: [M("平均振動 (7日)", "振動")] },
  }, { objects: { values: [cfByField("backColor", "振動 色", "equipment_health.平均振動 (7日)")], subTotals: [{ properties: { rowSubtotals: bool(false), columnSubtotals: bool(false) } }] }, containerObjects: title("振動 7 日平均（設備 × 日、直近 14 日）"), filters: RECENT_14_DAYS() }),
  visual("p3_e7", 20, 450, 760, 260, "lineChart", {
    Category: { projections: [Col("dim_date", "calendar_date", "日付")] },
    Y: { projections: [M("平均温度 (7日)", "温度 7 日平均")] },
    Series: { projections: [Col("dim_equipment", "equipment_name", "設備")] },
  }, { containerObjects: title("温度 7 日平均の推移（設備別）") }),
  visual("p3_e8", 790, 450, 470, 260, "tableEx", {
    Values: { projections: [Col("dim_equipment", "equipment_name", "設備"), M("状態 (最終日)", "状態"), M("平均温度 (7日) (最終日)", "温度"), M("平均振動 (7日) (最終日)", "振動"), M("異常回数 (最終日・行)", "異常")] },
  }, { objects: { values: [cfByField("backColor", "設備状態 色 (最終日)", "dim_equipment.equipment_name"), cfByField("backColor", "温度 色 (最終日)", "equipment_health.平均温度 (7日) (最終日)"), cfByField("backColor", "振動 色 (最終日)", "equipment_health.平均振動 (7日) (最終日)")] }, containerObjects: title("設備一覧（対象日）") }),
];

// デモデータの期間（from 以上 to 未満）でレポート全体を絞り込む。本番運用では相対日付フィルターに置き換える
function dateRangeFilter(from, to) {
  const col = { Column: { Expression: { SourceRef: { Source: "d" } }, Property: "calendar_date" } };
  const cmp = (kind, date) => ({ Comparison: { ComparisonKind: kind, Left: col, Right: { Literal: { Value: `datetime'${date}T00:00:00'` } } } });
  return {
    name: "demo_period", type: "Advanced", howCreated: "User",
    field: { Column: { Expression: { SourceRef: { Entity: "dim_date" } }, Property: "calendar_date" } },
    filter: { Version: 2, From: [{ Name: "d", Entity: "dim_date", Type: 0 }], Where: [{ Condition: { And: { Left: cmp(2, from), Right: cmp(3, to) } } }] },
  };
}

// ---------- テーマ（Tech Fusion デザインシステム） ----------
const theme = {
  name: "TechFusion",
  dataColors: ["#2B4F76", "#E45E32", "#6B7B8D", "#22C55E", "#F59E0B", "#223A5E", "#8A94A6", "#3B82F6", "#EF4444", "#A3B8D0"],
  background: "#FFFFFF", foreground: "#1B294B", tableAccent: "#2B4F76",
  good: "#1E8E3E", neutral: "#F9A825", bad: "#D93025",
  textClasses: { title: { fontFace: "Segoe UI Semibold", color: "#223A5E" }, label: { color: "#1B294B" } },
};

// ---------- パーツ組み立て ----------
const pages = [["p1", "生産実績", page1], ["p2", "品質管理", page2], ["p3", "設備稼働", page3]];
const parts = {
  "definition.pbir": JSON.stringify({ version: "4.0", datasetReference: { byConnection: { connectionString: `semanticmodelid=${MODEL_ID}`, pbiServiceModelId: null, pbiModelVirtualServerName: "sobe_wowvirtualserver", pbiModelDatabaseName: MODEL_ID, name: "EntityDataSource", connectionType: "pbiServiceXmlaStyleLive" } } }),
  "definition/version.json": JSON.stringify({ $schema: `${S}/versionMetadata/1.0.0/schema.json`, version: "2.0.0" }),
  "definition/report.json": JSON.stringify({
    $schema: `${S}/report/3.0.0/schema.json`,
    themeCollection: { customTheme: { name: "TechFusion.json", reportVersionAtImport: { visual: "1.8.50", report: "2.0.50", page: "1.3.50" }, type: "RegisteredResources" } },
    resourcePackages: [{ name: "RegisteredResources", type: "RegisteredResources", items: [{ name: "TechFusion.json", path: "TechFusion.json", type: "CustomTheme" }] }],
    settings: { useStylableVisualContainerHeader: true, defaultDrillFilterOtherVisuals: true },
    filterConfig: { filters: [dateRangeFilter(DEMO_FROM, DEMO_TO)] },
  }),
  "StaticResources/RegisteredResources/TechFusion.json": JSON.stringify(theme),
  "definition/pages/pages.json": JSON.stringify({ $schema: `${S}/pagesMetadata/1.0.0/schema.json`, pageOrder: pages.map((p) => p[0]), activePageName: "p1" }),
};
for (const [id, name, visuals] of pages) {
  parts[`definition/pages/${id}/page.json`] = JSON.stringify({ $schema: `${S}/page/2.0.0/schema.json`, name: id, displayName: name, displayOption: "FitToPage", height: 720, width: 1280 });
  for (const v of visuals) parts[`definition/pages/${id}/visuals/${v.name}/visual.json`] = JSON.stringify(v.json, null, 1);
}

const outDir = path.join(path.dirname(OUT), "report");
fs.rmSync(outDir, { recursive: true, force: true });
for (const [p, c] of Object.entries(parts)) {
  const f = path.join(outDir, p);
  fs.mkdirSync(path.dirname(f), { recursive: true });
  fs.writeFileSync(f, c);
}
fs.writeFileSync(OUT, JSON.stringify({
  displayName: REPORT_NAME,
  description: "dashboard-spec.md: 生産実績 / 品質管理 / 設備稼働",
  definition: { parts: Object.entries(parts).map(([p, c]) => ({ path: p, payload: Buffer.from(c, "utf8").toString("base64"), payloadType: "InlineBase64" })) },
}));
console.log("pages:", pages.map((p) => `${p[1]}(${p[2].length})`).join(", "), "parts:", Object.keys(parts).length);

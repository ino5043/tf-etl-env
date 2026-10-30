// semantic-model.md / dax-measures.md / dax-measures.dax と実テーブル定義から、Direct Lake セマンティックモデルの TMDL 定義を生成する
// Usage: node build_model.js <powerbi_dir> <schema.json> <sql_server> <sql_endpoint_id> <model_name> <out_body.json>
// 出力: <out_body.json>（作成 API の本文）、同じフォルダーに tmdl/（定義ファイル）と measure_tables.json（メジャー → ホームテーブル）
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const [PBI_DIR, SCHEMA, SQL_SERVER, SQL_EP, MODEL_NAME, OUT] = process.argv.slice(2);
if (!OUT) throw new Error("usage: build_model.js <powerbi_dir> <schema.json> <sql_server> <sql_endpoint_id> <model_name> <out_body.json>");
const schema = JSON.parse(fs.readFileSync(SCHEMA, "utf8"));
const uuid = () => crypto.randomUUID();
const T = (n) => "\t".repeat(n);
const q = (name) => (/^[A-Za-z_][A-Za-z0-9_]*$/.test(name) ? name : `'${name.replace(/'/g, "''")}'`);

// ---- 1. メジャー（式は .dax、書式・表示フォルダーは .md の表） ----
function parseDax(text) {
  const body = text.split(/\r?\n/);
  const measures = [];
  let cur = null;
  for (const line of body) {
    const m = line.match(/^\s*MEASURE\s+([A-Za-z_]+)\[(.+?)\]\s*=\s*(.*)$/);
    if (m) {
      if (cur) measures.push(cur);
      cur = { table: m[1], name: m[2], lines: m[3].trim() ? [m[3].trim()] : [] };
      continue;
    }
    if (/^\s*EVALUATE\b/.test(line)) { if (cur) measures.push(cur); cur = null; break; }
    // DEFINE 直下（インデント 4 以下）の VAR やコメントはメジャー本体ではないので、そこで区切る
    if (cur && line.trim() !== "" && line.match(/^\s*/)[0].length <= 4) { measures.push(cur); cur = null; continue; }
    if (cur) cur.lines.push(line);
  }
  if (cur) measures.push(cur);
  return measures.map((m) => {
    const lines = m.lines.filter((l, i, a) => !(l.trim() === "" && (i === a.length - 1 || a.slice(i).every((x) => x.trim() === ""))));
    const indent = Math.min(...lines.filter((l) => l.trim()).map((l) => l.match(/^\s*/)[0].length).filter((n, i) => i > 0 || lines.length === 1), 99);
    return { ...m, expr: lines.map((l, i) => (i === 0 && m.lines[0] === l && !/^\s/.test(l) ? l : l.slice(Math.min(indent, l.match(/^\s*/)[0].length)))).join("\n").trim() };
  });
}

function parseMdMeta(text) {
  const meta = {};
  let folder = null;
  for (const line of text.split(/\r?\n/)) {
    const h = line.match(/^#{3,4}\s+[\d.]*\s*`([^`]+)`/);
    if (h) { folder = h[1]; continue; }
    const r = line.match(/^\|([^|]*)\|\s*([^|]*)\|/);
    if (r && folder) {
      const names = [...r[1].matchAll(/`([^`]+)`/g)].map((x) => x[1]);
      if (names.length === 0) continue;
      const fmt = r[2].trim().replace(/^`|`$/g, "");
      // 「判定」など説明文の列を書式と取り違えないよう、書式らしい文字列だけを採用する
      const unquoted = fmt.replace(/"[^"]*"/g, "");
      const looksLikeFormat = fmt !== "" && /^[#0.,%yYmMdDhHnNsS/ :\\+\-;()]+$/.test(unquoted);
      // 同じ名前が後の章（検証など）に再登場しても、最初の定義を優先する
      for (const n of names) if (!meta[n]) meta[n] = { folder, format: looksLikeFormat ? fmt : null };
    }
  }
  return meta;
}

const measures = parseDax(fs.readFileSync(path.join(PBI_DIR, "dax-measures.dax"), "utf8"));
const meta = parseMdMeta(fs.readFileSync(path.join(PBI_DIR, "dax-measures.md"), "utf8"));
const missingMeta = measures.filter((m) => !meta[m.name]).map((m) => m.name);

// ---- 2. テーブル ----
const TABLES = {
  daily_production_kpi: { schema: "gold", entity: "daily_production_kpi", show: [] },
  equipment_health: { schema: "gold", entity: "equipment_health", show: ["status_summary"] },
  dim_date: { schema: "gold", entity: "dim_date", dateTable: "calendar_date", sortBy: { year_month: "year_month_sort", weekday_jp: "weekday_no" }, hide: ["year_month_sort", "weekday_no"] },
  dim_line: { schema: "gold", entity: "dim_line", hide: [] },
  dim_equipment: { schema: "gold", entity: "dim_equipment", hide: [] },
  dim_status: { schema: "gold", entity: "dim_status", sortBy: { status: "severity_order" }, hide: ["severity_order"] },
};
const FACTS = ["daily_production_kpi", "equipment_health"];
const typeMap = { string: "string", bigint: "int64", int: "int64", double: "double", boolean: "boolean", date: "dateTime", timestamp: "dateTime" };
const fmtMap = { date: "yyyy/mm/dd", timestamp: "yyyy/mm/dd hh:nn", int64: "#,0", double: "#,0.00" };

// 設計書に書式の列がないメジャー（閾値など）は名前から書式を補う。色・状態（文字列）は書式なし
function fallbackFormat(name) {
  if (/色|状態/.test(name)) return null;
  if (/^(達成率|不良率|歩留まり率)/.test(name)) return "0.0%";
  if (/^温度/.test(name)) return '0.0" ℃"';
  if (/^振動/.test(name)) return '0.00" mm/s"';
  if (/数/.test(name)) return "#,0";
  return null;
}

function tableTmdl(name) {
  const cfg = TABLES[name];
  const cols = schema[`${cfg.schema}.${cfg.entity}`];
  if (!cols) throw new Error(`schema not found: ${cfg.schema}.${cfg.entity}`);
  const out = [`table ${q(name)}`, `${T(1)}lineageTag: ${uuid()}`];
  if (cfg.dateTable) out.push(`${T(1)}dataCategory: Time`);
  out.push("");
  for (const m of measures.filter((x) => x.table === name)) {
    const exprLines = m.expr.split("\n");
    if (exprLines.length === 1) out.push(`${T(1)}measure ${q(m.name)} = ${exprLines[0]}`);
    else { out.push(`${T(1)}measure ${q(m.name)} =`); exprLines.forEach((l) => out.push(`${T(3)}${l}`)); }
    const mm = meta[m.name] || {};
    const fmt = mm.format || fallbackFormat(m.name);
    if (fmt) out.push(`${T(2)}formatString: ${fmt}`);
    if (mm.folder) out.push(`${T(2)}displayFolder: ${mm.folder}`);
    out.push(`${T(2)}lineageTag: ${uuid()}`, "");
  }
  for (const c of cols) {
    const dt = typeMap[c.type];
    if (!dt) throw new Error(`unknown type ${c.type} for ${name}.${c.name}`);
    const isFact = FACTS.includes(name);
    const hidden = isFact ? !(cfg.show || []).includes(c.name) : (cfg.hide || []).includes(c.name);
    out.push(`${T(1)}column ${q(c.name)}`, `${T(2)}dataType: ${dt}`);
    if (cfg.dateTable === c.name) out.push(`${T(2)}isKey`);
    if (hidden) out.push(`${T(2)}isHidden`);
    // ディメンションの整数（年・月・並べ替えキー等）は桁区切りなし
    const f = !isFact && dt === "int64" ? "0" : fmtMap[c.type] || fmtMap[dt];
    if (f && dt !== "string" && dt !== "boolean") out.push(`${T(2)}formatString: ${f}`);
    out.push(`${T(2)}lineageTag: ${uuid()}`, `${T(2)}summarizeBy: none`, `${T(2)}sourceColumn: ${c.name}`);
    if (cfg.sortBy && cfg.sortBy[c.name]) out.push(`${T(2)}sortByColumn: ${q(cfg.sortBy[c.name])}`);
    out.push("");
  }
  out.push(`${T(1)}partition ${q(name)} = entity`, `${T(2)}mode: directLake`, `${T(2)}source`,
    `${T(3)}entityName: ${cfg.entity}`, `${T(3)}schemaName: ${cfg.schema}`, `${T(3)}expressionSource: DatabaseQuery`, "");
  return out.join("\n");
}

const RELS = [
  ["daily_production_kpi", "production_date", "dim_date", "calendar_date"],
  ["daily_production_kpi", "line_id", "dim_line", "line_id"],
  ["equipment_health", "production_date", "dim_date", "calendar_date"],
  ["equipment_health", "line_id", "dim_line", "line_id"],
  ["equipment_health", "equipment_id", "dim_equipment", "equipment_id"],
  ["equipment_health", "status_summary", "dim_status", "status"],
];
const relTmdl = RELS.map(([ft, fc, tt, tc]) => [`relationship ${uuid()}`, `${T(1)}fromColumn: ${q(ft)}.${q(fc)}`, `${T(1)}toColumn: ${q(tt)}.${q(tc)}`, ""].join("\n")).join("\n");

const modelTmdl = [
  "model Model", `${T(1)}culture: ja-JP`, `${T(1)}defaultPowerBIDataSourceVersion: powerBI_V3`, `${T(1)}discourageImplicitMeasures`, `${T(1)}sourceQueryCulture: ja-JP`, "",
  ...Object.keys(TABLES).map((t) => `ref table ${q(t)}`), "",
].join("\n");

const exprTmdl = [
  "expression DatabaseQuery =", `${T(2)}let`, `${T(3)}database = Sql.Database("${SQL_SERVER}", "${SQL_EP}")`, `${T(2)}in`, `${T(3)}database`,
  `${T(1)}lineageTag: ${uuid()}`, "",
].join("\n");

const parts = {
  "definition.pbism": JSON.stringify({ version: "4.0", settings: {} }),
  "definition/database.tmdl": "database\n\tcompatibilityLevel: 1604\n",
  "definition/model.tmdl": modelTmdl,
  "definition/expressions.tmdl": exprTmdl,
  "definition/relationships.tmdl": relTmdl,
  ...Object.fromEntries(Object.keys(TABLES).map((t) => [`definition/tables/${t}.tmdl`, tableTmdl(t)])),
};

fs.mkdirSync(path.join(path.dirname(OUT), "tmdl"), { recursive: true });
for (const [p, c] of Object.entries(parts)) {
  const f = path.join(path.dirname(OUT), "tmdl", p);
  fs.mkdirSync(path.dirname(f), { recursive: true });
  fs.writeFileSync(f, c);
}
const body = {
  displayName: MODEL_NAME,
  description: "Direct Lake model on factory_lakehouse (gold)",
  definition: { parts: Object.entries(parts).map(([p, c]) => ({ path: p, payload: Buffer.from(c, "utf8").toString("base64"), payloadType: "InlineBase64" })) },
};
fs.writeFileSync(OUT, JSON.stringify(body));
fs.writeFileSync(path.join(path.dirname(OUT), "measure_tables.json"), JSON.stringify(Object.fromEntries(measures.map((m) => [m.name, m.table])), null, 1));
console.log(`measures: ${measures.length} (no format/folder meta: ${missingMeta.length}${missingMeta.length ? " -> " + missingMeta.join(", ") : ""})`);
console.log("by table:", JSON.stringify(measures.reduce((a, m) => ({ ...a, [m.table]: (a[m.table] || 0) + 1 }), {})));
console.log("parts:", Object.keys(parts).length, "bytes:", fs.statSync(OUT).size);

// dax-measures.dax / dax-measures.md と実テーブル定義から、Direct Lake セマンティックモデルの TMDL 定義を生成する
// Usage: node build_model.js <powerbi_dir> <schema.json> <sql_server> <sql_endpoint_id> <model_name> <out_body.json>
// 出力: <out_body.json>（作成 API の本文）、同じフォルダーに tmdl/（定義ファイル）と measure_tables.json（メジャー → ホームテーブル）
"use strict";
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const T = (n) => "\t".repeat(n);
const q = (name) => (/^[A-Za-z_][A-Za-z0-9_]*$/.test(name) ? name : `'${name.replace(/'/g, "''")}'`);
const TAB_WIDTH = 4;

// ---- 1. メジャー（式は .dax、書式・表示フォルダーは .md の表） ----
function expandTabs(line) {
  let out = "";
  for (const ch of line) out += ch === "\t" ? " ".repeat(TAB_WIDTH - (out.length % TAB_WIDTH)) : ch;
  return out;
}
const indentOf = (line) => line.match(/^ */)[0].length;

// MEASURE <table>[<name>] = <式の先頭>。table は識別子（数字可）または '...'（'' はエスケープ）、name 内の ]] は ]
const MEASURE_RE = /^( *)MEASURE\s+('(?:[^']|'')+'|[A-Za-z_][A-Za-z0-9_]*)\s*\[((?:[^\]]|\]\])+)\]\s*=\s*(.*)$/;
const IS_MEASURE_LINE = /^\s*MEASURE\b/;
const ENDS_ALL = /^\s*(EVALUATE|DEFINE)\b/;
// 式の途中としか読めない行（閉じ括弧・演算子で始まる行。// や -- のコメントは除く）が浅いインデントで現れたら、解析を誤っているので止める
const UNEXPECTED_DEDENT = /^\s*(?:[)\],+*&|]|-(?!-)|\/(?!\/))/;
const unquoteTable = (t) => (t.startsWith("'") ? t.slice(1, -1).replace(/''/g, "'") : t);

function finishMeasure(m) {
  const body = [...m.body];
  while (body.length && body[body.length - 1].trim() === "") body.pop();
  const nonBlank = body.filter((l) => l.trim() !== "");
  const minIndent = nonBlank.length ? Math.min(...nonBlank.map(indentOf)) : 0;
  const lines = [...(m.head ? [m.head] : []), ...body.map((l) => (l.trim() === "" ? "" : l.slice(minIndent).trimEnd()))];
  const expr = lines.join("\n").trim();
  // 式が空（本体が MEASURE 行と同じインデントに書かれた、本体がない、コメントだけ）なら解析を誤っているので止める
  if (expr.split("\n").every((l) => l.trim() === "" || /^\s*(\/\/|--)/.test(l))) throw new Error(`empty expression for measure ${m.table}[${m.name}]（本体は MEASURE 行より深くインデントする）`);
  return Object.freeze({ table: m.table, name: m.name, expr });
}

function parseDax(text) {
  const lines = text.split(/\r?\n/).map(expandTabs);
  const measures = [];
  let cur = null;
  for (const [i, line] of lines.entries()) {
    const m = line.match(MEASURE_RE);
    const blank = line.trim() === "";
    // 式の終わり: MEASURE 行と同じかそれより浅いインデントの空でない行（次のメジャー、DEFINE 直下の VAR やコメント、EVALUATE）
    if (cur && !blank && (m || ENDS_ALL.test(line) || indentOf(line) <= cur.indent)) {
      if (!m && UNEXPECTED_DEDENT.test(line)) throw new Error(`dax line ${i + 1}: unexpected dedent inside measure ${cur.name}`);
      measures.push(finishMeasure(cur));
      cur = null;
    }
    if (m) cur = { indent: m[1].length, table: unquoteTable(m[2]), name: m[3].replace(/\]\]/g, "]"), head: m[4].trim(), body: [] };
    else if (cur) cur.body.push(line);
  }
  if (cur) measures.push(finishMeasure(cur));
  const declared = lines.filter((l) => IS_MEASURE_LINE.test(l)).length;
  if (declared !== measures.length) throw new Error(`dax parse mismatch: MEASURE lines: ${declared}, parsed: ${measures.length}`);
  const dup = measures.map((x) => x.name).find((n, i, a) => a.indexOf(n) !== i);
  if (dup) throw new Error(`duplicate measure name: ${dup}`);
  return measures;
}

// 「判定」など説明文の列を書式と取り違えないよう、書式らしい文字列だけを採用する:
// 引用符の外が書式記号だけで構成され、かつ数値の桁（0 / #）か % を含むか、日付の形（yyyy、または mm と dd の両方）であること。
// mm/s・pt のような単位や説明の単語は書式とみなさない
function looksLikeFormat(fmt) {
  const unquoted = String(fmt).replace(/"[^"]*"/g, "");
  if (unquoted.trim() === "" || !/^[#0.,%yYmMdDhHnNsS/ :\\+\-;()]+$/.test(unquoted)) return false;
  return /[0#%]/.test(unquoted) || /yyyy/i.test(unquoted) || (/mm/i.test(unquoted) && /dd/i.test(unquoted));
}

function parseMdMeta(text) {
  const meta = {};
  let folder = null;
  for (const line of text.split(/\r?\n/)) {
    const h = line.match(/^#{3,4}\s+[\d.]*\s*`([^`]+)`/);
    if (h) { folder = h[1]; continue; }
    const r = line.match(/^\|([^|]*)\|\s*([^|]*)\|/);
    if (!r || !folder) continue;
    const names = [...r[1].matchAll(/`([^`]+)`/g)].map((x) => x[1]);
    const fmt = r[2].trim().replace(/^`|`$/g, "");
    // 同じ名前が後の章（検証など）に再登場しても、最初の定義を優先する
    for (const n of names) if (!meta[n]) meta[n] = { folder, format: looksLikeFormat(fmt) ? fmt : null };
  }
  return meta;
}

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
const TYPE_MAP = { string: "string", bigint: "int64", int: "int64", double: "double", boolean: "boolean", date: "dateTime", timestamp: "dateTime" };
const FMT_MAP = { date: "yyyy/mm/dd", timestamp: "yyyy/mm/dd hh:nn", int64: "#,0", double: "#,0.00" };
const RELS = [
  ["daily_production_kpi", "production_date", "dim_date", "calendar_date"],
  ["daily_production_kpi", "line_id", "dim_line", "line_id"],
  ["equipment_health", "production_date", "dim_date", "calendar_date"],
  ["equipment_health", "line_id", "dim_line", "line_id"],
  ["equipment_health", "equipment_id", "dim_equipment", "equipment_id"],
  ["equipment_health", "status_summary", "dim_status", "status"],
];

// 設計書に書式の列がないメジャー（閾値など）は名前から書式を補う。色・状態（文字列）は書式なし
function fallbackFormat(name) {
  if (/色|状態/.test(name)) return null;
  if (/^(達成率|不良率|歩留まり率)/.test(name)) return "0.0%";
  if (/^温度/.test(name)) return '0.0" ℃"';
  if (/^振動/.test(name)) return '0.00" mm/s"';
  if (/数/.test(name)) return "#,0";
  return null;
}

function measureTmdl(m, meta, uuid) {
  const exprLines = m.expr.split("\n");
  const out = exprLines.length === 1 ? [`${T(1)}measure ${q(m.name)} = ${exprLines[0]}`] : [`${T(1)}measure ${q(m.name)} =`, ...exprLines.map((l) => `${T(3)}${l}`)];
  const mm = meta[m.name] || {};
  const fmt = mm.format || fallbackFormat(m.name);
  if (fmt) out.push(`${T(2)}formatString: ${fmt}`);
  if (mm.folder) out.push(`${T(2)}displayFolder: ${mm.folder}`);
  return [...out, `${T(2)}lineageTag: ${uuid()}`, ""];
}

function columnTmdl(name, cfg, c, uuid) {
  const dt = TYPE_MAP[c.type];
  if (!dt) throw new Error(`unknown type ${c.type} for ${name}.${c.name}`);
  const isFact = FACTS.includes(name);
  const hidden = isFact ? !(cfg.show || []).includes(c.name) : (cfg.hide || []).includes(c.name);
  const out = [`${T(1)}column ${q(c.name)}`, `${T(2)}dataType: ${dt}`];
  if (cfg.dateTable === c.name) out.push(`${T(2)}isKey`);
  if (hidden) out.push(`${T(2)}isHidden`);
  // ディメンションの整数（年・月・並べ替えキー等）は桁区切りなし
  const f = !isFact && dt === "int64" ? "0" : FMT_MAP[c.type] || FMT_MAP[dt];
  if (f && dt !== "string" && dt !== "boolean") out.push(`${T(2)}formatString: ${f}`);
  out.push(`${T(2)}lineageTag: ${uuid()}`, `${T(2)}summarizeBy: none`, `${T(2)}sourceColumn: ${c.name}`);
  if (cfg.sortBy && cfg.sortBy[c.name]) out.push(`${T(2)}sortByColumn: ${q(cfg.sortBy[c.name])}`);
  return [...out, ""];
}

function tableTmdl(name, { schema, measures, meta, uuid }) {
  const cfg = TABLES[name];
  const cols = schema[`${cfg.schema}.${cfg.entity}`];
  if (!cols) throw new Error(`schema not found: ${cfg.schema}.${cfg.entity}`);
  return [
    `table ${q(name)}`, `${T(1)}lineageTag: ${uuid()}`, ...(cfg.dateTable ? [`${T(1)}dataCategory: Time`] : []), "",
    ...measures.filter((x) => x.table === name).flatMap((m) => measureTmdl(m, meta, uuid)),
    ...cols.flatMap((c) => columnTmdl(name, cfg, c, uuid)),
    `${T(1)}partition ${q(name)} = entity`, `${T(2)}mode: directLake`, `${T(2)}source`,
    `${T(3)}entityName: ${cfg.entity}`, `${T(3)}schemaName: ${cfg.schema}`, `${T(3)}expressionSource: DatabaseQuery`, "",
  ].join("\n");
}

// Power Query M の文字列リテラル（" は "" にする）。改行などの制御文字は TMDL を壊すため拒否する
function mString(s) {
  if (/[\u0000-\u001f\u007f]/.test(String(s))) throw new Error("control characters are not allowed in M string values");
  return `"${String(s).replace(/"/g, '""')}"`;
}

const toPart = ([p, c]) => ({ path: p, payload: Buffer.from(c, "utf8").toString("base64"), payloadType: "InlineBase64" });

function buildModel({ daxText, mdText, schema, sqlServer, sqlEndpointId, modelName, uuid = () => crypto.randomUUID() }) {
  const measures = parseDax(daxText);
  const meta = parseMdMeta(mdText);
  const ctx = { schema, measures, meta, uuid };
  const parts = {
    "definition.pbism": JSON.stringify({ version: "4.0", settings: {} }),
    "definition/database.tmdl": "database\n\tcompatibilityLevel: 1604\n",
    "definition/model.tmdl": ["model Model", `${T(1)}culture: ja-JP`, `${T(1)}defaultPowerBIDataSourceVersion: powerBI_V3`, `${T(1)}discourageImplicitMeasures`, `${T(1)}sourceQueryCulture: ja-JP`, "", ...Object.keys(TABLES).map((t) => `ref table ${q(t)}`), ""].join("\n"),
    "definition/expressions.tmdl": ["expression DatabaseQuery =", `${T(2)}let`, `${T(3)}database = Sql.Database(${mString(sqlServer)}, ${mString(sqlEndpointId)})`, `${T(2)}in`, `${T(3)}database`, `${T(1)}lineageTag: ${uuid()}`, ""].join("\n"),
    "definition/relationships.tmdl": RELS.map(([ft, fc, tt, tc]) => [`relationship ${uuid()}`, `${T(1)}fromColumn: ${q(ft)}.${q(fc)}`, `${T(1)}toColumn: ${q(tt)}.${q(tc)}`, ""].join("\n")).join("\n"),
    ...Object.fromEntries(Object.keys(TABLES).map((t) => [`definition/tables/${t}.tmdl`, tableTmdl(t, ctx)])),
  };
  const body = { displayName: modelName, description: "Direct Lake model on factory_lakehouse (gold)", definition: { parts: Object.entries(parts).map(toPart) } };
  return {
    body,
    parts,
    measures,
    measureTables: Object.fromEntries(measures.map((m) => [m.name, m.table])),
    missingMeta: measures.filter((m) => !meta[m.name]).map((m) => m.name),
  };
}

function writeOutputs(outFile, result) {
  const dir = path.dirname(outFile);
  for (const [p, c] of Object.entries(result.parts)) {
    const f = path.join(dir, "tmdl", p);
    fs.mkdirSync(path.dirname(f), { recursive: true });
    fs.writeFileSync(f, c);
  }
  fs.writeFileSync(outFile, JSON.stringify(result.body));
  fs.writeFileSync(path.join(dir, "measure_tables.json"), JSON.stringify(result.measureTables, null, 1));
}

function main(argv) {
  const [pbiDir, schemaFile, sqlServer, sqlEndpointId, modelName, out] = argv;
  if (!out) throw new Error("usage: build_model.js <powerbi_dir> <schema.json> <sql_server> <sql_endpoint_id> <model_name> <out_body.json>");
  const result = buildModel({
    daxText: fs.readFileSync(path.join(pbiDir, "dax-measures.dax"), "utf8"),
    mdText: fs.readFileSync(path.join(pbiDir, "dax-measures.md"), "utf8"),
    schema: JSON.parse(fs.readFileSync(schemaFile, "utf8")),
    sqlServer, sqlEndpointId, modelName,
  });
  writeOutputs(out, result);
  const { missingMeta, measures } = result;
  console.log(`measures: ${measures.length} (no format/folder meta: ${missingMeta.length}${missingMeta.length ? " -> " + missingMeta.join(", ") : ""})`);
  console.log("by table:", JSON.stringify(measures.reduce((a, m) => ({ ...a, [m.table]: (a[m.table] || 0) + 1 }), {})));
  console.log("parts:", Object.keys(result.parts).length, "bytes:", fs.statSync(out).size);
}

if (require.main === module) {
  try { main(process.argv.slice(2)); } catch (e) { console.error(`build_model failed: ${e.message}`); process.exit(1); }
}

module.exports = { parseDax, parseMdMeta, looksLikeFormat, expandTabs, mString, fallbackFormat, buildModel, writeOutputs, main, TABLES };

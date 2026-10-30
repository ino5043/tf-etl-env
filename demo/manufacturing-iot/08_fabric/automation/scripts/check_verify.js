// verify_pipeline の結果 JSON を deploy-guide.md 手順 6 の期待値と照合する（同梱サンプルデータを処理した場合の値）
// Usage: node check_verify.js <verify.json>   不一致があれば終了コード 1
"use strict";
const fs = require("fs");

const REQUIRED = ["counts", "layer_gaps", "gold_pk_duplicates", "equipment_alerts", "dead_letter", "high_defect", "totals"];

function evaluateVerify(r) {
  const missing = REQUIRED.filter((k) => !Array.isArray(r && r[k]));
  if (missing.length) throw new Error(`verify result is missing: ${missing.join(", ")}`);
  const counts = Object.fromEntries(r.counts.map((c) => [c.table_name, Number(c.row_count)]));
  const total = r.totals[0] || {};
  const checks = [
    ["bronze.raw_production", counts["bronze.raw_production"], 1036],
    ["silver.production_facts", counts["silver.production_facts"], 1030],
    ["meta.dead_letter_production", counts["meta.dead_letter_production"], 4],
    ["gold.daily_production_kpi", counts["gold.daily_production_kpi"], 150],
    ["gold.equipment_health", counts["gold.equipment_health"], 300],
    ["gold.dim_date", counts["gold.dim_date"], 365],
    ["層間の差（重複除去の日）", r.layer_gaps.map((g) => `${g.target_date}:${g.deduplicated}`), ["2026-10-11:1", "2026-10-25:1"]],
    ["Gold 主キー重複", r.gold_pk_duplicates.length, 0],
    ["設備アラート", r.equipment_alerts.length, 6],
    ["Dead Letter", r.dead_letter.length, 4],
    ["不良率 5% 超", r.high_defect.map((h) => `${h.production_date}:${h.line_id}`), ["2026-10-17:LINE-003", "2026-10-25:LINE-002"]],
    ["実績数量の合計", Number(total.actual), 560725],
    ["不良数量の合計", Number(total.defect), 9471],
  ].map(([label, actual, expected]) => ({ label, actual, expected, ok: JSON.stringify(actual) === JSON.stringify(expected) }));
  return { checks, ng: checks.filter((c) => !c.ok).length };
}

function formatVerify({ checks, ng }) {
  return [
    ...checks.map((c) => `${c.ok ? "OK" : "NG"}  ${c.label}: ${JSON.stringify(c.actual)}${c.ok ? "" : `（期待 ${JSON.stringify(c.expected)}）`}`),
    ng === 0 ? "すべて期待値どおり" : `${ng} 件が期待値と不一致`,
  ];
}

if (require.main === module) {
  try {
    const result = evaluateVerify(JSON.parse(fs.readFileSync(process.argv[2], "utf8")));
    formatVerify(result).forEach((l) => console.log(l));
    process.exit(result.ng === 0 ? 0 : 1);
  } catch (e) { console.error(`check_verify failed: ${e.message}`); process.exit(1); }
}

module.exports = { evaluateVerify, formatVerify };

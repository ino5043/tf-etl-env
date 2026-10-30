// verify_pipeline の結果 JSON を deploy-guide.md 手順 6 の期待値と照合する（同梱サンプルデータを処理した場合の値）
// Usage: node check_verify.js <verify.json>   不一致があれば終了コード 1
const fs = require("fs");

const r = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const checks = [];
const expect = (label, actual, expected) => checks.push({ label, actual, expected, ok: JSON.stringify(actual) === JSON.stringify(expected) });

const counts = Object.fromEntries(r.counts.map((c) => [c.table_name, Number(c.row_count)]));
expect("bronze.raw_production", counts["bronze.raw_production"], 1036);
expect("silver.production_facts", counts["silver.production_facts"], 1030);
expect("meta.dead_letter_production", counts["meta.dead_letter_production"], 4);
expect("gold.daily_production_kpi", counts["gold.daily_production_kpi"], 150);
expect("gold.equipment_health", counts["gold.equipment_health"], 300);
expect("gold.dim_date", counts["gold.dim_date"], 365);
expect("層間の差（重複除去の日）", r.layer_gaps.map((g) => `${g.target_date}:${g.deduplicated}`), ["2026-10-11:1", "2026-10-25:1"]);
expect("Gold 主キー重複", r.gold_pk_duplicates.length, 0);
expect("設備アラート", r.equipment_alerts.length, 6);
expect("Dead Letter", r.dead_letter.length, 4);
expect("不良率 5% 超", r.high_defect.map((h) => `${h.production_date}:${h.line_id}`), ["2026-10-17:LINE-003", "2026-10-25:LINE-002"]);
expect("実績数量の合計", Number(r.totals[0].actual), 560725);
expect("不良数量の合計", Number(r.totals[0].defect), 9471);

for (const c of checks) console.log(`${c.ok ? "OK" : "NG"}  ${c.label}: ${JSON.stringify(c.actual)}${c.ok ? "" : `（期待 ${JSON.stringify(c.expected)}）`}`);
const ng = checks.filter((c) => !c.ok).length;
console.log(ng === 0 ? "すべて期待値どおり" : `${ng} 件が期待値と不一致`);
process.exit(ng === 0 ? 0 : 1);

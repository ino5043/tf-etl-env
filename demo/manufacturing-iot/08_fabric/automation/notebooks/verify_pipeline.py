# 検証用 Notebook のコード（deploy-guide.md 手順 6 の検証 1〜5 を Spark SQL で実行し、結果を JSON で保存）
import json

def rows(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]

result = {}

result["counts"] = rows("""
SELECT 'bronze.raw_production' AS table_name, COUNT(*) AS row_count FROM bronze.raw_production
UNION ALL SELECT 'silver.production_facts', COUNT(*) FROM silver.production_facts
UNION ALL SELECT 'meta.dead_letter_production', COUNT(*) FROM meta.dead_letter_production
UNION ALL SELECT 'gold.daily_production_kpi', COUNT(*) FROM gold.daily_production_kpi
UNION ALL SELECT 'gold.equipment_health', COUNT(*) FROM gold.equipment_health
UNION ALL SELECT 'gold.dim_date', COUNT(*) FROM gold.dim_date
""")

result["layer_gaps"] = rows("""
SELECT CAST(b.d AS STRING) AS target_date, b.cnt AS bronze, COALESCE(s.cnt, 0) AS silver, COALESCE(e.cnt, 0) AS dead_letter,
       b.cnt - COALESCE(s.cnt, 0) - COALESCE(e.cnt, 0) AS deduplicated
FROM (SELECT _ingested_date AS d, COUNT(*) AS cnt FROM bronze.raw_production GROUP BY _ingested_date) b
LEFT JOIN (SELECT production_date AS d, COUNT(*) AS cnt FROM silver.production_facts GROUP BY production_date) s ON b.d = s.d
LEFT JOIN (SELECT _target_date AS d, COUNT(*) AS cnt FROM meta.dead_letter_production GROUP BY _target_date) e ON b.d = e.d
WHERE b.cnt - COALESCE(s.cnt, 0) - COALESCE(e.cnt, 0) <> 0
ORDER BY b.d
""")

result["gold_pk_duplicates"] = rows("""
SELECT 'daily_production_kpi' AS table_name, CAST(production_date AS STRING) AS production_date, line_id AS dim_key, COUNT(*) AS cnt
FROM gold.daily_production_kpi GROUP BY production_date, line_id HAVING COUNT(*) > 1
UNION ALL
SELECT 'equipment_health', CAST(production_date AS STRING), equipment_id, COUNT(*)
FROM gold.equipment_health GROUP BY production_date, equipment_id HAVING COUNT(*) > 1
""")

result["equipment_alerts"] = rows("""
SELECT CAST(production_date AS STRING) AS production_date, equipment_id, line_id,
       ROUND(rolling_7d_avg_temperature, 2) AS t7, ROUND(rolling_7d_avg_vibration, 2) AS v7,
       temperature_alert, vibration_alert, status_summary
FROM gold.equipment_health
WHERE temperature_alert OR vibration_alert
ORDER BY production_date, equipment_id
""")

result["dead_letter"] = rows("""
SELECT CAST(_target_date AS STRING) AS target_date, line_id, product_code, status, CAST(_rejection_reasons AS STRING) AS reasons
FROM meta.dead_letter_production ORDER BY _target_date
""")

result["high_defect"] = rows("""
SELECT CAST(production_date AS STRING) AS production_date, line_id, total_actual_quantity, total_defect_quantity, ROUND(defect_rate, 4) AS defect_rate
FROM gold.daily_production_kpi WHERE defect_rate > 0.05 ORDER BY production_date
""")

result["totals"] = rows("""
SELECT SUM(total_actual_quantity) AS actual, SUM(total_defect_quantity) AS defect FROM gold.daily_production_kpi
""")

result["tables"] = sorted(
    f"{r['namespace']}.{r['tableName']}" if r.get("namespace") else r["tableName"]
    for schema in ["bronze", "silver", "gold", "meta"]
    for r in [x.asDict() for x in spark.sql(f"SHOW TABLES IN {schema}").collect()]
)

payload = json.dumps(result, ensure_ascii=False, default=str)
try:
    fs_util = notebookutils.fs  # noqa: F821  (Fabric の組み込み)
except NameError:
    fs_util = mssparkutils.fs  # noqa: F821
fs_util.put("Files/_verify/verify_pipeline.json", payload, True)
print(payload)

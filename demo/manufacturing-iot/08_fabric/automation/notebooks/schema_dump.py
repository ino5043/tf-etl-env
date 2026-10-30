import json
tables = ["gold.daily_production_kpi", "gold.equipment_health", "gold.dim_date", "gold.dim_line", "gold.dim_equipment", "gold.dim_status"]
out = {t: [{"name": f.name, "type": f.dataType.simpleString()} for f in spark.table(t).schema.fields] for t in tables}
try:
    fs_util = notebookutils.fs  # noqa: F821
except NameError:
    fs_util = mssparkutils.fs  # noqa: F821
fs_util.put("Files/_verify/schema_dump.json", json.dumps(out), True)
print(json.dumps(out))

"""
Fabric Notebook 版（08_fabric/notebook/fabric_pipeline.py）のローカルスモークテスト

Delta 書き込み（Fabric 上で実行される I/O 部分）は対象外とし、
変換・品質ゲート・SCD2・Gold 集計の純粋関数を 08_fabric のサンプルデータで検証する。
"""

import importlib.util
import os
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

DEMO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_PATH = DEMO_ROOT / "08_fabric" / "notebook" / "fabric_pipeline.py"
GENERATOR_DIR = DEMO_ROOT / "08_fabric" / "sample_data"

START = date(2026, 10, 1)
DAYS = 30


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


nb = _load_module("fabric_pipeline", NOTEBOOK_PATH)
sys.path.insert(0, str(GENERATOR_DIR))
import generate_fabric_sample as gen  # noqa: E402


@pytest.fixture(scope="module")
def sample_dir(tmp_path_factory):
    """08_fabric のサンプルデータ一式（30 日分 + マスタ）を一時ディレクトリに生成する。"""
    out = tmp_path_factory.mktemp("fabric_sample")
    gen.generate(out, START, DAYS)
    return out


def _read_days(spark, sample_dir, first_day, last_day):
    paths = [
        str(sample_dir / f"production_{d:%Y%m%d}.csv")
        for d in pd.date_range(first_day, last_day)
    ]
    return nb.read_csv(spark, paths)


def _read_day(spark, sample_dir, day):
    path = str(sample_dir / f"production_{day.replace('-', '')}.csv")
    return nb.add_metadata(nb.read_csv(spark, path), path, "test", day)


class TestSampleDataGenerator:
    def test_generates_30_daily_files_and_masters(self, sample_dir):
        daily = sorted(sample_dir.glob("production_*.csv"))
        assert len(daily) == DAYS
        for name in ["line_master.csv", "product_master.csv", "equipment_master_seed.csv"]:
            assert (sample_dir / name).exists()

    def test_is_reproducible_with_fixed_seed(self, sample_dir, tmp_path):
        gen.generate(tmp_path, START, DAYS)
        for name in ["production_20261001.csv", "production_20261030.csv", "line_master.csv"]:
            assert (tmp_path / name).read_bytes() == (sample_dir / name).read_bytes()

    def test_total_size_is_modest(self, sample_dir):
        total = sum(p.stat().st_size for p in sample_dir.glob("*.csv"))
        assert total < 2 * 1024 * 1024

    def test_list_source_dates_returns_sorted_dates(self, sample_dir):
        dates = nb.list_source_dates(str(sample_dir))
        assert len(dates) == DAYS
        assert dates[0] == "2026-10-01"
        assert dates[-1] == "2026-10-30"


class TestSilver:
    def test_all_rows_are_accounted_for(self, spark, sample_dir):
        """入力 = 合格（重複排除後）+ 重複 + 不合格（行の消失がないこと）。"""
        bronze = _read_days(spark, sample_dir, "2026-10-01", "2026-10-30")
        result = nb.build_silver(bronze, target_date=None)
        assert result.input_count == result.valid_count + result.duplicate_count + result.error_count
        assert result.error_count == len(gen.ERROR_INJECTIONS)
        assert result.duplicate_count == len(gen.DUPLICATE_DAYS)

    def test_non_numeric_quantity_goes_to_dead_letter(self, spark, sample_dir):
        """数値変換失敗（NULL 条件）の行が消えずに不合格側へ分離されること。"""
        bronze = _read_day(spark, sample_dir, "2026-10-27")
        result = nb.build_silver(bronze, "2026-10-27")
        assert result.error_count == 1
        reasons = result.error_df.select("_rejection_reasons").first()[0]
        assert "accuracy_range" in reasons
        assert result.input_count == result.valid_count + result.error_count

    def test_resent_duplicate_keeps_latest(self, spark, sample_dir):
        bronze = _read_day(spark, sample_dir, "2026-10-11")
        result = nb.build_silver(bronze, "2026-10-11")
        assert result.duplicate_count == 1
        keys = result.silver_df.groupBy("production_date", "line_id", "product_code", "shift").count()
        assert keys.filter(F.col("count") > 1).count() == 0

    def test_target_date_mismatch_is_rejected(self, spark, sample_dir):
        bronze = _read_day(spark, sample_dir, "2026-10-02")
        result = nb.build_silver(bronze, "2026-10-03")
        assert result.valid_count == 0
        assert "target_date_match" in result.error_df.select("_rejection_reasons").first()[0]

    def test_quality_gate_warns_on_error_day_and_passes_clean_day(self, spark, sample_dir):
        warn = nb.build_silver(_read_day(spark, sample_dir, "2026-10-08"), "2026-10-08")
        clean = nb.build_silver(_read_day(spark, sample_dir, "2026-10-02"), "2026-10-02")
        assert nb.check_quality_gate(warn) == "warning"
        assert nb.check_quality_gate(clean) == "success"

    def test_quality_gate_stops_when_valid_ratio_too_low(self, spark, sample_dir):
        result = nb.build_silver(_read_day(spark, sample_dir, "2026-10-02"), "2026-10-03")
        with pytest.raises(nb.QualityGateError):
            nb.check_quality_gate(result)


GOLD_DDL = DEMO_ROOT / "03_schema" / "ddl" / "gold.sql"


def _ddl_columns(table):
    """gold.sql の CREATE TABLE から列名（定義順）を抽出する。"""
    text = GOLD_DDL.read_text(encoding="utf-8")
    body = text.split(f"CREATE TABLE IF NOT EXISTS gold.{table} (")[1].split(")\nUSING DELTA")[0]
    names = []
    for line in body.splitlines():
        line = line.strip()
        if line and not line.startswith("--"):
            names.append(line.split()[0])
    return names


class TestGold:
    def _silver(self, spark, sample_dir, first_day, last_day):
        bronze = _read_days(spark, sample_dir, first_day, last_day)
        return nb.build_silver(bronze, target_date=None).silver_df

    def test_kpi_columns_match_ddl(self, spark, sample_dir):
        silver = self._silver(spark, sample_dir, "2026-10-05", "2026-10-05")
        kpi = nb.build_daily_production_kpi(silver)
        assert kpi.columns == _ddl_columns("daily_production_kpi")
        types = dict(kpi.dtypes)
        assert types["total_planned_quantity"] == "bigint"
        assert types["record_count"] == "int"
        assert types["yield_rate"] == "double"
        assert types["_aggregated_at"] == "timestamp"

    def test_health_columns_match_ddl(self, spark, sample_dir):
        silver = self._silver(spark, sample_dir, "2026-10-01", "2026-10-07")
        health = nb.build_equipment_health(silver, "2026-10-07")
        assert health.columns == _ddl_columns("equipment_health")
        types = dict(health.dtypes)
        assert types["temperature_alert"] == "boolean"
        assert types["abnormal_count"] == "int"
        assert types["measurement_count"] == "int"

    def test_kpi_has_one_row_per_line_and_consistent_totals(self, spark, sample_dir):
        silver = self._silver(spark, sample_dir, "2026-10-05", "2026-10-05")
        kpi = nb.build_daily_production_kpi(silver).collect()
        assert len(kpi) == 5
        for row in kpi:
            assert row.total_good_quantity == row.total_actual_quantity - row.total_defect_quantity
            assert row.yield_rate == pytest.approx(round(row.total_good_quantity / row.total_actual_quantity, 4))
            assert row.day_shift_quantity + row.night_shift_quantity == row.total_actual_quantity

    def test_some_line_day_exceeds_5pct_defect_rate(self, spark, sample_dir):
        silver = self._silver(spark, sample_dir, "2026-10-01", "2026-10-30")
        kpi = nb.build_daily_production_kpi(silver)
        high = kpi.filter(F.col("defect_rate") > 0.05).select("production_date", "line_id").collect()
        assert any(str(r.production_date) == "2026-10-17" and r.line_id == "LINE-003" for r in high)

    def test_degrading_equipment_raises_alerts_at_period_end(self, spark, sample_dir):
        silver = self._silver(spark, sample_dir, "2026-10-24", "2026-10-30")
        health = {
            r.equipment_id: r
            for r in nb.build_equipment_health(silver, "2026-10-30").collect()
        }
        assert len(health) == 10
        assert health["EQ-007"].temperature_alert is True
        assert health["EQ-003"].vibration_alert is True
        assert health["EQ-007"].line_id == "LINE-004"
        others = [r for k, r in health.items() if k not in gen.DEGRADATION]
        assert not any(r.temperature_alert or r.vibration_alert for r in others)

    def test_no_alerts_at_period_start(self, spark, sample_dir):
        silver = self._silver(spark, sample_dir, "2026-10-01", "2026-10-07")
        rows = nb.build_equipment_health(silver, "2026-10-07").collect()
        assert not any(r.temperature_alert or r.vibration_alert for r in rows)
        assert all(r.line_id is not None and r.status_summary in nb.VALID_STATUSES for r in rows)

    def test_status_summary_is_most_severe_status(self, spark):
        rows = [
            ("2026-10-01", "LINE-001", "PRD-A001", "100", "90", "1", "EQ-001", "日勤", "OP-001",
             "40.0", "1.0", "正常", "2026-10-01 08:00:00"),
            ("2026-10-01", "LINE-001", "PRD-A002", "100", "90", "1", "EQ-001", "夜勤", "OP-002",
             "41.0", "1.1", "異常", "2026-10-01 21:00:00"),
            ("2026-10-01", "LINE-001", "PRD-A003", "100", "90", "1", "EQ-001", "日勤", "OP-003",
             "42.0", "1.2", "警告", "2026-10-01 09:00:00"),
        ]
        bronze = spark.createDataFrame(pd.DataFrame(rows, columns=nb.BRONZE_SCHEMA.names), schema=nb.BRONZE_SCHEMA)
        silver = nb.build_silver(bronze, "2026-10-01").silver_df
        row = nb.build_equipment_health(silver, "2026-10-01").first()
        assert row.status_summary == "異常"
        assert row.abnormal_count == 1
        assert row.measurement_count == 3


class TestDimensions:
    def _csv(self, spark, sample_dir, name):
        return spark.read.option("header", "true").csv(str(sample_dir / name))

    def test_dim_line_has_plants_for_rls(self, spark, sample_dir):
        dim = nb.build_dim_line(self._csv(spark, sample_dir, "line_master.csv"))
        plants = {r.line_id: r.plant_id for r in dim.collect()}
        assert plants == {
            "LINE-001": "PLANT-A", "LINE-002": "PLANT-A", "LINE-003": "PLANT-A",
            "LINE-004": "PLANT-B", "LINE-005": "PLANT-B",
        }
        assert {"line_name", "plant_name"} <= set(dim.columns)

    def test_dim_equipment_joins_line_name(self, spark, sample_dir):
        dim_line = nb.build_dim_line(self._csv(spark, sample_dir, "line_master.csv"))
        dim = nb.build_dim_equipment(self._csv(spark, sample_dir, "equipment_master_seed.csv"), dim_line)
        rows = dim.collect()
        assert len(rows) == 10
        assert all(r.equipment_name and r.line_name for r in rows)

    def test_dim_product_required_columns(self, spark, sample_dir):
        dim = nb.build_dim_product(self._csv(spark, sample_dir, "product_master.csv"))
        assert dim.count() == 20
        assert {"product_code", "product_name", "category"} <= set(dim.columns)

    def test_missing_required_master_column_raises(self, spark, sample_dir):
        df = self._csv(spark, sample_dir, "line_master.csv").drop("plant_id")
        with pytest.raises(ValueError):
            nb.build_dim_line(df)

    def test_dim_date_covers_full_year(self, spark):
        dim = nb.build_dim_date(spark, date(2026, 10, 1), date(2026, 10, 30))
        assert dim.count() == 365
        row = dim.filter(F.col("calendar_date") == "2026-10-04").first()
        assert row.weekday_jp == "日" and row.is_weekend is True
        assert row.year_month == "2026年10月" and row.fiscal_year == 2026

    def test_dim_status_order(self, spark):
        rows = {r.status: r.severity_order for r in nb.build_dim_status(spark).collect()}
        assert rows == {"正常": 1, "警告": 2, "異常": 3, "停止": 4}


SCD2_SCHEMA = StructType([
    StructField("equipment_id", StringType()),
    StructField("line_id", StringType()),
    StructField("latest_status", StringType()),
    StructField("valid_from", DateType()),
    StructField("valid_to", DateType()),
    StructField("is_current", BooleanType()),
    StructField("_updated_at", TimestampType()),
])
SNAPSHOT_SCHEMA = StructType([
    StructField("equipment_id", StringType()),
    StructField("line_id", StringType()),
    StructField("latest_status", StringType()),
])


class TestEquipmentScd2:
    def _existing(self, spark):
        ts = datetime(2026, 10, 1, 4, 0, 0)
        rows = [
            ("EQ-001", "LINE-001", "正常", date(2026, 10, 1), None, True, ts),
            ("EQ-002", "LINE-001", "正常", date(2026, 10, 1), None, True, ts),
        ]
        return spark.createDataFrame(pd.DataFrame(rows, columns=SCD2_SCHEMA.names), schema=SCD2_SCHEMA)

    def _snapshot(self, spark, rows):
        return spark.createDataFrame(pd.DataFrame(rows, columns=SNAPSHOT_SCHEMA.names), schema=SNAPSHOT_SCHEMA)

    def _rows(self, df):
        return {(r.equipment_id, str(r.valid_from)): r for r in df.collect()}

    def test_initial_load_creates_current_rows(self, spark):
        snap = self._snapshot(spark, [("EQ-001", "LINE-001", "正常")])
        rows = nb.apply_scd2(None, snap, "2026-10-01").collect()
        assert len(rows) == 1 and rows[0].is_current and rows[0].valid_to is None

    def test_change_closes_old_row_and_adds_new(self, spark):
        snap = self._snapshot(spark, [
            ("EQ-001", "LINE-001", "警告"),   # 変化あり
            ("EQ-002", "LINE-001", "正常"),   # 変化なし
            ("EQ-011", "LINE-005", "正常"),   # 新規
        ])
        rows = self._rows(nb.apply_scd2(self._existing(spark), snap, "2026-10-02"))
        assert len(rows) == 4
        old = rows[("EQ-001", "2026-10-01")]
        assert old.is_current is False and str(old.valid_to) == "2026-10-01"
        assert rows[("EQ-001", "2026-10-02")].latest_status == "警告"
        assert rows[("EQ-002", "2026-10-01")].is_current is True
        assert rows[("EQ-011", "2026-10-02")].is_current is True

    def test_rerun_same_day_is_idempotent(self, spark):
        snap = self._snapshot(spark, [("EQ-001", "LINE-001", "警告"), ("EQ-002", "LINE-001", "正常")])
        first = nb.apply_scd2(self._existing(spark), snap, "2026-10-02")
        second = nb.apply_scd2(first, snap, "2026-10-02")
        assert sorted(self._rows(first)) == sorted(self._rows(second))
        assert second.filter(F.col("is_current")).count() == 2

    def test_rerun_of_past_date_does_not_rewrite_history(self, spark):
        snap = self._snapshot(spark, [("EQ-001", "LINE-001", "停止")])
        later = nb.apply_scd2(
            self._existing(spark),
            self._snapshot(spark, [("EQ-001", "LINE-001", "警告")]),
            "2026-10-05",
        )
        rerun = nb.apply_scd2(later, snap, "2026-10-03")
        assert sorted(self._rows(later)) == sorted(self._rows(rerun))


def test_notebook_parameters_default_to_lakehouse_relative_paths():
    assert nb.SOURCE_DIR.startswith("Files/")
    assert nb.MASTER_DIR == "Files/sample_data"
    assert nb.table_name("gold_kpi") == "gold.daily_production_kpi"
    assert os.path.basename(nb.source_file_path(nb.SOURCE_DIR, "2026-10-01")) == "production_20261001.csv"

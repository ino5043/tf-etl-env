"""
Gold Layer テスト: KPI 集計・設備健全性スコア
"""

import sys
import os

import pandas as pd
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "05_implementation"),
)
from src.aggregate import build_daily_production_kpi, build_equipment_health


def _silver_schema():
    """Silver production_facts のスキーマ。"""
    return StructType([
        StructField("production_date", DateType(), True),
        StructField("line_id", StringType(), True),
        StructField("product_code", StringType(), True),
        StructField("planned_quantity", IntegerType(), True),
        StructField("actual_quantity", IntegerType(), True),
        StructField("defect_quantity", IntegerType(), True),
        StructField("good_quantity", IntegerType(), True),
        StructField("equipment_id", StringType(), True),
        StructField("shift", StringType(), True),
        StructField("operator_id", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("vibration", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("defect_rate", DoubleType(), True),
        StructField("achievement_rate", DoubleType(), True),
    ])


def _silver_row(**overrides):
    """Silver レコードのデフォルト値。"""
    from datetime import date as d

    defaults = {
        "production_date": d(2026, 1, 30),
        "line_id": "LINE-001",
        "product_code": "PRD-A001",
        "planned_quantity": 500,
        "actual_quantity": 480,
        "defect_quantity": 10,
        "good_quantity": 470,
        "equipment_id": "EQ-001",
        "shift": "日勤",
        "operator_id": "OP-001",
        "temperature": 45.0,
        "vibration": 1.5,
        "status": "正常",
        "defect_rate": 0.0208,
        "achievement_rate": 0.96,
    }
    defaults.update(overrides)
    return defaults


def _create_silver_df(spark, rows):
    pdf = pd.DataFrame(rows)
    return spark.createDataFrame(pdf, schema=_silver_schema())


# ---------------------------------------------------------------------------
# build_daily_production_kpi
# ---------------------------------------------------------------------------
class TestBuildDailyProductionKpi:
    """日次 × ライン別 KPI 集計テスト。"""

    def test_single_record_aggregation(self, spark):
        df = _create_silver_df(spark, [_silver_row()])
        result = build_daily_production_kpi(df)
        assert result.count() == 1
        row = result.first()
        assert row["total_planned"] == 500
        assert row["total_actual"] == 480
        assert row["total_defect"] == 10
        assert row["total_good"] == 470

    def test_aggregation_by_line(self, spark):
        rows = [
            _silver_row(line_id="LINE-001", actual_quantity=100),
            _silver_row(line_id="LINE-001", actual_quantity=200),
            _silver_row(line_id="LINE-002", actual_quantity=300),
        ]
        df = _create_silver_df(spark, rows)
        result = build_daily_production_kpi(df)
        assert result.count() == 2  # LINE-001, LINE-002

        line1 = result.filter(F.col("line_id") == "LINE-001").first()
        assert line1["total_actual"] == 300  # 100 + 200

    def test_shift_breakdown(self, spark):
        rows = [
            _silver_row(shift="日勤", actual_quantity=200),
            _silver_row(shift="夜勤", actual_quantity=300),
        ]
        df = _create_silver_df(spark, rows)
        result = build_daily_production_kpi(df)
        row = result.first()
        assert row["day_shift_actual"] == 200
        assert row["night_shift_actual"] == 300

    def test_achievement_rate_calculation(self, spark):
        rows = [
            _silver_row(planned_quantity=500, actual_quantity=480),
        ]
        df = _create_silver_df(spark, rows)
        result = build_daily_production_kpi(df)
        row = result.first()
        assert abs(row["achievement_rate"] - 0.96) < 0.001

    def test_defect_rate_calculation(self, spark):
        rows = [
            _silver_row(actual_quantity=100, defect_quantity=5),
        ]
        df = _create_silver_df(spark, rows)
        result = build_daily_production_kpi(df)
        row = result.first()
        assert abs(row["defect_rate"] - 0.05) < 0.001

    def test_has_calculated_at_column(self, spark):
        df = _create_silver_df(spark, [_silver_row()])
        result = build_daily_production_kpi(df)
        assert "calculated_at" in result.columns

    def test_zero_planned_gives_zero_achievement(self, spark):
        rows = [
            _silver_row(planned_quantity=0, actual_quantity=0),
        ]
        df = _create_silver_df(spark, rows)
        result = build_daily_production_kpi(df)
        row = result.first()
        assert row["achievement_rate"] == 0.0


# ---------------------------------------------------------------------------
# build_equipment_health
# ---------------------------------------------------------------------------
class TestBuildEquipmentHealth:
    """設備健全性メトリクス集計テスト。"""

    def test_single_record(self, spark):
        df = _create_silver_df(spark, [_silver_row()])
        result = build_equipment_health(df)
        assert result.count() == 1
        row = result.first()
        assert abs(row["avg_temperature"] - 45.0) < 0.1
        assert abs(row["avg_vibration"] - 1.5) < 0.01

    def test_aggregation_by_equipment(self, spark):
        rows = [
            _silver_row(equipment_id="EQ-001", temperature=40.0),
            _silver_row(equipment_id="EQ-001", temperature=50.0),
            _silver_row(equipment_id="EQ-002", temperature=60.0),
        ]
        df = _create_silver_df(spark, rows)
        result = build_equipment_health(df)
        assert result.count() == 2

        eq1 = result.filter(F.col("equipment_id") == "EQ-001").first()
        assert abs(eq1["avg_temperature"] - 45.0) < 0.1
        assert eq1["max_temperature"] == 50.0
        assert eq1["min_temperature"] == 40.0

    def test_abnormal_count(self, spark):
        rows = [
            _silver_row(status="正常"),
            _silver_row(status="異常"),
            _silver_row(status="停止"),
        ]
        df = _create_silver_df(spark, rows)
        result = build_equipment_health(df)
        row = result.first()
        assert row["abnormal_count"] == 2

    def test_health_status_normal(self, spark):
        rows = [
            _silver_row(status="正常", temperature=45.0, vibration=1.5),
        ]
        df = _create_silver_df(spark, rows)
        result = build_equipment_health(df)
        row = result.first()
        assert row["health_status"] == "正常"

    def test_health_status_abnormal_overrides(self, spark):
        """異常レコードがあれば、温度・振動に関わらず「要注意」。"""
        rows = [
            _silver_row(status="異常", temperature=45.0, vibration=1.5),
        ]
        df = _create_silver_df(spark, rows)
        result = build_equipment_health(df)
        row = result.first()
        assert row["health_status"] == "要注意"

    def test_health_status_high_temperature(self, spark):
        rows = [
            _silver_row(status="正常", temperature=75.0, vibration=1.5),
        ]
        df = _create_silver_df(spark, rows)
        result = build_equipment_health(df)
        row = result.first()
        assert row["health_status"] == "高温警告"

    def test_health_status_high_vibration(self, spark):
        rows = [
            _silver_row(status="正常", temperature=45.0, vibration=4.5),
        ]
        df = _create_silver_df(spark, rows)
        result = build_equipment_health(df)
        row = result.first()
        assert row["health_status"] == "振動警告"

    def test_has_calculated_at_column(self, spark):
        df = _create_silver_df(spark, [_silver_row()])
        result = build_equipment_health(df)
        assert "calculated_at" in result.columns

    def test_measurement_count(self, spark):
        rows = [_silver_row() for _ in range(5)]
        df = _create_silver_df(spark, rows)
        result = build_equipment_health(df)
        row = result.first()
        assert row["measurement_count"] == 5

"""
Silver Layer テスト: 型変換・品質チェック・重複排除・計算列
"""

import sys
import os
from datetime import date

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
    TimestampType,
)

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "05_implementation"),
)
from src.transform import (
    VALID_SHIFTS,
    VALID_STATUSES,
    add_computed_columns,
    cast_types,
    deduplicate,
    validate_quality,
)


def _bronze_row(**overrides):
    """Bronze レコードのデフォルト値を返すヘルパー。"""
    defaults = {
        "production_date": "2026-01-30",
        "line_id": "LINE-001",
        "product_code": "PRD-A001",
        "planned_quantity": "500",
        "actual_quantity": "480",
        "defect_quantity": "10",
        "equipment_id": "EQ-001",
        "shift": "日勤",
        "operator_id": "OP-001",
        "temperature": "45.0",
        "vibration": "1.5",
        "status": "正常",
        "created_at": "2026-01-30 10:00:00",
    }
    defaults.update(overrides)
    return defaults


def _create_bronze_df(spark, rows):
    """Bronze DataFrame（全 STRING）を生成するヘルパー。"""
    schema = StructType([
        StructField(name, StringType(), True)
        for name in [
            "production_date", "line_id", "product_code",
            "planned_quantity", "actual_quantity", "defect_quantity",
            "equipment_id", "shift", "operator_id",
            "temperature", "vibration", "status", "created_at",
        ]
    ])
    pdf = pd.DataFrame(rows)
    return spark.createDataFrame(pdf, schema=schema)


# ---------------------------------------------------------------------------
# cast_types
# ---------------------------------------------------------------------------
class TestCastTypes:
    """STRING → 正しいデータ型への変換テスト。"""

    def test_production_date_becomes_date(self, spark):
        df = _create_bronze_df(spark, [_bronze_row()])
        result = cast_types(df)
        assert result.schema["production_date"].dataType == DateType()

    def test_quantity_columns_become_integer(self, spark):
        df = _create_bronze_df(spark, [_bronze_row()])
        result = cast_types(df)
        for col_name in ["planned_quantity", "actual_quantity", "defect_quantity"]:
            assert result.schema[col_name].dataType == IntegerType(), (
                f"{col_name} should be IntegerType"
            )

    def test_sensor_columns_become_double(self, spark):
        df = _create_bronze_df(spark, [_bronze_row()])
        result = cast_types(df)
        for col_name in ["temperature", "vibration"]:
            assert result.schema[col_name].dataType == DoubleType(), (
                f"{col_name} should be DoubleType"
            )

    def test_created_at_becomes_timestamp(self, spark):
        df = _create_bronze_df(spark, [_bronze_row()])
        result = cast_types(df)
        assert result.schema["created_at"].dataType == TimestampType()

    def test_invalid_number_becomes_null(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(planned_quantity="abc")])
        result = cast_types(df)
        row = result.first()
        assert row["planned_quantity"] is None

    def test_invalid_date_becomes_null(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(production_date="not-a-date")])
        result = cast_types(df)
        row = result.first()
        assert row["production_date"] is None

    def test_valid_values_are_preserved(self, spark):
        df = _create_bronze_df(spark, [_bronze_row()])
        result = cast_types(df)
        row = result.first()
        assert row["planned_quantity"] == 500
        assert row["actual_quantity"] == 480
        assert abs(row["temperature"] - 45.0) < 0.01


# ---------------------------------------------------------------------------
# validate_quality
# ---------------------------------------------------------------------------
class TestValidateQuality:
    """品質チェック（合格/不合格分離）テスト。"""

    def test_valid_record_passes(self, spark):
        df = _create_bronze_df(spark, [_bronze_row()])
        typed_df = cast_types(df)
        valid_df, error_df = validate_quality(typed_df)
        assert valid_df.count() == 1
        assert error_df.count() == 0

    def test_null_production_date_fails(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(production_date="not-a-date")])
        typed_df = cast_types(df)  # "not-a-date" → NULL
        valid_df, error_df = validate_quality(typed_df)
        assert valid_df.count() == 0
        assert error_df.count() == 1

    def test_empty_line_id_fails(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(line_id="")])
        typed_df = cast_types(df)
        valid_df, error_df = validate_quality(typed_df)
        assert valid_df.count() == 0
        assert error_df.count() == 1

    def test_null_product_code_fails(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(product_code=None)])
        typed_df = cast_types(df)
        valid_df, error_df = validate_quality(typed_df)
        assert valid_df.count() == 0
        assert error_df.count() == 1

    def test_negative_quantity_fails(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(actual_quantity="-10")])
        typed_df = cast_types(df)
        valid_df, error_df = validate_quality(typed_df)
        assert valid_df.count() == 0
        assert error_df.count() == 1

    def test_temperature_out_of_range_fails(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(temperature="999.0")])
        typed_df = cast_types(df)
        valid_df, error_df = validate_quality(typed_df)
        assert valid_df.count() == 0
        assert error_df.count() == 1

    def test_null_temperature_passes(self, spark):
        """温度が NULL の場合は許容する（センサー欠損）。"""
        df = _create_bronze_df(spark, [_bronze_row(temperature=None)])
        typed_df = cast_types(df)
        valid_df, error_df = validate_quality(typed_df)
        assert valid_df.count() == 1
        assert error_df.count() == 0

    def test_invalid_status_fails(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(status="INVALID")])
        typed_df = cast_types(df)
        valid_df, error_df = validate_quality(typed_df)
        assert valid_df.count() == 0
        assert error_df.count() == 1

    def test_invalid_shift_fails(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(shift="三勤")])
        typed_df = cast_types(df)
        valid_df, error_df = validate_quality(typed_df)
        assert valid_df.count() == 0
        assert error_df.count() == 1

    def test_defect_exceeds_actual_fails(self, spark):
        """不良数 > 実績数は一貫性違反。"""
        df = _create_bronze_df(spark, [
            _bronze_row(actual_quantity="100", defect_quantity="200"),
        ])
        typed_df = cast_types(df)
        valid_df, error_df = validate_quality(typed_df)
        assert valid_df.count() == 0
        assert error_df.count() == 1

    def test_mixed_valid_and_invalid(self, spark):
        df = _create_bronze_df(spark, [
            _bronze_row(),  # valid
            _bronze_row(status="INVALID"),  # invalid
            _bronze_row(actual_quantity="-1"),  # invalid
        ])
        typed_df = cast_types(df)
        valid_df, error_df = validate_quality(typed_df)
        assert valid_df.count() == 1
        assert error_df.count() == 2


# ---------------------------------------------------------------------------
# deduplicate
# ---------------------------------------------------------------------------
class TestDeduplicate:
    """重複排除テスト。"""

    def test_keeps_latest_record(self, spark):
        rows = [
            _bronze_row(created_at="2026-01-30 08:00:00"),
            _bronze_row(created_at="2026-01-30 10:00:00"),  # newer → keep
        ]
        df = _create_bronze_df(spark, rows)
        typed_df = cast_types(df)
        result = deduplicate(typed_df)
        assert result.count() == 1
        row = result.first()
        assert row["created_at"].hour == 10

    def test_different_keys_are_kept(self, spark):
        rows = [
            _bronze_row(line_id="LINE-001"),
            _bronze_row(line_id="LINE-002"),
        ]
        df = _create_bronze_df(spark, rows)
        typed_df = cast_types(df)
        result = deduplicate(typed_df)
        assert result.count() == 2

    def test_different_shifts_are_kept(self, spark):
        rows = [
            _bronze_row(shift="日勤"),
            _bronze_row(shift="夜勤"),
        ]
        df = _create_bronze_df(spark, rows)
        typed_df = cast_types(df)
        result = deduplicate(typed_df)
        assert result.count() == 2

    def test_no_row_num_column_in_output(self, spark):
        rows = [_bronze_row(), _bronze_row()]
        df = _create_bronze_df(spark, rows)
        typed_df = cast_types(df)
        result = deduplicate(typed_df)
        assert "_row_num" not in result.columns


# ---------------------------------------------------------------------------
# add_computed_columns
# ---------------------------------------------------------------------------
class TestAddComputedColumns:
    """計算列追加テスト。"""

    def test_good_quantity_calculation(self, spark):
        df = _create_bronze_df(spark, [
            _bronze_row(actual_quantity="480", defect_quantity="10"),
        ])
        typed_df = cast_types(df)
        result = add_computed_columns(typed_df)
        row = result.first()
        assert row["good_quantity"] == 470

    def test_defect_rate_calculation(self, spark):
        df = _create_bronze_df(spark, [
            _bronze_row(actual_quantity="100", defect_quantity="5"),
        ])
        typed_df = cast_types(df)
        result = add_computed_columns(typed_df)
        row = result.first()
        assert abs(row["defect_rate"] - 0.05) < 0.001

    def test_achievement_rate_calculation(self, spark):
        df = _create_bronze_df(spark, [
            _bronze_row(planned_quantity="500", actual_quantity="480"),
        ])
        typed_df = cast_types(df)
        result = add_computed_columns(typed_df)
        row = result.first()
        assert abs(row["achievement_rate"] - 0.96) < 0.001

    def test_zero_planned_gives_zero_achievement(self, spark):
        df = _create_bronze_df(spark, [
            _bronze_row(planned_quantity="0", actual_quantity="0", defect_quantity="0"),
        ])
        typed_df = cast_types(df)
        result = add_computed_columns(typed_df)
        row = result.first()
        assert row["achievement_rate"] == 0.0

    def test_zero_actual_gives_zero_defect_rate(self, spark):
        df = _create_bronze_df(spark, [
            _bronze_row(actual_quantity="0", defect_quantity="0"),
        ])
        typed_df = cast_types(df)
        result = add_computed_columns(typed_df)
        row = result.first()
        assert row["defect_rate"] == 0.0

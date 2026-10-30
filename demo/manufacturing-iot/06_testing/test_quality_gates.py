"""
品質ゲートテスト: Bronze → Silver 昇格条件の検証

品質ルール（6次元）が正しく機能し、
基準を満たさないレコードが Dead Letter に分離されることをテスト。
"""

import sys
import os

import pandas as pd
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "05_implementation"),
)
from src.transform import (
    VALID_SHIFTS,
    VALID_STATUSES,
    TransformStats,
    cast_types,
    validate_quality,
)


def _bronze_row(**overrides):
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
# 完全性（Completeness）
# ---------------------------------------------------------------------------
class TestCompleteness:
    """必須フィールドの NULL / 空文字チェック。"""

    def test_all_mandatory_present_passes(self, spark):
        df = _create_bronze_df(spark, [_bronze_row()])
        typed = cast_types(df)
        valid, error = validate_quality(typed)
        assert valid.count() == 1

    def test_null_production_date_rejected(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(production_date=None)])
        typed = cast_types(df)
        valid, error = validate_quality(typed)
        assert error.count() == 1

    def test_null_line_id_rejected(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(line_id=None)])
        typed = cast_types(df)
        valid, error = validate_quality(typed)
        assert error.count() == 1

    def test_empty_line_id_rejected(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(line_id="  ")])
        typed = cast_types(df)
        valid, error = validate_quality(typed)
        assert error.count() == 1

    def test_null_product_code_rejected(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(product_code=None)])
        typed = cast_types(df)
        valid, error = validate_quality(typed)
        assert error.count() == 1

    def test_empty_product_code_rejected(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(product_code="")])
        typed = cast_types(df)
        valid, error = validate_quality(typed)
        assert error.count() == 1


# ---------------------------------------------------------------------------
# 正確性（Accuracy）
# ---------------------------------------------------------------------------
class TestAccuracy:
    """数値範囲のチェック。"""

    def test_valid_temperature_range_passes(self, spark):
        for temp in ["20.0", "50.0", "80.0"]:
            df = _create_bronze_df(spark, [_bronze_row(temperature=temp)])
            typed = cast_types(df)
            valid, _ = validate_quality(typed)
            assert valid.count() == 1, f"temp={temp} should pass"

    def test_temperature_below_range_rejected(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(temperature="19.9")])
        typed = cast_types(df)
        _, error = validate_quality(typed)
        assert error.count() == 1

    def test_temperature_above_range_rejected(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(temperature="80.1")])
        typed = cast_types(df)
        _, error = validate_quality(typed)
        assert error.count() == 1

    def test_valid_vibration_range_passes(self, spark):
        for vib in ["0.1", "2.5", "5.0"]:
            df = _create_bronze_df(spark, [_bronze_row(vibration=vib)])
            typed = cast_types(df)
            valid, _ = validate_quality(typed)
            assert valid.count() == 1, f"vibration={vib} should pass"

    def test_vibration_below_range_rejected(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(vibration="0.05")])
        typed = cast_types(df)
        _, error = validate_quality(typed)
        assert error.count() == 1

    def test_vibration_above_range_rejected(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(vibration="5.1")])
        typed = cast_types(df)
        _, error = validate_quality(typed)
        assert error.count() == 1

    def test_negative_actual_quantity_rejected(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(actual_quantity="-1")])
        typed = cast_types(df)
        _, error = validate_quality(typed)
        assert error.count() == 1

    def test_negative_planned_quantity_rejected(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(planned_quantity="-1")])
        typed = cast_types(df)
        _, error = validate_quality(typed)
        assert error.count() == 1

    def test_negative_defect_quantity_rejected(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(defect_quantity="-1")])
        typed = cast_types(df)
        _, error = validate_quality(typed)
        assert error.count() == 1


# ---------------------------------------------------------------------------
# 有効性（Validity）
# ---------------------------------------------------------------------------
class TestValidity:
    """Enum 値のチェック。"""

    def test_all_valid_statuses_pass(self, spark):
        for status in VALID_STATUSES:
            df = _create_bronze_df(spark, [_bronze_row(status=status)])
            typed = cast_types(df)
            valid, _ = validate_quality(typed)
            assert valid.count() == 1, f"status='{status}' should pass"

    def test_invalid_status_rejected(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(status="メンテナンス")])
        typed = cast_types(df)
        _, error = validate_quality(typed)
        assert error.count() == 1

    def test_all_valid_shifts_pass(self, spark):
        for shift in VALID_SHIFTS:
            df = _create_bronze_df(spark, [_bronze_row(shift=shift)])
            typed = cast_types(df)
            valid, _ = validate_quality(typed)
            assert valid.count() == 1, f"shift='{shift}' should pass"

    def test_invalid_shift_rejected(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(shift="深夜勤")])
        typed = cast_types(df)
        _, error = validate_quality(typed)
        assert error.count() == 1


# ---------------------------------------------------------------------------
# 一貫性（Consistency）
# ---------------------------------------------------------------------------
class TestConsistency:
    """フィールド間の整合性チェック。"""

    def test_defect_equals_actual_passes(self, spark):
        df = _create_bronze_df(spark, [
            _bronze_row(actual_quantity="100", defect_quantity="100"),
        ])
        typed = cast_types(df)
        valid, _ = validate_quality(typed)
        assert valid.count() == 1

    def test_defect_exceeds_actual_rejected(self, spark):
        df = _create_bronze_df(spark, [
            _bronze_row(actual_quantity="100", defect_quantity="101"),
        ])
        typed = cast_types(df)
        _, error = validate_quality(typed)
        assert error.count() == 1


# ---------------------------------------------------------------------------
# センサーデータの許容範囲（NULL 許容）
# ---------------------------------------------------------------------------
class TestSensorNullability:
    """温度・振動が NULL の場合は品質ゲートを通過する。"""

    def test_null_temperature_passes(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(temperature=None)])
        typed = cast_types(df)
        valid, error = validate_quality(typed)
        assert valid.count() == 1
        assert error.count() == 0

    def test_null_vibration_passes(self, spark):
        df = _create_bronze_df(spark, [_bronze_row(vibration=None)])
        typed = cast_types(df)
        valid, error = validate_quality(typed)
        assert valid.count() == 1
        assert error.count() == 0

    def test_both_sensors_null_passes(self, spark):
        df = _create_bronze_df(spark, [
            _bronze_row(temperature=None, vibration=None),
        ])
        typed = cast_types(df)
        valid, error = validate_quality(typed)
        assert valid.count() == 1
        assert error.count() == 0


# ---------------------------------------------------------------------------
# TransformStats データクラスのテスト
# ---------------------------------------------------------------------------
class TestTransformStats:
    """TransformStats イミュータブルデータクラスのテスト。"""

    def test_creation(self):
        stats = TransformStats(
            input_count=100,
            valid_count=95,
            error_count=5,
            error_rate=0.05,
        )
        assert stats.input_count == 100
        assert stats.valid_count == 95
        assert stats.error_count == 5
        assert abs(stats.error_rate - 0.05) < 0.001

    def test_immutability(self):
        stats = TransformStats(
            input_count=100,
            valid_count=95,
            error_count=5,
            error_rate=0.05,
        )
        with pytest.raises(AttributeError):
            stats.input_count = 200


# ---------------------------------------------------------------------------
# 複合品質ゲート（現実的なデータセット）
# ---------------------------------------------------------------------------
class TestIntegrationQualityGate:
    """CSV サンプルデータを使った統合テスト。"""

    def test_normal_data_high_pass_rate(self, spark, sample_data_dir):
        """正常データは高い通過率を持つべき。"""
        from src.extract import read_csv

        df = read_csv(spark, os.path.join(sample_data_dir, "production_normal.csv"))
        typed = cast_types(df)
        valid, error = validate_quality(typed)
        total = typed.count()
        pass_rate = valid.count() / total

        # 正常データは 90% 以上通過するはず
        assert pass_rate >= 0.90, (
            f"Normal data pass rate {pass_rate:.2%} is below 90%"
        )

    def test_error_data_low_pass_rate(self, spark, sample_data_dir):
        """異常データは低い通過率を持つべき。"""
        from src.extract import read_csv

        df = read_csv(spark, os.path.join(sample_data_dir, "production_error.csv"))
        typed = cast_types(df)
        valid, error = validate_quality(typed)
        total = typed.count()
        pass_rate = valid.count() / total

        # 異常データは 50% 以下であるはず
        assert pass_rate <= 0.50, (
            f"Error data pass rate {pass_rate:.2%} is above 50%"
        )

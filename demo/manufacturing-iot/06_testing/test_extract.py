"""
Bronze Layer テスト: CSV 読み込み・メタデータ付与・スキーマ検証
"""

import sys
import os

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# テスト対象モジュールのパスを追加
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "05_implementation"),
)
from src.extract import BRONZE_SCHEMA, add_metadata, read_csv


class TestBronzeSchema:
    """Bronze スキーマの構造テスト。"""

    def test_schema_has_13_columns(self):
        assert len(BRONZE_SCHEMA.fields) == 13

    def test_all_columns_are_string_type(self):
        for field in BRONZE_SCHEMA.fields:
            assert field.dataType == StringType(), (
                f"Column '{field.name}' should be StringType, "
                f"got {field.dataType}"
            )

    def test_required_columns_exist(self):
        column_names = [f.name for f in BRONZE_SCHEMA.fields]
        expected = [
            "production_date",
            "line_id",
            "product_code",
            "planned_quantity",
            "actual_quantity",
            "defect_quantity",
            "equipment_id",
            "shift",
            "operator_id",
            "temperature",
            "vibration",
            "status",
            "created_at",
        ]
        assert column_names == expected


class TestReadCsv:
    """CSV 読み込みテスト。"""

    def test_read_normal_csv(self, spark, sample_data_dir):
        df = read_csv(spark, os.path.join(sample_data_dir, "production_normal.csv"))
        assert df.count() == 500

    def test_read_normal_csv_has_correct_columns(self, spark, sample_data_dir):
        df = read_csv(spark, os.path.join(sample_data_dir, "production_normal.csv"))
        expected_cols = [f.name for f in BRONZE_SCHEMA.fields]
        assert df.columns == expected_cols

    def test_read_edge_csv(self, spark, sample_data_dir):
        df = read_csv(spark, os.path.join(sample_data_dir, "production_edge.csv"))
        assert df.count() == 50

    def test_read_error_csv(self, spark, sample_data_dir):
        df = read_csv(spark, os.path.join(sample_data_dir, "production_error.csv"))
        assert df.count() == 30

    def test_all_columns_are_string(self, spark, sample_data_dir):
        df = read_csv(spark, os.path.join(sample_data_dir, "production_normal.csv"))
        for field in df.schema.fields:
            assert field.dataType == StringType(), (
                f"Column '{field.name}' should be StringType"
            )


class TestAddMetadata:
    """メタデータ付与テスト。"""

    def test_adds_metadata_columns(self, spark, sample_data_dir):
        df = read_csv(spark, os.path.join(sample_data_dir, "production_normal.csv"))
        result = add_metadata(df, source_file="test.csv", batch_id="batch-001")

        metadata_cols = [
            "_ingested_at",
            "_source_file",
            "_batch_id",
            "_ingested_date",
        ]
        for col_name in metadata_cols:
            assert col_name in result.columns, f"Missing metadata column: {col_name}"

    def test_metadata_values_are_correct(self, spark, sample_data_dir):
        df = read_csv(spark, os.path.join(sample_data_dir, "production_normal.csv"))
        result = add_metadata(df, source_file="my_file.csv", batch_id="abc-123")

        row = result.select("_source_file", "_batch_id").first()
        assert row["_source_file"] == "my_file.csv"
        assert row["_batch_id"] == "abc-123"

    def test_metadata_does_not_drop_original_columns(self, spark, sample_data_dir):
        df = read_csv(spark, os.path.join(sample_data_dir, "production_normal.csv"))
        original_cols = set(df.columns)
        result = add_metadata(df, source_file="test.csv", batch_id="b1")
        assert original_cols.issubset(set(result.columns))

    def test_row_count_unchanged_after_metadata(self, spark, sample_data_dir):
        df = read_csv(spark, os.path.join(sample_data_dir, "production_normal.csv"))
        result = add_metadata(df, source_file="test.csv", batch_id="b1")
        assert result.count() == df.count()

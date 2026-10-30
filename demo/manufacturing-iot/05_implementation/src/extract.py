"""
Bronze Layer: CSV → raw_production

ソース CSV をそのまま取り込み、メタデータ列を付与する。
すべてのカラムは STRING 型で保持（型変換は Silver で実施）。
冪等性: _ingested_date パーティションの上書き（replaceWhere）。
"""

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

import logging

logger = logging.getLogger(__name__)

# Bronze スキーマ（ソース CSV の全カラムを STRING で定義）
BRONZE_SCHEMA = StructType([
    StructField("production_date", StringType(), True),
    StructField("line_id", StringType(), True),
    StructField("product_code", StringType(), True),
    StructField("planned_quantity", StringType(), True),
    StructField("actual_quantity", StringType(), True),
    StructField("defect_quantity", StringType(), True),
    StructField("equipment_id", StringType(), True),
    StructField("shift", StringType(), True),
    StructField("operator_id", StringType(), True),
    StructField("temperature", StringType(), True),
    StructField("vibration", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", StringType(), True),
])


def read_csv(
    spark: SparkSession,
    file_path: str,
    encoding: str = "utf-8",
) -> DataFrame:
    """CSV ファイルを STRING 型で読み込む。"""
    logger.info("Reading CSV", extra={
        "event": "csv_read_started",
        "file_path": file_path,
    })

    df = (
        spark.read
        .option("header", "true")
        .option("encoding", encoding)
        .option("mode", "PERMISSIVE")
        .schema(BRONZE_SCHEMA)
        .csv(file_path)
    )

    row_count = df.count()
    logger.info("CSV read completed", extra={
        "event": "csv_read_completed",
        "file_path": file_path,
        "row_count": row_count,
    })

    return df


def add_metadata(
    df: DataFrame,
    source_file: str,
    batch_id: str,
) -> DataFrame:
    """メタデータ列を付与する。"""
    now_utc = datetime.now(timezone.utc)
    ingested_at = now_utc.isoformat()

    return (
        df
        .withColumn("_ingested_at", F.lit(ingested_at))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_date", F.lit(
            now_utc.strftime("%Y-%m-%d")
        ))
    )


def load_to_bronze(
    df: DataFrame,
    table_path: str,
    target_date: str,
) -> dict:
    """
    Bronze テーブルに書き込む（冪等: パーティション上書き）。

    Returns:
        処理統計 dict
    """
    row_count = df.count()

    logger.info("Writing to Bronze", extra={
        "event": "bronze_write_started",
        "table_path": table_path,
        "target_date": target_date,
        "row_count": row_count,
    })

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_ingested_date = '{target_date}'")
        .save(table_path)
    )

    stats = {
        "layer": "bronze",
        "table": "raw_production",
        "target_date": target_date,
        "input_count": row_count,
        "output_count": row_count,
        "status": "success",
    }

    logger.info("Bronze write completed", extra={
        "event": "bronze_write_completed",
        **stats,
    })

    return stats


def extract(
    spark: SparkSession,
    file_path: str,
    table_path: str,
    target_date: str,
    batch_id: str,
) -> dict:
    """Extract パイプライン全体を実行する。"""
    df = read_csv(spark, file_path)
    df = add_metadata(df, source_file=file_path, batch_id=batch_id)
    stats = load_to_bronze(df, table_path, target_date)
    return stats

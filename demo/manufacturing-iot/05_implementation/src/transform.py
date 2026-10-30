"""
Silver Layer: Bronze → production_facts + equipment_master

Bronze の STRING データを正しい型に変換し、品質チェックを実施。
品質ゲートを通過したレコードのみ Silver に昇格。
不合格レコードは Dead Letter テーブルに分離。
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    TimestampType,
)

import logging

logger = logging.getLogger(__name__)

VALID_STATUSES = ["正常", "警告", "異常", "停止"]
VALID_SHIFTS = ["日勤", "夜勤"]


@dataclass(frozen=True)
class TransformStats:
    """変換処理の統計情報（イミュータブル）。"""

    input_count: int
    valid_count: int
    error_count: int
    error_rate: float


def cast_types(df: DataFrame) -> DataFrame:
    """STRING → 正しいデータ型に変換する。"""
    return (
        df
        .withColumn("production_date", F.to_date(F.col("production_date"), "yyyy-MM-dd"))
        .withColumn("planned_quantity", F.col("planned_quantity").cast(IntegerType()))
        .withColumn("actual_quantity", F.col("actual_quantity").cast(IntegerType()))
        .withColumn("defect_quantity", F.col("defect_quantity").cast(IntegerType()))
        .withColumn("temperature", F.col("temperature").cast(DoubleType()))
        .withColumn("vibration", F.col("vibration").cast(DoubleType()))
        .withColumn("created_at", F.to_timestamp(F.col("created_at"), "yyyy-MM-dd HH:mm:ss"))
    )


def add_computed_columns(df: DataFrame) -> DataFrame:
    """計算列を追加する。"""
    return (
        df
        .withColumn(
            "good_quantity",
            F.col("actual_quantity") - F.col("defect_quantity"),
        )
        .withColumn(
            "defect_rate",
            F.when(
                F.col("actual_quantity") > 0,
                F.round(F.col("defect_quantity") / F.col("actual_quantity"), 4),
            ).otherwise(0.0),
        )
        .withColumn(
            "achievement_rate",
            F.when(
                F.col("planned_quantity") > 0,
                F.round(F.col("actual_quantity") / F.col("planned_quantity"), 4),
            ).otherwise(0.0),
        )
    )


def validate_quality(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    品質チェックを実施し、合格/不合格に分離する。

    Returns:
        (valid_df, error_df) のタプル
    """
    # 必須カラムの NULL チェック
    mandatory_not_null = (
        F.col("production_date").isNotNull()
        & F.col("line_id").isNotNull()
        & (F.trim(F.col("line_id")) != "")
        & F.col("product_code").isNotNull()
        & (F.trim(F.col("product_code")) != "")
    )

    # 正確性チェック（範囲）
    accuracy_check = (
        (F.col("planned_quantity") >= 0)
        & (F.col("actual_quantity") >= 0)
        & (F.col("defect_quantity") >= 0)
        & (F.col("temperature").between(20.0, 80.0) | F.col("temperature").isNull())
        & (F.col("vibration").between(0.1, 5.0) | F.col("vibration").isNull())
    )

    # 有効性チェック（enum 値）
    validity_check = (
        F.col("status").isin(VALID_STATUSES)
        & F.col("shift").isin(VALID_SHIFTS)
    )

    # 一貫性チェック
    consistency_check = (
        F.col("defect_quantity") <= F.col("actual_quantity")
    )

    # 全条件を結合
    all_valid = mandatory_not_null & accuracy_check & validity_check & consistency_check

    valid_df = df.filter(all_valid)
    error_df = df.filter(~all_valid)

    return valid_df, error_df


def deduplicate(df: DataFrame) -> DataFrame:
    """重複レコードを排除する（一意性: date + line + product + shift）。"""
    from pyspark.sql.window import Window

    window = Window.partitionBy(
        "production_date", "line_id", "product_code", "shift"
    ).orderBy(F.col("created_at").desc())

    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def load_to_silver(
    df: DataFrame,
    table_path: str,
    target_date: str,
) -> None:
    """Silver テーブルに書き込む（冪等: パーティション上書き）。"""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"production_date = '{target_date}'")
        .save(table_path)
    )


def save_dead_letter(
    error_df: DataFrame,
    dead_letter_path: str,
    target_date: str,
) -> None:
    """不合格レコードを Dead Letter テーブルに書き込む。"""
    if error_df.count() == 0:
        return

    (
        error_df
        .withColumn("_rejected_at", F.current_timestamp())
        .withColumn("_rejection_reason", F.lit("quality_gate_bronze_to_silver"))
        .write
        .format("delta")
        .mode("append")
        .save(dead_letter_path)
    )


def transform(
    bronze_df: DataFrame,
    silver_table_path: str,
    dead_letter_path: str,
    target_date: str,
) -> TransformStats:
    """Transform パイプライン全体を実行する。"""
    input_count = bronze_df.count()

    logger.info("Transform started", extra={
        "event": "transform_started",
        "input_count": input_count,
        "target_date": target_date,
    })

    # 1. 型変換
    typed_df = cast_types(bronze_df)

    # 2. 品質チェック → 合格/不合格に分離
    valid_df, error_df = validate_quality(typed_df)

    # 3. 重複排除
    deduped_df = deduplicate(valid_df)

    # 4. 計算列追加
    enriched_df = add_computed_columns(deduped_df)

    # 5. Silver 書き込み
    valid_count = enriched_df.count()
    error_count = error_df.count()

    load_to_silver(enriched_df, silver_table_path, target_date)
    save_dead_letter(error_df, dead_letter_path, target_date)

    error_rate = round(error_count / input_count, 4) if input_count > 0 else 0.0

    stats = TransformStats(
        input_count=input_count,
        valid_count=valid_count,
        error_count=error_count,
        error_rate=error_rate,
    )

    logger.info("Transform completed", extra={
        "event": "transform_completed",
        "input_count": stats.input_count,
        "valid_count": stats.valid_count,
        "error_count": stats.error_count,
        "error_rate": stats.error_rate,
    })

    return stats

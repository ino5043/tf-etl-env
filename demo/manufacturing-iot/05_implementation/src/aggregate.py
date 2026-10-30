"""
Gold Layer: Silver → daily_production_kpi + equipment_health

Silver のクレンジング済みデータを集計し、BI 向けの分析テーブルを生成する。
冪等性: production_date パーティションの上書き。
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AggregateStats:
    """集計処理の統計情報（イミュータブル）。"""

    kpi_row_count: int
    health_row_count: int


def build_daily_production_kpi(silver_df: DataFrame) -> DataFrame:
    """日次 × ライン別の生産 KPI を集計する。"""
    return (
        silver_df
        .groupBy("production_date", "line_id")
        .agg(
            F.sum("planned_quantity").alias("total_planned"),
            F.sum("actual_quantity").alias("total_actual"),
            F.sum("defect_quantity").alias("total_defect"),
            F.sum("good_quantity").alias("total_good"),
            F.count("*").alias("record_count"),
            # シフト別実績
            F.sum(
                F.when(F.col("shift") == "日勤", F.col("actual_quantity")).otherwise(0)
            ).alias("day_shift_actual"),
            F.sum(
                F.when(F.col("shift") == "夜勤", F.col("actual_quantity")).otherwise(0)
            ).alias("night_shift_actual"),
        )
        .withColumn(
            "achievement_rate",
            F.when(
                F.col("total_planned") > 0,
                F.round(F.col("total_actual") / F.col("total_planned"), 4),
            ).otherwise(0.0),
        )
        .withColumn(
            "defect_rate",
            F.when(
                F.col("total_actual") > 0,
                F.round(F.col("total_defect") / F.col("total_actual"), 4),
            ).otherwise(0.0),
        )
        .withColumn("calculated_at", F.current_timestamp())
    )


def build_equipment_health(silver_df: DataFrame) -> DataFrame:
    """日次 × 設備別の健全性メトリクスを集計する。"""
    return (
        silver_df
        .groupBy("production_date", "equipment_id")
        .agg(
            F.avg("temperature").alias("avg_temperature"),
            F.max("temperature").alias("max_temperature"),
            F.min("temperature").alias("min_temperature"),
            F.avg("vibration").alias("avg_vibration"),
            F.max("vibration").alias("max_vibration"),
            F.min("vibration").alias("min_vibration"),
            F.count("*").alias("measurement_count"),
            F.sum(
                F.when(F.col("status").isin("異常", "停止"), 1).otherwise(0)
            ).alias("abnormal_count"),
        )
        .withColumn(
            "avg_temperature", F.round(F.col("avg_temperature"), 1),
        )
        .withColumn(
            "avg_vibration", F.round(F.col("avg_vibration"), 2),
        )
        .withColumn(
            "health_status",
            F.when(F.col("abnormal_count") > 0, "要注意")
            .when(F.col("max_temperature") > 70.0, "高温警告")
            .when(F.col("max_vibration") > 4.0, "振動警告")
            .otherwise("正常"),
        )
        .withColumn("calculated_at", F.current_timestamp())
    )


def load_to_gold(
    df: DataFrame,
    table_path: str,
    target_date: str,
) -> int:
    """Gold テーブルに書き込む（冪等: パーティション上書き）。"""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"production_date = '{target_date}'")
        .save(table_path)
    )
    return df.count()


def aggregate(
    silver_df: DataFrame,
    kpi_table_path: str,
    health_table_path: str,
    target_date: str,
) -> AggregateStats:
    """Aggregate パイプライン全体を実行する。"""
    logger.info("Aggregation started", extra={
        "event": "aggregate_started",
        "target_date": target_date,
    })

    kpi_df = build_daily_production_kpi(silver_df)
    health_df = build_equipment_health(silver_df)

    kpi_count = load_to_gold(kpi_df, kpi_table_path, target_date)
    health_count = load_to_gold(health_df, health_table_path, target_date)

    stats = AggregateStats(
        kpi_row_count=kpi_count,
        health_row_count=health_count,
    )

    logger.info("Aggregation completed", extra={
        "event": "aggregate_completed",
        "kpi_row_count": stats.kpi_row_count,
        "health_row_count": stats.health_row_count,
    })

    return stats

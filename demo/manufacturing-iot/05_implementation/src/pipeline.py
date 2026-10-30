"""
パイプライン オーケストレーション

Extract → Transform → Aggregate を順序実行し、
処理統計とSLAチェックをまとめて出力する。
"""

import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime

from pyspark.sql import SparkSession

from .extract import extract
from .transform import transform
from .aggregate import aggregate

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineResult:
    """パイプライン全体の実行結果（イミュータブル）。"""

    pipeline_name: str
    target_date: str
    batch_id: str
    start_time: str
    end_time: str
    duration_seconds: float
    extract_stats: dict
    transform_input_count: int
    transform_valid_count: int
    transform_error_count: int
    transform_error_rate: float
    gold_kpi_count: int
    gold_health_count: int
    overall_status: str


def run_pipeline(
    spark: SparkSession,
    target_date: str,
    config: dict,
) -> PipelineResult:
    """
    日次パイプラインを実行する。

    Args:
        spark: SparkSession
        target_date: 処理対象日（YYYY-MM-DD）
        config: pipeline_config.yaml の内容

    Returns:
        PipelineResult
    """
    batch_id = str(uuid.uuid4())[:8]
    start_time = datetime.utcnow()

    logger.info("Pipeline started", extra={
        "event": "pipeline_started",
        "pipeline_name": config["pipeline"]["name"],
        "target_date": target_date,
        "batch_id": batch_id,
    })

    layers = config["layers"]

    # --- Step 1: Extract (CSV → Bronze) ---
    file_path = config["source"]["path"].replace("{date}", target_date.replace("-", ""))
    extract_stats = extract(
        spark=spark,
        file_path=file_path,
        table_path=layers["bronze"]["path"],
        target_date=target_date,
        batch_id=batch_id,
    )

    # --- Step 2: Transform (Bronze → Silver) ---
    bronze_df = spark.read.format("delta").load(layers["bronze"]["path"]).filter(
        f"_ingested_date = '{target_date}'"
    )

    transform_stats = transform(
        bronze_df=bronze_df,
        silver_table_path=layers["silver"]["tables"]["production_facts"],
        dead_letter_path=layers["meta"]["dead_letter"],
        target_date=target_date,
    )

    # --- エラー率チェック ---
    error_threshold = config["sla"]["error_rate_threshold"]
    if transform_stats.error_rate > error_threshold:
        logger.warning("Error rate exceeded threshold", extra={
            "event": "error_rate_exceeded",
            "error_rate": transform_stats.error_rate,
            "threshold": error_threshold,
        })

    # --- Step 3: Aggregate (Silver → Gold) ---
    silver_df = spark.read.format("delta").load(
        layers["silver"]["tables"]["production_facts"]
    ).filter(f"production_date = '{target_date}'")

    aggregate_stats = aggregate(
        silver_df=silver_df,
        kpi_table_path=layers["gold"]["tables"]["daily_production_kpi"],
        health_table_path=layers["gold"]["tables"]["equipment_health"],
        target_date=target_date,
    )

    end_time = datetime.utcnow()
    duration = (end_time - start_time).total_seconds()

    overall_status = (
        "success" if transform_stats.error_rate <= error_threshold else "warning"
    )

    result = PipelineResult(
        pipeline_name=config["pipeline"]["name"],
        target_date=target_date,
        batch_id=batch_id,
        start_time=start_time.isoformat(),
        end_time=end_time.isoformat(),
        duration_seconds=round(duration, 2),
        extract_stats=extract_stats,
        transform_input_count=transform_stats.input_count,
        transform_valid_count=transform_stats.valid_count,
        transform_error_count=transform_stats.error_count,
        transform_error_rate=transform_stats.error_rate,
        gold_kpi_count=aggregate_stats.kpi_row_count,
        gold_health_count=aggregate_stats.health_row_count,
        overall_status=overall_status,
    )

    logger.info("Pipeline completed", extra={
        "event": "pipeline_completed",
        "pipeline_name": result.pipeline_name,
        "target_date": result.target_date,
        "duration_seconds": result.duration_seconds,
        "overall_status": result.overall_status,
        "transform_error_rate": result.transform_error_rate,
    })

    return result

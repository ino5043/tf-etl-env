# %% [markdown]
# # 工場生産実績 ETL — Fabric Notebook 版
#
# MES 日次 CSV（Files/）→ Bronze → Silver → Gold を Lakehouse の Delta テーブルとして構築し、
# Power BI（Direct Lake）用のディメンション（dim_date / dim_line / dim_equipment ほか）も作成する。
#
# - Bronze / Silver の変換ロジックは `05_implementation/src/` をミラーしている
#   （Fabric Notebook は 1 ファイルで完結させたいため import ではなく複製）。対応は各関数の docstring に記載
# - Gold は `03_schema/ddl/gold.sql` の列名・型どおりに出力する（Power BI のメジャーが DDL を前提とするため）
# - ディメンションは `08_fabric/powerbi/semantic-model.md` 4 章のスニペットと同じ列構成
#
# 使い方: Lakehouse を「既定の Lakehouse」としてアタッチし、すべてのセルを実行する。
# `# %%` はセル区切り。`build_ipynb.py` で .ipynb に変換してインポートできる。

# %% [parameters]
# --- パラメータセル（Data Pipeline の Notebook アクティビティから上書き可能） ---
# TARGET_DATE: 処理対象日（YYYY-MM-DD）。空文字なら SOURCE_DIR の全ファイルを日付順にバックフィル
TARGET_DATE = ""

# Lakehouse 相対パス（既定の Lakehouse に対して解決される）
# 絶対パスで指定する場合は OneLake の abfss 形式を使う:
#   abfss://<ワークスペース名>@onelake.dfs.fabric.microsoft.com/<Lakehouse名>.Lakehouse/Files/sample_data
SOURCE_DIR = "Files/sample_data"
MASTER_DIR = "Files/sample_data"   # Power BI 側（semantic-model.md 4.2）と同じ配置先
FILE_PATTERN = "production_{yyyymmdd}.csv"

# Lakehouse 作成時に「Lakehouse スキーマ」を有効化した場合 True（テーブル名 gold.daily_production_kpi 等）。
# 無効の場合 False（スキーマ接頭辞なしの daily_production_kpi 等）
USE_SCHEMAS = True

# Power BI 用ディメンション（dim_date / dim_line / dim_equipment / dim_product / dim_status）を作成する
BUILD_DIMENSIONS = True

# 品質ゲート閾値（05_implementation/config/pipeline_config.yaml と同値）
ERROR_RATE_WARN_THRESHOLD = 0.01   # sla.error_rate_threshold: 超過で overall_status=warning
MIN_VALID_RATIO = 0.90             # quality_gates.bronze_to_silver.min_completeness_score: 未満で停止

# 設備健全性アラート閾値（03_schema/ddl/gold.sql の equipment_health と同値）
ROLLING_WINDOW_DAYS = 7
TEMPERATURE_ALERT_THRESHOLD = 65.0
VIBRATION_ALERT_THRESHOLD = 3.5

# %%
import glob
import json
import logging
import os
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

PIPELINE_NAME = "daily_production_load"

# 論理名 → (スキーマ, テーブル)。スキーマは 03_schema/ddl/*.sql に合わせる
TABLES = {
    "bronze": ("bronze", "raw_production"),
    "silver_facts": ("silver", "production_facts"),
    "silver_equipment": ("silver", "equipment_master"),
    "dead_letter": ("meta", "dead_letter_production"),
    "run_log": ("meta", "pipeline_run_log"),
    "gold_kpi": ("gold", "daily_production_kpi"),
    "gold_health": ("gold", "equipment_health"),
    "dim_date": ("gold", "dim_date"),
    "dim_line": ("gold", "dim_line"),
    "dim_equipment": ("gold", "dim_equipment"),
    "dim_product": ("gold", "dim_product"),
    "dim_status": ("gold", "dim_status"),
}

# mirrors: src/extract.py BRONZE_SCHEMA
BRONZE_SCHEMA = StructType([
    StructField(name, StringType(), True)
    for name in [
        "production_date", "line_id", "product_code",
        "planned_quantity", "actual_quantity", "defect_quantity",
        "equipment_id", "shift", "operator_id",
        "temperature", "vibration", "status", "created_at",
    ]
])

# mirrors: src/transform.py
VALID_STATUSES = ["正常", "警告", "異常", "停止"]
VALID_SHIFTS = ["日勤", "夜勤"]
STATUS_SEVERITY = {"正常": 1, "警告": 2, "異常": 3, "停止": 4}   # semantic-model.md 4.3 と同値

# 03_schema/ddl/gold.sql の列順
KPI_COLUMNS = [
    "production_date", "line_id",
    "total_planned_quantity", "total_actual_quantity", "total_defect_quantity", "total_good_quantity",
    "achievement_rate", "defect_rate", "yield_rate",
    "record_count", "unique_products", "unique_operators",
    "day_shift_quantity", "night_shift_quantity",
    "_aggregated_at",
]
HEALTH_COLUMNS = [
    "production_date", "equipment_id", "line_id",
    "daily_avg_temperature", "daily_max_temperature", "daily_avg_vibration", "daily_max_vibration",
    "rolling_7d_avg_temperature", "rolling_7d_avg_vibration",
    "temperature_alert", "vibration_alert",
    "status_summary", "abnormal_count", "measurement_count",
    "_aggregated_at",
]


class QualityGateError(Exception):
    """品質ゲート不合格（Silver への昇格を停止）。"""


# --- 構造化ログ（JSON を標準出力へ。Notebook のセル出力にそのまま残る） ---
_logger = logging.getLogger(PIPELINE_NAME)
if not _logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(_handler)
    _logger.setLevel(logging.INFO)
    _logger.propagate = False


def log_event(event: str, level: int = logging.INFO, **fields) -> None:
    """event キーを持つ JSON 1 行ログを出力する。"""
    payload = {"event": event, "timestamp": datetime.now(timezone.utc).isoformat(), **fields}
    _logger.log(level, json.dumps(payload, ensure_ascii=False, default=str))


def table_name(key: str) -> str:
    """論理名から Lakehouse のテーブル名を返す（USE_SCHEMAS に応じて schema. を付ける）。"""
    schema, name = TABLES[key]
    return f"{schema}.{name}" if USE_SCHEMAS else name


# %% [markdown]
# ## Bronze: CSV → raw_production

# %%
def read_csv(spark: SparkSession, file_path) -> DataFrame:
    """CSV を全列 STRING で読み込む。mirrors: src/extract.py read_csv"""
    return (
        spark.read
        .option("header", "true")
        .option("encoding", "utf-8")
        .option("mode", "PERMISSIVE")
        .schema(BRONZE_SCHEMA)
        .csv(file_path)
    )


def add_metadata(df: DataFrame, source_file: str, batch_id: str, target_date: str) -> DataFrame:
    """
    メタデータ列を付与する。mirrors: src/extract.py add_metadata

    差分: `_ingested_date` は実行日ではなく処理対象日（target_date）とする。
    replaceWhere の述語と書き込みデータを一致させ、過去日のバックフィル・再実行を可能にするため。
    """
    return (
        df
        .withColumn("_ingested_at", F.lit(datetime.now(timezone.utc)))
        .withColumn("_source_file", F.lit(os.path.basename(source_file)))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_date", F.to_date(F.lit(target_date)))
    )


# %% [markdown]
# ## Silver: 型変換・品質チェック・重複排除・計算列

# %%
def _try_cast(column: str, sql_type: str):
    """変換失敗を NULL にする cast（ANSI モード有効な Spark 4 系でも例外にしない）。"""
    return F.expr(f"try_cast(`{column}` AS {sql_type})")


def cast_types(df: DataFrame) -> DataFrame:
    """
    STRING → 正しい型。mirrors: src/transform.py cast_types

    差分: cast / to_date の代わりに try_cast を使う。Spark 4 系（ANSI 既定有効）では
    'N/A' 等の変換で例外になり、品質ゲートで分離する前にジョブが失敗するため。
    日付・日時は MES の固定書式（yyyy-MM-dd / yyyy-MM-dd HH:mm:ss）が try_cast の標準書式と一致する。
    """
    return (
        df
        .withColumn("production_date", _try_cast("production_date", "DATE"))
        .withColumn("planned_quantity", _try_cast("planned_quantity", "INT"))
        .withColumn("actual_quantity", _try_cast("actual_quantity", "INT"))
        .withColumn("defect_quantity", _try_cast("defect_quantity", "INT"))
        .withColumn("temperature", _try_cast("temperature", "DOUBLE"))
        .withColumn("vibration", _try_cast("vibration", "DOUBLE"))
        .withColumn("created_at", _try_cast("created_at", "TIMESTAMP"))
    )


def quality_checks(target_date: str = None) -> list:
    """
    品質チェック（名前, 条件式）の一覧。mirrors: src/transform.py validate_quality の 4 条件

    追加: target_date 指定時は production_date の一致を検査する（適時性/一貫性）。
    パーティション上書き（replaceWhere）の述語外の行が混入すると書き込みが失敗するため。
    """
    checks = [
        ("mandatory_not_null",
         F.col("production_date").isNotNull()
         & F.col("line_id").isNotNull() & (F.trim(F.col("line_id")) != "")
         & F.col("product_code").isNotNull() & (F.trim(F.col("product_code")) != "")),
        ("accuracy_range",
         (F.col("planned_quantity") >= 0)
         & (F.col("actual_quantity") >= 0)
         & (F.col("defect_quantity") >= 0)
         & (F.col("temperature").between(20.0, 80.0) | F.col("temperature").isNull())
         & (F.col("vibration").between(0.1, 5.0) | F.col("vibration").isNull())),
        ("validity_enum",
         F.col("status").isin(VALID_STATUSES) & F.col("shift").isin(VALID_SHIFTS)),
        ("consistency_defect_le_actual",
         F.col("defect_quantity") <= F.col("actual_quantity")),
    ]
    if target_date:
        checks.append(("target_date_match", F.col("production_date") == F.to_date(F.lit(target_date))))
    return checks


def validate_quality(df: DataFrame, target_date: str = None) -> tuple:
    """
    品質チェックで合格/不合格に分離する。mirrors: src/transform.py validate_quality

    差分:
    - 条件式が NULL（例: 数値変換失敗）の行も不合格として扱う（coalesce(..., False)）。
      元実装では NULL 行が valid/error のどちらにも入らず消失し得るため。
    - 不合格行に `_rejection_reasons`（失敗したチェック名のカンマ区切り）を付与する。
    """
    checks = [(name, F.coalesce(cond, F.lit(False))) for name, cond in quality_checks(target_date)]
    all_valid = checks[0][1]
    for _name, cond in checks[1:]:
        all_valid = all_valid & cond

    valid_df = df.filter(all_valid)
    error_df = df.filter(~all_valid).withColumn(
        "_rejection_reasons",
        F.concat_ws(",", *[F.when(~cond, F.lit(name)) for name, cond in checks]),
    )
    return valid_df, error_df


def deduplicate(df: DataFrame) -> DataFrame:
    """date + line + product + shift で最新 created_at を残す。mirrors: src/transform.py deduplicate"""
    window = Window.partitionBy(
        "production_date", "line_id", "product_code", "shift"
    ).orderBy(F.col("created_at").desc())
    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def add_computed_columns(df: DataFrame) -> DataFrame:
    """good_quantity / defect_rate / achievement_rate。mirrors: src/transform.py add_computed_columns"""
    return (
        df
        .withColumn("good_quantity", F.col("actual_quantity") - F.col("defect_quantity"))
        .withColumn(
            "defect_rate",
            F.when(F.col("actual_quantity") > 0,
                   F.round(F.col("defect_quantity") / F.col("actual_quantity"), 4)).otherwise(0.0),
        )
        .withColumn(
            "achievement_rate",
            F.when(F.col("planned_quantity") > 0,
                   F.round(F.col("actual_quantity") / F.col("planned_quantity"), 4)).otherwise(0.0),
        )
    )


@dataclass(frozen=True)
class SilverResult:
    """Silver 変換結果（イミュータブル）。mirrors: src/transform.py TransformStats + DataFrame"""

    silver_df: DataFrame
    error_df: DataFrame
    input_count: int
    valid_count: int
    duplicate_count: int
    error_count: int
    error_rate: float


def build_silver(bronze_df: DataFrame, target_date: str) -> SilverResult:
    """Bronze → Silver の変換（書き込みなし）。mirrors: src/transform.py transform の手順 1〜4"""
    input_count = bronze_df.count()
    valid_df, error_df = validate_quality(cast_types(bronze_df), target_date)
    valid_before_dedup = valid_df.count()
    silver_df = add_computed_columns(deduplicate(valid_df)).withColumn(
        "_processed_at", F.current_timestamp()
    )
    valid_count = silver_df.count()
    error_count = error_df.count()
    return SilverResult(
        silver_df=silver_df,
        error_df=error_df,
        input_count=input_count,
        valid_count=valid_count,
        duplicate_count=valid_before_dedup - valid_count,
        error_count=error_count,
        error_rate=round(error_count / input_count, 4) if input_count > 0 else 0.0,
    )


def check_quality_gate(result: SilverResult) -> str:
    """
    品質ゲート判定。合格率 < MIN_VALID_RATIO なら停止、エラー率 > 閾値なら warning。
    mirrors: src/pipeline.py のエラー率チェック + pipeline_config.yaml quality_gates
    """
    if result.input_count == 0:
        raise QualityGateError("入力 0 件: ソース CSV が空、またはパスが誤っている")
    valid_ratio = 1.0 - result.error_rate
    if valid_ratio < MIN_VALID_RATIO:
        raise QualityGateError(
            f"合格率 {valid_ratio:.2%} が閾値 {MIN_VALID_RATIO:.0%} 未満のため Silver 昇格を停止"
        )
    return "warning" if result.error_rate > ERROR_RATE_WARN_THRESHOLD else "success"


# %% [markdown]
# ## Silver: equipment_master（SCD Type 2）

# %%
def build_equipment_snapshot(silver_df: DataFrame) -> DataFrame:
    """当日の設備スナップショット（設備ごとに最新レコードの line_id / status）。"""
    window = Window.partitionBy("equipment_id").orderBy(F.col("created_at").desc())
    return (
        silver_df
        .filter(F.col("equipment_id").isNotNull())
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select("equipment_id", "line_id", F.col("status").alias("latest_status"))
    )


def apply_scd2(existing_df, snapshot_df: DataFrame, as_of_date: str) -> DataFrame:
    """
    SCD Type 2 の適用結果（テーブル全体）を返す。03_schema/ddl/silver.sql equipment_master 準拠。
    src/ には未実装のため本 Notebook で新規実装（サロゲートキーは省略し equipment_id + valid_from を一意キーとする）。

    - 新規設備: valid_from=as_of で現行行を追加
    - 属性（line_id / latest_status）変化: 旧現行行を valid_to=as_of-1 で閉じ、新行を追加
    - 同日再実行で属性が変わった場合: 同日の現行行を置き換える（冪等）
    - as_of より新しい現行行がある場合（過去日の再実行）: 履歴を壊さないよう変更しない
    """
    as_of = F.to_date(F.lit(as_of_date))
    snap = snapshot_df.select(
        "equipment_id",
        F.col("line_id").alias("s_line_id"),
        F.col("latest_status").alias("s_status"),
    )
    new_rows_cols = [
        F.col("equipment_id"),
        F.col("s_line_id").alias("line_id"),
        F.col("s_status").alias("latest_status"),
        as_of.alias("valid_from"),
        F.lit(None).cast("date").alias("valid_to"),
        F.lit(True).alias("is_current"),
        F.current_timestamp().alias("_updated_at"),
    ]
    if existing_df is None:
        return snap.select(*new_rows_cols)

    history = existing_df.filter(~F.col("is_current"))
    current = existing_df.filter(F.col("is_current"))
    joined = current.join(snap, "equipment_id", "full_outer")

    in_current = F.col("valid_from").isNotNull()
    in_snap = F.col("s_line_id").isNotNull()
    changed = in_current & in_snap & (
        (F.col("line_id") != F.col("s_line_id")) | (F.col("latest_status") != F.col("s_status"))
    )
    applicable = changed & (F.col("valid_from") <= as_of)

    current_cols = ["equipment_id", "line_id", "latest_status", "valid_from", "valid_to",
                    "is_current", "_updated_at"]
    keep_current = joined.filter(in_current & ~applicable).select(*current_cols)
    closed = joined.filter(applicable & (F.col("valid_from") < as_of)).select(
        "equipment_id", "line_id", "latest_status", "valid_from",
        F.date_sub(as_of, 1).alias("valid_to"),
        F.lit(False).alias("is_current"),
        F.current_timestamp().alias("_updated_at"),
    )
    inserted = joined.filter((~in_current & in_snap) | applicable).select(*new_rows_cols)

    return history.select(*current_cols).unionByName(keep_current).unionByName(closed).unionByName(inserted)


# %% [markdown]
# ## Gold: daily_production_kpi / equipment_health（03_schema/ddl/gold.sql 準拠）

# %%
def build_daily_production_kpi(silver_df: DataFrame) -> DataFrame:
    """
    日次 × ライン別 KPI。集計ロジックは src/aggregate.py build_daily_production_kpi と同じ。

    差分（gold.sql に合わせる）: 列名（total_*_quantity / *_shift_quantity / _aggregated_at）、
    yield_rate・unique_products・unique_operators の追加、列順と型（INT/BIGINT）の固定。
    """
    return (
        silver_df
        .groupBy("production_date", "line_id")
        .agg(
            F.sum("planned_quantity").cast("bigint").alias("total_planned_quantity"),
            F.sum("actual_quantity").cast("bigint").alias("total_actual_quantity"),
            F.sum("defect_quantity").cast("bigint").alias("total_defect_quantity"),
            F.sum("good_quantity").cast("bigint").alias("total_good_quantity"),
            F.count("*").cast("int").alias("record_count"),
            F.countDistinct("product_code").cast("int").alias("unique_products"),
            F.countDistinct("operator_id").cast("int").alias("unique_operators"),
            F.sum(F.when(F.col("shift") == "日勤", F.col("actual_quantity")).otherwise(0))
            .cast("bigint").alias("day_shift_quantity"),
            F.sum(F.when(F.col("shift") == "夜勤", F.col("actual_quantity")).otherwise(0))
            .cast("bigint").alias("night_shift_quantity"),
        )
        .withColumn(
            "achievement_rate",
            F.when(F.col("total_planned_quantity") > 0,
                   F.round(F.col("total_actual_quantity") / F.col("total_planned_quantity"), 4))
            .otherwise(0.0).cast("double"),
        )
        .withColumn(
            "defect_rate",
            F.when(F.col("total_actual_quantity") > 0,
                   F.round(F.col("total_defect_quantity") / F.col("total_actual_quantity"), 4))
            .otherwise(0.0).cast("double"),
        )
        .withColumn(
            "yield_rate",
            F.when(F.col("total_actual_quantity") > 0,
                   F.round(F.col("total_good_quantity") / F.col("total_actual_quantity"), 4))
            .otherwise(0.0).cast("double"),
        )
        .withColumn("_aggregated_at", F.current_timestamp())
        .select(*KPI_COLUMNS)
    )


def build_equipment_daily(silver_df: DataFrame) -> DataFrame:
    """
    日次 × 設備別の当日メトリクス。集計ロジックは src/aggregate.py build_equipment_health と同じ。

    差分（gold.sql に合わせる）: 列名 daily_*、line_id（当日最新レコードの設置ライン）、
    status_summary（当日の最も深刻な状態）。min_* と health_status は DDL にないため出力しない。
    """
    severity = F.create_map(*[x for k, v in STATUS_SEVERITY.items() for x in (F.lit(k), F.lit(v))])
    severity_to_status = F.create_map(*[x for k, v in STATUS_SEVERITY.items() for x in (F.lit(v), F.lit(k))])
    return (
        silver_df
        .groupBy("production_date", "equipment_id")
        .agg(
            F.expr("max_by(line_id, created_at)").alias("line_id"),
            F.round(F.avg("temperature"), 1).alias("daily_avg_temperature"),
            F.max("temperature").alias("daily_max_temperature"),
            F.round(F.avg("vibration"), 2).alias("daily_avg_vibration"),
            F.max("vibration").alias("daily_max_vibration"),
            F.count("*").cast("int").alias("measurement_count"),
            F.sum(F.when(F.col("status").isin("異常", "停止"), 1).otherwise(0)).cast("int").alias("abnormal_count"),
            F.max(severity[F.col("status")]).alias("_max_severity"),
        )
        .withColumn("status_summary", severity_to_status[F.col("_max_severity")])
        .drop("_max_severity")
    )


def build_equipment_health(silver_window_df: DataFrame, target_date: str) -> DataFrame:
    """
    target_date の設備健全性（gold.sql equipment_health の全列）。

    7 日移動平均 = 直近 ROLLING_WINDOW_DAYS 日の日次平均の平均。
    アラート = 7 日平均温度 > 65℃ / 7 日平均振動 > 3.5 mm/s（src/ には未実装）。

    Args:
        silver_window_df: target_date を含む直近 ROLLING_WINDOW_DAYS 日分の Silver
    """
    daily = build_equipment_daily(silver_window_df)
    target = F.to_date(F.lit(target_date))
    window_start = F.date_sub(target, ROLLING_WINDOW_DAYS - 1)
    rolling = (
        daily
        .filter((F.col("production_date") >= window_start) & (F.col("production_date") <= target))
        .groupBy("equipment_id")
        .agg(
            F.round(F.avg("daily_avg_temperature"), 1).alias("rolling_7d_avg_temperature"),
            F.round(F.avg("daily_avg_vibration"), 2).alias("rolling_7d_avg_vibration"),
        )
    )
    return (
        daily
        .filter(F.col("production_date") == target)
        .join(rolling, "equipment_id", "left")
        .withColumn(
            "temperature_alert",
            F.coalesce(F.col("rolling_7d_avg_temperature") > TEMPERATURE_ALERT_THRESHOLD, F.lit(False)),
        )
        .withColumn(
            "vibration_alert",
            F.coalesce(F.col("rolling_7d_avg_vibration") > VIBRATION_ALERT_THRESHOLD, F.lit(False)),
        )
        .withColumn("_aggregated_at", F.current_timestamp())
        .select(*HEALTH_COLUMNS)
    )


# %% [markdown]
# ## Power BI 用ディメンション（semantic-model.md 4 章と同じ列構成）

# %%
def normalize_master(df: DataFrame, required: list, source: str) -> DataFrame:
    """マスタの BOM 除去・必須列チェック・キー列 trim。semantic-model.md 4.2 load_master_csv と同等。"""
    renamed = df.toDF(*[c.replace("﻿", "").strip() for c in df.columns])
    missing = [c for c in required if c not in renamed.columns]
    if missing:
        raise ValueError(f"{source} に必須列がない: {missing}")
    return renamed.select(
        *[F.trim(F.col(c)).alias(c) if c in required else F.col(c) for c in renamed.columns]
    )


def assert_unique(df: DataFrame, key: str, name: str) -> None:
    """一側キーの一意性を検証する（重複があると Direct Lake のリレーションでエラーになる）。"""
    total = df.count()
    distinct = df.select(key).distinct().count()
    null_keys = df.filter(F.col(key).isNull()).count()
    if total != distinct or null_keys > 0:
        raise ValueError(f"{name}.{key} が一意でない: rows={total}, distinct={distinct}, null={null_keys}")


def build_dim_line(line_master_df: DataFrame) -> DataFrame:
    """dim_line（line_id, line_name, plant_id, plant_name, ...）。plant_id は RLS に使う。"""
    dim = normalize_master(line_master_df, ["line_id", "line_name", "plant_id", "plant_name"], "line_master.csv")
    if "capacity_per_shift" in dim.columns:
        dim = dim.withColumn("capacity_per_shift", F.col("capacity_per_shift").cast("int"))
    assert_unique(dim, "line_id", "dim_line")
    return dim


def build_dim_equipment(equipment_master_df: DataFrame, dim_line_df: DataFrame) -> DataFrame:
    """dim_equipment（設備マスタ + 現行設置ラインの line_name）。"""
    equipment = normalize_master(
        equipment_master_df, ["equipment_id", "equipment_name", "line_id"], "equipment_master_seed.csv"
    )
    for column in ["rated_temperature_c", "baseline_temperature_c", "baseline_vibration_mm_s"]:
        if column in equipment.columns:
            equipment = equipment.withColumn(column, F.col(column).cast("double"))
    if "install_date" in equipment.columns:
        equipment = equipment.withColumn("install_date", _try_cast("install_date", "DATE"))
    dim = equipment.join(dim_line_df.select("line_id", "line_name"), on="line_id", how="left")
    assert_unique(dim, "equipment_id", "dim_equipment")
    return dim


def build_dim_product(product_master_df: DataFrame) -> DataFrame:
    """dim_product（明細ファクトを使う場合のみ Power BI で使用）。"""
    dim = normalize_master(product_master_df, ["product_code", "product_name", "category"], "product_master.csv")
    if "standard_cycle_time_sec" in dim.columns:
        dim = dim.withColumn("standard_cycle_time_sec", F.col("standard_cycle_time_sec").cast("int"))
    assert_unique(dim, "product_code", "dim_product")
    return dim


def build_dim_status(spark: SparkSession) -> DataFrame:
    """dim_status（状態を深刻度順に並べる）。semantic-model.md 4.3 と同値。"""
    rows = ", ".join(f"('{status}', {order})" for status, order in STATUS_SEVERITY.items())
    return spark.sql(f"SELECT * FROM VALUES {rows} AS t(status, severity_order)")


def build_dim_date(spark: SparkSession, min_date: date, max_date: date) -> DataFrame:
    """dim_date（min_date〜max_date を含む暦年全体）。semantic-model.md 4.1 と同じ列構成。"""
    start_date = f"{min_date.year}-01-01"
    end_date = f"{max_date.year}-12-31"
    return spark.sql(f"""
        SELECT
            d                                                         AS calendar_date,
            year(d)                                                   AS year,
            quarter(d)                                                AS quarter,
            month(d)                                                  AS month,
            concat(CAST(year(d) AS STRING), '年',
                   lpad(CAST(month(d) AS STRING), 2, '0'), '月')      AS year_month,
            year(d) * 100 + month(d)                                  AS year_month_sort,
            day(d)                                                    AS day_of_month,
            weekday(d) + 1                                            AS weekday_no,
            element_at(array('月','火','水','木','金','土','日'), weekday(d) + 1) AS weekday_jp,
            weekday(d) >= 5                                           AS is_weekend,
            date_sub(d, weekday(d))                                   AS week_start_date,
            CASE WHEN month(d) >= 4 THEN year(d) ELSE year(d) - 1 END AS fiscal_year
        FROM (
            SELECT explode(sequence(DATE'{start_date}', DATE'{end_date}', INTERVAL 1 DAY)) AS d
        )
    """)


# %% [markdown]
# ## I/O（Delta 書き込み・ファイル列挙）— Fabric 上で実行される部分

# %%
def ensure_schemas(spark: SparkSession) -> None:
    """スキーマ有効の Lakehouse で bronze / silver / gold / meta スキーマを作成する。"""
    if not USE_SCHEMAS:
        return
    for schema in sorted({schema for schema, _name in TABLES.values()}):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def overwrite_partition(
    spark: SparkSession, df: DataFrame, name: str, partition_col: str, target_date: str,
) -> None:
    """
    パーティション単位の上書き（冪等）。mirrors: src/*.py の load_to_* （replaceWhere）
    差分: 初回は partitionBy 付きで作成し、2 回目以降は replaceWhere で対象日のみ置換する。
    """
    writer = df.write.format("delta").mode("overwrite")
    if spark.catalog.tableExists(name):
        writer.option("replaceWhere", f"{partition_col} = '{target_date}'").saveAsTable(name)
    else:
        writer.partitionBy(partition_col).saveAsTable(name)


def overwrite_table(df: DataFrame, name: str) -> None:
    """全量上書き（小規模なマスタ・ディメンション用。再実行しても同じ結果）。"""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)


def list_source_dates(source_dir: str) -> list:
    """SOURCE_DIR の production_YYYYMMDD.csv から処理対象日（昇順）を得る。"""
    names = []
    try:
        names = [f.name for f in notebookutils.fs.ls(source_dir)]  # noqa: F821 - Fabric 組み込み
    except NameError:
        try:
            from notebookutils import mssparkutils  # 旧ランタイム互換
            names = [f.name for f in mssparkutils.fs.ls(source_dir)]
        except ImportError:
            names = [os.path.basename(p) for p in glob.glob(os.path.join(source_dir, "*.csv"))]

    prefix, suffix = FILE_PATTERN.split("{yyyymmdd}")
    dates = []
    for name in names:
        if name.startswith(prefix) and name.endswith(suffix):
            stamp = name[len(prefix):len(name) - len(suffix)]
            try:
                dates.append(datetime.strptime(stamp, "%Y%m%d").date().isoformat())
            except ValueError:
                log_event("source_file_skipped", logging.WARNING, file=name)
    return sorted(dates)


def source_file_path(source_dir: str, target_date: str) -> str:
    return f"{source_dir}/" + FILE_PATTERN.replace("{yyyymmdd}", target_date.replace("-", ""))


def read_master(spark: SparkSession, file_name: str) -> DataFrame:
    return spark.read.option("header", "true").option("encoding", "UTF-8").csv(f"{MASTER_DIR}/{file_name}")


def write_dimensions(spark: SparkSession) -> dict:
    """マスタ CSV と Gold の日付範囲から Power BI 用ディメンションを全量上書きする。"""
    dim_line = build_dim_line(read_master(spark, "line_master.csv")).localCheckpoint()
    dims = {
        "dim_line": dim_line,
        "dim_equipment": build_dim_equipment(read_master(spark, "equipment_master_seed.csv"), dim_line),
        "dim_product": build_dim_product(read_master(spark, "product_master.csv")),
        "dim_status": build_dim_status(spark),
    }
    bounds = spark.read.table(table_name("gold_kpi")).agg(
        F.min("production_date").alias("min_d"), F.max("production_date").alias("max_d")
    ).first()
    if bounds is None or bounds.min_d is None:
        raise ValueError(f"{table_name('gold_kpi')} が空のため dim_date の範囲を決定できない")
    dims["dim_date"] = build_dim_date(spark, bounds.min_d, bounds.max_d)

    counts = {}
    for key, df in dims.items():
        overwrite_table(df, table_name(key))
        counts[table_name(key)] = df.count()
    log_event("dimensions_written", **counts)
    return counts


def check_referential_integrity(spark: SparkSession) -> DataFrame:
    """ファクトのキーがディメンションに存在するか（semantic-model.md 4.4）。すべて 0 件が正常。"""
    kpi, health = spark.read.table(table_name("gold_kpi")), spark.read.table(table_name("gold_health"))
    dim_line = spark.read.table(table_name("dim_line"))
    dim_equipment = spark.read.table(table_name("dim_equipment"))
    dim_date = spark.read.table(table_name("dim_date")).withColumnRenamed("calendar_date", "production_date")
    checks = [
        ("daily_production_kpi.line_id", kpi.join(dim_line, "line_id", "left_anti")),
        ("equipment_health.line_id", health.join(dim_line, "line_id", "left_anti")),
        ("equipment_health.equipment_id", health.join(dim_equipment, "equipment_id", "left_anti")),
        ("daily_production_kpi.production_date", kpi.join(dim_date, "production_date", "left_anti")),
    ]
    rows = [(fk, df.count()) for fk, df in checks]
    log_event("referential_integrity_checked", orphan_rows=dict(rows))
    return spark.createDataFrame(rows, "fk STRING, orphan_rows BIGINT")


# %% [markdown]
# ## オーケストレーション — mirrors: src/pipeline.py run_pipeline

# %%
@dataclass(frozen=True)
class PipelineResult:
    """1 日分の実行結果（イミュータブル）。mirrors: src/pipeline.py PipelineResult"""

    pipeline_name: str
    target_date: str
    batch_id: str
    start_time: str
    end_time: str
    duration_seconds: float
    bronze_count: int
    silver_valid_count: int
    duplicate_count: int
    dead_letter_count: int
    error_rate: float
    gold_kpi_count: int
    gold_health_count: int
    equipment_alert_count: int
    overall_status: str


def _read_partition(spark: SparkSession, key: str, column: str, start: str, end: str = None) -> DataFrame:
    return spark.read.table(table_name(key)).filter(
        F.col(column).between(F.to_date(F.lit(start)), F.to_date(F.lit(end or start)))
    )


def run_pipeline(spark: SparkSession, target_date: str, source_dir: str) -> PipelineResult:
    """1 日分の Bronze → Silver → Gold を実行する。"""
    batch_id = str(uuid.uuid4())[:8]
    start = datetime.now(timezone.utc)
    log_event("pipeline_started", pipeline_name=PIPELINE_NAME, target_date=target_date, batch_id=batch_id)

    # --- Step 1: Extract (CSV → Bronze) ---
    file_path = source_file_path(source_dir, target_date)
    bronze_df = add_metadata(read_csv(spark, file_path), file_path, batch_id, target_date)
    overwrite_partition(spark, bronze_df, table_name("bronze"), "_ingested_date", target_date)
    bronze_today = _read_partition(spark, "bronze", "_ingested_date", target_date)

    # --- Step 2: Transform (Bronze → Silver) + 品質ゲート ---
    silver = build_silver(bronze_today, target_date)
    status = check_quality_gate(silver)  # 合格率が閾値未満なら QualityGateError で停止
    if status == "warning":
        log_event("error_rate_exceeded", logging.WARNING, target_date=target_date,
                  error_rate=silver.error_rate, threshold=ERROR_RATE_WARN_THRESHOLD)

    overwrite_partition(spark, silver.silver_df, table_name("silver_facts"), "production_date", target_date)
    # Dead Letter も対象日パーティションを上書き（元実装の append だと再実行で重複するため）
    dead_letter_df = (
        silver.error_df
        .withColumn("_target_date", F.to_date(F.lit(target_date)))
        .withColumn("_rejected_at", F.current_timestamp())
    )
    overwrite_partition(spark, dead_letter_df, table_name("dead_letter"), "_target_date", target_date)
    log_event("silver_written", target_date=target_date, input_count=silver.input_count,
              valid_count=silver.valid_count, duplicate_count=silver.duplicate_count,
              error_count=silver.error_count, error_rate=silver.error_rate)

    # --- Step 2b: equipment_master（SCD2、小規模のため全量上書き） ---
    silver_today = _read_partition(spark, "silver_facts", "production_date", target_date)
    equipment_table = table_name("silver_equipment")
    existing = (
        spark.read.table(equipment_table).localCheckpoint()
        if spark.catalog.tableExists(equipment_table) else None
    )
    scd2_df = apply_scd2(existing, build_equipment_snapshot(silver_today), target_date).localCheckpoint()
    scd2_df.write.format("delta").mode("overwrite").saveAsTable(equipment_table)

    # --- Step 3: Aggregate (Silver → Gold) ---
    kpi_df = build_daily_production_kpi(silver_today)
    overwrite_partition(spark, kpi_df, table_name("gold_kpi"), "production_date", target_date)

    window_start = (date.fromisoformat(target_date) - timedelta(days=ROLLING_WINDOW_DAYS - 1)).isoformat()
    silver_window = _read_partition(spark, "silver_facts", "production_date", window_start, target_date)
    health_df = build_equipment_health(silver_window, target_date)
    overwrite_partition(spark, health_df, table_name("gold_health"), "production_date", target_date)

    gold_health = _read_partition(spark, "gold_health", "production_date", target_date)
    end = datetime.now(timezone.utc)
    result = PipelineResult(
        pipeline_name=PIPELINE_NAME,
        target_date=target_date,
        batch_id=batch_id,
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        duration_seconds=round((end - start).total_seconds(), 2),
        bronze_count=silver.input_count,
        silver_valid_count=silver.valid_count,
        duplicate_count=silver.duplicate_count,
        dead_letter_count=silver.error_count,
        error_rate=silver.error_rate,
        gold_kpi_count=_read_partition(spark, "gold_kpi", "production_date", target_date).count(),
        gold_health_count=gold_health.count(),
        equipment_alert_count=gold_health.filter(F.col("temperature_alert") | F.col("vibration_alert")).count(),
        overall_status=status,
    )
    log_event("pipeline_completed", **asdict(result))
    return result


def write_run_log(spark: SparkSession, results: list) -> None:
    """実行結果を meta.pipeline_run_log に追記する（監視・SLA レポート用）。"""
    if results:
        spark.createDataFrame([asdict(r) for r in results]).write.format("delta").mode("append") \
            .saveAsTable(table_name("run_log"))


def show(df: DataFrame) -> None:
    """Fabric では display()、ローカルでは show() で表示する。"""
    try:
        display(df)  # noqa: F821 - Fabric Notebook 組み込み
    except NameError:
        df.show(truncate=False)


def main(spark: SparkSession) -> list:
    """TARGET_DATE（空ならバックフィル）を処理し、ディメンションを更新して結果一覧を返す。"""
    ensure_schemas(spark)
    dates = [TARGET_DATE] if TARGET_DATE else list_source_dates(SOURCE_DIR)
    if not dates:
        raise FileNotFoundError(f"{SOURCE_DIR} に {FILE_PATTERN} 形式のファイルがない")
    log_event("run_started", target_dates=dates)

    results = []
    try:
        for target_date in dates:  # SCD2 と 7 日移動平均のため必ず日付昇順で処理する
            results.append(run_pipeline(spark, target_date, SOURCE_DIR))
    except Exception as exc:
        log_event("pipeline_failed", logging.ERROR, error_type=type(exc).__name__,
                  error_message=str(exc), completed_dates=[r.target_date for r in results])
        raise
    finally:
        write_run_log(spark, results)

    show(spark.createDataFrame([asdict(r) for r in results]).select(
        "target_date", "bronze_count", "silver_valid_count", "duplicate_count",
        "dead_letter_count", "error_rate", "gold_kpi_count", "equipment_alert_count",
        "overall_status", "duration_seconds",
    ))

    if BUILD_DIMENSIONS:
        write_dimensions(spark)
        show(check_referential_integrity(spark))
    return results


# %%
if __name__ == "__main__":
    # Fabric Notebook では `spark` が定義済み。ローカル実行時のみ生成する
    try:
        _spark = spark  # noqa: F821
    except NameError:
        _spark = SparkSession.builder.getOrCreate()
    main(_spark)

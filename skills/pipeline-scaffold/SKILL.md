---
name: pipeline-scaffold
description: Generate ETL pipeline templates for Airflow DAG, ADF Pipeline, or Fabric Notebook based on project requirements.
---

# Pipeline Scaffold

ユーザーの要件に基づいて、ETL パイプラインのテンプレートコードと設定ファイルを生成する。

---

## 対応テンプレート

| テンプレート | ファイル形式 | ユースケース |
|-------------|-------------|-------------|
| Airflow DAG | Python (.py) | オンプレミス / クラウド汎用オーケストレーション |
| Azure Data Factory | JSON (.json) | Azure ネイティブ ETL パイプライン |
| Fabric Notebook | Python (.py) | Microsoft Fabric Lakehouse 環境 |

---

## 入力パラメータ

スキル起動時に以下の情報をユーザーから収集する。

| パラメータ | 必須 | 説明 | 例 |
|-----------|------|------|-----|
| データソース | Yes | 入力元のシステム・形式 | PostgreSQL, CSV, REST API |
| ターゲット | Yes | 出力先のシステム・形式 | Delta Table, Parquet, Snowflake |
| 頻度 | Yes | 実行スケジュール | 日次, 時間単位, リアルタイム |
| SLA | No | 処理完了期限 | AM 6:00 JST までに完了 |
| データ量 | No | 概算の処理量 | 100万レコード/日 |
| Medallion レイヤー | No | 対象レイヤー | Bronze, Silver, Gold |

---

## 出力構成

生成されるファイル構成は以下のとおり。

```
pipeline/
├── src/
│   ├── extract.py          # 抽出処理
│   ├── transform.py        # 変換処理
│   ├── load.py             # ロード処理
│   └── pipeline.py         # パイプライン統合
├── config/
│   ├── pipeline_config.yaml # パイプライン設定
│   └── env.yaml            # 環境変数テンプレート
├── tests/
│   ├── test_extract.py     # 抽出テスト
│   ├── test_transform.py   # 変換テスト
│   └── conftest.py         # テストフィクスチャ
└── README.md               # パイプライン説明
```

---

## Medallion レイヤー対応

各レイヤーの責務に応じたテンプレートを生成する。

| レイヤー | 責務 | 主な処理 |
|---------|------|---------|
| Bronze | 生データ取り込み | スキーマ推論, メタデータ付与, 冪等ロード |
| Silver | クレンジング・正規化 | NULL 処理, 型変換, 重複排除, データ品質チェック |
| Gold | ビジネスロジック適用 | 集計, KPI 計算, ディメンション結合 |

---

## Airflow DAG テンプレート

```python
"""
DAG: {{ dag_id }}
Description: {{ description }}
Schedule: {{ schedule_interval }}
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="{{ dag_id }}",
    default_args=default_args,
    description="{{ description }}",
    schedule_interval="{{ schedule_interval }}",
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "{{ layer }}"],
    max_active_runs=1,
) as dag:

    def extract(**context):
        """データソースからの抽出処理"""
        execution_date = context["ds"]
        # TODO: 抽出ロジックを実装
        return {"record_count": 0, "execution_date": execution_date}

    def transform(**context):
        """データ変換・クレンジング処理"""
        ti = context["ti"]
        extract_result = ti.xcom_pull(task_ids="extract")
        # TODO: 変換ロジックを実装
        return {"record_count": 0}

    def load(**context):
        """ターゲットへのロード処理"""
        ti = context["ti"]
        transform_result = ti.xcom_pull(task_ids="transform")
        # TODO: ロードロジックを実装
        return {"record_count": 0}

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task
```

---

## PySpark パイプラインテンプレート

```python
"""
PySpark ETL Pipeline: {{ pipeline_name }}
Layer: {{ layer }}
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from dataclasses import dataclass
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineConfig:
    """パイプライン設定（イミュータブル）"""
    source_path: str
    target_path: str
    process_date: str
    layer: str = "bronze"
    write_mode: str = "overwrite"


@dataclass(frozen=True)
class PipelineResult:
    """パイプライン実行結果"""
    input_count: int
    output_count: int
    error_count: int
    duration_seconds: float
    status: str


def create_spark_session(app_name: str) -> SparkSession:
    """SparkSession を生成する"""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .getOrCreate()
    )


def extract(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """データソースから抽出する"""
    logger.info("Extract started", extra={
        "source_path": config.source_path,
        "process_date": config.process_date,
    })
    # TODO: 抽出ロジックを実装
    df = spark.read.format("delta").load(config.source_path)
    logger.info("Extract completed", extra={"record_count": df.count()})
    return df


def transform(df: DataFrame, config: PipelineConfig) -> DataFrame:
    """データ変換・クレンジングを実行する"""
    logger.info("Transform started")
    result_df = (
        df
        .withColumn("_etl_loaded_at", F.current_timestamp())
        .withColumn("_etl_process_date", F.lit(config.process_date))
    )
    # TODO: 変換ロジックを実装
    logger.info("Transform completed", extra={"record_count": result_df.count()})
    return result_df


def load(df: DataFrame, config: PipelineConfig) -> int:
    """ターゲットにロードする（冪等）"""
    logger.info("Load started", extra={
        "target_path": config.target_path,
        "write_mode": config.write_mode,
    })
    count = df.count()
    df.write.format("delta").mode(config.write_mode).save(config.target_path)
    logger.info("Load completed", extra={"record_count": count})
    return count


def run_pipeline(config: PipelineConfig) -> PipelineResult:
    """パイプラインを実行する"""
    start_time = datetime.now()
    spark = create_spark_session(f"etl-{config.layer}")

    try:
        raw_df = extract(spark, config)
        input_count = raw_df.count()

        transformed_df = transform(raw_df, config)
        output_count = load(transformed_df, config)

        duration = (datetime.now() - start_time).total_seconds()
        return PipelineResult(
            input_count=input_count,
            output_count=output_count,
            error_count=input_count - output_count,
            duration_seconds=duration,
            status="success",
        )
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error("Pipeline failed", extra={"error": str(e)}, exc_info=True)
        return PipelineResult(
            input_count=0, output_count=0, error_count=0,
            duration_seconds=duration, status="failed",
        )
```

---

## 生成手順

1. ユーザーからパラメータを収集する
2. Medallion レイヤーに応じたテンプレートを選択する
3. テンプレート変数を埋め込む
4. テストファイルのスタブを生成する
5. 設定ファイル（YAML）を生成する
6. 生成されたコードの概要を説明する

---

## 注意事項

- 生成されたコードはスケルトンであり、ビジネスロジックの実装はユーザーが行う
- 冪等性を確保するため、ロード処理には必ず DELETE-INSERT または MERGE パターンを使用する
- 処理統計（件数、エラー率、処理時間）のログ出力を必ず含める
- テスト用のフィクスチャ（conftest.py）には SparkSession のセットアップを含める

"""
テスト共通フィクスチャ

SparkSession をローカルモードで起動し、テスト間で共有する。
Delta Lake 書き込みテストは一時ディレクトリを使用。

Windows 環境対応:
- JAVA_HOME を JDK 17 に設定
- Arrow を有効化（Python ワーカー問題の回避）
- PYSPARK_PYTHON を明示指定
"""

import os
import sys

import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def _configure_env():
    """Windows 環境で PySpark が動作するよう環境変数を設定する。"""
    # JDK 17 (Temurin) を優先
    jdk17_path = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
    if os.path.exists(jdk17_path):
        os.environ["JAVA_HOME"] = jdk17_path
        os.environ["PATH"] = os.path.join(jdk17_path, "bin") + ";" + os.environ["PATH"]

    # PySpark ワーカーに現在の Python を使用させる
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


_configure_env()


@pytest.fixture(scope="session")
def spark():
    """ローカルモードの SparkSession（セッション単位で共有）。"""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("manufacturing-iot-test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # src/ は Spark 3.5（Fabric Runtime 1.3）前提で cast 失敗 = NULL を期待する。
        # PySpark 4.x は ANSI モードが既定で有効なため、3.5 と同じ挙動に揃える
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


def create_df(spark_session, data, columns, schema=None):
    """Python リストから Spark DataFrame を作成するヘルパー。

    Windows 環境で Python ワーカーのソケット問題を回避するため、
    Pandas DataFrame 経由 + Arrow で変換する。

    Args:
        spark_session: SparkSession
        data: 行のリスト（タプル or リスト）
        columns: カラム名のリスト
        schema: オプションの StructType。指定時は型変換に使用。

    Returns:
        pyspark.sql.DataFrame
    """
    pdf = pd.DataFrame(data, columns=columns)
    if schema is not None:
        return spark_session.createDataFrame(pdf, schema=schema)
    return spark_session.createDataFrame(pdf)


@pytest.fixture()
def tmp_path_delta(tmp_path):
    """Delta テーブル書き込み用の一時ディレクトリ。"""
    delta_dir = tmp_path / "delta"
    delta_dir.mkdir()
    return str(delta_dir)


@pytest.fixture()
def sample_data_dir():
    """サンプルデータディレクトリのパスを返す。"""
    return os.path.join(
        os.path.dirname(__file__),
        "sample_data",
    )

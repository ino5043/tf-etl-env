---
name: data-profiling
description: Execute data profiling analysis and generate reports with statistics, distributions, and anomaly detection.
---

# Data Profiling

データプロファイリングを実行し、統計情報、分布、異常検出を含むレポートを生成する。

---

## プロファイリング項目

| カテゴリ | 項目 | 説明 |
|---------|------|------|
| 基本統計 | レコード数 | テーブル全体の行数 |
| | カラム数 | カラムの総数 |
| | データ型 | 各カラムのデータ型 |
| 完全性 | NULL 率 | NULL 値の割合 |
| | 空文字率 | 空文字列の割合 |
| | 充填率 | 値が存在する割合 |
| 一意性 | カーディナリティ | ユニーク値の数 |
| | 一意性率 | ユニーク値の割合 |
| | 重複数 | 重複レコード数 |
| 分布 | 最小値・最大値 | 数値・日付の範囲 |
| | 平均・中央値・標準偏差 | 数値カラムの統計量 |
| | 頻度分布 | 上位 N 件の値と出現回数 |
| | パーセンタイル | p25, p50, p75, p95, p99 |
| パターン | 値のパターン | 文字列のフォーマットパターン |
| | 正規表現マッチ率 | 期待パターンへの適合率 |
| 異常値 | 外れ値 | IQR 法による外れ値検出 |
| | 値域逸脱 | 期待範囲外の値 |
| 鮮度 | 最新データ日時 | 最も新しいレコードの日時 |
| | データ遅延 | 現在時刻との差分 |

---

## PySpark プロファイリングコード

```python
"""
Data Profiling Engine
PySpark を使用してデータプロファイリングを実行する。
"""
from dataclasses import dataclass, field
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    NumericType, StringType, DateType, TimestampType,
)
from datetime import datetime
from typing import Any
import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ColumnProfile:
    """カラムプロファイル"""
    column_name: str
    data_type: str
    total_count: int
    null_count: int
    null_rate: float
    distinct_count: int
    uniqueness_rate: float
    min_value: Any = None
    max_value: Any = None
    mean_value: float = None
    stddev_value: float = None
    top_values: list = field(default_factory=list)


@dataclass
class TableProfile:
    """テーブルプロファイル"""
    table_name: str
    profiled_at: str
    total_rows: int
    total_columns: int
    columns: list[ColumnProfile] = field(default_factory=list)

    def to_markdown(self) -> str:
        """Markdown テーブル形式でレポートを生成する"""
        lines = []
        lines.append(f"# Data Profile: {self.table_name}")
        lines.append(f"")
        lines.append(f"**Profiled at**: {self.profiled_at}")
        lines.append(f"**Total Rows**: {self.total_rows:,}")
        lines.append(f"**Total Columns**: {self.total_columns}")
        lines.append(f"")

        # サマリーテーブル
        lines.append("## Column Summary")
        lines.append("")
        lines.append(
            "| Column | Type | NULL Rate | Distinct | "
            "Uniqueness | Min | Max |"
        )
        lines.append(
            "|--------|------|-----------|----------|"
            "-----------|-----|-----|"
        )

        for col in self.columns:
            lines.append(
                f"| {col.column_name} "
                f"| {col.data_type} "
                f"| {col.null_rate:.2%} "
                f"| {col.distinct_count:,} "
                f"| {col.uniqueness_rate:.2%} "
                f"| {col.min_value} "
                f"| {col.max_value} |"
            )

        # 数値カラムの統計
        numeric_cols = [
            c for c in self.columns if c.mean_value is not None
        ]
        if numeric_cols:
            lines.append("")
            lines.append("## Numeric Statistics")
            lines.append("")
            lines.append(
                "| Column | Mean | StdDev | Min | Max |"
            )
            lines.append(
                "|--------|------|--------|-----|-----|"
            )
            for col in numeric_cols:
                lines.append(
                    f"| {col.column_name} "
                    f"| {col.mean_value:.2f} "
                    f"| {col.stddev_value:.2f} "
                    f"| {col.min_value} "
                    f"| {col.max_value} |"
                )

        # Top Values
        cols_with_top = [
            c for c in self.columns if c.top_values
        ]
        if cols_with_top:
            lines.append("")
            lines.append("## Top Values")
            for col in cols_with_top:
                lines.append(f"")
                lines.append(f"### {col.column_name}")
                lines.append(f"| Value | Count |")
                lines.append(f"|-------|-------|")
                for val, cnt in col.top_values:
                    lines.append(f"| {val} | {cnt:,} |")

        return "\n".join(lines)


def profile_column(
    df: DataFrame, col_name: str, total_count: int
) -> ColumnProfile:
    """単一カラムのプロファイルを生成する"""
    col_type = str(df.schema[col_name].dataType)

    # NULL 統計
    null_count = df.filter(F.col(col_name).isNull()).count()
    null_rate = null_count / total_count if total_count > 0 else 0

    # カーディナリティ
    distinct_count = df.select(col_name).distinct().count()
    non_null_count = total_count - null_count
    uniqueness_rate = (
        distinct_count / non_null_count if non_null_count > 0 else 0
    )

    # 基本プロファイル
    profile_args = {
        "column_name": col_name,
        "data_type": col_type,
        "total_count": total_count,
        "null_count": null_count,
        "null_rate": null_rate,
        "distinct_count": distinct_count,
        "uniqueness_rate": uniqueness_rate,
    }

    # Min/Max
    stats = df.select(
        F.min(col_name).alias("min_val"),
        F.max(col_name).alias("max_val"),
    ).first()
    profile_args["min_value"] = stats["min_val"]
    profile_args["max_value"] = stats["max_val"]

    # 数値カラムの追加統計
    field_type = df.schema[col_name].dataType
    if isinstance(field_type, NumericType):
        num_stats = df.select(
            F.mean(col_name).alias("mean_val"),
            F.stddev(col_name).alias("stddev_val"),
        ).first()
        profile_args["mean_value"] = num_stats["mean_val"]
        profile_args["stddev_value"] = num_stats["stddev_val"]

    # Top Values（上位10件）
    top_values_rows = (
        df.filter(F.col(col_name).isNotNull())
        .groupBy(col_name)
        .count()
        .orderBy(F.desc("count"))
        .limit(10)
        .collect()
    )
    profile_args["top_values"] = [
        (str(row[col_name]), row["count"]) for row in top_values_rows
    ]

    return ColumnProfile(**profile_args)


def profile_table(
    df: DataFrame, table_name: str
) -> TableProfile:
    """テーブル全体のプロファイルを生成する"""
    start_time = datetime.now()
    total_count = df.count()

    logger.info("Profiling started", extra={
        "table_name": table_name,
        "total_rows": total_count,
        "total_columns": len(df.columns),
    })

    columns = []
    for col_name in df.columns:
        col_profile = profile_column(df, col_name, total_count)
        columns.append(col_profile)

    duration = (datetime.now() - start_time).total_seconds()

    logger.info("Profiling completed", extra={
        "table_name": table_name,
        "duration_seconds": duration,
    })

    return TableProfile(
        table_name=table_name,
        profiled_at=datetime.now().isoformat(),
        total_rows=total_count,
        total_columns=len(df.columns),
        columns=columns,
    )
```

---

## 異常検出パターン

### IQR 法による外れ値検出

```python
def detect_outliers_iqr(
    df: DataFrame, col_name: str, factor: float = 1.5
) -> DataFrame:
    """IQR 法で外れ値を検出する"""
    quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.01)
    q1, q3 = quantiles[0], quantiles[1]
    iqr = q3 - q1
    lower_bound = q1 - factor * iqr
    upper_bound = q3 + factor * iqr

    outliers = df.filter(
        (F.col(col_name) < lower_bound) |
        (F.col(col_name) > upper_bound)
    )

    logger.info("Outlier detection completed", extra={
        "column": col_name,
        "q1": q1,
        "q3": q3,
        "iqr": iqr,
        "lower_bound": lower_bound,
        "upper_bound": upper_bound,
        "outlier_count": outliers.count(),
    })

    return outliers
```

### データ鮮度チェック

```python
def check_data_freshness(
    df: DataFrame,
    timestamp_col: str,
    max_delay_hours: int = 24,
) -> dict:
    """データの鮮度を確認する"""
    latest = df.select(F.max(timestamp_col)).first()[0]
    now = datetime.now()
    delay_hours = (now - latest).total_seconds() / 3600

    return {
        "latest_timestamp": str(latest),
        "current_time": str(now),
        "delay_hours": round(delay_hours, 2),
        "is_fresh": delay_hours <= max_delay_hours,
        "max_delay_hours": max_delay_hours,
    }
```

---

## プロファイリング実行手順

1. 対象テーブルの DataFrame を取得する
2. `profile_table()` を実行してプロファイルを生成する
3. `to_markdown()` で Markdown レポートを出力する
4. 異常検出（外れ値、鮮度チェック）を実行する
5. 結果を確認し、データ品質の改善点を特定する

---

## レポート出力例

```markdown
# Data Profile: orders

**Profiled at**: 2026-03-11T10:30:00
**Total Rows**: 1,234,567
**Total Columns**: 8

## Column Summary

| Column | Type | NULL Rate | Distinct | Uniqueness | Min | Max |
|--------|------|-----------|----------|-----------|-----|-----|
| order_id | StringType | 0.00% | 1,234,567 | 100.00% | ... | ... |
| customer_id | StringType | 0.05% | 98,765 | 8.00% | ... | ... |
| total_amount | DecimalType | 0.12% | 45,678 | 3.70% | 0.00 | 999999.99 |
| status | StringType | 0.00% | 5 | 0.00% | ... | ... |
```

---

## 注意事項

- 大規模テーブル（1億行超）では、サンプリングを検討する（`df.sample(0.1)`）
- プロファイリングは I/O 負荷が高いため、オフピーク時間に実行する
- 結果は Delta Table に保存して、時系列での品質推移を追跡する
- 初回プロファイリングの結果をデータコントラクトの品質ルール設定に活用する
